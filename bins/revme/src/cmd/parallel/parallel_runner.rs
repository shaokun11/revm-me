

use serde_json::{Map, Value};
use crate::cmd::statetest::{
    merkle_trie::state_merkle_trie_root,
    utils::recover_address,
    models::MultiTestSuite,
};
use revm::{
    db::{State, CacheState},
    occda::Occda,
    task::Task,
    inspectors::NoOpInspector,
    primitives::{keccak256, Bytes, TxKind, B256, Bytecode, SpecId, AccountInfo, Env},
    profiler,
    Evm
};

use std::{
    fmt::Debug,
    path::PathBuf,
    time::Instant,
};

use thiserror::Error;
use tokio::runtime::Runtime;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug, Error)]
#[error("Test {name} failed: {kind}")]
pub struct TestError {
    pub name: String,
    pub kind: TestErrorKind,
}

#[derive(Debug, Error)]
pub enum TestErrorKind {
    #[error("logs root mismatch: got {got}, expected {expected}")]
    LogsRootMismatch { got: B256, expected: B256 },
    #[error("state root mismatch: got {got}, expected {expected}")]
    StateRootMismatch { got: B256, expected: B256 },
    #[error("unknown private key: {0:?}")]
    UnknownPrivateKey(B256),
    #[error("unexpected exception: got {got_exception:?}, expected {expected_exception:?}")]
    UnexpectedException {
        expected_exception: Option<String>,
        got_exception: Option<String>,
    },
    #[error("unexpected output: got {got_output:?}, expected {expected_output:?}")]
    UnexpectedOutput {
        expected_output: Option<Bytes>,
        got_output: Option<Bytes>,
    },
    #[error(transparent)]
    SerdeDeserialize(#[from] serde_json::Error),
    #[error("thread panicked")]
    Panic,
}

pub fn run_sequential(
    path: &PathBuf
) -> Result<(), TestError> {
    let s = std::fs::read_to_string(path).unwrap();
    // println!("s: {:#?}", s);
    let suite: MultiTestSuite = serde_json::from_str(&s).map_err(|e| TestError {
        name: path.to_string_lossy().into_owned(),
        kind: e.into(),
    })?;

    let name = suite.0.keys().next().unwrap();
    let unit = suite.0.get(name).unwrap();

    let mut cache_state = CacheState::new(false);
    for (address, info) in unit.pre.clone() {
        let code_hash = keccak256(&info.code);
        let bytecode = Bytecode::new_raw(info.code);
        let acc_info = AccountInfo {
            balance: info.balance,
            code_hash,
            code: Some(bytecode),
            nonce: info.nonce,
        };
        cache_state.insert_account_with_storage(address, acc_info, info.storage);
    }

    let mut env = Box::<Env>::default();
    // for mainnet
    env.cfg.chain_id = 1;
    // env.cfg.spec_id is set down the road

    // block env
    env.block.number = unit.env.current_number;
    env.block.coinbase = unit.env.current_coinbase;
    env.block.timestamp = unit.env.current_timestamp;
    env.block.gas_limit = unit.env.current_gas_limit;
    env.block.basefee = unit.env.current_base_fee.unwrap_or_default();
    env.block.difficulty = unit.env.current_difficulty;
    // after the Merge prevrandao replaces mix_hash field in block and replaced difficulty opcode in EVM.
    env.block.prevrandao = unit.env.current_random;

    let mut cache = cache_state.clone();
        cache.set_state_clear_flag(SpecId::enabled(SpecId::CANCUN, SpecId::SPURIOUS_DRAGON));

    let mut state = State::builder()
            .with_cached_prestate(cache)
            .with_bundle_update()
            .build();
    let timer = Instant::now();
    for (idx, tx) in unit.transaction.iter().enumerate() {
        profiler::start(&format!("{}", idx));

        let genesis = profiler::get_genesis();
        let duration_u64 = || (
            Instant::now().duration_since(genesis).as_nanos() as u64
        ).into();
        let mut description = Map::new();
        description.insert("type".to_string(), Value::String("transaction".to_string()));
        description.insert("transaction_clone::start".to_string(), duration_u64());
        env.tx.caller = if let Some(address) = tx.sender {
            address
        } else {
            recover_address(tx.secret_key.as_slice()).ok_or_else(|| TestError {
                name: name.clone(),
                kind: TestErrorKind::UnknownPrivateKey(tx.secret_key),
            })?
        };
        env.tx.gas_price = tx
            .gas_price
            .or(tx.max_fee_per_gas)
            .unwrap_or_default();
        env.tx.gas_priority_fee = tx.max_priority_fee_per_gas;
        // EIP-4844
        env.tx.blob_hashes = tx.blob_versioned_hashes.clone();
        env.tx.max_fee_per_blob_gas = tx.max_fee_per_blob_gas;
        env.tx.gas_limit = tx.gas_limit.saturating_to();
        env.tx.data = tx.data.clone();

        env.tx.value = tx.value;
        
        env.tx.access_list = tx
                .access_lists
                .get(0)
                .and_then(Option::as_deref)
                .cloned()
                .unwrap_or_default();
        let to = match tx.to {
            Some(add) => TxKind::Call(add),
            None => TxKind::Create,
        };
        env.tx.transact_to = to;
        description.insert("transaction_clone::end".to_string(), duration_u64());

        description.insert("evm_build::start".to_string(), duration_u64());
        let mut evm = Evm::builder()
        .with_db(&mut state)
        .modify_env(|e| e.clone_from(&env))
        .with_spec_id(SpecId::CANCUN)
        .build();
        description.insert("evm_build::end".to_string(), duration_u64());

        description.insert("evm_transact_commit::start".to_string(), duration_u64());
        match evm.transact_commit() {
            Ok(_) => {},
            Err(e) => {
                let kind = TestErrorKind::UnexpectedException {
                    expected_exception: None,
                    got_exception: Some(e.to_string()),
                };
                return Err(TestError { name: name.clone(), kind });
            },
        }
        description.insert("evm_transact_commit::end".to_string(), duration_u64());
        description.insert("status".to_string(), Value::String("success".to_string()));
        profiler::notes(&format!("{}", idx), &mut description);
        profiler::end(&format!("{}", idx));
        // println!("run_sequential_idx: {:?}", idx);
    };
    let elapsed = timer.elapsed();
    println!("Execution time: {:?}", elapsed);
    println!("\nState root: {:#?}", state_merkle_trie_root(state.cache.trie_account()));
    profiler::dump_json("./profiler_output.json");

    Ok(())
}

pub fn run_parallel(
    num_of_threads: usize,
    path: &PathBuf
) -> Result<(), TestError> {
    // println!("path: {:?}", path);
    let s = std::fs::read_to_string(path).unwrap();
    // println!("s: {:#?}", s);
    let suite: MultiTestSuite = serde_json::from_str(&s).map_err(|e| TestError {
        name: path.to_string_lossy().into_owned(),
        kind: e.into(),
    })?;

    let name = suite.0.keys().next().unwrap();
    let unit = suite.0.get(name).unwrap();
    
    let mut cache_state = CacheState::new(false);
    for (address, info) in unit.pre.clone() {
        let code_hash = keccak256(&info.code);
        let bytecode = Bytecode::new_raw(info.code);
        let acc_info = AccountInfo {
            balance: info.balance,
            code_hash,
            code: Some(bytecode),
            nonce: info.nonce,
        };
        cache_state.insert_account_with_storage(address, acc_info, info.storage);
    }
    let mut cache = cache_state.clone();
        cache.set_state_clear_flag(SpecId::enabled(SpecId::CANCUN, SpecId::SPURIOUS_DRAGON));

    let state = State::builder()
            .with_cached_prestate(cache)
            .with_bundle_update()
            .build();


    let mut env = Box::<Env>::default();
    // for mainnet
    env.cfg.chain_id = 1;
    // env.cfg.spec_id is set down the road

    // block env
    env.block.number = unit.env.current_number;
    env.block.coinbase = unit.env.current_coinbase;
    env.block.timestamp = unit.env.current_timestamp;
    env.block.gas_limit = unit.env.current_gas_limit;
    env.block.basefee = unit.env.current_base_fee.unwrap_or_default();
    env.block.difficulty = unit.env.current_difficulty;
    // after the Merge prevrandao replaces mix_hash field in block and replaced difficulty opcode in EVM.
    env.block.prevrandao = unit.env.current_random;
    
    // TODO: commented out, need to fix
    let mut occda = Occda::new( num_of_threads);

    let mut tasks: Vec<Task<_>> = vec![];
    let mut idx = 0;
    // post and execution

    for tx in unit.transaction.iter() {

        env.tx.caller = if let Some(address) = tx.sender {
            address
        } else {
            recover_address(tx.secret_key.as_slice()).ok_or_else(|| TestError {
                name: name.clone(),
                kind: TestErrorKind::UnknownPrivateKey(tx.secret_key),
            })?
        };
        
        env.tx.gas_price = tx
            .gas_price
            .or(tx.max_fee_per_gas)
            .unwrap_or_default();
        env.tx.gas_priority_fee = tx.max_priority_fee_per_gas;
        // EIP-4844
        env.tx.blob_hashes = tx.blob_versioned_hashes.clone();
        env.tx.max_fee_per_blob_gas = tx.max_fee_per_blob_gas;
        env.tx.gas_limit = tx.gas_limit.saturating_to();
        env.tx.data = tx.data.clone();

        env.tx.value = tx.value;
        
        env.tx.access_list = tx
                .access_lists
                .get(0)
                .and_then(Option::as_deref)
                .cloned()
                .unwrap_or_default();
        let to = match tx.to {
            Some(add) => TxKind::Call(add),
            None => TxKind::Create,
        };
        env.tx.transact_to = to;

        let inspector = NoOpInspector;
        tasks.push(Task::new(env.clone(), idx, -1, SpecId::CANCUN, Some(inspector)));
        idx += 1;
    }

    let h_tx = occda.init(tasks, None);


    let rt = Runtime::new().map_err(|_| TestError {
        name: "Runtime failed".to_string(),
        kind: TestErrorKind::Panic,
    })?;
    rt.block_on(async {
        let timer = Instant::now();
        let state_arc = Arc::new(RwLock::new(state));
        let _ = occda.main_with_db(h_tx, state_arc.clone()).await;
        let elapsed = timer.elapsed();
        profiler::dump_json("./profiler_output.json");
        println!("Execution time: {:?}", elapsed);
        let state_read = state_arc.read();
        // println!("trie_account: {:#?}", state.cache);
        println!("\nState root: {:#?}", state_merkle_trie_root(state_read.cache.trie_account()));
    });

    Ok(())
}