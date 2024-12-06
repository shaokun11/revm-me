mod parallel_runner;
use std::path::PathBuf;
use structopt::StructOpt;
pub use parallel_runner::TestError as Error;

use parallel_runner::{run_parallel, run_sequential, TestError};

#[derive(StructOpt, Debug)]
pub struct Cmd {
    #[structopt(short, long, parse(try_from_str), default_value = "true")]
    parallel: bool,

    test_file: PathBuf,

    #[structopt(short, long, default_value = "2")]
    num_of_threads: usize,
}

impl Cmd {
    /// Run statetest command.
    pub fn run(&self) -> Result<(), TestError> {
        if self.parallel {
            println!("Running in parallel mode");
            run_parallel(self.num_of_threads, &self.test_file)?;
        } else {
            println!("Running in sequential mode");
            run_sequential(&self.test_file)?;
        }
        
        Ok(())
    }
}