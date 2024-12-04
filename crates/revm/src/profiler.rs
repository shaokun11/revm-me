use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::{
    collections::HashMap, fs::File, io::Write, sync::Mutex, thread::ThreadId, time::Instant,
};

static PROFILER: Lazy<Mutex<Profiler>> = Lazy::new(|| {
    Mutex::new(Profiler {
        genesis: Instant::now(),
        raw_events: HashMap::new(),
    })
});

struct Profiler {
    genesis: Instant,
    raw_events: HashMap<
        ThreadId, // thread id
        HashMap<
            String, // task name
            Vec<(
                u128,               // start time
                Option<u128>,       // end time (optional)
                Map<String, Value>, // other description
            )>,
        >,
    >,
}

impl Profiler {
    fn global() -> &'static Mutex<Profiler> {
        &PROFILER
    }

    fn insert_current_thread(&mut self) -> bool {
        let thread_id = std::thread::current().id();
        let non_exist = !self.raw_events.contains_key(&thread_id);
        if non_exist {
            self.raw_events.insert(thread_id, HashMap::new());
        }
        non_exist
    }

    fn insert_current_task(&mut self, name: &str) -> bool {
        let thread_id = std::thread::current().id();
        let task_raw_events = self.raw_events.get_mut(&thread_id).unwrap();
        let non_exist = !task_raw_events.contains_key(name);
        if non_exist {
            task_raw_events.insert(name.to_string(), vec![]);
        }
        non_exist
    }

    fn must_get(&self, name: &str) -> &Vec<(u128, Option<u128>, Map<String, Value>)> {
        let thread_id = std::thread::current().id();
        self.raw_events.get(&thread_id).unwrap().get(name).unwrap()
    }

    fn must_get_mut(&mut self, name: &str) -> &mut Vec<(u128, Option<u128>, Map<String, Value>)> {
        let thread_id = std::thread::current().id();
        self.raw_events
            .get_mut(&thread_id)
            .unwrap()
            .get_mut(name)
            .unwrap()
    }
    
}

pub fn get_genesis() -> Instant {
    let profiler = Profiler::global().lock().unwrap();
    profiler.genesis
}

pub fn start(name: &str) {
    let mut profiler = Profiler::global().lock().unwrap();
    let genesis = profiler.genesis;
    profiler.insert_current_thread();
    match profiler.insert_current_task(name) {
        false => assert!(
            profiler.must_get(name).last().unwrap().1.is_some(),
            "the last event must be end"
        ),
        true => (),
    };
    profiler.must_get_mut(name).push((
        Instant::now().duration_since(genesis).as_nanos(),
        None,
        Map::new(),
    ));
}

pub fn end(name: &str) {
    let mut profiler = Profiler::global().lock().unwrap();
    assert!(
        profiler.must_get(name).last().unwrap().1.is_none(),
        "the last event must be start"
    );
    profiler.must_get_mut(name).last_mut().unwrap().1 =
        Some(Instant::now().duration_since(profiler.genesis).as_nanos());
}

pub fn note(name: &str, key: &str, value: Value) {
    let mut profiler = Profiler::global().lock().unwrap();
    assert!(
        profiler.must_get(name).last().unwrap().1.is_none(),
        "the last event must be start"
    );
    profiler
        .must_get_mut(name)
        .last_mut()
        .unwrap()
        .2
        .insert(key.to_string(), value);
}

pub fn notes(name: &str, description: &mut Map<String, Value>) {
    let mut profiler = Profiler::global().lock().unwrap();
    profiler
        .must_get_mut(name)
        .last_mut()
        .unwrap()
        .2
        .append(description);
}

pub fn note_time(name: &str, key: &str) {
    let mut profiler = Profiler::global().lock().unwrap();
    let genesis = profiler.genesis;
    assert!(
        profiler.must_get(name).last().unwrap().1.is_none(),
        "the last event must be start"
    );
    profiler.must_get_mut(name).last_mut().unwrap().2.insert(
        key.to_string(),
        (Instant::now().duration_since(genesis).as_nanos() as u64).into(),
    );
}

pub fn dump() -> String {
    let profiler = Profiler::global().lock().unwrap();
    let now = Instant::now().duration_since(profiler.genesis).as_nanos();

    let mut output_frontend: Map<String, Value> = Map::new();
    output_frontend.insert("title".into(), Value::Object(Map::new()));
    output_frontend.insert("details".into(), Value::Array(vec![]));

    for (_thread_id, thread_events) in &profiler.raw_events {
        let mut detail = vec![];
        for (name, raw_events) in thread_events {
            for event in raw_events {
                let (start, end_opt, description) = event;
                let duration = end_opt.unwrap_or(now) - start;

                match description.get("type") {
                    Some(Value::String(type_str)) => match type_str.as_str() {
                        "transaction" => detail.push(json!({
                            "type": "transaction",
                            "tx": name,
                            "runtime": duration,
                            "start": start,
                            "end": end_opt,
                            "status": description.get("status").unwrap().as_str().unwrap(),
                            "detail": description,
                        })),
                        "commit" => detail.push(json!({
                            "type": "commit",
                            "tx": description.get("tx").unwrap().as_str().unwrap(),
                            "runtime": duration,
                            "start": start,
                            "end": end_opt,
                            "detail": description,
                        })),
                        other_type => detail.push(json!({
                            "type": other_type,
                            "name": name,
                            "runtime": duration,
                            "start": start,
                            "end": end_opt,
                            "detail": description,
                        })),
                    }
                    _ => detail.push(json!({
                        "type": "other",
                        "name": name,
                        "runtime": duration,
                        "start": start,
                        "end": end_opt,
                        "detail": description,
                    }))
                }
            }
        }
        output_frontend
            .get_mut("details")
            .unwrap()
            .as_array_mut()
            .unwrap()
            .push(Value::Array(detail));
    }

    serde_json::to_string_pretty(&output_frontend).unwrap()
}

pub fn dump_json(output_path: &str) {
    let result_json = dump();
    let mut file = File::create(output_path).unwrap();
    file.write_all(result_json.as_bytes()).unwrap();
}
