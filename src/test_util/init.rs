use config::File;

use core::time;
use std::{env, path::PathBuf, sync::Once, thread};

use crate::SETTINGS;

#[allow(dead_code)]
static INIT: Once = Once::new();

#[allow(dead_code)]
pub fn test_init() {
    INIT.call_once(|| {
        env_logger::init();

        match env::var("MONGO_PASSWORD") {
            Ok(_) => {}
            Err(_) => env::set_var("MONGO_PASSWORD", "test123"),
        }

        match env::var("AWS_ACCESS_KEY_ID") {
            Ok(_) => {}
            Err(_) => env::set_var("AWS_ACCESS_KEY_ID", "minioadmin"),
        }

        match env::var("AWS_SECRET_ACCESS_KEY") {
            Ok(_) => {}
            Err(_) => env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        }

        let mut testpath = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        testpath.push("resources/test/config.yaml");

        let conf_path = testpath.to_str().unwrap();
        SETTINGS
            .write()
            .unwrap()
            .merge(File::with_name(conf_path))
            .unwrap();
    });

    let mut is_completed = INIT.is_completed();
    let wait_for_completion_wait = time::Duration::from_millis(500);
    while !is_completed {
        thread::sleep(wait_for_completion_wait);
        is_completed = INIT.is_completed();
    }
}
