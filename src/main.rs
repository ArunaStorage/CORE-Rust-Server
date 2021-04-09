#[macro_use]
extern crate lazy_static;

extern crate async_trait;
extern crate chrono;
extern crate clap;
extern crate config;
extern crate futures;
extern crate prost;
extern crate prost_types;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate tokio;
extern crate uuid;

mod auth;
mod database;
mod objectstorage;
mod server;

use config::{Config, File};
use std::sync::RwLock;

use log::{debug, error, info, log_enabled, Level};

use clap::{App, Arg, SubCommand};
use server::server::start_server;

lazy_static! {
    static ref SETTINGS: RwLock<Config> = RwLock::new(Config::default());
}

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> ResultWrapper<()> {
    conf();

    env_logger::init();

    start_server().await
}

fn conf() {
    let cli = App::new("CORS server")
        .version("0.1")
        .author("Marius D.")
        .about("Server implementation of the ScienceObjectsDB")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true),
        )
        .get_matches();

    let conf_file = cli
        .value_of("config")
        .unwrap_or("resources/local/config.yaml");

    {
        SETTINGS
            .write()
            .unwrap()
            .merge(File::with_name(conf_file))
            .unwrap();
    }
}
