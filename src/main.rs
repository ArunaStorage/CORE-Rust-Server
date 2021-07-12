#[macro_use]
extern crate lazy_static;

mod auth;
mod database;
mod handler;
mod models;
mod objectstorage;
mod server;
mod test_util;

use config::{Config, File};
use std::sync::RwLock;

use clap::{App, Arg};
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
        .unwrap_or("resources/test/config.yaml");

    {
        SETTINGS
            .write()
            .unwrap()
            .merge(File::with_name(conf_file))
            .unwrap();
    }
}
