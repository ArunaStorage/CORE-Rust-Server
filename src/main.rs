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

use std::io::Write;

lazy_static! {
    static ref SETTINGS: RwLock<Config> = RwLock::new(Config::default());
}

type ResultWrapper<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
async fn main() -> ResultWrapper<()> {
    conf();

    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{}:{} {} [{}] - {}",
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .init();

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
