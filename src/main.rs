extern crate hyper;
extern crate kafka;
#[macro_use]
extern crate log;
extern crate log4rs;
extern crate s3;
extern crate uuid;

use std::env;
use std::fs::File;
use std::io::{Read, Write};
use uuid::Uuid;

use hyper::server::{Server, Request, Response};
use hyper::status::StatusCode;

struct Config {
    s3_bucket: String,
    s3_region: Option<String>,
    s3_access_key: String,
    s3_secret_key: String,
    kafka_server: String, // FIXME server:port type
    kafka_topic: String,
    kafka_timeout: i32, // ms
    kafka_required_acks: i16,
}

impl Config {
    pub fn api_protocol(&self) -> &'static str {
        // TODO
        // When testing we route all API traffic over HTTP so we can
        // sniff/record it, but everywhere else we use https
        //if self.env == ::Env::Test {"http"} else {"https"}
        "http"
    }
}

fn main() {
    log4rs::init_file("config/log.toml", Default::default()).unwrap();

    let default_port = 5000;
    let port;
    match env::var("PORT") {
        Ok(val) => port = val,
        Err(_) => {
            println!("$PORT unset, using default {}", default_port);
            port = format!("{}", default_port);
        }
    }

    let address = &*format!("0.0.0.0:{}", port);

    info!("Listening on {}", address);
    Server::http(address).unwrap().handle(server).unwrap();
}

fn server(mut req: Request, mut res: Response) {
    info!("incoming connection from {}", req.remote_addr);
    match req.method {
        hyper::Post => {
            let mut res = res.start().unwrap();

            let mut buffer = String::new();
            req.read_to_string(&mut buffer).unwrap();
            debug!("raw POST: {:?}", &buffer);
            let uuid = store_message(buffer);
            res.write_all(uuid.to_hyphenated_string().as_bytes()).unwrap();
            res.end().unwrap();
        },
        hyper::Get => {
            let mut res = res.start().unwrap();
            res.write_all("symbolapi, see github.com/rhelmer/collector".as_bytes()).unwrap();
        },
        _ => { *res.status_mut() = StatusCode::MethodNotAllowed },
    }

    debug!("finished serving request");
}

fn store_message(message: String) -> Uuid {
    // TODO read from config file
    let config = Config{
        s3_bucket: format!("collector-test-123"),
        s3_region: Some(format!("us-east-1")),
        s3_access_key: format!("abc123"),
        s3_secret_key: format!("123abc"),
        kafka_server: format!("localhost:9092"),
        kafka_topic: format!("collector-test-topic"),
        kafka_timeout: 1000, // ms
        kafka_required_acks: 0, // do not wait for ack
    };

    let uuid = Uuid::new_v4();
    // first store to persistent storage (S3)
    /* FIXME this library expects curl, maybe switch to hyper::client?
    let bucket = s3::Bucket::new(config.s3_bucket.clone(),
                                 config.s3_region.clone(),
                                 config.s3_access_key.clone(),
                                 config.s3_secret_key.clone(),
                                 config.api_protocol());


    let _ = bucket.put(&handle, format!("v1/{}", uuid.to_hyphenated_string(),
                       message, "binary/octet-stream")
    */

    // FIXME store to local FS for the moment
    let mut f = File::create(format!("./{}", uuid)).unwrap();
    f.write_all(message.as_bytes()).unwrap();

    // finally, store UUID in kafka for later consumption
    let mut client = kafka::client::KafkaClient::new(vec!(format!("{}", config.kafka_server)));
    match client.load_metadata_all() {
        Ok(x) => debug!("connected to kafka {:?}", x),
        Err(x) => error!("error connecting to kafka {:?}", x)
    }

    match client.send_message(config.kafka_required_acks, config.kafka_timeout,
                              config.kafka_topic, message.to_owned().into_bytes()) {
        Ok(x) => debug!("sent message to kafka: {:?}", x),
        Err(x) => error!("error sending message to kafka {:?}", x)
    }

    uuid
}
