extern crate zookeeper;

use hyper::header::{ContentType, Headers};
use reqwest;
use self::zookeeper::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper};
use serde_json;
use serde_json::Value;
use std::time::Duration;
use std::process::exit;

struct ConnectionWatcher;
impl Watcher for ConnectionWatcher {

    fn handle(&self, e: WatchedEvent) {

    }

}

fn is_ssl_enabled(master_host: &str) -> bool {
    // We will attempt to access the /version endpoint with https first.  If that fails, fallback to HTTP.
    // Am not aware of any other mechanism to get the scheme from Zookeeper.
    let mut https_url = String::from("https://");
    https_url.push_str(master_host);
    https_url.push_str("/version");

    let mut headers = Headers::new();

    headers.set(ContentType::json());

    match reqwest::get(https_url.as_str()) {
        Ok(response) => true,
        _ => false
    }

}

pub fn discover_mesos_leader(master_config: &str) -> String {
    // Unless explicitly specified, we will assume any URL specified as the mesos master is the ZK ensemble.
    if master_config.starts_with("http") {
        return String::from(master_config);

    } else {
        let zk = zookeeper::ZooKeeper::connect(master_config, Duration::from_secs(15), ConnectionWatcher);

        match zk {

            Ok(zk_client) => {
                let mut children = zk_client.get_children("/", false).unwrap();
                children.sort();

                for node in children {

                    if node.starts_with("json.info_") {
                        let mut path = String::from("/");
                        path.push_str(node.as_str());

                        let node_content = zk_client.get_data(path.as_str(), false).unwrap().0;
                        let node_json = String::from_utf8(node_content).unwrap();

                        let value: Value = serde_json::from_str(node_json.as_str()).unwrap();

                        let mut leader_host = String::from(value["hostname"].as_str().unwrap());
                        leader_host.push_str(":");
                        leader_host.push_str(value["port"].as_i64().unwrap().to_string().as_str());

                        let mut leader_uri = String::from(
                            match is_ssl_enabled(leader_host.as_str()) {
                                true => "https://",
                                _ => "http://"
                            }
                        );

                        leader_uri.push_str(leader_host.as_str());
                        zk_client.close();

                        return leader_uri;
                    }

                }

                error!("Unable to find any Mesos leaders in Zookeeper: {}", master_config);
                exit(1);

            },
            _ => {
                error!("Unable to connect to Zookeeper cluster: {}", master_config);
                exit(1);
            }
        };

    }

}