extern crate zookeeper;

use hyper::header::{ContentType, Headers};
use reqwest;
use self::zookeeper::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper, KeeperState};
use serde_json;
use serde_json::Value;
use std::time::Duration;
use std::process::exit;
use std::sync::{Arc, Mutex};

struct WatcherStateHandler {
    state: KeeperState
}

impl WatcherStateHandler {

    pub fn set_state(&mut self, state: KeeperState) {
        self.state = state;
    }

    pub fn get_state(&self) -> KeeperState {
        return self.state.clone();
    }
}

struct ConnectionWatcher {
    handler: Arc<Mutex<WatcherStateHandler>>
}

impl Watcher for ConnectionWatcher {

    fn handle(&self, e: WatchedEvent) {
        let mut state_handler = self.handler.lock().unwrap();
        state_handler.set_state(e.keeper_state);
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

        let shared_state = Arc::new(
            Mutex::new(
                WatcherStateHandler { state: KeeperState::Disconnected }
            )
        );

        let watcher = ConnectionWatcher { handler: shared_state.clone() };
        let zk = zookeeper::ZooKeeper::connect(master_config, Duration::from_secs(15), watcher);

        let zk_client = match zk {

            Ok(zk_client) => {
                zk_client
            },
            Err(e) => {
                error!("Unable to connect to Zookeeper cluster: {}\n{}", master_config, e);
                exit(1);
            }
        };

        let mut children = zk_client.get_children("/", false).unwrap();
        children.sort();

        let mut leader: Option<String> = None;

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
                leader = Some(leader_uri);
                break;
            }

        }

        match shared_state.lock().unwrap().get_state() {
            KeeperState::SyncConnected | KeeperState::ConnectedReadOnly => {
                zk_client.close();
            },
            _ => {}
        };

        match leader {
            Some(leader) => {
                return leader;
            },
            _ => {
                error!("Unable to find any Mesos leaders in Zookeeper: {}", master_config);
                exit(1);
            }
        };

    }

}