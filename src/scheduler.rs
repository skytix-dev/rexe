use base64::decode;
use chrono;
use console;
use hyper::header::{ContentType, Headers};
use mesos;
use network;
use rand::{Rng, thread_rng};
use regex;
use reqwest;
use serde_json;
use serde_json::Value;
use std::io::Read;
use std::io::stdout;
use std::io::Write;
use std::process::exit;
use std::str::from_utf8;
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Mutex;
use std::sync::RwLock;
use timer::{Guard, Timer};
use types;
use strum::AsStaticRef;

header! { (MesosStreamId, "Mesos-Stream-Id") => [String] }

#[derive(Debug,PartialEq,AsStaticStr)]
enum SchedulerState {
    Started,
    Subscribed,
    Scheduled,
    Running,
}

/**
 * In order to get the sandbox path, we need to know the work_dir on the agent we are talking to.
 * so we need to get that from the agent state endpoint.
 **/
fn get_sandbox_path(agent_state_url: String, agent_id: &str, framework_id: &str, executor_id: &str, container_id: &str) -> String {
    let url = agent_state_url.as_str();
    let state = reqwest::get(url).unwrap().text().unwrap();

    let json: Value = serde_json::from_str(state.as_str()).unwrap();

    let mut path: String = match json["flags"]["work_dir"].as_str() {
        Some(path) => String::from(path),
        None => {
            error!("Unable to get work_dir from Mesos agent state.  Using default /var/lib/mesos");
            String::from("/var/lib/mesos")
        }
    };

    path.push_str("/slaves/");
    path.push_str(agent_id);
    path.push_str("/frameworks/");
    path.push_str(framework_id);
    path.push_str("/executors/");
    path.push_str(executor_id);
    path.push_str("/runs/");
    path.push_str(container_id);

    path
}

pub struct Scheduler<'a> {
    console: Option<Box<console::Console>>,
    state: SchedulerState,
    scheduler_url: &'a str,
    task_info: &'a types::RequestedTaskInfo,
    framework_id: String,
    stream_id: String,
    agent_id: Option<String>,
    agent_scheme: Option<String>,
    agent_hostname: Option<String>,
    agent_port: Option<i32>,
    task_id: Option<String>,
    sandbox_path: Option<String>,
    timeout_timer: Timer,
    timeout_timer_guard: Option<Guard>,
    timeout_timer_state_tx: Option<Sender<SchedulerState>>
}

impl<'a, 'b: 'a> Scheduler<'a> {

    fn new(scheduler_url: &'a str, task_info: &'a types::RequestedTaskInfo, stream_id: String) -> Scheduler<'a> {
        let running_state = Arc::new(RwLock::new(true));

        let new_scheduler = Scheduler {
            console: None,
            state: SchedulerState::Started,
            scheduler_url,
            task_info,
            framework_id: String::from(""),
            stream_id,
            agent_id: None,
            agent_scheme: None,
            agent_hostname: None,
            agent_port: None,
            task_id: None,
            sandbox_path: None,
            timeout_timer: Timer::new(),
            timeout_timer_guard: None,
            timeout_timer_state_tx: None
        };

        new_scheduler
    }

    fn start(&mut self) {
        let (tx, rx): (Sender<SchedulerState>, Receiver<SchedulerState>) = mpsc::channel();

        self.timeout_timer_state_tx = Some(tx);

        if (self.task_info.timeout > 0) {

            self.timeout_timer_guard = Some(self.timeout_timer.schedule_with_delay(chrono::Duration::seconds(self.task_info.timeout), move || {

                match rx.try_recv() {
                    Err(e) => {
                        // Exit.
                        error!("Timeout waiting for acceptable resource offer from Mesos");
                        exit(10);
                    },
                    Ok(state) => {

                        if state != SchedulerState::Scheduled {
                            error!("Unexpected Scheduler state: {}", state.as_static());
                            exit(20);
                        }
                    }

                }

            }));

        }

    }

    fn is_scheduled(&self) -> bool {

        match self.state {
            SchedulerState::Scheduled => true,
            SchedulerState::Running => true,
            _ => false
        }
        
    }

    fn set_subscribed(&mut self, framework_id: String) {
        self.state = SchedulerState::Subscribed;
        self.framework_id = framework_id;
    }

    fn handle_message(&mut self, message: String) {
        let value: Value = serde_json::from_str(message.as_str()).unwrap();
        let message_type = value["type"].as_str().unwrap();

        match message_type {

            "SUBSCRIBED" => {
                let framework_id = value["subscribed"]["framework_id"]["value"].as_str().unwrap();
                self.set_subscribed(String::from(framework_id));

                if self.task_info.verbose_output {
                    println!("Subscribed to Mesos with framework_id: {}", self.framework_id);
                }

            },
            "OFFERS" => {

                if self.task_info.verbose_output {
                    println!("New Offers:\n{}", message);
                }

                match value["offers"]["offers"].as_array() {
                    Some(offers) => {

                        for offer_value in offers {
                            let offer = types::Offer::from(offer_value);

                            if !self.is_scheduled() {

                                if self.is_useable_offer(&offer) {
                                    self.accept_offer(&offer);
                                    continue;
                                }

                            }

                            let scheduled = self.is_scheduled();

                            self.decline_offer(&offer, match scheduled {
                                true => 600f32,
                                false => 5f32
                            });

                        }

                    },

                    None => println!("Didn't find any offers")
                }

            },
            "HEARTBEAT" => {
                // Cool story, bro.
            },
            "UPDATE" => {

                match value["update"]["status"]["state"].as_str() {
                    Some(state) => match state {

                        "TASK_ERROR" |
                        "TASK_FAILED" |
                        "TASK_KILLED" |
                        "TASK_KILLING" |
                        "TASK_DROPPED" |
                        "REASON_EXECUTOR_TERMINATED" |
                        "REASON_CONTAINER_LAUNCH_FAILED" => {

                            error!("{}\n{}",
                                    value["update"]["status"]["reason"],
                                    value["update"]["status"]["message"],
                            );

                            self.deregister_exit(1);
                        },
                        "TASK_RUNNING" => {
                            // We need to get the uuid from the message to send an acknowledgement of it.
                            let mut set_running: bool = false;

                            if self.task_info.verbose_output {
                                println!("Task Running:\n{}", message);
                            }

                            match self.state {

                                SchedulerState::Scheduled => {
                                    // Acknowledge the message and change our status
                                    set_running = true;
                                },
                                _ => {
                                    // TODO: Possibly a bit of error handling here.
                                }

                            }

                            match value["update"]["status"]["uuid"].as_str() {

                                Some(uuid) => {
                                    // Send acknowledgement.
                                    self.acknowledge(uuid);
                                },
                                None => {
                                    // Do nothing.
                                }

                            };

                            let executor_id = value["update"]["status"]["executor_id"]["value"].as_str().unwrap();
                            let container_id = value["update"]["status"]["container_status"]["container_id"]["value"].as_str().unwrap();

                            if set_running {
                                self.state = SchedulerState::Running;

                                let mut agent_url: String = match self.agent_scheme {
                                    Some(ref url) => String::from(url.as_str()),
                                    None => String::from("")
                                };

                                agent_url.push_str("://");
                                agent_url.push_str(self.agent_hostname.as_ref().unwrap().as_str());
                                agent_url.push_str(":");
                                agent_url.push_str(&self.agent_port.unwrap().to_string());

                                let mut api_url = agent_url.clone();
                                let mut agent_state_url = agent_url.clone();

                                api_url.push_str("/api/v1");
                                agent_state_url.push_str("/state");

                                let mut console: Box<console::Console> = match self.task_info.tty_mode {
                                    types::TTYMode::Headless => Box::new(
                                        console::HeadlessConsole::new(
                                            api_url.as_str(),
                                            get_sandbox_path(
                                                agent_state_url,
                                                self.agent_id.as_ref().unwrap().as_str(),
                                                self.framework_id.as_str(),
                                                executor_id,
                                                container_id
                                            ).as_str(),
                                            self.task_info.stderr
                                        )
                                    ),
                                    types::TTYMode::Interactive => Box::new(
                                        console::InteractiveConsole::new(
                                            api_url.as_str(),
                                            value["update"]["status"]["container_status"]["container_id"]["value"].as_str().unwrap(),
                                            self.task_info.stderr
                                        )
                                    )
                                };

                                self.console = Some(console);

                                if self.task_info.verbose_output {
                                    println!("Task is now running")
                                }

                            }

                        },
                        "TASK_FINISHED" => {
                            // Finished.  Return no error.  We need to get stdout for it.

                            match self.state {

                                SchedulerState::Running => {

                                    if self.task_info.verbose_output {
                                        println!("Task has finished")
                                    }

                                    match self.console {

                                        Some(ref mut console) => console.finish(),
                                        None => {
                                            // No console to close.
                                        }

                                    };

                                },
                                _ => {
                                    println!("Unable to output STDOUT due to inconsistent state.  TASK_FINISHED received before app was marked as running.");
                                }

                            }

                            self.deregister_exit(0);
                        },
                        "TASK_STARTING" => {
                            if self.task_info.verbose_output {
                                println!("Task is starting:\n{}", message);
                            }

                            match value["update"]["status"]["uuid"].as_str() {

                                Some(uuid) => {
                                    // Send acknowledgement.
                                    self.acknowledge(uuid);
                                },
                                None => {
                                    // Do nothing.
                                }

                            };
                        },
                        _ => println!("Unhandled update state: {}\n{}", state, message),
                    },
                    None => {
                        println!("Empty state");
                    }

                }

            },
            _ => println!("Unhandled event message: {}", message),
        };

    }

    fn deregister_exit(&self, exit_code: i32) {
        let request = types::teardown_request(&self.framework_id);
        let body_content = serde_json::to_string(&request).unwrap();

        if !self.deliver_request(body_content) {
            println!("Unable to send teardown call to master. Exiting anyway.");
        }

        exit(exit_code);
    }

    fn acknowledge(&mut self, uuid: &str) {
        let agent_id = self.agent_id.as_ref().unwrap();
        let task_id = self.task_id.as_ref().unwrap();

        let request = types::acknowledge_request(&self.framework_id, agent_id, task_id, uuid);

        let body_content = serde_json::to_string(&request).unwrap();

        if !self.deliver_request(body_content) {
            println!("Problem with sending acknowledge message to the server.");
        }

    }

    fn accept_offer(&mut self, offer: &types::Offer) {
        let task_id: String = thread_rng().gen_ascii_chars().take(10).collect();
        
        let request = types::accept_request(
            &self.framework_id,
            &offer.offer_id,
            &offer.agent_id,
            &task_id,
            &self.task_info,
            &self.task_info.tty_mode
        );

        let body_content = serde_json::to_string(&request).unwrap();

        if self.task_info.verbose_output {
            println!("Offer: {}", body_content);
        }

        let output = body_content.clone();

        if self.deliver_request(body_content) {
            self.state = SchedulerState::Scheduled;
            self.task_id = Some(task_id.clone());
            self.agent_id = Some(offer.agent_id.clone());
            self.agent_scheme = Some(offer.scheme.clone());
            self.agent_hostname = Some(offer.hostname.clone());
            self.agent_port = Some(offer.port.clone());

            self.timeout_timer_state_tx.as_ref().unwrap().send(SchedulerState::Scheduled);

        } else {
            error!("Error sending acceptance offer to mesos\n\n{}", output);
            self.deregister_exit(1);
        }

    }

    fn decline_offer(&mut self, offer: &types::Offer, refuse_seconds: f32) {

        let request = types::decline_request(
            &self.framework_id,
            &offer.offer_id,
            match (&self.state) {
                &SchedulerState::Running => true,
                _ => false
            },
            refuse_seconds
        );

        let body_content = serde_json::to_string(&request).unwrap();

        if !self.deliver_request(body_content) {
            println!("Error sending decline to master");
        }

    }

    fn is_useable_attribute(&self, attr_name: &String, attr_value: &String, offer: &types::Offer) -> bool {

        for (name, value) in &offer.attributes {

            if name.eq(attr_name.as_str()) {

                if attr_value.starts_with("/") {
                    let pattern = attr_value.trim_matches('/');
                    let attr_regex = regex::Regex::new(pattern).unwrap();

                    return attr_regex.is_match(value.as_str());

                } else {
                    return value.eq(attr_value.as_str());
                }

            }

        }

        return false;
    }

    fn is_useable_offer(&self, offer: &types::Offer) -> bool {
        // Real basic check for now.  Does the offer have enough resources for us?
        for (key, value) in self.task_info.attrs.iter() {

            if !self.is_useable_attribute(key, value, offer) {
                return false;
            }

        }

        offer.cpus >= self.task_info.cpus &&
            offer.gpus >= self.task_info.gpus &&
            offer.disk >= self.task_info.disk &&
            offer.mem >= self.task_info.mem
    }

    fn deliver_request(&self, body: String) -> bool {
        let mut scheduler_url: String = String::from(self.scheduler_url);
        scheduler_url.push_str("/api/v1/scheduler");

        let mut headers = Headers::new();

        headers.set(ContentType::json());
        headers.set(MesosStreamId(String::from(&*self.stream_id)));

        let client: reqwest::Client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .build().unwrap();

        match client.post(scheduler_url.as_str())
            .body(body)
            .send() {

            Ok(_) => true,
            Err(error) => {
                error!("{}", error);
                false
            }

        }

    }

}

pub fn execute<'a>(mesos_host: &'a str, task_info: &'a types::RequestedTaskInfo) {
    let mut scheduler_uri: String = mesos::discover_mesos_leader(mesos_host);
    scheduler_uri.push_str("/api/v1/scheduler");

    let url : &str = &scheduler_uri[..];

    if task_info.verbose_output {
        println!("Sending request to: {}", url)
    }

    let subscribe_request = types::SubscribeRequest {
        message_type: String::from("SUBSCRIBE"),
        subscribe: types::SubscribeType {
            framework_info: types::FrameworkInfo {
                user: String::from("root"),
                name: String::from("RExe task executor"),
                capabilities: vec![],
            }
        }
    };

    let body_content = serde_json::to_string(&subscribe_request).unwrap();

    if task_info.verbose_output {
        println!("Subscribe message: {}", body_content);
    }

    let mut headers = Headers::new();

    headers.set(ContentType::json());

    let client: reqwest::Client = reqwest::ClientBuilder::new()
        .default_headers(headers)
        .build().unwrap();

    match client.post(url)
        .body(body_content)
        .send() {

        Ok(mut response) => {

            match network::get_header_string_value("Mesos-Stream-Id", response.headers()) {

                Some(id) => {

                    if !id.is_empty() {

                        if task_info.verbose_output {
                            println!("Stream id {}", id);
                        }

                        let mut scheduler = Scheduler::new(&scheduler_uri, task_info, id);
                        scheduler.start();

                        loop {
                            let message = network::read_next_message(&mut response);
                            scheduler.handle_message(message);
                        }

                    } else {
                        error!("Received empty steam id from Mesos");
                        exit(1);
                    }

                },
                None => {
                    error!("Unable to get stream id from Mesos");
                }

            };

        },
        Err(e) => {
            error!("{}", e);
        }

    };

}
