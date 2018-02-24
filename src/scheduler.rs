use std::str::from_utf8;
use std::process::exit;
use std::io::stdout;
use std::io::Write;
use std::io::Read;

use hyper::header::{Headers, ContentType};
use reqwest;
use serde_json;
use serde_json::{Value};
use types;
use console;
use rand::{thread_rng, Rng};
use base64::{decode};

use regex;


header! { (MesosStreamId, "Mesos-Stream-Id") => [String] }

enum SchedulerState {
    Started,
    Subscribed,
    Scheduled,
    Running,
}

pub struct Scheduler<'a> {
    operator: &'a console::Console,
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
    sandbox_path: Option<String>
}

impl<'a, 'b: 'a> Scheduler<'a> {

    fn new(operator: &'a console::Console, scheduler_url: &'a str, task_info: &'a types::RequestedTaskInfo, stream_id: String) -> Scheduler<'a> {

        let new_scheduler = Scheduler {
            operator: operator,
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
        };

        new_scheduler
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

                            self.decline_offer(&offer);
                        }

                    },

                    None => println!("Didnt find any offers")
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

                            let bytes = value["update"]["status"]["data"].as_str();
                            let data: Vec<u8> = decode(bytes.unwrap()).unwrap();
                            let unwrapped = &String::from_utf8(data).unwrap();
                            let value: Value = serde_json::from_str(unwrapped).unwrap();

                            if set_running {
                                self.sandbox_path = Some(String::from(value[0]["Mounts"][0]["Source"].as_str().unwrap()));
                                self.state = SchedulerState::Running;

                                if self.task_info.verbose_output {
                                    println!("Task is now running")
                                }

                            }

                        }
                        "TASK_FINISHED" => {
                            // Finished.  Return no error.  We need to get stdout for it.

                            match self.state {

                                SchedulerState::Running => {

                                    if self.task_info.verbose_output {
                                        println!("Task has finished")
                                    }

                                    self.fetch_stdout();
                                },
                                _ => {
                                    println!("Unable to output STDOUT due to inconsistent state.  TASK_FINISHED received before app was marked as running.");
                                }

                            }

                            self.deregister_exit(0);
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

    fn fetch_stdout(&mut self) {

        let mut agent_url: String = match self.agent_scheme {
            Some(ref url) => String::from(url.as_str()),
            None => String::from("")
        };

        agent_url.push_str("://");
        agent_url.push_str(self.agent_hostname.as_ref().unwrap().as_str());
        agent_url.push_str(":");
        agent_url.push_str(&self.agent_port.unwrap().to_string());
        agent_url.push_str("/files/download?path=");

        match self.sandbox_path {

            Some(ref path) => {
                agent_url.push_str(str::replace(path.as_str(), "/", "%2F").as_str());
            },
            _ => {}

        };

        agent_url.push_str("/stdout");

        match reqwest::get(agent_url.as_str()) {

            Ok(ref mut response) => {
                writeln!(stdout(), "{}", response.text().unwrap());
            },
            Err(error) => {
                error!("Failed to read STDOUT from agent: {}", error);
            }

        }

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

            //TODO: Set more props here

        } else {
            error!("Error sending acceptance offer to mesos\n\n{}", output);
            self.deregister_exit(1);
        }

    }

    fn decline_offer(&mut self, offer: &types::Offer) {

        let request = types::decline_request(
            &self.framework_id,
            &offer.offer_id
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

fn get_header_string_value<'a>(name: &'a str, headers: &'a Headers) -> Option<String> {
    let header_option = headers.get_raw(name);
    let mut value = String::from("");

    match header_option {

        Some(header) => {

            for line in header.into_iter() {
                let data: &[u8] = line;

                match String::from_utf8(Vec::from(data)) {
                    Ok(field_value) => value.push_str(&field_value[..]),
                    Err(_) => error!("Error while reading header value")
                }

            }

        },
        None => {},
    }

    Some(value)
}

fn read_next_message(response: &mut reqwest::Response) -> String {
    let mut msg_length_str = String::from("");
    let mut buffer: String = String::from("");
    let mut have_msg_length = false;

    while !have_msg_length {
        let mut buf: Vec<u8> = vec![0; 1];

        match response.read(&mut buf[..]) {

            Ok(_) => {
                let resp_str = from_utf8(&buf).unwrap();

                for c in resp_str.chars() {

                    if !have_msg_length {
                        // We need to start reading looking for a newline character.  This string will then be the number of bytes we need to read for the next message.
                        if c == '\n' {
                            have_msg_length = true;

                        } else {
                            msg_length_str.push_str(c.to_string().as_str());
                        }

                    } else {
                        // We may have found the end-of-line and these characters are part of the message body.
                        buffer.push_str(c.to_string().as_str());
                    }

                }

            },
            Err(e) => {
                error!("{}", e);
                exit(1);
            }

        };

    }

    let msg_len = msg_length_str.parse::<usize>().unwrap() - buffer.len();
    let mut buf: Vec<u8> = vec![0; msg_len];

    match response.read_exact(&mut buf[..]) {

        Ok(_) => {

            match from_utf8(&buf) {
                Ok(value) => buffer.push_str(value),
                Err(_) => {}
            }

        },
        Err(e) => {
            error!("{}", e);
            exit(1);
        }
    }

    buffer
}

pub fn execute<'a>(console: &'a console::Console, mesos_host: &'a str, task_info: &'a types::RequestedTaskInfo) {
    let mut scheduler_uri :String = String::from("http://");

    scheduler_uri.push_str(mesos_host);

    let mut scheduler_path = scheduler_uri.clone();
    scheduler_path.push_str("/api/v1/scheduler");

    let url : &str = &scheduler_path[..];

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

            match get_header_string_value("Mesos-Stream-Id", response.headers()) {

                Some(id) => {

                    if !id.is_empty() {

                        if task_info.verbose_output {
                            println!("Stream id {}", id);
                        }

                        let mut scheduler = Scheduler::new(console, &scheduler_uri, task_info, id);

                        loop {
                            let message = read_next_message(&mut response);
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
