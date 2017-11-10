use std::str::from_utf8;
use std::process::exit;

use futures;
use futures::Future;
use futures::stream::Stream;

use hyper::Client;
use hyper::Request;
use hyper::Body;
use hyper::Response;
use hyper::Method;
use hyper::Chunk;
use hyper::StatusCode;
use hyper::client::HttpConnector;
use hyper::header::{Headers, ContentType};
use hyper::mime::Mime;
use hyper::Uri;
use tokio_core::reactor::Core;
use serde_json;
use serde_json::{Value};
use types;
use regex::Regex;
use ctrlc;
use rand::{thread_rng, Rng};
use base64::{decode};

header! { (MesosStreamId, "Mesos-Stream-Id") => [String] }

enum SchedulerState {
    Started,
    Subscribed,
    Scheduled,
    Running,
}

pub struct Scheduler<'a, 'b: 'a> {
    state: SchedulerState,
    scheduler_url: &'a str,
    task_info: &'b types::RequestedTaskInfo,
    framework_id: String,
    stream_id: String,
    agent_id: Option<String>,
    agent_scheme: Option<String>,
    agent_hostname: Option<String>,
    agent_port: Option<i32>,
    task_id: Option<String>,
    sandbox_path: Option<String>
}

impl<'a, 'b: 'a> Scheduler<'a, 'b> {

    fn new(scheduler_url: &'a str, task_info: &'b types::RequestedTaskInfo, stream_id: String) -> Scheduler<'a, 'b> {

        let mut new_scheduler = Scheduler {
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

                unsafe {

                    if ::VERBOSE_OUTPUT {
                        println!("Subscribed to Mesos with framework_id: {}", self.framework_id);
                    }

                }
            },
            "OFFERS" => {

                match value["offers"]["offers"].as_array() {
                    Some(offers) => {

                        for offer_value in offers {
                            let offer_id = offer_value["id"]["value"].as_str().unwrap();
                            let offer = types::Offer::from(offer_value);

                            if !self.is_scheduled() {

                                if self.is_useable_offer(&offer) {
                                    self.accept_offer(&offer);

                                } else {
                                    // Decline the offer.
                                    self.decline_offer(&offer);
                                }
                            } else {
                                // We are already scheduled, decline the offer.
                                self.decline_offer(&offer);
                            }

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
                                    value["update"]["status"]["reason"].as_str().unwrap(),
                                    value["update"]["status"]["message"].as_str().unwrap(),
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
                                // We need to get the executor ID
                                self.sandbox_path = Some(String::from(value[0]["Mounts"][0]["Source"].as_str().unwrap()));
                                self.state = SchedulerState::Running;
                            }

                        }
                        "TASK_FINISHED" => {
                            // Finished.  Return no error.  We need to get stdout for it.

                            match self.state {

                                SchedulerState::Running => {
                                    self.output_stdout();
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

    fn output_stdout(&mut self) {
        let mut core = Core::new().unwrap();
        let client: Client<HttpConnector> = Client::new(&core.handle());
        let mut agent_url: String = self.agent_scheme.take().unwrap();

        agent_url.push_str("://");
        agent_url.push_str(&self.agent_hostname.take().unwrap());
        agent_url.push_str(":");
        agent_url.push_str(&self.agent_port.take().unwrap().to_string());

        let sandbox_path = self.sandbox_path.take().unwrap();
        let sandbox_clone = sandbox_path.clone();

        agent_url.push_str("/files/download?path=");
        agent_url.push_str(str::replace(sandbox_clone.as_str(), "/", "%2F").as_str());
        agent_url.push_str("/stdout");

        self.sandbox_path = Some(sandbox_path);

        let mut request = Request::new(Method::Get, agent_url.parse().unwrap());

        let work = client.request(request).and_then(|response: Response| {

            match response.status() {
                StatusCode::Ok => {
                    response.body().concat2()
                        .and_then(move |body| {
                            let stringify = from_utf8(&body).unwrap();
                            println!("{}", stringify);
                            futures::future::ok(())
                        }
                        ).wait();
                },

                _ => {
                    let status_code = &response.status().as_u16();

                    response.body().concat2()
                        .and_then(move |body| {


                            let stringify = from_utf8(&body).unwrap();
                            error!("Error:\n\n {}\n{}", status_code, stringify);
                            futures::future::ok(())
                        }
                        ).wait();

                },
            };

            Ok(())
        });

        core.run(work);
    }

    fn deregister_exit(&self, exit_code: i32) {
        let request = types::teardown_request(&self.framework_id);
        let body_content = serde_json::to_string(&request).unwrap();

        if !self.deliver_request(Body::from(body_content)) {
            println!("Unable to send teardown call to master. Exitting anyway.");
        }

        exit(exit_code);
    }

    fn acknowledge(&mut self, uuid: &str) {
        let agent_id = self.agent_id.take().unwrap();
        let task_id = self.task_id.take().unwrap();

        let request = types::acknowledge_request(&self.framework_id, &agent_id, &task_id, uuid);

        let body_content = serde_json::to_string(&request).unwrap();

        if !self.deliver_request(Body::from(body_content)) {
            println!("Problem with sending acknowledge message to the server.");
        }

    }

    fn accept_offer(&mut self, offer: &types::Offer) {
        let id = &offer.offer_id;
        let task_id: String = thread_rng().gen_ascii_chars().take(10).collect();
        
        let request = types::accept_request(
            &self.framework_id,
            &offer.offer_id,
            &offer.agent_id,
            &task_id,
            &self.task_info
        );

        let body_content = serde_json::to_string(&request).unwrap();
        let output = body_content.clone();

        if self.deliver_request(Body::from(body_content)) {
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
        let id = &offer.offer_id;

        let request = types::decline_request(
            &self.framework_id,
            &offer.offer_id
        );

        let body_content = serde_json::to_string(&request).unwrap();

        if !self.deliver_request(Body::from(body_content)) {
            println!("Error sending decline to master");
        }

    }

    fn is_useable_offer(&self, offer: &types::Offer) -> bool {
        // Real basic check for now.  Does the offer have enough resources for us?
        //TODO: Make this check attributes.
        offer.cpus >= self.task_info.cpus &&
            offer.gpus >= self.task_info.gpus &&
            offer.disk >= self.task_info.disk &&
            offer.mem >= self.task_info.mem
    }

    fn deliver_request(&self, body: Body) -> bool {
        let mut core = Core::new().unwrap();
        let client: Client<HttpConnector> = Client::new(&core.handle());

        let mime: Mime = "application/json".parse().unwrap();

        let mut scheduler_url: String = String::from(self.scheduler_url);
        scheduler_url.push_str("/api/v1/scheduler");

        let mut request = Request::new(Method::Post, scheduler_url.parse().unwrap());

        request.headers_mut().set(ContentType(mime));
        request.headers_mut().set(MesosStreamId(String::from(&*self.stream_id)));
        request.set_body(body);

        let work = client.request(request).and_then(|response: Response| {
            let mut success: bool = false;

            match response.status() {
                StatusCode::Accepted => success = true,
                _ => {
                    let status_code = &response.status().as_u16();

                    response.body().concat2()
                        .and_then(move |body| {
                            let stringify = from_utf8(&body).unwrap();
                            println!("Error:\n\n {}", stringify);
                            futures::future::ok(())
                        }
                    ).wait();

                    success = false
                },
            };

            Ok(success)
        });

        match core.run(work) {
            Ok(value) => value,
            Err(_) => false,
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
                    Err(e) => error!("Error while reading header value")
                }

            }

        },
        None => {},
    }

    Some(value)
}

pub fn execute<'a>(mesos_host: &'a str, task_info: &'a types::RequestedTaskInfo) {
    let mut scheduler_uri :String = String::from("http://");
    let mut core = Core::new().unwrap();

    let client: Client<HttpConnector> = Client::new(&core.handle());

    scheduler_uri.push_str(mesos_host);

    let mut scheduler_path = scheduler_uri.clone();
    scheduler_path.push_str("/api/v1/scheduler");

    let url : &str = &scheduler_path[..];
    let mut request = Request::new(Method::Post, url.parse().unwrap());
    let mime :Mime = "application/json".parse().unwrap();

    unsafe {

        if ::VERBOSE_OUTPUT {
            println!("Sending request to: {}", url)
        }

    }

    request.headers_mut().set(ContentType(mime));

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

    unsafe {

        if ::VERBOSE_OUTPUT {
            println!("Subscribe content:\n\n{}", body_content);
        }

    }

    request.set_body(Body::from(body_content));

    let work = client.request(request).and_then(|res: Response| {
        let mut stream_id: String;

        match res.status() {

            StatusCode::Ok => {
                let header_value = get_header_string_value("Mesos-Stream-Id", res.headers());

                match header_value {
                    Some(value) =>  {

                        unsafe {

                            if ::VERBOSE_OUTPUT {
                                println!("Stream id {}", value);
                            }

                        }

                        stream_id = value;
                    },
                    None => {
                        error!("Unable to read Mesos-Stream-Id from subscribe response");
                        exit(1);
                    },
                };
            },
            _ => {
                error!("Unable to subscribe as a scheduler to Mesos");
                exit(1);
            }
        };

        let mut buffer: String = String::from("");
        let mut message_length: usize = 0;

        let regex: Regex = Regex::new(r"^((\d+)\n).*$").unwrap();

        let mut scheduler = Scheduler::new(&scheduler_uri, task_info, stream_id);

        res.body().for_each(move |chunk: Chunk| {

            match from_utf8(chunk.as_ref()) {
                Ok(value) => buffer.push_str(value),
                Err(_) => {}
            }

            // If we have a message length set, then we know we are waiting for more bytes to come in.
            if message_length == 0 {
                let local_buffer = buffer.clone();
                let local_buffer_str = local_buffer.as_str();

                if regex.is_match(local_buffer_str) {
                    let groups = regex.captures(local_buffer_str).unwrap();

                    message_length = groups.get(2).unwrap().as_str().parse::<usize>().unwrap();
                    let capture = groups.get(1).unwrap().as_str();

                    buffer.drain(..capture.len());
                }
                
            }

            if message_length > 0 && buffer.len() >= message_length {
                let message: String = buffer.drain(..message_length).collect();
                message_length = 0;

                scheduler.handle_message(message);
            }

            futures::future::ok::<_,_>(())
        })

    });

    core.run(work).unwrap();
}
