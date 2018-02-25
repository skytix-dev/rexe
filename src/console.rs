use base64::decode;
use hyper::header::{Accept, ContentType, Headers};
use network;
use reqwest;
use serde_json;
use serde_json::Value;
use std::io::Read;
use std::io::stderr;
use std::io::stdout;
use std::io::Write;
use std::num::Wrapping;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::thread;
use std::time;
use types;

/**
 * Makes use of the Mesos Operator API to stream STDIN and STDOUT to the running container.
 */

pub trait Console {
    fn finish(&mut self);
}

pub struct ConsoleState {
    running: Arc<Mutex<bool>>,
    stdout_thread: Option<thread::JoinHandle<()>>,
    stderr_thread: Option<thread::JoinHandle<()>>,
    stdin_thread: Option<thread::JoinHandle<()>>
}

pub struct HeadlessConsole {
    agent_url: String,
    sandbox_path: String,
    state: ConsoleState
}

pub struct InteractiveConsole {
    agent_url: String,
    container_id: String,
    state: ConsoleState
}

fn attach_container_output(agent_url: String, container_id: String, mut stdout: Box<Write + Send>, mut stderr: Option<Box<Write + Send>>, running_state: Arc<Mutex<bool>>) -> thread::JoinHandle<()> {

    thread::spawn(move || {

        let request = types::AttachContainerOutputRequest {
            request_type: String::from("ATTACH_CONTAINER_OUTPUT"),
            attach_container_output: types::AttachContainerOutput {
                container_id: types::ValueContainer {
                    value: container_id.clone()
                }
            }
        };

        let body_content = serde_json::to_string(&request).unwrap();
        let mut headers = Headers::new();

        headers.set(ContentType::json());

        let client: reqwest::Client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .build().unwrap();

        let url = agent_url.as_str();

        println!("Body: {}", body_content);

        match client.post(url)
            .body(body_content)
            .send() {

            Ok(mut response) => {

                loop {
                    let message_content = network::read_next_message(&mut response);
                    let message: types::AttachContainerOutputMessage = serde_json::from_str(message_content.as_str()).unwrap();

                    match message.message_type.as_str() {

                        "DATA" => {

                            match message.data.output_type.as_str() {

                                "STDOUT" => {
                                    let data: Vec<u8> = decode(message.data.data.as_str()).unwrap();
                                    stdout.write(&data[..]);
                                },
                                "STDERR" => {

                                    match stderr {

                                        Some(ref mut writer) => {
                                            let data: Vec<u8> = decode(message.data.data.as_str()).unwrap();
                                            writer.write(&data[..]);
                                        },
                                        None => {
                                            // Ignoring stderr.
                                        }

                                    }

                                },
                                _ => {
                                    error!("Unknown data output: {}", message.data.output_type);
                                }

                            }

                        },
                        _ => {
                            error!("Unhandled message type of {}", message.message_type)
                        }

                    }

                    let running = running_state.lock().unwrap();

                    if !*running {
                        break;
                    }

                }

            },
            Err(e) => {
                println!("Uhoh!");
                error!("{}", e);
            }

        };

    })
}

fn create_data_file_read_thread(agent_url: String, sandbox_path: String, mut writer: Box<Write + Send>, running_state: Arc<Mutex<bool>>) -> thread::JoinHandle<()> {

    thread::spawn(move || {
        let read_size: u32 = 100000; // Read in 100kB chunks
        let mut offset: u32 = 0;
        let mut last_read_size: u32 = 0;
        let mut last_running: bool = false;

        loop {
            {
                let running = running_state.lock().unwrap();

                if *running || last_running || last_read_size > 0 {

                    let request = types::ReadFileRequest {
                        request_type: String::from("READ_FILE"),
                        read_file: types::ReadFileRequestData {
                            path: sandbox_path.clone(),
                            offset: offset,
                            length: read_size
                        }
                    };

                    let body_content = serde_json::to_string(&request).unwrap();

                    let mut headers = Headers::new();

                    headers.set(ContentType::json());

                    let client: reqwest::Client = reqwest::ClientBuilder::new()
                        .default_headers(headers)
                        .build().unwrap();

                    let url = agent_url.as_str();

                    match client.post(url)
                        .body(body_content)
                        .send() {

                        Ok(ref mut response) => {
                            let response_content = response.text().unwrap();
                            let response: types::ReadFileResponse = serde_json::from_str(response_content.as_str()).unwrap();
                            let data: Vec<u8> = decode(response.read_file.data.as_str()).unwrap();

                            last_read_size = data.len() as u32;
                            writer.write(&data[..]);

                            offset = offset.wrapping_add(last_read_size);
                        },
                        Err(e) => {
                            error!("Error reading from agent: {}", e);
                        }

                    };

                } else {
                    break;
                }

                last_running = *running;
            }

            if last_read_size == 0 {
                thread::sleep(time::Duration::from_millis(1000));
            }

        }

    })

}

impl Console for HeadlessConsole {

    fn finish(&mut self) {
        self.state.set_running(false);

        match self.state.stdout_thread.take() {
            Some(handle) => {
                handle.join().unwrap();
            },
            None => {}
        };

        match self.state.stderr_thread.take() {
            Some(handle) => {
                handle.join().unwrap();
            },
            None => {}
        };

    }

}

impl Console for InteractiveConsole {

    fn finish(&mut self) {

    }

}

impl HeadlessConsole {

    pub fn new(agent_url: &str, sandbox_path: &str, show_stderr: bool) -> HeadlessConsole {
        let running = Arc::new(Mutex::new(true));
        let mut stdout_path = String::from(sandbox_path);
        let mut stderr_path = String::from(sandbox_path);
        let stdout_running = Arc::clone(&running);
        let stderr_running = Arc::clone(&running);

        stdout_path.push_str("/stdout");
        stderr_path.push_str("/stderr");

        HeadlessConsole {
            agent_url: String::from(agent_url),
            sandbox_path: String::from(sandbox_path),
            state: ConsoleState {
                running,
                stdout_thread: Some(
                    create_data_file_read_thread(
                        String::from(agent_url),
                        stdout_path,
                        Box::new(stdout()),
                        stdout_running
                    )
                ),
                stdin_thread: None,
                stderr_thread: match show_stderr {
                    true => Some(
                        create_data_file_read_thread(
                            String::from(agent_url),
                            stderr_path,
                            Box::new(stderr()),
                            stderr_running
                        )
                    ),
                    false => None
                }

            }

        }

    }

}

impl ConsoleState {

    fn set_running(&mut self, state: bool) {
        let mut running = self.running.lock().unwrap();
        *running = state;
    }

}

impl InteractiveConsole {

    pub fn new(agent_url: &str, container_id: &str, show_stderr: bool) -> InteractiveConsole {
        let running = Arc::new(Mutex::new(true));
        let thread_running = Arc::clone(&running);

        InteractiveConsole {
            agent_url: String::from(agent_url),
            container_id: String::from(container_id),
            state: ConsoleState {
                running,
                stdout_thread: Some(
                    attach_container_output(
                        String::from(agent_url),
                        String::from(container_id),
                        Box::new(stdout()),
                        match show_stderr {
                            true => Some(Box::new(stderr())),
                            false => None
                        },
                        thread_running
                    )
                ),
                stdin_thread: None,
                stderr_thread: None
            }

        }

    }

}
