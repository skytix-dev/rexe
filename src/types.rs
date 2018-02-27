use serde_json::Value;
use std::collections::HashMap;
use terminal_size::{Height, terminal_size, Width};

pub enum TTYMode {
    Interactive,
    Headless
}

pub struct RequestedTaskInfo {
    pub executor: String,
    pub image_name: Option<String>,
    pub cpus: f32,
    pub gpus: i32,
    pub mem: f32,
    pub disk: f32,
    pub args: String,
    pub env_args: HashMap<String, String>,
    pub verbose_output: bool,
    pub tty_mode: TTYMode,
    pub attrs: HashMap<String, String>,
    pub force_pull: bool,
    pub stderr: bool,
    pub shell: bool
}

#[derive(Serialize, Debug)]
pub struct Scalar {
    value: f32
}

#[derive(Serialize, Debug)]
pub enum ContainerInfoType {
    #[serde(rename = "DOCKER")]
    Docker,
    #[serde(rename = "MESOS")]
    Mesos
}

#[derive(Serialize, Debug)]
pub enum DockerInfoNetwork {
    #[serde(rename = "HOST")]
    Host,
    #[serde(rename = "BRIDGE")]
    Bridge,
    #[serde(rename = "NONE")]
    None,
    #[serde(rename = "USER")]
    User
}

#[derive(Serialize, Debug)]
pub enum OperationType {
    #[serde(rename = "UNKNOWN")]
    Unknwon,
    #[serde(rename = "LAUNCH")]
    Launch,
    #[serde(rename = "LAUNCH_GROUP")]
    LaunchGroup,
    #[serde(rename = "RESERVE")]
    Reserve,
    #[serde(rename = "UNRESERVE")]
    Unreserve,
    #[serde(rename = "CREATE")]
    Create,
    #[serde(rename = "DESTROY")]
    Destroy,
    #[serde(rename = "CREATE_VOLUME")]
    CreateVolume,
    #[serde(rename = "CREATE_BLOCK")]
    CreateBlock,
    #[serde(rename = "DESTROY_BLOCK")]
    DestroyBlock
}

#[derive(Serialize, Debug)]
pub enum VolumeMode {
    RW,
    RO
}

#[derive(Serialize)]
pub struct Volume {
    mode: VolumeMode,
    container_path: String,
    host_path: String
}

#[derive(Serialize)]
pub struct Parameter {
    key: String,
    value: String
}

#[derive(Serialize)]
pub struct CommandInfo {
    value: String,
    arguments: Vec<String>,
    shell: bool,
    environment: Option<Environment>
}

#[derive(Serialize)]
pub struct PortMapping {
    host_port: i32,
    container_port: i32,
    protocol: String
}

#[derive(Serialize)]
pub struct DockerInfo {
    image: String,
    network: DockerInfoNetwork,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    port_mappings: Vec<PortMapping>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    parameters: Vec<Parameter>,
    privileged: bool,
    force_pull_image: bool,
}

#[derive(Serialize)]
pub struct WindowSize {
    rows: u16,
    columns: u16
}

#[derive(Serialize)]
pub struct TTYInfo {
    window_size: WindowSize
}

#[derive(Serialize)]
pub struct ContainerInfo {
    #[serde(rename = "type")]
    container_type: ContainerInfoType,
    docker: DockerInfo,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    volumes: Vec<Volume>,
    tty_info: Option<TTYInfo>
}

#[derive(Serialize, Debug)]
pub struct Resource {
    name: String,
    #[serde(rename = "type")]
    resource_type: String,
    scalar: Scalar
}

#[derive(Serialize)]
pub enum ExecutorInfoType {
    #[serde(rename = "UNKNOWN")]
    Unknown,
    #[serde(rename = "DEFAULT")]
    Default,
    #[serde(rename = "CUSTOM")]
    Custom
}

#[derive(Serialize)]
pub struct Variable {
    name: String,
    value: String
}

#[derive(Serialize)]
pub struct Environment {
    variables: Vec<Variable>
}

#[derive(Serialize)]
pub struct TaskInfo {
    name: String,
    task_id: ValueContainer,
    agent_id: ValueContainer,
    container: Option<ContainerInfo>,
    command: CommandInfo,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    resources: Vec<Resource>
}

pub struct Offer {
    pub offer_id: String,
    pub agent_id: String,
    pub hostname: String,
    pub port: i32,
    pub scheme: String,
    pub cpus: f32,
    pub gpus: i32,
    pub mem: f32,
    pub disk: f32,
    pub attributes: HashMap<String, String>
}

impl Offer {

     pub fn from(value: &Value) -> Offer {
         let cpus: f32;
         let gpus: i32;
         let mem: f32;
         let disk: f32;

         match value["resources"].as_array() {
             Some(resources) => {
                 let mut resource_map: HashMap<String, String> = HashMap::new();

                 for resource in resources {

                     match resource["name"].as_str() {

                         Some(name) => {
                             resource_map.insert(String::from(name), match resource["scalar"]["value"].as_f64() {
                                 Some(value) => value.to_string(),
                                 None => String::from("0.0")
                             });
                         },
                         None => {
                             // Not too sure yet what to do if we don't find a name.  Suspect response.
                         }

                     }

                 }

                 cpus = match resource_map.get("cpus") {
                     Some(value) => value.parse::<f32>().unwrap(),
                     None => 0.0f32
                 };

                 gpus = match resource_map.get("gpus") {
                     Some(value) => value.parse::<i32>().unwrap(),
                     None => 0
                 };

                 mem = match resource_map.get("mem") {
                     Some(value) => value.parse::<f32>().unwrap(),
                     None => 0.0f32
                 };

                 disk = match resource_map.get("disk") {
                     Some(value) => value.parse::<f32>().unwrap(),
                     None => 0.0f32
                 };

             },
             None => {
                 // Set everything to be be 0 I guess?
                 cpus = 0.0;
                 gpus = 0;
                 mem = 0.0;
                 disk = 0.0;
             }
         };

         let mut attributes: HashMap<String, String> = HashMap::new();

         match value["attributes"].as_array() {

             Some(attrs) => {

                 for attr in attrs {

                     match attr["name"].as_str() {

                         Some(name) => {

                             attributes.insert(String::from(name), match attr["text"]["value"].as_str() {
                                 Some(value) => String::from(value),
                                 None => String::from("")
                             });

                         },
                         None => {
                             // Not too sure yet what to do if we don't find a name.  Suspect response.
                         }

                     }

                 }
             },
             None => {
                 // Nothing to do.
             }

         };

         // If the hostname attribute isn't explicitly set as an attribute, we will implicitly add
         // the hostname in the offer
         if !attributes.contains_key("hostname") {
             attributes.insert(String::from("hostname"), String::from(value["hostname"].as_str().unwrap()));
         }

         Offer {
             offer_id: String::from(value["id"]["value"].as_str().unwrap()),
             agent_id: String::from(value["agent_id"]["value"].as_str().unwrap()),
             hostname: String::from(value["url"]["address"]["hostname"].as_str().unwrap()),
             port: value["url"]["address"]["port"].as_i64().unwrap() as i32,
             scheme: String::from(value["url"]["scheme"].as_str().unwrap()),
             cpus,
             gpus,
             mem,
             disk,
             attributes
         }

     }

}

#[derive(Serialize)]
pub struct Capability {
    #[serde(rename = "type")]
    pub capability_type: String,
}

#[derive(Serialize)]
pub struct FrameworkInfo {
    pub user: String,
    pub name: String,
    pub capabilities: Vec<Capability>,
}

#[derive(Serialize)]
pub struct SubscribeType {
    pub framework_info: FrameworkInfo,
}

#[derive(Serialize)]
pub struct SubscribeRequest {
    #[serde(rename = "type")]
    pub message_type: String,
    pub subscribe: SubscribeType,
}

#[derive(Deserialize, Debug)]
pub enum Type {
    #[serde(rename = "UNKNOWN")]
    Unknown,
    #[serde(rename = "SUBSCRIBED")]
    Subscribed,
    #[serde(rename = "OFFERS")]
    Offers,
    #[serde(rename = "INVERSE_OFFERS")]
    InverseOffers,
    #[serde(rename = "RESCIND")]
    Rescind,
    #[serde(rename = "RESCIND_INVERSE_OFFER")]
    RescindInverseOffer,
    #[serde(rename = "UPDATE")]
    Update,
    #[serde(rename = "OFFER_OPERATION_UPDATE")]
    OfferOperationUpdate,
    #[serde(rename = "MESSAGE")]
    Message,
    #[serde(rename = "FAILURE")]
    Failure,
    #[serde(rename = "ERROR")]
    Error,
}

#[derive(Deserialize, Serialize)]
pub struct FrameworkID {
    pub value: String,
}

#[derive(Deserialize)]
pub struct Subscribed {
    #[serde(rename = "framework_id")]
    pub framework_id: FrameworkID,
    pub heartbeat_interval_seconds: i32,
}

#[derive(Deserialize)]
pub struct Event {
    #[serde(rename = "type")]
    event_type: Type,
}

#[derive(Serialize)]
pub struct ValueContainer {
    pub value: String,
}

#[derive(Serialize)]
pub struct Launch {
    task_infos: Vec<TaskInfo>
}

#[derive(Serialize)]
pub struct Accept {
    offer_ids: Vec<ValueContainer>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    operations: Vec<Operation>,
}

#[derive(Serialize)]
pub struct Operation {
    #[serde(rename = "type")]
    operation_type: OperationType,
    launch: Launch,
}

#[derive(Serialize, Debug)]
pub enum CallType {
    #[serde(rename = "UNKNOWN")]
    Unknown,
    #[serde(rename = "SUBSCRIBE")]
    Subscribe,
    #[serde(rename = "TEARDOWN")]
    Teardown,
    #[serde(rename = "ACCEPT")]
    Accept,
    #[serde(rename = "DECLINE")]
    Decline,
    #[serde(rename = "ACCEPT_INVERSE_OFFERS")]
    AcceptInverseOffers,
    #[serde(rename = "DECLINE_INVERSE_OFFERS")]
    DeclineInverseOffers,
    #[serde(rename = "REVIVE")]
    Revive,
    #[serde(rename = "KILL")]
    Kill,
    #[serde(rename = "SHUTDOWN")]
    Shutdown,
    #[serde(rename = "ACKNOWLEDGE")]
    Acknowledge,
    #[serde(rename = "ACKNOWLEDGE_OFFER_OPERATION_UPDATE")]
    AcknowledgeOfferOperationUpdate,
    #[serde(rename = "RECONCILE")]
    Reconcile,
    #[serde(rename = "RECONCILE_OFFER_OPERATIONS")]
    ReconcileOfferOperations,
    #[serde(rename = "MESSAGE")]
    Message,
    #[serde(rename = "REQUEST")]
    Request,
    #[serde(rename = "SUPPRESS")]
    Suppress
}

#[derive(Serialize)]
pub struct Call {
    #[serde(rename = "type")]
    message_type: CallType,
    framework_id: FrameworkID,
    accept: Accept,
}

#[derive(Serialize)]
pub struct TeardownCall {
    #[serde(rename = "type")]
    message_type: CallType,
    framework_id: FrameworkID
}

#[derive(Serialize)]
pub struct Acknowledge {
    agent_id: ValueContainer,
    task_id: ValueContainer,
    uuid: String
}

#[derive(Serialize)]
pub struct AcknowledgeCall {
    #[serde(rename = "type")]
    message_type: CallType,
    framework_id: FrameworkID,
    acknowledge: Acknowledge
}

#[derive(Serialize)]
pub struct ReadFileRequestData {
    pub length: u32,
    pub offset: u32,
    pub path: String
}

#[derive(Deserialize)]
pub struct ReadFileResponseData {
    pub size: u32,
    pub data: String
}

#[derive(Serialize)]
pub struct ReadFileRequest {
    #[serde(rename = "type")]
    pub request_type: String,
    pub read_file: ReadFileRequestData
}

#[derive(Deserialize)]
pub struct ReadFileResponse {
    #[serde(rename = "type")]
    pub request_type: String,
    pub read_file: ReadFileResponseData
}

#[derive(Serialize)]
pub struct AttachContainerOutput {
    pub container_id: ValueContainer
}

#[derive(Serialize)]
pub struct AttachContainerOutputRequest {
    #[serde(rename = "type")]
    pub request_type: String,
    pub attach_container_output: AttachContainerOutput
}

#[derive(Deserialize)]
pub struct ContainerOutputData {
    #[serde(rename = "type")]
    pub output_type: String,
    pub data: String
}

#[derive(Deserialize)]
pub struct AttachContainerOutputMessage {
    #[serde(rename = "type")]
    pub message_type: String,
    pub data: ContainerOutputData
}

fn build_resources(task_info: &RequestedTaskInfo) -> Vec<Resource> {
    let mut resources = vec![

        Resource {
            name: String::from("cpus"),
            resource_type: String::from("SCALAR"),
            scalar: Scalar { value: task_info.cpus }
        },
        Resource {
            name: String::from("mem"),
            resource_type: String::from("SCALAR"),
            scalar: Scalar { value: task_info.mem }
        }
    ];

    match task_info.disk {
        0.0 => {},
        _ => {
            resources.push(

                Resource {
                    name: String::from("disk"),
                    resource_type: String::from("SCALAR"),
                    scalar: Scalar { value: task_info.disk }
                }

            )

        }

    }

    match task_info.gpus {
        0 => {},
        _ => {
            resources.push(

                Resource {
                    name: String::from("gpus"),
                    resource_type: String::from("SCALAR"),
                    scalar: Scalar { value: task_info.gpus as f32 }
                }

            )

        }

    }

    resources
}

fn split(input: String) -> Vec<String> {
    let mut vec: Vec<String> = vec![];

    for split in input.split_whitespace() {
        vec.push(String::from(split));
    }

    vec
}

fn get_argument_value(input: &str) -> String {
    String::from(&*split(String::from(input))[0])
}

fn get_arguments(input: &str) -> Vec<String> {
    split(String::from(input))[1..].to_vec()
}

pub fn accept_request<'a, 'b: 'a>(framework_id: &'a str, offer_id: &'a str, agent_id: &'a str, task_id: &'a str, task_info: &'b RequestedTaskInfo, tty_mode: &TTYMode) -> Call {
    let env_args = task_info.env_args.clone();

    let mut env_vars: Vec<Variable> = vec![];
    let environment: Option<Environment>;

    if task_info.env_args.len() > 0 {

        for (key, value) in env_args {

            env_vars.push(Variable {
                name: key,
                value: value
            });

        }

        environment = Some(Environment {
            variables: env_vars
        });

    } else {
        environment = None;
    }

    Call {
        message_type: CallType::Accept,
        framework_id: FrameworkID { value: String::from(framework_id) },
        accept: Accept {
            offer_ids: vec![ValueContainer {
                value: String::from(offer_id),
            }],
            operations: vec![
                Operation {
                    operation_type: OperationType::Launch,
                    launch: Launch {
                        task_infos: vec![
                            TaskInfo {
                                name: String::from("rexe-command"),
                                task_id: ValueContainer { value: String::from(task_id) },
                                agent_id: ValueContainer { value: String::from(agent_id) },
                                container: match task_info.executor.as_str() {
                                    "docker" => Some(ContainerInfo {
                                        container_type: ContainerInfoType::Docker,
                                        volumes: vec![],
                                        docker: DockerInfo {
                                            image: match task_info.image_name {
                                                Some(ref image) => image.clone(),
                                                None => String::from("")
                                            },
                                            force_pull_image: task_info.force_pull,
                                            privileged: false,
                                            network: DockerInfoNetwork::Bridge,
                                            parameters: vec![],
                                            port_mappings: vec![]
                                        },
                                        tty_info: match *tty_mode {
                                            TTYMode::Headless => None,
                                            TTYMode::Interactive => {
                                                // We are going to grab the current window size to set as the tty size in Mesos.
                                                let terminal_size = terminal_size();

                                                if task_info.verbose_output {
                                                    println!("Interactive mode");
                                                }

                                                if let Some((Width(w), Height(h))) = terminal_size {

                                                    if task_info.verbose_output {
                                                        println!("Your terminal is {} cols wide and {} lines tall", w, h);
                                                    }

                                                    Some(TTYInfo {

                                                        window_size: WindowSize {
                                                            rows: h,
                                                            columns: w
                                                        }

                                                    })

                                                } else {

                                                    if task_info.verbose_output {
                                                        println!("Unable to get terminal size.  Using default of 120x40");
                                                    }

                                                    Some(TTYInfo {

                                                        window_size: WindowSize {
                                                            rows: 40,
                                                            columns: 120
                                                        }

                                                    })

                                                }

                                            }
                                        }
                                    }),
                                    _ => None
                                },
                                command: CommandInfo {
                                    value: match task_info.shell {
                                        true => {
                                            String::from(&*task_info.args)
                                        },
                                        false => {
                                            get_argument_value(&*task_info.args)
                                        }
                                    },
                                    arguments: match task_info.shell {
                                        true => vec![],
                                        false => get_arguments(&*task_info.args)
                                    },
                                    shell: task_info.shell,
                                    environment
                                },
                                resources: {
                                    build_resources(&task_info)
                                }
                            }
                        ]
                    }
                }
            ]
        }

    }

}

pub fn decline_request<'a>(framework_id: &'a str, offer_id: &'a str) -> Call {
    // Sending an accept message with no operations is the same as a decline.  Means less code.

    Call {
        message_type: CallType::Accept,
        framework_id: FrameworkID { value: String::from(framework_id) },
        accept: Accept {
            offer_ids: vec![ValueContainer {
                value: String::from(offer_id),
            }],
            operations: vec![]
        }
    }

}

pub fn teardown_request<'a>(framework_id: &'a str) -> TeardownCall {

    TeardownCall {
        message_type: CallType::Teardown,
        framework_id: FrameworkID { value: String::from(framework_id) },
    }
}

pub fn acknowledge_request(framework_id: &str, agent_id: &str, task_id: &str, uuid: &str) -> AcknowledgeCall {

    AcknowledgeCall {
        message_type: CallType::Acknowledge,
        framework_id: FrameworkID { value: String::from(framework_id) },
        acknowledge: Acknowledge {
            agent_id: ValueContainer { value: String::from(agent_id) },
            task_id: ValueContainer { value: String::from(task_id) },
            uuid: String::from(uuid)
        }

    }

}

