use serde_json::{Value};
use std::collections::HashMap;

pub struct RequestedTaskInfo {
    pub image_name: String,
    pub cpus: f32,
    pub gpus: i32,
    pub mem: f32,
    pub disk: f32,
    pub args: String,
    pub env_args: HashMap<String, String>,
    pub verbose_output: bool
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
pub struct ContainerInfo {
    #[serde(rename = "type")]
    container_type: ContainerInfoType,
    docker: DockerInfo,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    volumes: Vec<Volume>
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
    container: ContainerInfo,
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

                 cpus = resource_map.get("cpus").unwrap().parse::<f32>().unwrap();

                 gpus = match resource_map.get("gpus") {
                     Some(value) => value.parse::<i32>().unwrap(),
                     None => 0
                 };

                 mem = resource_map.get("mem").unwrap().parse::<f32>().unwrap();
                 disk = resource_map.get("disk").unwrap().parse::<f32>().unwrap();
             },
             None => {
                 // Set everything to be be 0 I guess?
                 cpus = 0.0;
                 gpus = 0;
                 mem = 0.0;
                 disk = 0.0;
             }
         };

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
    value: String,
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

pub fn accept_request<'a, 'b: 'a>(framework_id: &'a str, offer_id: &'a str, agent_id: &'a str, task_id: &'a str, task_info: &'b RequestedTaskInfo) -> Call {
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
                                container: {
                                    ContainerInfo {
                                        container_type: ContainerInfoType::Docker,
                                        volumes: vec![],
                                        docker: DockerInfo {
                                            image: String::from(&*task_info.image_name),
                                            force_pull_image: true,
                                            privileged: false,
                                            network: DockerInfoNetwork::Bridge,
                                            parameters: vec![],
                                            port_mappings: vec![]
                                        }
                                    }
                                },
                                command: CommandInfo {
                                    value: get_argument_value(&*task_info.args),
                                    arguments: get_arguments(&*task_info.args),
                                    shell: false,
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

