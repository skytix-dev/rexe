extern crate base64;
extern crate clap;
extern crate ctrlc;
extern crate env_logger;
#[macro_use]
extern crate hyper;
#[macro_use]
extern crate log;
extern crate rand;
extern crate regex;
extern crate reqwest;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate terminal_size;
extern crate timer;
extern crate chrono;
extern crate strum;
#[macro_use]
extern crate strum_macros; // 0.10.0

use clap::{App, Arg, ArgMatches};
use regex::Regex;
use std::collections::HashMap;
use types::RequestedTaskInfo;

mod scheduler;
mod console;
mod types;
mod network;
mod mesos;

fn generate_task_info<'a>(ref matches: &'a ArgMatches) -> RequestedTaskInfo {
    let executor: String = String::from(matches.value_of("executor").unwrap());
    let verbose_output: bool = matches.occurrences_of("verbose") > 0;
    let stderr: bool = matches.occurrences_of("stderr") > 0;
    let cpus_param = matches.value_of("cpus").unwrap().parse::<f32>();

    let shell: bool = match executor.as_str() {
        "exec" => true,
        _ => matches.occurrences_of("shell") > 0
    };

    let cpus: f32;

    let image_name = matches.value_of("IMAGE");

    if cpus_param.is_ok() {
        cpus = cpus_param.unwrap();

        if cpus <= 0.0 {
            error!("Number of CPUs required must be greater than 0");
            std::process::exit(1);
        }

    } else {
        error!("Number of CPUs specified is not a valid number");
        std::process::exit(1);
    }

    let mem_param = matches.value_of("mem").unwrap().parse::<f32>();
    let mem: f32;

    if mem_param.is_ok() {
        mem = mem_param.unwrap();

        if mem <= 0.0 {
            error!("Memory specified must be greater than 0");
            std::process::exit(1);
        }
    } else {
        error!("Number of CPUs specified is not a valid number");
        std::process::exit(1);
    }


    let disk: f32 = match matches.value_of("disk") {
        Some(value) => match value.parse::<f32>() {
            Ok(parsed_value) => parsed_value,
            Err(_) => {
                error!("Disk size specified is not a valid number");
                std::process::exit(1);
            }
        },
        None => 0.0,
    };

    let gpus_param = matches.value_of("gpus");
    let gpus: i32;

    if gpus_param.is_some() {
        let value = gpus_param.unwrap().parse::<i32>();

        if value.is_ok() {
            gpus = value.unwrap();

            if gpus <= 0 {
                error!("Number of GPUs required must be greater than 0");
                std::process::exit(1);
            }
        } else {
            error!("Number of GPUs specified is not a valid number");
            std::process::exit(1);
        }
    } else {
        gpus = 0;
    }

    let args: String;

    if matches.is_present("ARGS") {
        let args_param = matches.values_of("ARGS");
        let args_list: Vec<_> = args_param.unwrap().collect();

        let mut concat_list: String = String::from("");

        for arg in args_list {
            concat_list.push_str(arg);
            concat_list.push_str(" ");
        }

        args = concat_list;

    } else {
        args = String::from("");
    }

    let mut env_options: HashMap<String, String> = HashMap::new();
    let attr_regex = Regex::new(r"^(.+?)=(.+?)$").unwrap();

    if matches.is_present("env") {
        let args_list: Vec<_> = matches.values_of("env").unwrap().collect();

        for arg in args_list {

            if attr_regex.is_match(arg) {
                let groups = attr_regex.captures(arg).unwrap();

                env_options.insert(
                    String::from(groups.get(1).unwrap().as_str()),
                    String::from(groups.get(2).unwrap().as_str())
                );

            }

        }

    }

    let mut volumes: Vec<(String, String, Option<String>)> = vec![];

    if matches.is_present("volume") {
        let volume_defs: Vec<_> = matches.values_of("volume").unwrap().collect();

        for def in volume_defs {
            let parts: Vec<&str> = def.split(":").collect();

            match parts.len() {
                2 => volumes.push((String::from(parts[0]), String::from(parts[1]), None)),
                3 => {
                    let mode = parts[2].to_uppercase();

                    if mode == "RO" || mode == "RW" {
                        volumes.push((String::from(parts[0]), String::from(parts[1]), Some(String::from(mode))));

                    } else {
                        error!("{} is not a valid VolumeMode", mode);
                        std::process::exit(1);
                    }
                },
                _ => {
                    error!("Too many volume parameters");
                    std::process::exit(1);
                }
            };

        }
    }

    let tty = matches.occurrences_of("tty") > 0;
    let tty_mode;

    if matches.occurrences_of("interactive") > 0 {
        tty_mode = types::TTYMode::Interactive;

    } else {
        tty_mode = types::TTYMode::Headless;
    }

    let mut attrs: HashMap<String, String> = HashMap::new();

    if matches.is_present("attr") {
        let attrs_list: Vec<_> = matches.values_of("attr").unwrap().collect();

        for arg in attrs_list {

            if attr_regex.is_match(arg) {
                let groups = attr_regex.captures(arg).unwrap();

                attrs.insert(
                    String::from(groups.get(1).unwrap().as_str()),
                    String::from(groups.get(2).unwrap().as_str())
                );

            }

        }

    }

    let mut timeout: i64 = match matches.value_of("timeout") {
        Some(value) => value.parse::<i64>().unwrap(),
        None => 60
    };

    RequestedTaskInfo {
        executor,
        image_name: match matches.value_of("IMAGE") {
            Some(image_name) => Some(String::from(image_name)),
            None => None
        },
        cpus,
        gpus,
        mem,
        disk,
        args,
        env_args: env_options,
        verbose_output,
        tty,
        tty_mode,
        attrs,
        volumes,
        force_pull: matches.occurrences_of("force_pull") > 0,
        stderr,
        shell,
        timeout
    }
}

fn main() {
    let logger = env_logger::init();

    if logger.is_ok() {
        let matches = App::new("Remote Executor")
            .version("0.7.4")
            .author("Marc Dergacz. <marc@skytix.com.au>")
            .about("Synchronously execute tasks inside Mesos with STDOUT")

            .arg(Arg::with_name("mesos")
                .required(true)
                .help("Mesos master/zookeeper URL.  RExe will perform leader discovery if provided a zookeeper URL otherwise http[s] can be provided.  Eg. master1:2181,master2:2181,master3:2181/mesos or http://master1:5050")
                .value_name("MESOS_MASTER")
                .index(1)
            )
            .arg(Arg::with_name("executor")
                .index(2)
                .help("Mesos executor to use")
                .possible_values(&["docker", "exec"])
                .value_name("EXECUTOR")
                .required(true)
            )
            .arg(Arg::with_name("IMAGE")
                .index(3)
                .help("Name of docker image")
                .required(false)
                .takes_value(true)
            )
            .arg(Arg::with_name("attr")
                .short("a")
                .required(false)
                .multiple(true)
                .help("Match an agent's attribute with the given value or pattern.  RExe will AND all attributes specified.  Eg. attribute=value or attribute=/value/ ")
                .takes_value(true))
            .arg(Arg::with_name("cpus")
                .short("c")
                .long("cpus")
                .value_name("#CPUS")
                .default_value("1")
                .help("Specify the number of cpus required")
                .takes_value(true))
            .arg(Arg::with_name("mem")
                .short("m")
                .long("memory")
                .value_name("MEMORY")
                .help("Specify the amount memory required")
                .default_value("256")
                .takes_value(true))
            .arg(Arg::with_name("disk")
                .short("d")
                .long("disk")
                .value_name("DISK")
                .required(false)
                .help("Specify the amount memory required")
                .takes_value(true))
            .arg(Arg::with_name("env")
                .short("e")
                .required(false)
                .multiple(true)
                .help("Environment variables to pass to container.  Eg. ENV_NAME=value")
                .takes_value(true))
            .arg(Arg::with_name("force_pull")
                .long("force-pull")
                .required(false)
                .help("Force pull image")
                .takes_value(false))
            .arg(Arg::with_name("gpus")
                .short("g")
                .value_name("#GPUS")
                .required(false)
                .help("Specify the number of GPUs required")
                .takes_value(true))
            .arg(Arg::with_name("tty")
                .short("t")
                .required(false)
                .help("Attach TTY")
                .takes_value(false))
            .arg(Arg::with_name("timeout")
                .short("T")
                .required(false)
                .help("Resource wait timeout. Time in seconds on how long RExe should wait for usable resource offers before giving up. Default: 60.  Set to <= 0 to wait indefinitely.")
                .takes_value(true))
            .arg(Arg::with_name("shell")
                .short("s")
                .required(false)
                .help("Invoke with shell mode on CommandInfo.  Always enabled when executor is 'exec'.")
                .takes_value(false))
            .arg(Arg::with_name("volume")
                .short("v")
                .required(false)
                .multiple(true)
                .help("Volume mapping - host_path:container_path:[RO|RW].  Eg.  /host/path:/container/path:RO  Defaults to RW access.")
                .takes_value(true))
            .arg(Arg::with_name("verbose")
                .long("verbose")
                .required(false)
                .help("Verbose output")
            )
            .arg(Arg::with_name("stderr")
                .long("stderr")
                .required(false)
                .help("Fetch STDERR as well")
            )
            .arg(Arg::with_name("ARGS")
                .help("Image arguments")
                .required(false)
                .multiple(true)
                .last(true)
            )
            .get_matches();

        let mesos_master = matches.value_of("mesos").unwrap();
        let task_info = generate_task_info(&matches);

        if task_info.verbose_output {
            println!("Executing task {}", mesos_master);
        }

        scheduler::execute(
            &mesos_master,
            &task_info
        );

    } else {
        error!("Unable to initialise logger");
    }

}