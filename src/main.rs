extern crate clap;
extern crate env_logger;
extern crate regex;
extern crate serde;
extern crate serde_json;
extern crate ctrlc;
extern crate rand;
extern crate base64;
extern crate reqwest;
extern crate terminal_size;

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate hyper;

mod scheduler;
mod console;
mod types;

use clap::{Arg, App, ArgMatches};
use types::RequestedTaskInfo;
use std::collections::HashMap;
use regex::Regex;

fn generate_task_info<'a>(ref matches: &'a ArgMatches) -> RequestedTaskInfo {
    let verbose_output: bool = matches.occurrences_of("verbose") > 0;
    let cpus_param = matches.value_of("cpus").unwrap().parse::<f32>();
    let cpus: f32;

    let image_name = String::from(matches.value_of("IMAGE").unwrap());

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

    RequestedTaskInfo {
        image_name,
        cpus,
        gpus,
        mem,
        disk,
        args,
        env_args: env_options,
        verbose_output,
        tty_mode,
        attrs,
        force_pull: matches.occurrences_of("force_pull") > 0
    }
}

fn main() {
    let logger = env_logger::init();

    if logger.is_ok() {
        let matches = App::new("Remote Executor")
            .version("1.0")
            .author("Marc D. <marc@skytix.com.au>")
            .about("Synchronously execute tasks inside Mesos with STDOUT")
            .arg(Arg::with_name("mesos")
                .required(true)
                .help("Mesos master host:port")
                .index(1)
                .takes_value(true))
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
                .help("Specify the number of cpus required.  Default: 1")
                .takes_value(true))
            .arg(Arg::with_name("mem")
                .short("m")
                .long("memory")
                .value_name("MEMORY")
                .help("Specify the amount memory required. Default: 256")
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
                .help("Environment variables")
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
            .arg(Arg::with_name("verbose")
                .short("v")
                .required(false)
                .multiple(true)
                .help("Verbose output")
            )
            .arg(Arg::with_name("IMAGE")
                .short("i")
                .help("Name of docker image")
                .required(true)
                .takes_value(true)
            )
            .arg(
                Arg::with_name("interactive")
                .short("I")
                .help("Use interactive mode. STDIN will be redirected to container")
                .required(false)
                .multiple(false)
            )
            .arg(Arg::with_name("ARGS")
                .help("Image arguments")
                .index(2)
                .required(false)
            )
            .get_matches();

        let mesos_master = matches.value_of("mesos").unwrap();
        let task_info = generate_task_info(&matches);

        if task_info.verbose_output {
            println!("Executing task {}", mesos_master);
        }

        scheduler::execute(
            &*console::new(&mesos_master, &task_info),
            &mesos_master,
            &task_info
        );

    } else {
        error!("Unable to initialise logger");
    }

}