extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate tokio_core;
extern crate regex;
extern crate serde;
extern crate serde_json;
extern crate ctrlc;
extern crate rand;
extern crate base64;

#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate hyper;

mod scheduler;
mod types;

use clap::{Arg, App, ArgMatches};
use types::RequestedTaskInfo;

fn generate_task_info<'a>(ref matches: &'a ArgMatches) -> RequestedTaskInfo {
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
        Some(value) => match value.parse::<f32>(){
            Ok(parsed_value) => parsed_value,
            Err(e) => {
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

    RequestedTaskInfo {
        image_name,
        cpus,
        gpus,
        mem,
        disk,
        args,
    }
    
}

fn main() {
    let logger = env_logger::init();

    if logger.is_ok() {

        let matches = App::new("Remote Executor")
            .version("1.0")
            .author("Marc D. <xfiremd@live.com>")
            .about("Synchronously execute tasks under Mesos")
            .arg(Arg::with_name("mesos")
                .short("m")
                .value_name("MESOS HOST")
                .required(true)
                .help("Mesos master host:port")
                .takes_value(true))
            .arg(Arg::with_name("cpus")
                .short("c")
                .value_name("#CPUS")
                .required(true)
                .help("Specify the number of cpus required")
                .takes_value(true))
            .arg(Arg::with_name("mem")
                .short("M")
                .value_name("MEMORY")
                .required(true)
                .help("Specify the amount memory required")
                .takes_value(true))
            .arg(Arg::with_name("disk")
                .short("d")
                .value_name("DISK")
                .required(false)
                .help("Specify the amount memory required")
                .takes_value(true))
            .arg(Arg::with_name("gpus")
                .short("g")
                .value_name("#GPUS")
                .required(false)
                .help("Specify the number of GPUs required")
                .takes_value(true))
            .arg(Arg::with_name("IMAGE")
                .help("Name of docker image")
                .required(true)
                .index(1)
            )
            .arg(Arg::with_name("ARGS")
                .help("Image arguments")
                .index(2)
                .required(false)
                .multiple(true)
            )
            .get_matches();

        let mesos_master = matches.value_of("mesos").unwrap();
        let task_info = generate_task_info(&matches);

        println!("Executing task {}", mesos_master);

        scheduler::execute(&mesos_master, &task_info);


    } else {
        error!("Unable to initialise logger");
    }

}