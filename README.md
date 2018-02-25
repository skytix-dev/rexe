# rexe
Remote Execution tool for Mesos.

RExe is a tool to synchronously execute jobs inside Mesos and output the contents of the job to STDOUT.

## Build Instructions
RExe if built with Rust so you will want to download the compiler for your OS (https://www.rust-lang.org/en-US/install.html).

Once checked out, simple build the project using Cargo as below:

`cargo build --release`

## Usage

rexe <MESOS_URL> <IMAGE> <OPTIONS> -- <COMMAND_ARGS>


Example:

`rexe 10.9.10.1:5050 ubuntu:latest -c 2 -m 1024 -a attribute=/pattern/ -a attribute2=value -e ENV_VAR=value --force-pull -- ls -la /`

```
Remote Executor 0.4.0
Marc D. <marc@skytix.com.au>
Synchronously execute tasks inside Mesos with STDOUT

USAGE:
    rexe.exe [FLAGS] [OPTIONS] <MESOS_MASTER> <IMAGE> [--] [ARGS]...

FLAGS:
        --force-pull    Force pull image
    -h, --help          Prints help information
        --stderr        Fetch STDERR as well
    -V, --version       Prints version information
        --verbose       Verbose output

OPTIONS:
    -a <attr>...             Match an agent's attribute with the given value or pattern.  RExe will AND all attributes
                             specified.  Eg. attribute=value or attribute=/value/ 
    -c, --cpus <#CPUS>       Specify the number of cpus required [default: 1]
    -d, --disk <DISK>        Specify the amount memory required
    -e <env>...              Environment variables to pass to container.  Eg. ENV_NAME=value
    -g <#GPUS>               Specify the number of GPUs required
    -m, --memory <MEMORY>    Specify the amount memory required [default: 256]

ARGS:
    <MESOS_MASTER>    Mesos master host:port
    <IMAGE>           Name of docker image
    <ARGS>...         Image arguments

```
