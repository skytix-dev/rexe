# rexe
Remote Execution tool for Mesos.

RExe is a tool to synchronously execute jobs inside Mesos and output the contents of the job to STDOUT.

## Build Instructions
RExe if built with Rust so you will want to download the compiler for your OS (https://www.rust-lang.org/en-US/install.html).

Once checked out, simple build the project using Cargo as below:

`cargo build --release`

## Usage

rexe <MESOS_URL> <OPTIONS> <COMMAND_ARGS>

Example:

`rexe 10.9.10.1:5050 -c 2 -m 1024 -a attribute=/pattern/ -a attribute2=value -e ENV_VAR=value -i ubuntu:latest --force-pull "ls -la /"`
