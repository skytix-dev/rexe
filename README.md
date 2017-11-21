# rexe
Remote Execution tool for Mesos.

RExe is a tool to synchronously execute jobs inside Mesos and output the contents of the job to STDOUT.

## Build Instructions
RExe if built with Rust so you will want to download the compiler for your OS (https://www.rust-lang.org/en-US/install.html).

Once checked out, simple build the project using Cargo as below:

`cargo build --release`

## Usage

rexe -m <mesos_url> -i <docker image> -c <#cpus) -M <#memory> -d <#disk> -e <ENV_NAME>:<ENV_VALUE> <ARGS>

Example:

`rexe -m "localhost:5050" -i busybox -c 1 -m 128 echo 'Hello World!'`
