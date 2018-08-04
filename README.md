# rexe
Remote Execution tool for Mesos.

RExe is a tool to synchronously execute jobs inside Mesos and output the contents of the job to STDOUT.

## Build Instructions
RExe if built with Rust so you will want to download the compiler for your OS (https://www.rust-lang.org/en-US/install.html).

Once checked out, simply build the project using Cargo as below:

`cargo build --release`

## Usage

rexe <MESOS_URL> <IMAGE> <OPTIONS> -- <COMMAND_ARGS>


Example:

`rexe 10.9.10.1:2181/mesos docker ubuntu:latest -c 2 -m 1024 -a attribute=/pattern/ -a attribute2=value -e ENV_VAR=value -v /mnt/storage:/storage:RW --force-pull -- ls -la /`
`rexe 10.9.10.1:2181/mesos exec -c 2 -m 1024 -a attribute=/pattern/ -a attribute2=value -e ENV_VAR=value -- ls -la /`


```
RRemote Executor 0.7.4
 Marc Dergacz. <marc@skytix.com.au>
 Synchronously execute tasks inside Mesos with STDOUT
 
 USAGE:
     rexe.exe [FLAGS] [OPTIONS] <MESOS_MASTER> <EXECUTOR> [IMAGE] [-- <ARGS>...]
 
 FLAGS:
         --force-pull    Force pull image
     -h, --help          Prints help information
     -s                  Invoke with shell mode on CommandInfo.  Always enabled when executor is 'exec'.
         --stderr        Fetch STDERR as well
     -t                  Attach TTY
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
     -T <timeout>             Resource wait timeout. Time in seconds on how long RExe should wait for usable resource
                              offers before giving up. Default: 60.  Set to <= 0 to wait indefinitely.
     -v <volume>...           Volume mapping - host_path:container_path:[RO|RW].  Eg.  /host/path:/container/path:RO
                              Defaults to RW access.
 
 ARGS:
     <MESOS_MASTER>    Mesos master/zookeeper URL.  RExe will perform leader discovery if provided a zookeeper URL
                       otherwise http[s] can be provided.  Eg. master1:2181,master2:2181,master3:2181/mesos or
                       http://master1:5050
     <EXECUTOR>        Mesos executor to use [possible values: docker, exec]
     <IMAGE>           Name of docker image
     <ARGS>...         Image arguments

```
