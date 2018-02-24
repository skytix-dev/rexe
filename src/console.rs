use types;

/**
 * Makes use of the Mesos Operator API to stream STDIN and STDOUT to the running container.
 */

pub trait Console {

}

pub struct HeadlessConsole {

}

pub struct InteractiveConsole {

}

impl Console for HeadlessConsole {

}

impl Console for InteractiveConsole {

}

impl HeadlessConsole {

    pub fn new(mesos_host: &str) -> HeadlessConsole {

        HeadlessConsole {

        }

    }

}

impl InteractiveConsole {

    pub fn new(mesos_host: &str) -> InteractiveConsole {

        InteractiveConsole {

        }

    }

}

pub fn new<'a>(mesos_host: &'a str, task_info: &'a types::RequestedTaskInfo) -> Box<Console> {

    match task_info.tty_mode {
        types::TTYMode::Headless => Box::new(HeadlessConsole::new(mesos_host)),
        types::TTYMode::Interactive => Box::new(InteractiveConsole::new(mesos_host))
    }

}