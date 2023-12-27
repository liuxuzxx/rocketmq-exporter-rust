use self::command::Command;

pub mod command;

pub fn broker_information_command() {
    let command = Command::new(28, String::from(""));
    command.encode();
}
