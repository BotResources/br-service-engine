use std::process::Command;

pub fn send_signal(pid: u32, name: &str) {
    let status = Command::new("kill")
        .arg(format!("-{name}"))
        .arg(pid.to_string())
        .status()
        .expect("kill is available");
    assert!(status.success(), "kill -{name} {pid} was refused");
}
