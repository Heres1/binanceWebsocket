use std::process::Command;

fn main() {
    // 获取 git 短哈希
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output();

    let git_hash = match output {
        Ok(o) if o.status.success() => String::from_utf8(o.stdout)
            .unwrap_or_default()
            .trim()
            .to_string(),
        _ => "unknown".to_string(),
    };

    println!("cargo:rustc-env=GIT_HASH={}", git_hash);

    // git变化时重新运行
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/");
}
