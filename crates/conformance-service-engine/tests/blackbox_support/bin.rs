use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const READY_TIMEOUT: Duration = Duration::from_secs(60);

pub fn example_service_bin() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| locate_or_build("example-service", "example-service", "EXAMPLE_SERVICE_BIN"))
        .clone()
}

pub fn example_twin_bin() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| locate_or_build("example-twin", "example-twin", "EXAMPLE_TWIN_BIN"))
        .clone()
}

fn target_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        return PathBuf::from(dir);
    }
    let exe = std::env::current_exe().expect("the test executable has a path");
    exe.parent()
        .and_then(|deps| deps.parent())
        .and_then(|profile| profile.parent())
        .expect("the test executable lives under <target>/<profile>/deps")
        .to_path_buf()
}

fn locate_or_build(package: &str, bin: &str, env_var: &str) -> PathBuf {
    if let Ok(explicit) = std::env::var(env_var) {
        let path = PathBuf::from(explicit);
        assert!(
            path.exists(),
            "{env_var} points at a missing binary: {}",
            path.display()
        );
        return path;
    }
    let status = Command::new(env!("CARGO"))
        .args(["build", "--locked", "-p", package, "--bin", bin])
        .status()
        .expect("cargo builds the example binary for the black-box battery");
    assert!(status.success(), "building the {bin} binary failed");
    let path = target_dir().join("debug").join(bin);
    assert!(
        path.exists(),
        "the built {bin} binary is not at {}",
        path.display()
    );
    path
}

pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let port = listener.local_addr().expect("read the bound port").port();
    drop(listener);
    port
}

pub struct SpawnEnv {
    pub database_url: String,
    pub owner_database_url: String,
    pub app_role: String,
    pub nats_url: String,
    pub pod: String,
    pub port: u16,
    pub blobs: Option<BlobEnv>,
    pub session_bounds: Option<SessionBounds>,
}

pub struct SessionBounds {
    pub ttl: Duration,
    pub max_age: Duration,
}

pub struct BlobEnv {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

pub struct Spawned {
    child: Child,
    base_url: String,
    log: PathBuf,
}

impl Spawned {
    pub async fn service(env: SpawnEnv) -> Spawned {
        let log = std::env::temp_dir().join(format!("bb-{}-{}.log", env.pod, env.port));
        let out = std::fs::File::create(&log).expect("create the child log file");
        let err = out.try_clone().expect("clone the child log handle");

        let mut command = Command::new(example_service_bin());
        command
            .env("DATABASE_URL", &env.database_url)
            .env("DATABASE_URL_OWNER", &env.owner_database_url)
            .env("APP_ROLE", &env.app_role)
            .env("NATS_URL", &env.nats_url)
            .env("ENGINE_CHANNEL", "example")
            .env("POD_ID", &env.pod)
            .env("HTTP_ADDR", format!("127.0.0.1:{}", env.port))
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err));
        if let Some(blob) = &env.blobs {
            command
                .env("S3_ENDPOINT", &blob.endpoint)
                .env("S3_BUCKET", &blob.bucket)
                .env("S3_ACCESS_KEY", &blob.access_key)
                .env("S3_SECRET_KEY", &blob.secret_key)
                .env("S3_REGION", &blob.region);
        }
        if let Some(bounds) = &env.session_bounds {
            command
                .env("SESSION_TTL_MS", bounds.ttl.as_millis().to_string())
                .env("SESSION_MAX_AGE_MS", bounds.max_age.as_millis().to_string());
        }

        let child = command.spawn().expect("spawn the example-service binary");
        let base_url = format!("http://127.0.0.1:{}", env.port);
        let mut spawned = Spawned {
            child,
            base_url,
            log,
        };
        spawned.wait_ready().await;
        spawned
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn wait_ready(&mut self) {
        let http = reqwest::Client::new();
        let readyz = format!("{}/readyz", self.base_url);
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().expect("read the child process state") {
                panic!(
                    "the example-service binary exited during boot with {status}:\n{}",
                    self.tail_log()
                );
            }
            if let Ok(response) = http.get(&readyz).send().await
                && response.status().as_u16() == 200
            {
                return;
            }
            if Instant::now() >= deadline {
                panic!(
                    "the example-service binary never answered 200 on /readyz within {:?}:\n{}",
                    READY_TIMEOUT,
                    self.tail_log()
                );
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }

    fn tail_log(&self) -> String {
        std::fs::read_to_string(&self.log)
            .map(|text| {
                text.lines()
                    .rev()
                    .take(40)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default()
    }

    pub async fn shutdown(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.log);
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.log);
    }
}

pub struct TwinHandle {
    child: Child,
}

impl Drop for TwinHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn spawn_twin(nats_url: &str, board_id: uuid::Uuid) -> TwinHandle {
    let child = Command::new(example_twin_bin())
        .env("NATS_URL", nats_url)
        .env("BOARD_ID", board_id.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the example-twin binary");
    TwinHandle { child }
}
