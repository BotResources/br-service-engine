use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use br_util_nats_fabric::{Fabric, INTEGRATION_CMD, INTEGRATION_EVT, KV_PUBLISHED_LANGUAGE};
use uuid::Uuid;

const BOOT_TIMEOUT: Duration = Duration::from_secs(20);
const SPAWN_ATTEMPTS: usize = 5;
pub const DUPLICATE_WINDOW: Duration = Duration::from_secs(120);

pub struct TestNats {
    child: Child,
    port: u16,
    store: PathBuf,
    name: String,
}

impl TestNats {
    pub async fn spawn() -> Self {
        for _ in 0..SPAWN_ATTEMPTS {
            if let Some(server) = Self::try_spawn().await {
                return server;
            }
        }
        panic!(
            "nats-server did not come up on any of {SPAWN_ATTEMPTS} freshly picked ports; \
             a broker that lost a port race must never be mistaken for someone else's"
        );
    }

    async fn try_spawn() -> Option<Self> {
        let port = free_port();
        let name = format!("se-nats-{}", Uuid::now_v7().simple());
        let store = std::env::temp_dir().join(&name);
        std::fs::create_dir_all(&store).expect("create the ephemeral JetStream store");

        let child = Command::new("nats-server")
            .args([
                "-js",
                "-a",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-n",
                &name,
                "-sd",
                store.to_str().expect("a utf-8 store path"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("nats-server must be on PATH for the conformance battery");

        let mut server = Self {
            child,
            port,
            store,
            name,
        };
        server.await_ready().await.then_some(server)
    }

    pub fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    pub async fn fabric(&self) -> Fabric {
        Fabric::connect(&self.url())
            .await
            .expect("the fabric dials the ephemeral broker")
    }

    pub async fn provision(&self) {
        let js = self.jetstream().await;
        for (name, subject) in [
            (INTEGRATION_CMD, "integration.cmd.>"),
            (INTEGRATION_EVT, "integration.evt.>"),
        ] {
            js.create_stream(async_nats::jetstream::stream::Config {
                name: name.to_string(),
                subjects: vec![subject.to_string()],
                duplicate_window: DUPLICATE_WINDOW,
                ..Default::default()
            })
            .await
            .unwrap_or_else(|e| panic!("declare the {name} stream: {e}"));
        }
        js.create_key_value(async_nats::jetstream::kv::Config {
            bucket: KV_PUBLISHED_LANGUAGE.to_string(),
            history: 1,
            ..Default::default()
        })
        .await
        .expect("declare the PUBLISHED_LANGUAGE bucket");
    }

    async fn jetstream(&self) -> async_nats::jetstream::Context {
        let client = async_nats::connect(&self.url())
            .await
            .expect("connect to the ephemeral broker");
        async_nats::jetstream::new(client)
    }

    async fn await_ready(&mut self) -> bool {
        let deadline = Instant::now() + BOOT_TIMEOUT;
        loop {
            if self.exited() {
                return false;
            }
            if let Ok(client) = async_nats::connect(&self.url()).await {
                return client.server_info().server_name == self.name;
            }
            if Instant::now() >= deadline {
                panic!("nats-server did not accept connections on {}", self.url());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn exited(&mut self) -> bool {
        self.child
            .try_wait()
            .expect("read the nats-server process state")
            .is_some()
    }
}

impl Drop for TestNats {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.store);
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let port = listener.local_addr().expect("read the bound port").port();
    drop(listener);
    port
}
