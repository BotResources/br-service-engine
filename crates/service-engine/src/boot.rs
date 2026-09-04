use br_util_axum_readiness::ReadinessHandle;
use sqlx::{PgPool, Row};

use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::schema::SCHEMA;
use crate::transport::PgListenNotify;
use crate::transport::probe::{ListenerProbe, POOLER_REASON};

pub const REASON_POSTURE: &str = "verifying the PostgreSQL boot posture";
pub const REASON_POSTURE_REFUSED: &str = "the PostgreSQL boot posture was refused: the engine \
                                          must run under the low-privilege application role";
pub const REASON_LISTEN: &str = "establishing the impact listener";
pub const REASON_LISTEN_FAILED: &str = "the impact listener could not be established";
pub const REASON_MIRRORS: &str = "waiting for the registered mirrors to converge";

pub async fn establish_transport(
    pool: PgPool,
    config: &EngineConfig,
    readiness: &ReadinessHandle,
) -> Result<PgListenNotify, EngineError> {
    establish_transport_with_probe(pool, config, readiness, ListenerProbe::new()).await
}

pub async fn establish_transport_with_probe(
    pool: PgPool,
    config: &EngineConfig,
    readiness: &ReadinessHandle,
    probe: ListenerProbe,
) -> Result<PgListenNotify, EngineError> {
    readiness.set_not_ready(REASON_POSTURE);
    if let Err(e) = assert_posture(&pool).await {
        tracing::error!(error = %e, "the PostgreSQL boot posture was refused");
        readiness.set_not_ready(REASON_POSTURE_REFUSED);
        return Err(e);
    }
    readiness.set_not_ready(REASON_LISTEN);
    match PgListenNotify::connect_with_probe(pool, config, probe).await {
        Ok(transport) => {
            readiness.set_not_ready(REASON_MIRRORS);
            Ok(transport)
        }
        Err(e) => {
            tracing::error!(error = %e, "the impact listener could not be established");
            readiness.set_not_ready(listener_reason(&e));
            Err(e)
        }
    }
}

pub fn listener_reason(error: &EngineError) -> &'static str {
    match error {
        EngineError::ProbeTimeout { .. } => POOLER_REASON,
        _ => REASON_LISTEN_FAILED,
    }
}

pub async fn assert_posture(pool: &PgPool) -> Result<(), EngineError> {
    let row = sqlx::query(
        "SELECT r.rolsuper, r.rolbypassrls, \
                pg_has_role(r.oid, d.datdba, 'MEMBER') AS owns_database, \
                (SELECT pg_has_role(r.oid, n.nspowner, 'MEMBER') \
                 FROM pg_namespace n WHERE n.nspname = $1) AS owns_engine_schema \
         FROM pg_roles r, pg_database d \
         WHERE r.rolname = current_user AND d.datname = current_database()",
    )
    .bind(SCHEMA)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| EngineError::Posture("the runtime role is absent from pg_roles".to_string()))?;

    if row.get::<bool, _>("rolsuper") {
        return Err(refused("is a superuser"));
    }
    if row.get::<bool, _>("rolbypassrls") {
        return Err(refused(
            "carries rolbypassrls, so every RLS window would be a lie",
        ));
    }
    match row.get::<Option<bool>, _>("owns_engine_schema") {
        None => {
            return Err(EngineError::Posture(format!(
                "schema {SCHEMA} is absent: the engine migration set has not been applied by the \
                 owner role"
            )));
        }
        Some(true) => {
            return Err(refused(&format!(
                "owns schema {SCHEMA}, or may assume the role that does"
            )));
        }
        Some(false) => {}
    }
    if row.get::<bool, _>("owns_database") {
        return Err(refused(
            "owns this database, or may assume the role that does",
        ));
    }
    Ok(())
}

fn refused(what: &str) -> EngineError {
    EngineError::Posture(format!(
        "the runtime role {what}; the engine runs under the low-privilege app role, never the \
         owner"
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_probe_timeout_names_the_pooler_as_the_reason_readiness_stays_down() {
        let reason = listener_reason(&EngineError::ProbeTimeout {
            probe: "listener",
            timeout: Duration::from_secs(2),
        });
        assert!(reason.contains("listener"));
        assert!(reason.contains("pooler"));
    }

    #[test]
    fn any_other_listener_failure_reports_itself_rather_than_blaming_a_pooler() {
        let reason = listener_reason(&EngineError::Posture("nope".into()));
        assert!(!reason.contains("pooler"));
    }

    #[test]
    fn no_readiness_reason_carries_a_lower_layer_error_or_a_connection_detail() {
        let secret = "hunter2";
        let leaky = EngineError::Posture(format!(
            "postgresql://someone:{secret}@10.0.0.4:5432/engine_db is wrong"
        ));
        for reason in [
            REASON_POSTURE,
            REASON_POSTURE_REFUSED,
            REASON_LISTEN,
            REASON_MIRRORS,
            listener_reason(&leaky),
            listener_reason(&EngineError::ProbeTimeout {
                probe: "listener",
                timeout: Duration::from_secs(2),
            }),
        ] {
            assert!(!reason.contains("hunter2"));
            assert!(!reason.contains("postgresql://"));
            assert!(!reason.contains("10.0.0.4"));
        }
    }
}
