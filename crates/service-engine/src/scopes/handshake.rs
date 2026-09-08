use std::time::Duration;

use async_nats::Subscriber;
use br_core_integration::{
    Actor, CommandCoords, EventMetadata, IntegrationCommand, IntegrationEvent, ServiceAccountId,
};
use br_core_scope::{DeclareServiceScopes, ScopeDeclaration, ServiceKey, ServiceScopesRejected};
use br_scope_declaration_contract::{
    VERSION, accepted_event_coords, command_type, declare_command_coords, rejected_event_coords,
};
use chrono::Utc;
use futures_util::StreamExt;
use futures_util::stream::SelectAll;
use serde::Deserialize;
use uuid::Uuid;

use crate::nats::{INTEGRATION_EVT, Nats, event_subject};

use super::ScopeError;

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

const DECLARING_SERVICE_NAMESPACE: Uuid =
    Uuid::from_u128(0x6f3a_1c8e_4b27_4d59_9e10_a3f2_77c5_8d41);

pub(crate) async fn run_handshake(
    nats: &Nats,
    declaration: ScopeDeclaration,
) -> Result<(), ScopeError> {
    let service = declaration.manifest().key.clone();
    let subjects = ConfirmationSubjects::render();
    let command_subject = render_command_subject(&declare_command_coords().expect(CONTRACT));
    let correlation_id = Uuid::now_v7();
    let command = build_command(&service, correlation_id, declaration);

    let mut awaiter = ConfirmationAwaiter::open(nats, &subjects).await?;

    loop {
        publish_declaration(nats, &command_subject, &command, &service, correlation_id).await;
        match awaiter.await_correlation(correlation_id).await? {
            Some(matched) if matched.subject == subjects.accepted => {
                tracing::info!(
                    service = %service,
                    correlation_id = %correlation_id,
                    "scope declaration accepted by identity"
                );
                return Ok(());
            }
            Some(matched) => {
                if let Some(rejected) =
                    decode_rejection(&matched.payload, &service, &matched.subject)
                {
                    return Err(ScopeError::Rejected {
                        reason: format!("scope declaration rejected: {}", rejected.reason),
                    });
                }
            }
            None => tracing::info!(
                service = %service,
                correlation_id = %correlation_id,
                "no scope-declaration confirmation yet; re-publishing (identity may be down, \
                 readiness stays DOWN)"
            ),
        }
    }
}

struct ConfirmationSubjects {
    accepted: String,
    rejected: String,
}

impl ConfirmationSubjects {
    fn render() -> Self {
        Self {
            accepted: event_subject(&accepted_event_coords().expect(CONTRACT)),
            rejected: event_subject(&rejected_event_coords().expect(CONTRACT)),
        }
    }
}

struct ConfirmationAwaiter {
    messages: SelectAll<Subscriber>,
}

impl ConfirmationAwaiter {
    async fn open(nats: &Nats, subjects: &ConfirmationSubjects) -> Result<Self, ScopeError> {
        let context = nats.context();
        context
            .get_stream(INTEGRATION_EVT)
            .await
            .map_err(|_| ScopeError::Handshake(no_stream(INTEGRATION_EVT)))?;
        let mut messages = SelectAll::new();
        for subject in [subjects.accepted.clone(), subjects.rejected.clone()] {
            let subscriber = context
                .client()
                .subscribe(subject)
                .await
                .map_err(|error| ScopeError::Handshake(Box::new(error)))?;
            messages.push(subscriber);
        }
        Ok(Self { messages })
    }

    async fn await_correlation(
        &mut self,
        correlation_id: Uuid,
    ) -> Result<Option<CorrelatedMatch>, ScopeError> {
        let wait = tokio::time::sleep(WAIT_TIMEOUT);
        tokio::pin!(wait);
        loop {
            tokio::select! {
                () = &mut wait => return Ok(None),
                next = self.messages.next() => {
                    let Some(message) = next else {
                        return Err(ScopeError::Handshake(
                            "the confirmation subscription ended (NATS connection closed)".into(),
                        ));
                    };
                    let Ok(probe) = serde_json::from_slice::<CorrelationProbe>(&message.payload)
                    else {
                        continue;
                    };
                    if probe.metadata.correlation_id == correlation_id {
                        return Ok(Some(CorrelatedMatch {
                            subject: message.subject.as_str().to_string(),
                            payload: message.payload.to_vec(),
                        }));
                    }
                }
            }
        }
    }
}

struct CorrelatedMatch {
    subject: String,
    payload: Vec<u8>,
}

#[derive(Deserialize)]
struct CorrelationProbe {
    metadata: EventMetadata,
}

async fn publish_declaration(
    nats: &Nats,
    subject: &str,
    command: &IntegrationCommand<DeclareServiceScopes>,
    service: &ServiceKey,
    correlation_id: Uuid,
) {
    let bytes = match serde_json::to_vec(command) {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::error!(service = %service, %error, "encoding the scope declaration failed");
            return;
        }
    };
    let acked = async {
        nats.context()
            .publish(subject.to_string(), bytes.into())
            .await?
            .await
    }
    .await;
    if let Err(error) = acked {
        tracing::warn!(
            service = %service,
            correlation_id = %correlation_id,
            %error,
            "scope-declaration publish failed; will retry after the next wait"
        );
    }
}

fn decode_rejection(
    payload: &[u8],
    service: &ServiceKey,
    subject: &str,
) -> Option<ServiceScopesRejected> {
    match serde_json::from_slice::<IntegrationEvent<ServiceScopesRejected>>(payload) {
        Ok(event) => {
            tracing::error!(
                service = %service,
                reason_code = %event.payload.reason,
                "scope declaration rejected by identity; readiness DOWN, no retry"
            );
            Some(event.payload)
        }
        Err(error) => {
            tracing::error!(
                service = %service,
                subject = %subject,
                %error,
                "a scope-declaration rejection failed to decode; awaiting a well-formed \
                 confirmation, readiness stays DOWN"
            );
            None
        }
    }
}

fn build_command(
    service: &ServiceKey,
    correlation_id: Uuid,
    declaration: ScopeDeclaration,
) -> IntegrationCommand<DeclareServiceScopes> {
    IntegrationCommand::new(
        Uuid::now_v7(),
        command_type(),
        VERSION,
        Utc::now(),
        EventMetadata::new(declaring_actor(service), correlation_id),
        DeclareServiceScopes::new(declaration),
    )
}

fn declaring_actor(service: &ServiceKey) -> Actor {
    let id = Uuid::new_v5(&DECLARING_SERVICE_NAMESPACE, service.as_str().as_bytes());
    Actor::Service(ServiceAccountId::from(id))
}

fn render_command_subject(coords: &CommandCoords) -> String {
    format!(
        "integration.cmd.{}.{}.{}.v{}",
        coords.receiver.as_str(),
        coords.aggregate.as_str(),
        coords.verb.as_str(),
        coords.version
    )
}

const CONTRACT: &str = "the frozen scope-declaration contract renders valid coordinates";

fn no_stream(name: &str) -> crate::error::BoxedError {
    format!("stream {name} is absent; gitops declares it, the engine only binds it").into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_confirmation_subjects_are_the_two_frozen_event_subjects() {
        let subjects = ConfirmationSubjects::render();
        assert_eq!(
            subjects.accepted,
            "integration.evt.identity.service_scope.accepted.v1"
        );
        assert_eq!(
            subjects.rejected,
            "integration.evt.identity.service_scope.rejected.v1"
        );
    }

    #[test]
    fn the_declare_command_renders_the_frozen_command_subject() {
        let subject = render_command_subject(&declare_command_coords().unwrap());
        assert_eq!(subject, "integration.cmd.identity.service_scope.declare.v1");
    }

    #[test]
    fn the_declaring_actor_is_a_deterministic_v5_service_identity() {
        let service = ServiceKey::new("project").unwrap();
        let actor = declaring_actor(&service);
        assert!(actor.is_service());
        assert_eq!(actor, declaring_actor(&service));
        assert_ne!(actor, declaring_actor(&ServiceKey::new("billing").unwrap()));
    }

    #[test]
    fn the_command_carries_the_correlation_id_and_the_frozen_type() {
        let service = ServiceKey::new("project").unwrap();
        let correlation_id = Uuid::now_v7();
        let declaration = crate::scopes::ScopeManifest::of(&[&["project:read"]])
            .declaration()
            .unwrap();
        let command = build_command(&service, correlation_id, declaration);
        assert_eq!(command.command_type, "service_scope.declare");
        assert_eq!(command.version, 1);
        assert_eq!(command.metadata.correlation_id, correlation_id);
    }

    #[test]
    fn a_probe_reads_the_correlation_id_of_a_confirmation_envelope() {
        let correlation_id = Uuid::now_v7();
        let service = ServiceKey::new("project").unwrap();
        let event = IntegrationEvent::new(
            Uuid::now_v7(),
            "service_scope.accepted",
            VERSION,
            Utc::now(),
            EventMetadata::new(declaring_actor(&service), correlation_id),
            serde_json::json!({ "service": "project" }),
        );
        let bytes = serde_json::to_vec(&event).unwrap();
        let probe: CorrelationProbe = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(probe.metadata.correlation_id, correlation_id);
    }
}
