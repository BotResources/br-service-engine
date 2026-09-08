use std::time::Duration;

use async_nats::Subscriber;
use br_core_integration::{EventMetadata, IntegrationCommand, IntegrationEvent};
use br_core_scope::{DeclareServiceScopes, ScopeDeclaration, ServiceKey, ServiceScopesRejected};
use futures_util::StreamExt;
use futures_util::stream::SelectAll;
use serde::Deserialize;
use uuid::Uuid;

use crate::nats::{INTEGRATION_EVT, Nats};

use super::ScopeError;
use super::wire::{ConfirmationSubjects, build_command, command_subject};

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn run_handshake(
    nats: &Nats,
    declaration: ScopeDeclaration,
) -> Result<(), ScopeError> {
    let service = declaration.manifest().key.clone();
    let subjects = ConfirmationSubjects::render();
    let command_subject = command_subject();
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

fn no_stream(name: &str) -> crate::error::BoxedError {
    format!("stream {name} is absent; gitops declares it, the engine only binds it").into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use br_core_integration::{Actor, ServiceAccountId};
    use br_scope_declaration_contract::VERSION;
    use chrono::Utc;

    #[test]
    fn a_probe_reads_the_correlation_id_of_a_confirmation_envelope() {
        let correlation_id = Uuid::now_v7();
        let actor = Actor::Service(ServiceAccountId::from(Uuid::now_v7()));
        let event = IntegrationEvent::new(
            Uuid::now_v7(),
            "service_scope.accepted",
            VERSION,
            Utc::now(),
            EventMetadata::new(actor, correlation_id),
            serde_json::json!({ "service": "project" }),
        );
        let bytes = serde_json::to_vec(&event).unwrap();
        let probe: CorrelationProbe = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(probe.metadata.correlation_id, correlation_id);
    }
}
