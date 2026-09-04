use std::sync::Arc;

use br_util_directory::{
    DirectoryError, DirectoryProjector, Impact as DirectoryImpact, ImpactStager,
};
use sqlx::PgConnection;

use crate::error::EngineError;
use crate::impact::{ForeignKey, Impact};
use crate::mirror::{MirrorHandle, MirrorRun};
use crate::name::MirrorName;
use crate::transport::ImpactTransport;

pub struct DirectoryImpactStager {
    transport: Arc<dyn ImpactTransport>,
}

impl DirectoryImpactStager {
    pub fn new(transport: Arc<dyn ImpactTransport>) -> Self {
        Self { transport }
    }
}

#[async_trait::async_trait]
impl ImpactStager for DirectoryImpactStager {
    async fn stage_in(
        &self,
        conn: &mut PgConnection,
        impacts: &[DirectoryImpact],
    ) -> Result<(), DirectoryError> {
        let impacts = impacts
            .iter()
            .map(translate)
            .collect::<Result<Vec<Impact>, EngineError>>()
            .map_err(|error| DirectoryError::Stager(Box::new(error)))?;
        self.transport
            .stage_in(conn, &impacts)
            .await
            .map_err(|error| DirectoryError::Stager(Box::new(error)))
    }
}

fn translate(impact: &DirectoryImpact) -> Result<Impact, EngineError> {
    match impact {
        DirectoryImpact::ForeignChanged { foreign } => Ok(Impact::ForeignChanged {
            foreign: ForeignKey::new(foreign.namespace(), foreign.key())?,
        }),
        other => Err(EngineError::Service(
            format!("the directory mirror staged an impact this engine cannot address: {other:?}")
                .into(),
        )),
    }
}

pub fn directory_mirror(name: MirrorName, projector: Arc<DirectoryProjector>) -> MirrorHandle {
    let reconcile = {
        let projector = projector.clone();
        move || {
            let projector = projector.clone();
            Box::pin(async move {
                projector
                    .reconcile()
                    .await
                    .map(|_| ())
                    .map_err(|error| EngineError::Service(Box::new(error)))
            }) as MirrorRun
        }
    };
    let watch = {
        let projector = projector.clone();
        move || {
            let projector = projector.clone();
            Box::pin(async move {
                projector
                    .watch()
                    .await
                    .map_err(|error| EngineError::Service(Box::new(error)))
            }) as MirrorRun
        }
    };
    MirrorHandle::new(name, reconcile, watch)
        .with_progress(move || projector.progress().borrow().changes)
}
