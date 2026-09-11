use super::{ActionName, Gate, Gated};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateMismatch {
    Surface {
        only_in_affordances: Vec<ActionName>,
        only_in_actions: Vec<ActionName>,
    },
    Divergent {
        action: ActionName,
        via_gate: Option<Gate>,
        via_affordance: Option<Gate>,
    },
}

impl std::fmt::Display for GateMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Surface {
                only_in_affordances,
                only_in_actions,
            } => write!(
                f,
                "the affordance surface and the declared gate set diverge: \
                 only in affordances {only_in_affordances:?}, only in the gate set {only_in_actions:?}"
            ),
            Self::Divergent {
                action,
                via_gate,
                via_affordance,
            } => write!(
                f,
                "action {action} resolves to a different verdict through the gate ({via_gate:?}) \
                 than through the affordance surface ({via_affordance:?})"
            ),
        }
    }
}

impl std::error::Error for GateMismatch {}

pub fn check_gates_match_affordances<G: Gated>(
    aggregate: &G,
    principal: &G::Principal,
) -> Result<(), GateMismatch> {
    let affordances = aggregate.affordances(principal);
    let surface: Vec<ActionName> = affordances.names().collect();

    let only_in_affordances: Vec<ActionName> = surface
        .iter()
        .filter(|name| !G::ACTIONS.contains(name))
        .copied()
        .collect();
    let only_in_actions: Vec<ActionName> = G::ACTIONS
        .iter()
        .filter(|name| !surface.contains(name))
        .copied()
        .collect();
    if !only_in_affordances.is_empty() || !only_in_actions.is_empty() {
        return Err(GateMismatch::Surface {
            only_in_affordances,
            only_in_actions,
        });
    }

    for action in G::ACTIONS {
        let via_gate = aggregate.gate(*action, principal);
        let via_affordance = affordances.get(*action);
        if via_gate != via_affordance {
            return Err(GateMismatch::Divergent {
                action: *action,
                via_gate,
                via_affordance,
            });
        }
    }
    Ok(())
}
