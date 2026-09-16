use super::*;

mod reasons {
    use super::Reason;

    pub const NO_SCOPE: Reason = Reason::new("MISSING_SCOPE");
    pub const ALREADY_CLOSED: Reason = Reason::new("ALREADY_CLOSED");
    pub const NOT_CLOSED: Reason = Reason::new("NOT_CLOSED");
}

struct Doc {
    closed: bool,
}

struct Actor {
    scoped: bool,
}

const CLOSE: ActionName = ActionName::from_static("close");
const REOPEN: ActionName = ActionName::from_static("reopen");

crate::gated! {
    Doc, Actor;
    "close" => fn close_gate(this, actor) {
        if !actor.scoped {
            return Gate::blocked(reasons::NO_SCOPE);
        }
        if this.closed {
            return Gate::blocked(reasons::ALREADY_CLOSED);
        }
        Gate::allowed()
    }
    "reopen" => fn reopen_gate(this, _actor) {
        if this.closed {
            Gate::allowed()
        } else {
            Gate::blocked(reasons::NOT_CLOSED)
        }
    }
}

#[test]
fn a_blocked_gate_carries_a_stable_code_that_require_surfaces_verbatim() {
    let gate = Gate::blocked(reasons::ALREADY_CLOSED);
    assert!(!gate.is_allowed());
    assert_eq!(gate.reason().map(|r| r.code()), Some("ALREADY_CLOSED"));
    assert_eq!(gate.require().unwrap_err().as_str(), "ALREADY_CLOSED");
    assert!(Gate::allowed().require().is_ok());
}

#[test]
fn the_projector_affordance_and_the_mutation_gate_are_the_one_function() {
    let open = Doc { closed: false };
    let scoped = Actor { scoped: true };

    let affordances = open.affordances(&scoped);
    assert_eq!(affordances.get(CLOSE), Some(Gate::allowed()));
    assert_eq!(open.gate(CLOSE, &scoped), Some(Gate::allowed()));
    assert_eq!(open.close_gate(&scoped), Gate::allowed());
    assert_eq!(affordances.get(CLOSE), open.gate(CLOSE, &scoped));
}

#[test]
fn a_blocked_affordance_and_the_gate_that_refuses_the_mutation_share_one_reason_code() {
    let closed = Doc { closed: true };
    let scoped = Actor { scoped: true };

    let shown = closed.affordances(&scoped).get(CLOSE).unwrap();
    let refused = closed.close_gate(&scoped).require().unwrap_err();

    assert_eq!(shown.reason().unwrap().code(), refused.code());
    assert_eq!(refused.code(), "ALREADY_CLOSED");
}

#[test]
fn the_declared_action_set_equals_the_rendered_affordance_surface() {
    let doc = Doc { closed: false };
    let actor = Actor { scoped: false };
    assert_eq!(Doc::ACTIONS, &[CLOSE, REOPEN]);
    let surface: Vec<ActionName> = doc.affordances(&actor).names().collect();
    assert_eq!(surface, vec![CLOSE, REOPEN]);
    check_gates_match_affordances(&doc, &actor).expect("the macro keeps the two sides identical");
}

#[test]
fn the_check_flags_a_hand_rolled_impl_whose_gate_diverges_from_its_affordance() {
    struct Drifted;
    impl Gated for Drifted {
        type Principal = ();
        const ACTIONS: &'static [ActionName] = &[CLOSE];
        fn gate(&self, _action: ActionName, _p: &()) -> Option<Gate> {
            Some(Gate::allowed())
        }
        fn affordances(&self, _p: &()) -> Affordances {
            Affordances::from_pairs([(CLOSE, Gate::blocked(reasons::ALREADY_CLOSED))])
        }
    }
    let mismatch = check_gates_match_affordances(&Drifted, &()).unwrap_err();
    assert!(matches!(mismatch, GateMismatch::Divergent { action, .. } if action == CLOSE));
}

#[test]
fn the_check_flags_an_affordance_surface_that_omits_a_declared_action() {
    struct Missing;
    impl Gated for Missing {
        type Principal = ();
        const ACTIONS: &'static [ActionName] = &[CLOSE, REOPEN];
        fn gate(&self, _action: ActionName, _p: &()) -> Option<Gate> {
            Some(Gate::allowed())
        }
        fn affordances(&self, _p: &()) -> Affordances {
            Affordances::from_pairs([(CLOSE, Gate::allowed())])
        }
    }
    let mismatch = check_gates_match_affordances(&Missing, &()).unwrap_err();
    assert!(matches!(
        mismatch,
        GateMismatch::Surface { only_in_actions, .. } if only_in_actions == vec![REOPEN]
    ));
}

#[test]
fn an_allowed_gate_serializes_as_allowed_and_a_blocked_one_carries_its_code() {
    assert_eq!(
        serde_json::to_value(Gate::allowed()).unwrap(),
        serde_json::json!({ "allowed": true })
    );
    assert_eq!(
        serde_json::to_value(Gate::blocked(reasons::NOT_CLOSED)).unwrap(),
        serde_json::json!({ "allowed": false, "reason": "NOT_CLOSED" })
    );
}

#[test]
fn an_affordance_set_serializes_as_a_map_from_action_name_to_verdict() {
    let doc = Doc { closed: true };
    let actor = Actor { scoped: false };
    assert_eq!(
        serde_json::to_value(doc.affordances(&actor)).unwrap(),
        serde_json::json!({
            "close": { "allowed": false, "reason": "MISSING_SCOPE" },
            "reopen": { "allowed": true },
        })
    );
}

#[test]
#[should_panic(expected = "SCREAMING_SNAKE_CASE")]
fn constructing_a_reason_from_a_non_screaming_code_panics() {
    // `Reason::new` is `const`, so at a `const` site this panic is a compile
    // error; called at runtime with a bad code it panics, which this asserts.
    let _ = Reason::new("already_closed");
}

#[test]
fn parse_accepts_screaming_snake_and_refuses_every_other_shape() {
    assert!(Reason::parse("ALREADY_CLOSED").is_ok());
    assert!(Reason::parse("A1_B2").is_ok());
    // one leading capital then one-or-more capitals/digits/underscores
    assert!(Reason::parse("AB").is_ok());
    // rejects lower-case, the pre-0.2 convention
    assert!(Reason::parse("already_closed").is_err());
    // rejects a single character (the `+` demands at least two)
    assert!(Reason::parse("A").is_err());
    // rejects a leading digit, a leading underscore, punctuation and spaces
    assert!(Reason::parse("1_BAD").is_err());
    assert!(Reason::parse("_BAD").is_err());
    assert!(Reason::parse("NOT-CLOSED").is_err());
    assert!(Reason::parse("NOT CLOSED").is_err());
    assert!(Reason::parse("").is_err());
}

#[test]
fn a_reason_decoded_from_the_wire_is_rejected_unless_it_is_screaming_snake() {
    // the shape a blocked gate carries; a lower-case code from an older peer is
    // refused at the deserialization boundary rather than trusted.
    let ok: Result<Gate, _> =
        serde_json::from_value(serde_json::json!({ "allowed": false, "reason": "ALREADY_CLOSED" }));
    assert!(ok.is_ok());
    let bad: Result<Gate, _> =
        serde_json::from_value(serde_json::json!({ "allowed": false, "reason": "already_closed" }));
    assert!(bad.is_err());
}
