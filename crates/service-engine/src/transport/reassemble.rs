use std::collections::HashMap;
use std::collections::VecDeque;

use uuid::Uuid;

use crate::error::TransportError;
use crate::impact::Impact;
use crate::transport::payload::Frame;

pub const MAX_OPEN_GROUPS: usize = 64;

#[derive(Debug, PartialEq)]
pub enum Accepted {
    Buffered,
    BufferedAfterDrop,
    Complete(Vec<Impact>),
}

#[derive(Debug, Default)]
pub struct Reassembler {
    open: HashMap<Uuid, Partial>,
    order: VecDeque<Uuid>,
    dropped: u64,
}

#[derive(Debug)]
struct Partial {
    received: Vec<Option<Vec<Impact>>>,
    outstanding: usize,
}

impl Reassembler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn accept(&mut self, frame: Frame) -> Result<Accepted, TransportError> {
        if frame.parts == 1 {
            return Ok(Accepted::Complete(frame.impacts));
        }
        let parts = usize::from(frame.parts);
        let mut dropped = false;
        if !self.open.contains_key(&frame.group) {
            dropped = self.evict_until_free();
            self.order.push_back(frame.group);
            self.open.insert(
                frame.group,
                Partial {
                    received: vec![None; parts],
                    outstanding: parts,
                },
            );
        }
        let partial = self
            .open
            .get_mut(&frame.group)
            .expect("the group was just inserted");
        if partial.received.len() != parts {
            return Err(TransportError::Frame(format!(
                "group {} was announced with {} parts and now with {parts}",
                frame.group,
                partial.received.len()
            )));
        }
        let slot = &mut partial.received[usize::from(frame.part)];
        if slot.is_none() {
            partial.outstanding -= 1;
            *slot = Some(frame.impacts);
        }
        if partial.outstanding > 0 {
            return Ok(if dropped {
                Accepted::BufferedAfterDrop
            } else {
                Accepted::Buffered
            });
        }
        Ok(Accepted::Complete(self.take(frame.group)))
    }

    pub fn clear(&mut self) {
        self.open.clear();
        self.order.clear();
    }

    pub fn open_groups(&self) -> usize {
        self.open.len()
    }

    pub fn dropped_groups(&self) -> u64 {
        self.dropped
    }

    fn take(&mut self, group: Uuid) -> Vec<Impact> {
        self.order.retain(|candidate| *candidate != group);
        self.open
            .remove(&group)
            .expect("the group was just completed")
            .received
            .into_iter()
            .flat_map(|part| part.expect("a complete group has every part"))
            .collect()
    }

    fn evict_until_free(&mut self) -> bool {
        let before = self.dropped;
        while self.open.len() >= MAX_OPEN_GROUPS {
            match self.order.pop_front() {
                Some(oldest) => {
                    self.open.remove(&oldest);
                    self.dropped += 1;
                }
                None => break,
            }
        }
        self.dropped > before
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impact::Dims;
    use crate::name::NounName;
    use crate::transport::NOTIFY_PAYLOAD_LIMIT;
    use crate::transport::payload::encode;
    use crate::wire::Noun;

    struct Padded;
    impl Noun for Padded {
        type Key = String;
        const NAME: NounName = NounName::from_static("padded");
    }

    fn impacts(count: usize, width: usize) -> Vec<Impact> {
        (0..count)
            .map(|i| {
                let key = format!("{i:0width$}", width = width);
                Impact::resource::<Padded>(&key, Dims::EMPTY).expect("a padded key encodes")
            })
            .collect()
    }

    fn split() -> (Vec<Impact>, Vec<Frame>) {
        let staged = impacts(200, 200);
        let frames = encode(&staged, NOTIFY_PAYLOAD_LIMIT)
            .expect("the list encodes")
            .iter()
            .map(|payload| Frame::parse(payload).expect("a rendered frame parses"))
            .collect();
        (staged, frames)
    }

    #[test]
    fn an_incomplete_group_yields_nothing_until_its_last_part_arrives() {
        let (staged, frames) = split();
        let mut reassembler = Reassembler::new();
        let (last, rest) = frames.split_last().expect("more than one frame");
        for frame in rest {
            assert_eq!(
                reassembler.accept(frame.clone()).unwrap(),
                Accepted::Buffered
            );
        }
        assert_eq!(reassembler.open_groups(), 1);
        assert_eq!(
            reassembler.accept(last.clone()).unwrap(),
            Accepted::Complete(staged)
        );
        assert_eq!(reassembler.open_groups(), 0);
    }

    #[test]
    fn a_redelivered_frame_of_an_open_group_is_a_no_op() {
        let (staged, frames) = split();
        let mut reassembler = Reassembler::new();
        assert_eq!(
            reassembler.accept(frames[0].clone()).unwrap(),
            Accepted::Buffered
        );
        assert_eq!(
            reassembler.accept(frames[0].clone()).unwrap(),
            Accepted::Buffered
        );
        let mut heard = Vec::new();
        for frame in &frames[1..] {
            if let Accepted::Complete(complete) = reassembler.accept(frame.clone()).unwrap() {
                heard = complete;
            }
        }
        assert_eq!(heard, staged);
        assert_eq!(reassembler.open_groups(), 0);
    }

    #[test]
    fn a_group_that_changes_its_part_count_between_frames_is_refused() {
        let group = Uuid::now_v7();
        let mut reassembler = Reassembler::new();
        let first = Frame {
            group,
            part: 0,
            parts: 3,
            impacts: impacts(1, 4),
        };
        assert_eq!(reassembler.accept(first).unwrap(), Accepted::Buffered);
        let contradiction = Frame {
            group,
            part: 0,
            parts: 2,
            impacts: impacts(1, 4),
        };
        assert!(matches!(
            reassembler.accept(contradiction),
            Err(TransportError::Frame(_))
        ));
    }

    #[test]
    fn the_reassembler_evicts_the_oldest_incomplete_group_rather_than_growing_without_bound() {
        let mut reassembler = Reassembler::new();
        let mut outcomes = Vec::new();
        for _ in 0..MAX_OPEN_GROUPS + 10 {
            let frame = Frame {
                group: Uuid::now_v7(),
                part: 0,
                parts: 2,
                impacts: impacts(1, 4),
            };
            outcomes.push(reassembler.accept(frame).unwrap());
        }
        assert_eq!(reassembler.open_groups(), MAX_OPEN_GROUPS);
        assert!(
            outcomes[..MAX_OPEN_GROUPS]
                .iter()
                .all(|outcome| *outcome == Accepted::Buffered)
        );
        assert!(
            outcomes[MAX_OPEN_GROUPS..]
                .iter()
                .all(|outcome| *outcome == Accepted::BufferedAfterDrop),
            "an evicted group is a lost wake and must be reported so the transport can repair it \
             with a Reset"
        );
        assert_eq!(reassembler.dropped_groups(), 10);
    }

    #[test]
    fn a_single_part_group_is_never_buffered() {
        let mut reassembler = Reassembler::new();
        let staged = impacts(2, 4);
        let frame = Frame {
            group: Uuid::now_v7(),
            part: 0,
            parts: 1,
            impacts: staged.clone(),
        };
        assert_eq!(
            reassembler.accept(frame).unwrap(),
            Accepted::Complete(staged)
        );
        assert_eq!(reassembler.open_groups(), 0);
    }
}
