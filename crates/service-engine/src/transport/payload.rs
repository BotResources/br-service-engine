use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::TransportError;
use crate::impact::Impact;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frame {
    #[serde(rename = "g")]
    pub group: Uuid,
    #[serde(rename = "p")]
    pub part: u16,
    #[serde(rename = "n")]
    pub parts: u16,
    #[serde(rename = "i")]
    pub impacts: Vec<Impact>,
}

impl Frame {
    pub fn render(&self) -> Result<String, TransportError> {
        serde_json::to_string(self).map_err(TransportError::Payload)
    }

    pub fn parse(payload: &str) -> Result<Self, TransportError> {
        let frame: Self = serde_json::from_str(payload).map_err(TransportError::Payload)?;
        if frame.parts == 0 || frame.part >= frame.parts {
            return Err(TransportError::Frame(format!(
                "part {} of {} is out of range",
                frame.part, frame.parts
            )));
        }
        Ok(frame)
    }
}

pub fn header_width() -> Result<usize, TransportError> {
    let widest = Frame {
        group: Uuid::nil(),
        part: u16::MAX,
        parts: u16::MAX,
        impacts: Vec::new(),
    };
    Ok(widest.render()?.len() - "[]".len())
}

pub fn within_notify_limit(payload_len: usize, limit: usize) -> bool {
    payload_len < limit
}

pub fn encode(impacts: &[Impact], limit: usize) -> Result<Vec<String>, TransportError> {
    if impacts.is_empty() {
        return Ok(Vec::new());
    }
    let packed = pack(impacts, limit)?;
    let group = Uuid::now_v7();
    let parts = u16::try_from(packed.len())
        .map_err(|_| TransportError::Frame(format!("{} parts exceed one group", packed.len())))?;
    let mut rendered = Vec::with_capacity(packed.len());
    for (index, impacts) in packed.into_iter().enumerate() {
        let payload = Frame {
            group,
            part: index as u16,
            parts,
            impacts,
        }
        .render()?;
        if !within_notify_limit(payload.len(), limit) {
            return Err(TransportError::PayloadTooLarge {
                size: payload.len(),
                limit,
            });
        }
        rendered.push(payload);
    }
    Ok(rendered)
}

fn pack(impacts: &[Impact], limit: usize) -> Result<Vec<Vec<Impact>>, TransportError> {
    let header = header_width()?;
    let mut packed: Vec<Vec<Impact>> = Vec::new();
    let mut current: Vec<Impact> = Vec::new();
    let mut current_width = 0usize;
    for impact in impacts {
        let width = serde_json::to_string(impact)
            .map_err(TransportError::Payload)?
            .len();
        let alone = header + "[]".len() + width;
        if !within_notify_limit(alone, limit) {
            return Err(TransportError::PayloadTooLarge { size: alone, limit });
        }
        let appended = header + "[]".len() + current_width + width + commas_for(current.len() + 1);
        if !current.is_empty() && !within_notify_limit(appended, limit) {
            packed.push(std::mem::take(&mut current));
            current_width = 0;
        }
        current.push(impact.clone());
        current_width += width;
    }
    if !current.is_empty() {
        packed.push(current);
    }
    Ok(packed)
}

fn commas_for(items: usize) -> usize {
    items.saturating_sub(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impact::Dims;
    use crate::name::NounName;
    use crate::transport::reassemble::{Accepted, Reassembler};
    use crate::wire::Noun;

    const LIMIT: usize = crate::transport::NOTIFY_PAYLOAD_LIMIT;

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

    fn hear(payloads: &[String]) -> Vec<Impact> {
        let mut reassembler = Reassembler::new();
        let mut heard = Vec::new();
        for payload in payloads {
            let frame = Frame::parse(payload).expect("a rendered frame parses");
            if let Accepted::Complete(complete) =
                reassembler.accept(frame).expect("a coherent group")
            {
                heard.extend(complete);
            }
        }
        assert_eq!(reassembler.open_groups(), 0);
        heard
    }

    #[test]
    fn a_short_impact_list_rides_in_one_frame_and_is_heard_whole() {
        let staged = impacts(3, 8);
        let payloads = encode(&staged, LIMIT).unwrap();
        assert_eq!(payloads.len(), 1);
        assert_eq!(hear(&payloads), staged);
    }

    #[test]
    fn a_list_above_the_notify_limit_is_split_into_frames_that_each_fit_and_is_heard_whole() {
        let staged = impacts(200, 200);
        let payloads = encode(&staged, LIMIT).unwrap();
        assert!(
            payloads.len() > 1,
            "a list of this size must not ride in one notification"
        );
        for payload in &payloads {
            assert!(payload.len() <= LIMIT, "{} bytes", payload.len());
        }
        assert_eq!(hear(&payloads), staged);
    }

    #[test]
    fn an_impact_that_cannot_fit_a_frame_alone_is_refused_rather_than_silently_dropped() {
        let staged = impacts(1, LIMIT + 1);
        let refused = encode(&staged, LIMIT).unwrap_err();
        assert!(matches!(
            refused,
            TransportError::PayloadTooLarge { size, limit } if size > limit && limit == LIMIT
        ));
    }

    #[test]
    fn a_frame_whose_part_index_escapes_its_group_is_refused() {
        let frame = Frame {
            group: Uuid::now_v7(),
            part: 3,
            parts: 2,
            impacts: Vec::new(),
        };
        let payload = frame.render().unwrap();
        assert!(matches!(
            Frame::parse(&payload),
            Err(TransportError::Frame(_))
        ));
    }

    #[test]
    fn an_empty_impact_list_notifies_nothing() {
        assert!(encode(&[], LIMIT).unwrap().is_empty());
    }

    #[test]
    fn a_payload_at_the_postgres_hard_limit_is_refused_and_one_byte_under_is_admitted() {
        assert!(within_notify_limit(LIMIT - 1, LIMIT));
        assert!(!within_notify_limit(LIMIT, LIMIT));
        assert!(!within_notify_limit(LIMIT + 1, LIMIT));
    }
}
