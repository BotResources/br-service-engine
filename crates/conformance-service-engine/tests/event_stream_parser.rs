#[path = "blackbox_support/event_stream.rs"]
mod event_stream;

use event_stream::{Block, EventStreamParser};

fn blocks_of(chunks: &[&[u8]]) -> Vec<Block> {
    let mut parser = EventStreamParser::default();
    let mut blocks = Vec::new();
    for chunk in chunks {
        parser.push(chunk);
        while let Some(block) = parser.next_block() {
            blocks.push(block);
        }
    }
    blocks
}

fn event(event: &str, data: &str) -> Block {
    Block {
        event: Some(event.to_string()),
        data: Some(data.to_string()),
        ..Block::default()
    }
}

#[test]
fn a_blank_line_ends_a_block_and_the_next_one_follows() {
    assert_eq!(
        blocks_of(&[b"event: next\ndata: {}\n\nevent: complete\ndata:\n\n"]),
        vec![event("next", "{}"), event("complete", "")]
    );
}

#[test]
fn a_block_without_its_blank_line_is_held_back() {
    let mut parser = EventStreamParser::default();
    parser.push(b"event: next\ndata: {}\n");
    assert!(parser.next_block().is_none());
    assert!(parser.has_unterminated_input());
    parser.push(b"\n");
    assert_eq!(parser.next_block(), Some(event("next", "{}")));
    assert!(!parser.has_unterminated_input());
}

#[test]
fn crlf_cr_and_lf_line_ends_give_the_same_block() {
    let expected = vec![event("next", "{}")];
    assert_eq!(blocks_of(&[b"event: next\r\ndata: {}\r\n\r\n"]), expected);
    assert_eq!(blocks_of(&[b"event: next\rdata: {}\r\r"]), expected);
    assert_eq!(blocks_of(&[b"event: next\ndata: {}\n\n"]), expected);
    assert_eq!(blocks_of(&[b"event: next\r\ndata: {}\n\n"]), expected);
}

#[test]
fn a_crlf_split_between_two_chunks_is_one_line_end() {
    let expected = vec![event("next", "{}")];
    assert_eq!(
        blocks_of(&[b"event: next\r\ndata: {}\r\n\r", b"\n"]),
        expected
    );
    assert_eq!(
        blocks_of(&[b"event: next\r", b"\ndata: {}\r\n\r\n"]),
        expected
    );
}

#[test]
fn a_frame_fed_one_byte_at_a_time_gives_the_same_blocks() {
    let frame = "event: next\r\ndata: {\"a\":\"é\"}\r\n\r\n: ping\n\nevent: complete\ndata:\n\n";
    let whole = blocks_of(&[frame.as_bytes()]);
    let bytes: Vec<&[u8]> = frame.as_bytes().chunks(1).collect();
    assert_eq!(blocks_of(&bytes), whole);
    assert_eq!(whole.len(), 3);
}

#[test]
fn a_multibyte_character_split_between_two_chunks_survives() {
    let frame = "event: next\ndata: {\"name\":\"é\"}\n\n".as_bytes();
    let cut = frame
        .iter()
        .position(|byte| *byte == 0xC3)
        .expect("a multibyte lead byte")
        + 1;
    assert_eq!(
        blocks_of(&[&frame[..cut], &frame[cut..]]),
        vec![event("next", "{\"name\":\"é\"}")]
    );
}

#[test]
fn a_half_received_multibyte_character_is_unterminated_input() {
    let mut parser = EventStreamParser::default();
    parser.push(&"é".as_bytes()[..1]);
    assert!(parser.has_unterminated_input());
}

#[test]
fn several_data_lines_join_with_a_newline() {
    assert_eq!(
        blocks_of(&[b"data: one\ndata: two\ndata:\ndata: four\n\n"]),
        vec![Block {
            data: Some("one\ntwo\n\nfour".to_string()),
            ..Block::default()
        }]
    );
}

#[test]
fn only_one_leading_space_of_a_value_is_dropped() {
    assert_eq!(
        blocks_of(&[b"event:next\ndata:  padded\n\n"]),
        vec![event("next", " padded")]
    );
}

#[test]
fn a_field_without_a_colon_has_an_empty_value() {
    assert_eq!(blocks_of(&[b"event\ndata\n\n"]), vec![event("", "")]);
}

#[test]
fn a_comment_only_block_is_a_comment_block() {
    let blocks = blocks_of(&[b": keep-alive\n\n"]);
    assert_eq!(blocks.len(), 1);
    assert!(blocks[0].is_comment_only());
    assert_eq!(blocks[0].comments, vec![" keep-alive".to_string()]);
}

#[test]
fn a_comment_inside_an_event_block_does_not_split_it() {
    let blocks = blocks_of(&[b"event: next\n: ping\ndata: {}\n\n"]);
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].event.as_deref(), Some("next"));
    assert_eq!(blocks[0].data.as_deref(), Some("{}"));
    assert_eq!(blocks[0].comments, vec![" ping".to_string()]);
}

#[test]
fn the_id_line_is_read_and_a_nul_id_is_ignored() {
    let blocks = blocks_of(&[b"id: 7\ndata: x\n\nid: a\0b\ndata: y\n\n"]);
    assert_eq!(blocks[0].id.as_deref(), Some("7"));
    assert_eq!(blocks[1].id, None);
}

#[test]
fn unknown_fields_are_ignored() {
    assert_eq!(
        blocks_of(&[b"retry: 10\nweird: 1\nevent: next\ndata: {}\n\n"]),
        vec![event("next", "{}")]
    );
}

#[test]
fn extra_blank_lines_dispatch_nothing() {
    let mut parser = EventStreamParser::default();
    parser.push(b"\n\n\r\n\r\n");
    assert!(parser.next_block().is_none());
    assert!(!parser.has_unterminated_input());
}

#[test]
fn unterminated_input_is_reported_for_a_diagnostic() {
    let mut parser = EventStreamParser::default();
    parser.push(b"event: next\ndata: {");
    assert!(parser.unterminated().contains("data: {"));
}
