use super::*;
use crate::accumulator::new_registry;

#[test]
fn an_empty_fold_reads_from_the_very_first_sequence() {
    assert_eq!(floor(None), -1);
    assert_eq!(floor(Some(ChunkSeq::ZERO)), 0);
    assert_eq!(floor(Some(ChunkSeq::new(9).unwrap())), 9);
}

#[tokio::test]
async fn the_fold_cache_of_never_sealed_keys_stays_bounded_by_its_capacity() {
    let pg = PgPool::connect_lazy("postgresql://engine@127.0.0.1:1/engine")
        .expect("a lazy pool never dials");
    let mut reader = ChunkReader::with_registry(pg, new_registry());
    reader.set_capacity(8);
    let accumulator = AccumulatorName::from_static("tokens");
    for n in 0..1_000u64 {
        let key = KeyBytes::encode(&format!("abandoned-{n}")).expect("a key encodes");
        reader.store(
            accumulator.clone(),
            key,
            Box::new(String::new()) as ErasedState,
            Some(ChunkSeq::new(n).unwrap()),
        );
        assert!(
            reader.cached_folds() <= 8,
            "a stream of unsealed keys never grows the cache past its capacity"
        );
    }
    assert_eq!(reader.cached_folds(), 8);
}
