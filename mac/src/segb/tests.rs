//! Tests for `segb`, and the SEGB and protobuf fixtures the other modules' tests build on.

use anyhow::{Context as _, Result, bail, ensure};
use serde_json::json;

use super::{Segment, decode, to_json};
use crate::tri;

/// A protobuf field of a fixture, by field number.
#[derive(Clone, Copy)]
pub(crate) enum Proto<'bytes> {
    /// A length-delimited field.
    Bytes(u64, &'bytes [u8]),
    /// A 64-bit float.
    Double(u64, f64),
    /// A varint.
    Varint(u64, u64),
}

/// Encode protobuf fields for fixtures.
pub(crate) fn proto(fields: &[Proto<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    for field in fields {
        match *field {
            Proto::Bytes(number, bytes) => {
                put_varint(&mut out, (number << 3_u32) | 2);
                put_varint(&mut out, u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                out.extend_from_slice(bytes);
            }
            Proto::Double(number, value) => {
                put_varint(&mut out, (number << 3_u32) | 1);
                out.extend_from_slice(&value.to_le_bytes());
            }
            Proto::Varint(number, value) => {
                put_varint(&mut out, number << 3_u32);
                put_varint(&mut out, value);
            }
        }
    }
    out
}

/// Append `value` as a varint.
fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        let [low, ..] = value.to_le_bytes();
        out.push(low | 0x80);
        value = value.wrapping_shr(7);
    }
    let [low, ..] = value.to_le_bytes();
    out.push(low);
}

/// The records of the sample file: written, deleted, written.
fn sample() -> Vec<(i32, f64, Vec<u8>)> {
    vec![
        (
            1,
            100.0,
            proto(&[Proto::Varint(3, 1), Proto::Bytes(6, b"com.a")]),
        ),
        (3, 101.0, vec![0; 5]),
        (
            1,
            102.0,
            proto(&[Proto::Varint(3, 0), Proto::Bytes(6, b"com.a")]),
        ),
    ]
}

/// Build a SEGB v1 file from (state, time, data) records.
pub(crate) fn segb_v1(records: &[(i32, f64, Vec<u8>)]) -> Vec<u8> {
    let mut body = Vec::new();
    for (state, time, data) in records.iter().cloned() {
        body.extend_from_slice(&i32::try_from(data.len()).unwrap_or(i32::MAX).to_le_bytes());
        body.extend_from_slice(&state.to_le_bytes());
        body.extend_from_slice(&time.to_le_bytes());
        body.extend_from_slice(&time.to_le_bytes());
        body.extend_from_slice(&crc32fast::hash(&data).to_le_bytes());
        body.extend_from_slice(&0_i32.to_le_bytes());
        body.extend_from_slice(&data);
        body.resize(body.len().next_multiple_of(8), 0);
    }
    let end = u32::try_from(body.len().saturating_add(56)).unwrap_or(u32::MAX);
    let mut out = end.to_le_bytes().to_vec();
    out.resize(52, 0);
    out.extend_from_slice(b"SEGB");
    out.extend_from_slice(&body);
    out
}

/// Build a SEGB v2 file from (state, time, data) records, with one zeroed trailer slot.
pub(crate) fn segb_v2(records: &[(i32, f64, Vec<u8>)]) -> Vec<u8> {
    let mut out = b"SEGB".to_vec();
    let count = i32::try_from(records.len().saturating_add(1)).unwrap_or(i32::MAX);
    out.extend_from_slice(&count.to_le_bytes());
    out.resize(32, 0);
    let mut trailer = Vec::new();
    for (state, time, data) in records.iter().cloned() {
        out.extend_from_slice(&crc32fast::hash(&data).to_le_bytes());
        out.extend_from_slice(&0_i32.to_le_bytes());
        out.extend_from_slice(&data);
        let end = i32::try_from(out.len().saturating_sub(32)).unwrap_or(i32::MAX);
        out.resize(out.len().next_multiple_of(4), 0);
        trailer.extend_from_slice(&end.to_le_bytes());
        trailer.extend_from_slice(&state.to_le_bytes());
        trailer.extend_from_slice(&time.to_le_bytes());
    }
    trailer.extend_from_slice(&[0_u8; 16]);
    out.extend_from_slice(&trailer);
    out
}

/// Decode nested, repeated, float and binary fields without a schema, and reject a bad message.
#[test]
fn decodes_protobuf_without_a_schema() -> Result<()> {
    let inner = proto(&[Proto::Varint(1, 7)]);
    let message = proto(&[
        Proto::Varint(1, 300),
        Proto::Bytes(2, b"Song"),
        Proto::Bytes(2, b"Other"),
        Proto::Bytes(3, &inner),
        Proto::Double(4, 1.5),
        Proto::Bytes(5, &[0xff, 0x00]),
    ]);
    let decoded = tri!(to_json(&message, 0).context("not a message"));
    ensure!(
        decoded
            == json!({"1": 300_u64, "2": ["Song", "Other"], "3": {"1": 7_u64}, "4": 1.5_f64, "5": "hex:ff00"}),
        "fields by number, a repeated field as a list, bytes as hex"
    );
    ensure!(
        decode(&[0x0b]).is_none(),
        "a truncated key is not a message"
    );
    Ok(())
}

/// Skip a record with a bad checksum, stop at a truncated tail, and reject a file that is not SEGB.
#[test]
fn drops_bad_checksums_and_survives_truncation() -> Result<()> {
    let mut file = segb_v1(&sample());
    let Some(byte) = file.get_mut(88) else {
        bail!("the first record's data starts at 88");
    };
    *byte ^= 0xff;
    ensure!(
        tri!(Segment { bytes: &file }.written()).len() == 1,
        "the corrupted record is dropped"
    );
    let cut = segb_v1(&sample());
    let Some(truncated) = cut.get(..cut.len().saturating_sub(16)) else {
        bail!("the sample file is longer than 16 bytes");
    };
    ensure!(
        tri!(Segment { bytes: truncated }.records()).len() == 2,
        "a truncated last record ends the list"
    );
    ensure!(
        Segment { bytes: b"nope" }.records().is_err(),
        "not a SEGB file"
    );
    let short = segb_v2(&sample());
    let Some(head) = short.get(..20) else {
        bail!("the sample file is longer than 20 bytes");
    };
    ensure!(
        tri!(Segment { bytes: head }.records()).is_empty(),
        "a v2 file cut before its trailer has no records"
    );
    Ok(())
}

/// Read the same records from a v1 and a v2 file.
#[test]
fn reads_v1_and_v2_alike() -> Result<()> {
    for file in [segb_v1(&sample()), segb_v2(&sample())] {
        let segment = Segment { bytes: &file };
        let all = tri!(segment.records());
        ensure!(all.len() == 3, "three records");
        ensure!(
            all.iter().all(|record| record.crc_ok),
            "every checksum matches"
        );
        let live = tri!(segment.written());
        ensure!(live.len() == 2, "two written records");
        let Some(last) = live.last() else {
            bail!("no written records");
        };
        ensure!(
            last.time.total_cmp(&102.0_f64).is_eq(),
            "the second written record is the last one"
        );
        let expected = tri!(sample().pop().context("the sample has records")).2;
        ensure!(last.data == expected, "its data is intact");
    }
    Ok(())
}
