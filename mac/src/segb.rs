//! Read Biome SEGB segment files (v1 and v2) and decode their protobuf records without a schema.
//!
//! The layouts follow ccl-segb (CCL Forensics, MIT):
//! - v1: a 56-byte header with the end of data at 0 (u32) and `SEGB` at 52. Records follow, each a 32-byte header
//!   (length i32, state i32, two f64 times, crc32 u32, unknown i32) and the data, padded to 8 bytes.
//! - v2: a 32-byte header with `SEGB` at 0 and the entry count at 4 (i32). Records follow from 32, each an 8-byte
//!   header (crc32 u32, unknown i32) and the data, padded to 4 bytes. A trailer at the end of the file holds one
//!   16-byte entry per record: end offset relative to 32 (i32), state (i32) and time (f64).

#[cfg(test)]
pub mod tests;

use anyhow::{Result, bail};
use serde_json::{Map, Value};

use crate::tri;

/// Nested messages below this depth stay `hex:<bytes>`.
const MAX_DEPTH: usize = 8;
/// Entry state of a record that holds live data. 3 is deleted, 4 empty.
const WRITTEN: i32 = 1;

/// A decoded protobuf field value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Field<'data> {
    /// A length-delimited field: a string, raw bytes or a nested message.
    Bytes(&'data [u8]),
    /// A 32-bit field, read as `f32`.
    Fixed32(f32),
    /// A 64-bit field, read as `f64`.
    Fixed64(f64),
    /// A varint.
    Varint(u64),
}

impl Field<'_> {
    /// The field as JSON: a varint as a number, a fixed field as a float, bytes as a string when printable UTF-8,
    /// a nested object when they decode as a message at `depth`, and `hex:<bytes>` otherwise.
    fn json(self, depth: usize) -> Value {
        match self {
            Self::Bytes(data) => {
                let text = str::from_utf8(data).ok().filter(|candidate| {
                    !candidate.chars().any(|character| {
                        character.is_control() && !matches!(character, '\n' | '\r' | '\t')
                    })
                });
                match text {
                    Some(printable) => Value::from(printable),
                    None if depth < MAX_DEPTH && !data.is_empty() => {
                        to_json(data, depth.saturating_add(1)).unwrap_or_else(|| hex(data))
                    }
                    None => hex(data),
                }
            }
            Self::Fixed32(single) => Value::from(f64::from(single)),
            Self::Fixed64(double) => Value::from(double),
            Self::Varint(number) => Value::from(number),
        }
    }
}

/// One record: state, time (seconds since 2001) and data.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Record {
    /// Whether the data matches its checksum.
    pub crc_ok: bool,
    /// The record's bytes, a protobuf message.
    pub data: Vec<u8>,
    /// The entry state; `WRITTEN` for live data.
    pub state: i32,
    /// Seconds since 2001.
    pub time: f64,
}

/// The bytes of a SEGB segment file.
pub(super) struct Segment<'file> {
    /// The whole file.
    pub bytes: &'file [u8],
}

impl Segment<'_> {
    /// Every record, in file order. A truncated tail ends the list.
    pub(crate) fn records(&self) -> Result<Vec<Record>> {
        if self.bytes.get(52..56) == Some(b"SEGB") {
            Ok(self.v1())
        } else if self.bytes.get(0..4) == Some(b"SEGB") {
            Ok(self.v2())
        } else {
            bail!("not a SEGB file")
        }
    }

    /// The records of a v1 file.
    fn v1(&self) -> Vec<Record> {
        let bytes = self.bytes;
        let data_end = u32_at(bytes, 0)
            .and_then(|end| usize::try_from(end).ok())
            .map_or(0, |end| end.min(bytes.len()));
        let mut out = Vec::new();
        let mut pos = 56_usize;
        while pos.saturating_add(32) <= data_end {
            let (Some(len), Some(state), Some(time), Some(crc)) = (
                i32_at(bytes, pos),
                i32_at(bytes, pos.saturating_add(4)),
                f64_at(bytes, pos.saturating_add(8)),
                u32_at(bytes, pos.saturating_add(24)),
            ) else {
                break;
            };
            let start = pos.saturating_add(32);
            let Some(data) = usize::try_from(len)
                .ok()
                .and_then(|length| start.checked_add(length))
                .and_then(|stop| bytes.get(start..stop))
            else {
                break;
            };
            out.push(Record {
                crc_ok: crc32fast::hash(data) == crc,
                data: data.to_vec(),
                state,
                time,
            });
            pos = start.saturating_add(data.len()).next_multiple_of(8);
        }
        out
    }

    /// The records of a v2 file, from its trailer.
    fn v2(&self) -> Vec<Record> {
        let bytes = self.bytes;
        let count = i32_at(bytes, 4)
            .and_then(|count| usize::try_from(count).ok())
            .unwrap_or(0);
        let Some(trailer) = count
            .checked_mul(16)
            .and_then(|size| bytes.len().checked_sub(size))
        else {
            return Vec::new();
        };
        let mut entries: Vec<(usize, i32, f64)> = (0..count)
            .filter_map(|index| {
                let Some(at) = index
                    .checked_mul(16)
                    .and_then(|offset| trailer.checked_add(offset))
                else {
                    return None;
                };
                let Some(end) = i32_at(bytes, at).and_then(|end| usize::try_from(end).ok()) else {
                    return None;
                };
                let Some(state) = i32_at(bytes, at.saturating_add(4)) else {
                    return None;
                };
                // Zeroed slots have state 0 and point at nothing.
                if !matches!(state, 1_i32 | 3_i32) {
                    return None;
                }
                f64_at(bytes, at.saturating_add(8)).map(|time| (end, state, time))
            })
            .collect();
        entries.sort_by_key(|entry| entry.0);
        let mut out: Vec<Record> = Vec::new();
        let mut pos = 32_usize;
        let mut prev_end = None;
        for (end, state, time) in entries {
            // A record written and later deleted has two trailer entries for the same data.
            if prev_end == Some(end) {
                if let Some(last) = out.last().cloned() {
                    out.push(Record {
                        state,
                        time,
                        ..last
                    });
                }
                continue;
            }
            let stop = end.saturating_add(32);
            // A stale entry can point into data a newer record already consumed.
            if stop < pos.saturating_add(8) {
                continue;
            }
            let (Some(entry), Some(crc)) = (bytes.get(pos..stop), u32_at(bytes, pos)) else {
                break;
            };
            let Some(data) = entry.get(8..) else {
                break;
            };
            out.push(Record {
                crc_ok: crc32fast::hash(data) == crc,
                data: data.to_vec(),
                state,
                time,
            });
            prev_end = Some(end);
            pos = stop.next_multiple_of(4);
        }
        out
    }

    /// The written records with a good checksum: the ones to read.
    pub(crate) fn written(&self) -> Result<Vec<Record>> {
        Ok(tri!(self.records())
            .into_iter()
            .filter(|record| record.state == WRITTEN && record.crc_ok)
            .collect())
    }
}

/// The `LEN` bytes at `at`, or `None` when they run past the end.
fn bytes_at<const LEN: usize>(bytes: &[u8], at: usize) -> Option<[u8; LEN]> {
    let Some(end) = at.checked_add(LEN) else {
        return None;
    };
    let Some(slice) = bytes.get(at..end) else {
        return None;
    };
    slice.try_into().ok()
}

/// Decode a protobuf message into (field number, value) pairs, or `None` if it is not one.
///
/// Groups are not supported.
pub(super) fn decode(bytes: &[u8]) -> Option<Vec<(u64, Field<'_>)>> {
    let mut out = Vec::new();
    let mut pos = 0_usize;
    while pos < bytes.len() {
        let Some(key) = varint(bytes, &mut pos) else {
            return None;
        };
        let (number, wire) = (key >> 3_u32, key & 7);
        if number == 0 {
            return None;
        }
        let value = match wire {
            0 => {
                let Some(varint_value) = varint(bytes, &mut pos) else {
                    return None;
                };
                Field::Varint(varint_value)
            }
            1 => {
                let Some(fixed) = bytes_at(bytes, pos) else {
                    return None;
                };
                pos = pos.saturating_add(8);
                Field::Fixed64(f64::from_le_bytes(fixed))
            }
            2 => {
                let Some(len) = varint(bytes, &mut pos).and_then(|len| usize::try_from(len).ok())
                else {
                    return None;
                };
                let Some(data) = pos.checked_add(len).and_then(|end| bytes.get(pos..end)) else {
                    return None;
                };
                pos = pos.saturating_add(len);
                Field::Bytes(data)
            }
            5 => {
                let Some(fixed) = bytes_at(bytes, pos) else {
                    return None;
                };
                pos = pos.saturating_add(4);
                Field::Fixed32(f32::from_le_bytes(fixed))
            }
            _ => return None,
        };
        out.push((number, value));
    }
    Some(out)
}

/// The little-endian `f64` at `at`.
fn f64_at(bytes: &[u8], at: usize) -> Option<f64> {
    bytes_at(bytes, at).map(f64::from_le_bytes)
}

/// Bytes as `hex:<lower-case hex>`.
pub(super) fn hex(bytes: &[u8]) -> Value {
    let mut text = String::from("hex:");
    for byte in bytes {
        text.extend(char::from_digit(u32::from(byte >> 4_u32), 16));
        text.extend(char::from_digit(u32::from(byte & 0xf), 16));
    }
    Value::from(text)
}

/// The little-endian `i32` at `at`.
fn i32_at(bytes: &[u8], at: usize) -> Option<i32> {
    bytes_at(bytes, at).map(i32::from_le_bytes)
}

/// A protobuf message as JSON, field number to value, without a schema.
///
/// Varints are numbers, 64-bit and 32-bit fields are floats, and length-delimited fields are a string when they are
/// printable UTF-8, a nested object when they decode as a message, and `hex:<bytes>` otherwise. A repeated field is
/// an array. `depth` is how deep this message is nested; pass 0 for a record.
pub(super) fn to_json(bytes: &[u8], depth: usize) -> Option<Value> {
    let Some(decoded) = decode(bytes) else {
        return None;
    };
    let mut fields: Map<String, Value> = Map::new();
    for (number, field) in decoded {
        let value = field.json(depth);
        if let Some(list) = fields
            .entry(number.to_string())
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
        {
            list.push(value);
        }
    }
    // A field seen once is its value, a repeated one stays a list.
    for slot in fields.values_mut() {
        if let Some(single) = slot
            .as_array_mut()
            .filter(|list| list.len() == 1)
            .and_then(Vec::pop)
        {
            *slot = single;
        }
    }
    Some(Value::Object(fields))
}

/// The little-endian `u32` at `at`.
fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    bytes_at(bytes, at).map(u32::from_le_bytes)
}

/// Read a varint at `pos` and move `pos` past it.
fn varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0_u64;
    for shift in (0_u32..64).step_by(7) {
        let Some(&byte) = bytes.get(*pos) else {
            return None;
        };
        *pos = pos.saturating_add(1);
        value |= u64::from(byte & 0x7f).wrapping_shl(shift);
        if byte < 0x80 {
            return Some(value);
        }
    }
    None
}
