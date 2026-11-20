//! Read the metadata of every photo and video in the Photos library: when, where, what camera, and how it was used.
//!
//! No image data. Assets are sent again when Photos changes them (favourite, trash, edit), so the newest copy wins.

#[cfg(test)]
mod tests;

use alloc::collections::BTreeMap;
use std::path::Path;

use anyhow::Result;
use rusqlite::params;
use serde_json::{Value, json};

use crate::apple::open_copy;
use crate::browser::{col, columns, exists};
use crate::tri;

/// Output key and column per table alias: `a` ZASSET, `x` ZADDITIONALASSETATTRIBUTES, `e` ZEXTENDEDATTRIBUTES.
///
/// Times are seconds since 2001, as Photos stores them.
const FIELDS: &[(&str, &str, &str)] = &[
    ("uuid", "a", "ZUUID"),
    ("created", "a", "ZDATECREATED"),
    ("added", "a", "ZADDEDDATE"),
    ("modified", "a", "ZMODIFICATIONDATE"),
    ("kind", "a", "ZKIND"),
    ("kind_subtype", "a", "ZKINDSUBTYPE"),
    ("width", "a", "ZWIDTH"),
    ("height", "a", "ZHEIGHT"),
    ("duration", "a", "ZDURATION"),
    ("lat", "a", "ZLATITUDE"),
    ("lon", "a", "ZLONGITUDE"),
    ("favorite", "a", "ZFAVORITE"),
    ("hidden", "a", "ZHIDDEN"),
    ("trashed", "a", "ZTRASHEDSTATE"),
    ("trashed_at", "a", "ZTRASHEDDATE"),
    ("screenshot", "a", "ZISDETECTEDSCREENSHOT"),
    ("filename", "a", "ZFILENAME"),
    ("uti", "a", "ZUNIFORMTYPEIDENTIFIER"),
    ("mine", "a", "ZCLOUDISMYASSET"),
    ("saved_type", "a", "ZSAVEDASSETTYPE"),
    ("capture_device", "a", "ZDERIVEDCAMERACAPTUREDEVICE"),
    ("last_shared", "a", "ZLASTSHAREDDATE"),
    ("aesthetic_score", "a", "ZOVERALLAESTHETICSCORE"),
    ("original_filename", "x", "ZORIGINALFILENAME"),
    ("size_bytes", "x", "ZORIGINALFILESIZE"),
    ("timezone", "x", "ZTIMEZONENAME"),
    ("timezone_offset", "x", "ZTIMEZONEOFFSET"),
    ("timezone_inferred_offset", "x", "ZINFERREDTIMEZONEOFFSET"),
    ("exif_time", "x", "ZEXIFTIMESTAMPSTRING"),
    ("gps_accuracy", "x", "ZGPSHORIZONTALACCURACY"),
    ("imported_by", "x", "ZIMPORTEDBYBUNDLEIDENTIFIER"),
    ("imported_by_name", "x", "ZIMPORTEDBYDISPLAYNAME"),
    ("editor", "x", "ZEDITORBUNDLEID"),
    ("title", "x", "ZTITLE"),
    ("views", "x", "ZVIEWCOUNT"),
    ("plays", "x", "ZPLAYCOUNT"),
    ("shares", "x", "ZSHARECOUNT"),
    ("last_viewed", "x", "ZLASTVIEWEDDATE"),
    ("camera_make", "e", "ZCAMERAMAKE"),
    ("camera_model", "e", "ZCAMERAMODEL"),
    ("lens", "e", "ZLENSMODEL"),
    ("iso", "e", "ZISO"),
    ("aperture", "e", "ZAPERTURE"),
    ("shutter_speed", "e", "ZSHUTTERSPEED"),
    ("focal_length", "e", "ZFOCALLENGTH"),
    ("focal_length_35mm", "e", "ZFOCALLENGTHIN35MM"),
    ("flash", "e", "ZFLASHFIRED"),
    ("fps", "e", "ZFPS"),
    ("codec", "e", "ZCODEC"),
    ("bitrate", "e", "ZBITRATE"),
];

/// The position after the last asset sent: its modification time and uuid.
pub(super) type Cursor = (f64, String);

/// A detected face: the person's uuid and name, each null when unknown.
type Face = (Option<String>, Option<String>);

/// A search index finding: its category and text.
type Label = (i64, String);

/// The Photos library database.
pub(super) struct Photos<'db> {
    /// `Pictures/Photos Library.photoslibrary/database/Photos.sqlite`.
    pub db: &'db Path,
}

/// Per asset uuid, the CRC of its labels as last sent.
pub(super) type Sent = BTreeMap<String, u32>;

impl Photos<'_> {
    /// Up to `limit` assets changed after `since`, oldest change first, as JSON objects, and the cursor after them.
    ///
    /// # Errors
    ///
    /// Fails when the library cannot be copied or read.
    pub(crate) fn assets(&self, since: &Cursor, limit: usize) -> Result<(Vec<Value>, Cursor)> {
        if !tri!(exists(self.db)) {
            return Ok((Vec::new(), since.clone()));
        }
        let copy = tri!(open_copy(self.db));
        let asset = tri!(columns(&copy.conn, "ZASSET"));
        let additional = tri!(columns(&copy.conn, "ZADDITIONALASSETATTRIBUTES"));
        let extended = tri!(columns(&copy.conn, "ZEXTENDEDATTRIBUTES"));
        let pairs: Vec<String> = FIELDS
            .iter()
            .map(|&(key, alias, name)| {
                let have = match alias {
                    "a" => &asset,
                    "x" => &additional,
                    _ => &extended,
                };
                format!("'{key}', {}", col(have, alias, name))
            })
            .collect();
        let sql = format!(
            "SELECT coalesce(a.ZMODIFICATIONDATE, a.ZADDEDDATE, 0) AS m, a.ZUUID, json_object({})
             FROM ZASSET a
             LEFT JOIN ZADDITIONALASSETATTRIBUTES x ON x.ZASSET = a.Z_PK
             LEFT JOIN ZEXTENDEDATTRIBUTES e ON e.ZASSET = a.Z_PK
             WHERE m > ?1 OR (m = ?1 AND a.ZUUID > ?2)
             ORDER BY m, a.ZUUID LIMIT ?3",
            pairs.join(", ")
        );
        let cap = tri!(i64::try_from(limit));
        let mut stmt = tri!(copy.conn.prepare(&sql));
        let rows = tri!(stmt.query_map(params![since.0, since.1, cap], |row| {
            Ok((
                tri!(row.get::<_, f64>(0)),
                tri!(row.get::<_, String>(1)),
                tri!(row.get::<_, String>(2)),
            ))
        }));
        let mut out = Vec::new();
        let mut cursor = since.clone();
        for row in rows {
            let (modified, uuid, json) = tri!(row);
            out.push(tri!(serde_json::from_str(&json)));
            cursor = (modified, uuid);
        }
        Ok((out, cursor))
    }

    /// What Photos' on-device analysis found in each asset, for up to `limit` assets whose findings changed.
    ///
    /// Labels come from the search index `search/psi.sqlite` next to `db`, as `[category, text]`: scenes, objects,
    /// plants and animals, landmarks, places, events, text read from the image (one word each), and more; the
    /// categories are undocumented. People are `[person uuid, name]` per detected face, with nulls for a face not
    /// grouped into a person or a person without a name. Returns the assets and `sent` updated.
    ///
    /// # Errors
    ///
    /// Fails when the library or the search index cannot be copied or read.
    pub(crate) fn labels(&self, sent: &Sent, limit: usize) -> Result<(Vec<Value>, Sent)> {
        let psi = self.db.with_file_name("search").join("psi.sqlite");
        if !tri!(exists(self.db)) || !tri!(exists(&psi)) {
            return Ok((Vec::new(), sent.clone()));
        }
        let mut found: BTreeMap<String, (Vec<Label>, Vec<Face>)> = BTreeMap::new();

        let index = tri!(open_copy(&psi));
        let mut groups = tri!(index.conn.prepare(
            "SELECT a.uuid_0, a.uuid_1, g.category, g.content_string
             FROM ga JOIN assets a ON a.rowid = ga.assetid JOIN groups g ON g.rowid = ga.groupid
             WHERE g.content_string <> '' ORDER BY 1, 2, 3, 4",
        ));
        let labelled = tri!(groups.query_map([], |row| {
            Ok((
                tri!(row.get::<_, i64>(0)),
                tri!(row.get::<_, i64>(1)),
                tri!(row.get::<_, i64>(2)),
                tri!(row.get::<_, String>(3)),
            ))
        }));
        for row in labelled {
            let (low, high, category, raw) = tri!(row);
            // The index ends its strings with a NUL, and some are nothing else.
            let text = raw.trim_end_matches('\0');
            // Left out: dates and seasons (1100-1109), media types (1900-1999), file names (2100), cameras (2300)
            // and photographic styles (2900), which the asset fields already hold, and "Text Found" (1205), which
            // the text words imply.
            let skipped = matches!(category, 1100..=1109 | 1205 | 1900..=1999 | 2100 | 2300 | 2900);
            if !skipped && !text.is_empty() {
                // The index stores an asset's uuid as its 16 bytes read as two little-endian signed integers, so
                // the bytes of each come back in uuid order once swapped.
                let hex = format!(
                    "{:016X}{:016X}",
                    low.cast_unsigned().swap_bytes(),
                    high.cast_unsigned().swap_bytes()
                );
                let mut uuid = String::with_capacity(36);
                for (position, digit) in hex.char_indices() {
                    if matches!(position, 8 | 12 | 16 | 20) {
                        uuid.push('-');
                    }
                    uuid.push(digit);
                }
                found
                    .entry(uuid)
                    .or_default()
                    .0
                    .push((category, text.to_owned()));
            }
        }

        let library = tri!(open_copy(self.db));
        let mut faces = tri!(library.conn.prepare(
            "SELECT a.ZUUID, p.ZPERSONUUID, coalesce(nullif(p.ZFULLNAME, ''), nullif(p.ZDISPLAYNAME, ''))
             FROM ZDETECTEDFACE f JOIN ZASSET a ON a.Z_PK = f.ZASSETFORFACE LEFT JOIN ZPERSON p ON p.Z_PK = f.ZPERSONFORFACE
             ORDER BY 1, 2",
        ));
        let detected = tri!(faces.query_map([], |row| {
            Ok((
                tri!(row.get::<_, String>(0)),
                tri!(row.get(1)),
                tri!(row.get(2)),
            ))
        }));
        for row in detected {
            let (uuid, person, name) = tri!(row);
            found.entry(uuid).or_default().1.push((person, name));
        }

        let mut next = sent.clone();
        let mut out = Vec::new();
        for (uuid, (labels, people)) in found {
            if out.len() == limit {
                break;
            }
            let value = json!({"uuid": uuid, "labels": labels, "people": people});
            let crc = crc32fast::hash(value.to_string().as_bytes());
            if sent.get(&uuid) != Some(&crc) {
                let _: Option<u32> = next.insert(uuid, crc);
                out.push(value);
            }
        }
        Ok((out, next))
    }
}
