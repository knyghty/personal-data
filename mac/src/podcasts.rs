//! Read the play state of Apple Podcasts episodes from the app's library database, which iCloud keeps in step with
//! the iPhone: what was played, how far, how often and when.
//!
//! Only episodes with some activity (played, saved, bookmarked or downloaded) are sent, and an episode is sent
//! again when any of its fields change.

#[cfg(test)]
mod tests;

use alloc::collections::BTreeMap;
use std::path::Path;

use anyhow::Result;
use serde_json::Value;

use crate::apple::open_copy;
use crate::browser::{col, columns, exists};
use crate::tri;

/// Column and the condition on it that counts as activity; a column this version lacks is left out.
const ACTIVITY: &[(&str, &str)] = &[
    ("ZPLAYCOUNT", "> 0"),
    ("ZPLAYHEAD", "> 0"),
    ("ZPLAYSTATE", "<> 0"),
    ("ZLASTDATEPLAYED", "IS NOT NULL"),
    ("ZSAVED", "= 1"),
    ("ZISBOOKMARKED", "= 1"),
    ("ZDOWNLOADDATE", "IS NOT NULL"),
];

/// Output key and column per table alias: `e` ZMTEPISODE, `p` ZMTPODCAST. Times are seconds since 2001.
const FIELDS: &[(&str, &str, &str)] = &[
    ("uuid", "e", "ZUUID"),
    ("store_id", "e", "ZSTORETRACKID"),
    ("title", "e", "ZTITLE"),
    ("author", "e", "ZAUTHOR"),
    ("published", "e", "ZPUBDATE"),
    ("duration", "e", "ZDURATION"),
    ("playhead", "e", "ZPLAYHEAD"),
    ("play_count", "e", "ZPLAYCOUNT"),
    ("play_state", "e", "ZPLAYSTATE"),
    ("play_state_source", "e", "ZPLAYSTATESOURCE"),
    ("play_state_manually_set", "e", "ZPLAYSTATEMANUALLYSET"),
    ("play_state_modified", "e", "ZPLAYSTATELASTMODIFIEDDATE"),
    ("last_played", "e", "ZLASTDATEPLAYED"),
    ("marked_played", "e", "ZLASTUSERMARKEDASPLAYEDDATE"),
    ("saved", "e", "ZSAVED"),
    ("bookmarked", "e", "ZISBOOKMARKED"),
    ("downloaded", "e", "ZDOWNLOADDATE"),
    ("up_next_added", "e", "ZUPNEXTADDEDDATE"),
    ("user_deleted", "e", "ZUSERDELETED"),
    ("episode_type", "e", "ZEPISODETYPE"),
    ("episode_number", "e", "ZEPISODENUMBER"),
    ("season_number", "e", "ZSEASONNUMBER"),
    ("video", "e", "ZVIDEO"),
    ("enclosure_url", "e", "ZENCLOSUREURL"),
    ("web_url", "e", "ZWEBPAGEURL"),
    ("podcast_uuid", "e", "ZPODCASTUUID"),
    ("podcast", "p", "ZTITLE"),
    ("podcast_author", "p", "ZAUTHOR"),
    ("podcast_store_id", "p", "ZSTORECOLLECTIONID"),
    ("feed_url", "p", "ZFEEDURL"),
    ("podcast_web_url", "p", "ZWEBPAGEURL"),
    ("subscribed", "p", "ZSUBSCRIBED"),
    ("podcast_added", "p", "ZADDEDDATE"),
];

/// The Apple Podcasts library database.
pub(super) struct Podcasts<'db> {
    /// `MTLibrary.sqlite` under the app's group container.
    pub db: &'db Path,
}

/// Per episode uuid, the CRC of its fields as last sent.
pub(super) type Sent = BTreeMap<String, u32>;

impl Podcasts<'_> {
    /// Up to `limit` episodes with activity whose fields changed since `sent`, as JSON objects, and `sent` updated.
    ///
    /// # Errors
    ///
    /// Fails when the library cannot be copied or read.
    pub(crate) fn episodes(&self, sent: &Sent, limit: usize) -> Result<(Vec<Value>, Sent)> {
        if !tri!(exists(self.db)) {
            return Ok((Vec::new(), sent.clone()));
        }
        let copy = tri!(open_copy(self.db));
        let episode = tri!(columns(&copy.conn, "ZMTEPISODE"));
        let podcast = tri!(columns(&copy.conn, "ZMTPODCAST"));
        let pairs: Vec<String> = FIELDS
            .iter()
            .map(|&(key, alias, name)| {
                let have = if alias == "e" { &episode } else { &podcast };
                format!("'{key}', {}", col(have, alias, name))
            })
            .collect();
        let active: Vec<String> = ACTIVITY
            .iter()
            .filter(|&&(name, _)| episode.iter().any(|have| have == name))
            .map(|&(name, test)| format!("e.{name} {test}"))
            .collect();
        if active.is_empty() {
            return Ok((Vec::new(), sent.clone()));
        }
        let sql = format!(
            "SELECT e.ZUUID, json_object({}) FROM ZMTEPISODE e LEFT JOIN ZMTPODCAST p ON p.Z_PK = e.ZPODCAST
             WHERE e.ZUUID IS NOT NULL AND ({}) ORDER BY e.ZUUID",
            pairs.join(", "),
            active.join(" OR ")
        );
        let mut stmt = tri!(copy.conn.prepare(&sql));
        let rows = tri!(stmt.query_map([], |row| {
            Ok((tri!(row.get::<_, String>(0)), tri!(row.get::<_, String>(1))))
        }));
        let mut next = sent.clone();
        let mut out = Vec::new();
        for row in rows {
            if out.len() == limit {
                break;
            }
            let (uuid, json) = tri!(row);
            let crc = crc32fast::hash(json.as_bytes());
            if sent.get(&uuid) != Some(&crc) {
                let _: Option<u32> = next.insert(uuid, crc);
                out.push(tri!(serde_json::from_str(&json)));
            }
        }
        Ok((out, next))
    }
}
