//! Read the Obsidian vault, which iCloud keeps in step with the iPhone, as metadata only. The text never leaves
//! the Mac.
//!
//! Each note is sent as its path, created and modified times, size, word and line counts, tags and wiki links. A
//! note is sent again when any of those change, compared by CRC in the state file, and once more as deleted when it
//! is gone. Hidden files and directories (`.obsidian`, `.trash`, iCloud placeholders) are skipped.

#[cfg(test)]
mod tests;

use alloc::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context as _, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};

use crate::tri;

/// Punctuation that ends an inline tag without being part of it.
const TAG_END: &[char] = &['.', ',', ';', ':', '!', '?', ')', ']', '"', '\''];

/// One file of the vault, before it is reduced to its metadata.
struct Note<'file> {
    /// The file.
    path: &'file Path,
    /// Its path within the vault, with `/` separators.
    relative: &'file str,
}

impl Note<'_> {
    /// The metadata as sent: path, folder, name, extension, times, size, counts, tags and links.
    ///
    /// Tags are `#word` tokens of the body and the `tags` of the front matter; links are the targets of `[[...]]`,
    /// without alias, heading or block. A file that is not UTF-8 text has null counts and no tags or links.
    fn row(&self) -> Result<Value> {
        let meta =
            tri!(fs::metadata(self.path).with_context(|| format!("stat {}", self.path.display())));
        let modified = rfc3339(tri!(meta.modified()));
        let created = meta.created().ok().map(rfc3339);
        let (folder, name) = self
            .relative
            .rsplit_once('/')
            .map_or(("", self.relative), |(folder, name)| (folder, name));
        let (stem, ext) = name
            .rsplit_once('.')
            .filter(|&(stem, _)| !stem.is_empty())
            .map_or((name, ""), |(stem, ext)| (stem, ext));
        let text = fs::read_to_string(self.path).ok();
        let (front, body) = text.as_deref().map_or((None, None), |whole| {
            let (front, rest) = Text(whole).split_front_matter();
            (front, Some(rest))
        });
        let mut tags = BTreeSet::new();
        let mut links = BTreeSet::new();
        if let Some(matter) = front {
            matter.front_matter_tags(&mut tags);
        }
        if ext == "md"
            && let Some(markdown) = body
        {
            tags.extend(markdown.inline_tags());
            links.extend(markdown.wiki_links());
        }
        Ok(json!({
            "path": self.relative,
            "folder": folder,
            "name": stem,
            "ext": ext,
            "created": created,
            "modified": modified,
            "bytes": meta.len(),
            "words": body.map(|rest| rest.0.split_whitespace().count()),
            "lines": body.map(|rest| rest.0.lines().count()),
            "tags": tags,
            "links": links,
        }))
    }
}

/// Per vault path, the CRC of its row as last sent.
pub(super) type Sent = BTreeMap<String, u32>;

/// The text of a note, or a part of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Text<'note>(pub &'note str);

impl<'note> Text<'note> {
    /// Add the `tags` of a front matter block to `tags`: `tags: [a, b]`, `tags: a, b` or a `- a` list under
    /// `tags:`.
    fn front_matter_tags(self, tags: &mut BTreeSet<String>) {
        let mut in_list = false;
        for line in self.0.lines() {
            if let Some(rest) = line
                .strip_prefix("tags:")
                .or_else(|| line.strip_prefix("tag:"))
            {
                let inline = rest.trim().trim_start_matches('[').trim_end_matches(']');
                tags.extend(
                    inline
                        .split(',')
                        .map(|tag| tag.trim().trim_matches('"').trim_start_matches('#'))
                        .filter(|tag| !tag.is_empty())
                        .map(str::to_owned),
                );
                in_list = inline.is_empty();
            } else if in_list && let Some(item) = line.trim_start().strip_prefix("- ") {
                let tag = item.trim().trim_matches('"').trim_start_matches('#');
                if !tag.is_empty() {
                    let _: bool = tags.insert(tag.to_owned());
                }
            } else {
                in_list = false;
            }
        }
    }

    /// The `#word` tokens that Obsidian reads as tags: a `#` followed by a letter, headings excluded.
    fn inline_tags(self) -> impl Iterator<Item = String> + 'note {
        self.0.split_whitespace().filter_map(|word| {
            word.strip_prefix('#')
                .map(|tag| tag.trim_end_matches(TAG_END))
                .filter(|tag| tag.chars().next().is_some_and(char::is_alphabetic))
                .map(str::to_owned)
        })
    }

    /// The front matter between the `---` lines at the top, when there is one, and the rest.
    pub(crate) fn split_front_matter(self) -> (Option<Self>, Self) {
        let Some(after_open) = self
            .0
            .strip_prefix("---\n")
            .or_else(|| self.0.strip_prefix("---\r\n"))
        else {
            return (None, self);
        };
        let Some(end) = after_open
            .find("\n---\n")
            .or_else(|| after_open.find("\n---\r\n"))
            .or_else(|| after_open.strip_suffix("\n---").map(str::len))
        else {
            return (None, self);
        };
        let (front, rest) = after_open.split_at(end);
        let body = rest
            .strip_prefix("\n---\n")
            .or_else(|| rest.strip_prefix("\n---\r\n"))
            .unwrap_or_default();
        (Some(Self(front.trim_end_matches('\r'))), Self(body))
    }

    /// The targets of the `[[...]]` links, without `|alias`, `#heading` or `^block`.
    fn wiki_links(self) -> impl Iterator<Item = String> + 'note {
        self.0.split("[[").skip(1).filter_map(|rest| {
            rest.split_once("]]")
                .map(|(inner, _)| {
                    inner
                        .split(['|', '#', '^'])
                        .next()
                        .unwrap_or_default()
                        .trim()
                        .to_owned()
                })
                .filter(|target| !target.is_empty())
        })
    }
}

/// The Obsidian vault.
pub(super) struct Vault<'dir> {
    /// The vault directory, the one Obsidian opens.
    pub dir: &'dir Path,
}

impl Vault<'_> {
    /// Every file in the vault, sorted, skipping directories and files whose name starts with a dot.
    fn files(&self) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        let mut dirs = vec![self.dir.to_path_buf()];
        while let Some(dir) = dirs.pop() {
            for item in tri!(fs::read_dir(&dir).with_context(|| format!("read {}", dir.display())))
            {
                let entry = tri!(item);
                if entry.file_name().to_string_lossy().starts_with('.') {
                    continue;
                }
                let path = entry.path();
                if tri!(entry.file_type()).is_dir() {
                    dirs.push(path);
                } else {
                    files.push(path);
                }
            }
        }
        files.sort();
        Ok(files)
    }

    /// Up to `limit` rows for notes whose metadata changed since `sent`, then for notes that are gone, and `sent`
    /// updated.
    ///
    /// # Errors
    ///
    /// Fails when the vault or a note cannot be read.
    pub(crate) fn notes(&self, sent: &Sent, limit: usize) -> Result<(Vec<Value>, Sent)> {
        if !self.dir.is_dir() {
            return Ok((Vec::new(), sent.clone()));
        }
        let files = tri!(self.files());
        let mut present = BTreeSet::new();
        let mut next = sent.clone();
        let mut out = Vec::new();
        for path in &files {
            let relative = tri!(path.strip_prefix(self.dir))
                .components()
                .map(|part| part.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            let _: bool = present.insert(relative.clone());
            if out.len() == limit {
                continue;
            }
            let row = tri!(
                Note {
                    path,
                    relative: &relative,
                }
                .row()
            );
            let crc = crc32fast::hash(row.to_string().as_bytes());
            if sent.get(&relative) != Some(&crc) {
                let _: Option<u32> = next.insert(relative, crc);
                out.push(row);
            }
        }
        for gone in sent.keys().filter(|path| !present.contains(*path)) {
            if out.len() == limit {
                break;
            }
            let _: Option<u32> = next.remove(gone);
            out.push(json!({"path": gone, "deleted": true}));
        }
        Ok((out, next))
    }
}

/// `time` in RFC 3339, whole seconds, UTC.
fn rfc3339(time: SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339_opts(SecondsFormat::Secs, true)
}
