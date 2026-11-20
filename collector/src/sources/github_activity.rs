//! Pull one user's GitHub activity: the event feed hourly, plus authored commits, the contributions collection,
//! issues, pull requests, comments, discussions, stars, gists and follows daily.
//!
//! Activity in public repos and in the user's own repos is stored in full. Activity in other private repos, such as
//! an employer's, keeps only its times, kinds and counts: no repo name, title, number, URL, body, message or SHA is stored.

#[cfg(test)]
mod tests;

use alloc::sync::Arc;
use core::time::Duration as Pause;
use std::collections::HashMap;

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Duration, FixedOffset, SecondsFormat, Utc};
use reqwest::RequestBuilder;
use serde::Deserialize;
use serde_json::value::{RawValue, to_raw_value};
use serde_json::{Value, json};
use tokio::time::sleep;
use tracing::{info, warn};
use url::Url;

use super::Source;
use super::steam::changed;
use crate::app::App;
use crate::storage::{self, File, windows};
use crate::tri;

/// Kind, `user` connection and node fields of each type paged in `updatedAt` order.
const CONNECTIONS: [(&str, &str, &str); 4] = [
    (
        "issue_comment",
        "issueComments",
        "databaseId createdAt updatedAt url body issue { number title url } pullRequest { number } repository { nameWithOwner isPrivate }",
    ),
    (
        "issue",
        "issues",
        "databaseId number title url state stateReason createdAt updatedAt closedAt body repository { nameWithOwner isPrivate }",
    ),
    (
        "pull_request",
        "pullRequests",
        "databaseId number title url state isDraft createdAt updatedAt closedAt mergedAt additions deletions changedFiles body repository { nameWithOwner isPrivate }",
    ),
    (
        "discussion",
        "repositoryDiscussions",
        "databaseId number title url createdAt updatedAt closedAt answerChosenAt category { name } body repository { nameWithOwner isPrivate }",
    ),
];
/// GraphQL pages of 100 per type per run; each costs one point of 5,000 an hour.
const CONNECTION_PAGES_PER_RUN: usize = 40;
/// The contributions collection of one window.
// ponytail: first 100 nodes per connection per 30-day window; `totalCount` in the reply shows when that cuts.
const CONTRIBUTIONS_QUERY: &str = "query($login: String!, $from: DateTime!, $to: DateTime!) {
  user(login: $login) {
    contributionsCollection(from: $from, to: $to) {
      startedAt endedAt hasAnyRestrictedContributions restrictedContributionsCount
      totalCommitContributions totalPullRequestContributions totalPullRequestReviewContributions
      totalIssueContributions totalRepositoryContributions
      contributionCalendar { totalContributions weeks { contributionDays { date contributionCount } } }
      commitContributionsByRepository(maxRepositories: 100) {
        repository { nameWithOwner isPrivate }
        contributions(first: 100) { totalCount nodes { occurredAt commitCount } }
      }
      pullRequestContributions(first: 100) {
        totalCount
        nodes { occurredAt pullRequest { number title url createdAt mergedAt closedAt additions deletions changedFiles repository { nameWithOwner isPrivate } } }
      }
      pullRequestReviewContributions(first: 100) {
        totalCount
        nodes { occurredAt pullRequestReview { url state submittedAt } pullRequest { number url repository { nameWithOwner isPrivate } } }
      }
      issueContributions(first: 100) {
        totalCount
        nodes { occurredAt issue { number title url createdAt closedAt repository { nameWithOwner isPrivate } } }
      }
      repositoryContributions(first: 100) { totalCount nodes { occurredAt repository { nameWithOwner isPrivate } } }
    }
  }
}";
/// Contributions count on the author date, so re-fetch the last week.
const CONTRIBUTIONS_REFETCH_DAYS: i64 = 7;
/// Thirty-day contribution windows per run.
const CONTRIBUTIONS_WINDOWS_PER_RUN: usize = 60;
/// When the account was created, to start the contributions backfill from.
const CREATED_QUERY: &str = "query($login: String!) { user(login: $login) { createdAt } }";
/// The user's discussion comments.
// ponytail: first 100 only, and the connection has no order to page by; the user has a handful. Page if `totalCount` passes 100.
const DISCUSSION_COMMENTS_QUERY: &str = "query($login: String!) {
  user(login: $login) {
    repositoryDiscussionComments(first: 100) {
      totalCount
      nodes { databaseId createdAt updatedAt url body isAnswer discussion { number title url repository { nameWithOwner isPrivate } } }
    }
  }
}";
/// The event feed serves at most 300 events, in pages of 100.
const EVENT_PAGES: usize = 3;
/// Fields of a GraphQL node that identify a repo or what was done in it.
const IDENTIFYING: [&str; 6] = ["nameWithOwner", "title", "url", "number", "body", "name"];
/// The REST and GraphQL media type.
const JSON: &str = "application/vnd.github+json";
/// Attempts per request when GitHub answers 5xx, waiting 5 s, then 10 s, between them.
const RETRIES: u64 = 3;
/// The pause after a 5xx, multiplied by the attempt.
const RETRY_PAUSE_MS: u64 = if cfg!(test) { 0 } else { 5000 };
/// Re-search this far before the newest commit, for commits that land on the default branch after their author date.
const SEARCH_LOOKBACK_DAYS: i64 = 14;
/// Search allows 30 requests a minute.
const SEARCH_PACE_MS: u64 = if cfg!(test) { 0 } else { 2100 };
/// Commit search serves at most 10 pages of 100 per query.
const SEARCH_PAGES: usize = 10;
/// Commit search requests per run.
const SEARCH_REQUESTS_PER_RUN: usize = 30;
/// Directory name under `raw/`.
const SOURCE: &str = "github";
/// Adds `starred_at` to each starred repo.
const STAR_JSON: &str = "application/vnd.github.star+json";

/// A follower or followed account.
#[derive(Deserialize)]
struct Account {
    /// The login.
    login: String,
}

/// The author signature of a searched commit.
#[derive(Deserialize)]
struct CommitInner {
    /// Who wrote it, and when.
    author: Signature,
}

/// What a searched commit is and where it is.
#[derive(Deserialize)]
struct CommitMeta {
    /// The commit itself.
    commit: CommitInner,
    /// The repo it is in.
    repository: SearchRepo,
    /// The SHA, to recognise a commit seen before.
    sha: String,
}

/// What an event is and where it happened.
#[derive(Deserialize)]
struct EventMeta {
    /// The id, which grows over time.
    id: String,
    /// Missing counts as private, so an unexpected shape is redacted rather than kept.
    #[serde(default)]
    public: bool,
    /// The repo it happened in.
    repo: RepoMeta,
}

/// A GitHub API client for one user.
struct Gh<'app> {
    /// The REST and GraphQL base URL.
    api: &'app str,
    /// The collector.
    app: &'app App,
    /// The data repo. The collector commits to it as the token owner, which would swamp everything else.
    exclude: &'app str,
    /// The personal access token.
    token: &'app str,
    /// The login whose activity is pulled.
    user: &'app str,
}

/// The GitHub activity source.
pub(super) struct GitHubActivity;

/// The repo of an event.
#[derive(Deserialize)]
struct RepoMeta {
    /// `owner/name`.
    name: String,
}

/// A page of commit search results.
#[derive(Deserialize)]
struct SearchPage {
    /// The commits, verbatim.
    items: Vec<Box<RawValue>>,
    /// How many commits match in all.
    total_count: u64,
}

/// The repo of a searched commit.
#[derive(Deserialize)]
struct SearchRepo {
    /// `owner/name`.
    full_name: String,
    /// Missing counts as private, as for events.
    private: Option<bool>,
}

/// Who did something, and when.
#[derive(Deserialize)]
struct Signature {
    /// The author date, with its offset.
    date: DateTime<FixedOffset>,
}

/// Files of one kind, and the cursors and hashes to store once they are committed.
type Stage = (Vec<File>, Vec<(String, Value)>);

impl<'app> Gh<'app> {
    /// Drop GraphQL errors, which can name an organisation, null nodes hidden from the token and nodes in the data
    /// repo, and redact the rest. Return the number of nodes left.
    fn clean(&self, reply: &mut Value, field: &str) -> usize {
        if let Some(obj) = reply.as_object_mut() {
            drop(obj.remove("errors"));
        }
        let Some(items) = reply
            .pointer_mut(&format!("/data/user/{field}/nodes"))
            .and_then(Value::as_array_mut)
        else {
            return 0;
        };
        items.retain(|node| {
            !node.is_null() && !mentions(node, &|name, _private| name == self.exclude)
        });
        self.redact_nodes(items);
        items.len()
    }

    /// Search commits authored by the user in ascending author date, and keep the ones not seen before.
    async fn commits(&self, now: DateTime<Utc>) -> Result<Stage> {
        let cursor: DateTime<Utc> = match self.app.store.get("github/cursor/commit") {
            Some(stored) => stored,
            None => tri!("2008-01-01T00:00:00Z".parse()),
        };
        let mut seen: HashMap<String, DateTime<Utc>> =
            self.app.store.get("github/seen/commit").unwrap_or_default();
        let mut newest = cursor;
        // ponytail: commits that reach the default branch over 14 days after their author date are missed; widen the lookback if that matters.
        let mut since = tri!(
            cursor
                .checked_sub_signed(Duration::days(SEARCH_LOOKBACK_DAYS))
                .context("search lookback")
        );
        let mut files = Vec::new();
        let mut requests = 0_usize;
        'queries: loop {
            let query = format!(
                "author:{} -repo:{} author-date:>={}",
                self.user,
                self.exclude,
                since.format("%Y-%m-%dT%H:%M:%SZ")
            );
            for page in 1..=SEARCH_PAGES {
                if requests == SEARCH_REQUESTS_PER_RUN {
                    break 'queries;
                }
                if requests > 0 {
                    sleep(Pause::from_millis(SEARCH_PACE_MS)).await;
                }
                requests = requests.saturating_add(1);
                let url = tri!(Url::parse_with_params(
                    &format!("{}/search/commits", self.api),
                    [
                        ("q", query.as_str()),
                        ("sort", "author-date"),
                        ("order", "asc"),
                        ("per_page", "100"),
                        ("page", page.to_string().as_str()),
                    ],
                ));
                let text = match self.get(&url).await {
                    Ok(text) => text,
                    // Results come oldest first, so keep what this run found and go on from there next run.
                    Err(error) if error.to_string().contains("rate limit") => {
                        warn!("github: commit search rate-limited, stopping for this run");
                        break 'queries;
                    }
                    Err(error) => return Err(error),
                };
                let result: SearchPage =
                    tri!(serde_json::from_str(&text).context("parse commit search"));
                let full = result.items.len() == 100;
                let fresh = tri!(self.fresh_commits(result.items, &mut seen, &mut newest));
                if !fresh.is_empty() {
                    files.push(File {
                        path: storage::path(
                            SOURCE,
                            "commit",
                            now,
                            Some(&requests.to_string()),
                            "json",
                        ),
                        content: tri!(storage::envelope(
                            now,
                            json!({"q": query, "sort": "author-date", "order": "asc", "page": page, "total_count": result.total_count, "commits": fresh.len()}),
                            &tri!(serde_json::to_string(&fresh)),
                        )),
                    });
                }
                if !full {
                    break 'queries;
                }
            }
            // Search stops at 1000 results; go on from the newest author date.
            since = newest;
        }
        let keep_from = tri!(
            newest
                .checked_sub_signed(Duration::days(SEARCH_LOOKBACK_DAYS))
                .context("seen window")
        );
        seen.retain(|_sha, at| *at >= keep_from);
        let state = vec![
            ("github/cursor/commit".to_owned(), json!(newest)),
            ("github/seen/commit".to_owned(), json!(seen)),
        ];
        Ok((files, state))
    }

    /// Page a `user` connection in ascending `updatedAt` from the stored cursor, one file per page. The cursor
    /// encodes the last node's `updatedAt` and id, so later runs fetch what was created or changed since, such as a
    /// pull request merged after it was opened, and an edit does not shift it.
    async fn connection(
        &self,
        now: DateTime<Utc>,
        kind: &str,
        field: &str,
        nodes: &str,
    ) -> Result<Stage> {
        let key = format!("github/cursor/{kind}");
        let mut after: Option<String> = self.app.store.get(&key);
        let query = format!(
            "query($login: String!, $after: String) {{ user(login: $login) {{ {field}(first: 100, after: $after, \
             orderBy: {{field: UPDATED_AT, direction: ASC}}) {{ totalCount pageInfo {{ endCursor hasNextPage }} nodes {{ {nodes} }} }} }} }}"
        );
        let mut files = Vec::new();
        for page in 1..=CONNECTION_PAGES_PER_RUN {
            let request = json!({"login": self.user, "after": after});
            let text = tri!(self.graphql(&query, &request).await);
            let mut reply: Value = tri!(serde_json::from_str(&text));
            let info = reply
                .pointer(&format!("/data/user/{field}/pageInfo"))
                .cloned()
                .unwrap_or(Value::Null);
            if self.clean(&mut reply, field) > 0 {
                files.push(File {
                    path: storage::path(SOURCE, kind, now, Some(&page.to_string()), "json"),
                    content: tri!(storage::envelope(
                        now,
                        request,
                        &tri!(serde_json::to_string(&reply))
                    )),
                });
            }
            if let Some(end) = info.get("endCursor").and_then(Value::as_str) {
                after = Some(end.to_owned());
            }
            if info.get("hasNextPage") != Some(&Value::Bool(true)) {
                break;
            }
        }
        Ok((files, vec![(key, json!(after))]))
    }

    /// One file per 30-day window from the stored cursor, or from account creation, and always the last week.
    async fn contributions(&self, now: DateTime<Utc>) -> Result<Stage> {
        let cursor: Option<DateTime<Utc>> = self.app.store.get("github/cursor/contributions");
        let refetch_from = tri!(
            now.checked_sub_signed(Duration::days(CONTRIBUTIONS_REFETCH_DAYS))
                .context("refetch window")
        );
        let start = if let Some(stored) = cursor {
            stored.min(refetch_from)
        } else {
            let text = tri!(
                self.graphql(CREATED_QUERY, &json!({"login": self.user}))
                    .await
            );
            let reply: Value = tri!(serde_json::from_str(&text));
            let created = tri!(
                reply
                    .pointer("/data/user/createdAt")
                    .and_then(Value::as_str)
                    .with_context(|| format!("no GitHub user {}", self.user))
            );
            tri!(created.parse::<DateTime<Utc>>())
        };
        let wins = windows(
            start,
            now,
            Duration::days(30),
            CONTRIBUTIONS_WINDOWS_PER_RUN,
        );
        let mut files = Vec::new();
        for &(from, to) in &wins {
            let request = json!({"login": self.user, "from": from, "to": to});
            let text = tri!(self.graphql(CONTRIBUTIONS_QUERY, &request).await);
            let mut reply: Value = tri!(serde_json::from_str(&text));
            // Errors, such as SAML enforcement, name the organisation; graphql() has logged them.
            if let Some(obj) = reply.as_object_mut() {
                drop(obj.remove("errors"));
            }
            redact_contributions(&mut reply, self.user);
            files.push(File {
                path: storage::path(SOURCE, "contributions", from, None, "json"),
                content: tri!(storage::envelope(
                    now,
                    request,
                    &tri!(serde_json::to_string(&reply))
                )),
            });
        }
        let state = wins
            .last()
            .map(|window| ("github/cursor/contributions".to_owned(), json!(window.1)));
        Ok((files, state.into_iter().collect()))
    }

    /// The user's discussion comments, stored when they changed.
    async fn discussion_comments(&self, now: DateTime<Utc>) -> Result<Stage> {
        let request = json!({"login": self.user});
        let text = tri!(self.graphql(DISCUSSION_COMMENTS_QUERY, &request).await);
        let mut reply: Value = tri!(serde_json::from_str(&text));
        let _left: usize = self.clean(&mut reply, "repositoryDiscussionComments");
        let cleaned = tri!(serde_json::to_string(&reply));
        let key = "github/sha/discussion_comment";
        let Some(sha) = changed(self.app, key, &cleaned) else {
            return Ok(Stage::default());
        };
        let file = File {
            path: storage::path(SOURCE, "discussion_comment", now, None, "json"),
            content: tri!(storage::envelope(now, request, &cleaned)),
        };
        Ok((vec![file], vec![(key.to_owned(), json!(sha))]))
    }

    /// Commit the events newer than the stored id, except those in the data repo.
    async fn events(&self) -> Result<()> {
        let now = Utc::now();
        let cursor: Option<u64> = self.app.store.get("github/cursor/event");
        let mut newest = cursor;
        let mut fresh: Vec<Box<RawValue>> = Vec::new();
        let mut pages = 0_usize;
        let mut reached = false;
        for page in 1..=EVENT_PAGES {
            let url = tri!(Url::parse_with_params(
                &format!("{}/users/{}/events", self.api, self.user),
                [("per_page", "100"), ("page", page.to_string().as_str())],
            ));
            let text = tri!(self.get(&url).await);
            let events: Vec<Box<RawValue>> =
                tri!(serde_json::from_str(&text).context("parse events"));
            pages = page;
            let full = events.len() == 100;
            for event in events {
                let meta: EventMeta =
                    tri!(serde_json::from_str(event.get()).context("parse event"));
                let id: u64 = tri!(meta.id.parse().context("event id"));
                if cursor.is_some_and(|stored| id <= stored) {
                    reached = true;
                    continue;
                }
                newest = newest.max(Some(id));
                if meta.repo.name == self.exclude {
                    continue;
                }
                fresh.push(tri!(self.keep_event(&meta.repo, meta.public, event)));
            }
            if reached || !full {
                break;
            }
        }
        if cursor.is_some() && !reached && pages == EVENT_PAGES {
            warn!("github: over 300 events since the last run, older ones are lost");
        }
        if !fresh.is_empty() {
            let file = File {
                path: storage::path(SOURCE, "event", now, None, "json"),
                content: tri!(storage::envelope(
                    now,
                    json!({"path": format!("/users/{}/events", self.user), "pages": pages, "after_id": cursor, "excluded_repo": self.exclude, "events": fresh.len()}),
                    &tri!(serde_json::to_string(&fresh)),
                )),
            };
            drop(tri!(
                self.app
                    .save(&[file], &format!("github: event ({})", fresh.len()))
                    .await
            ));
        }
        if newest != cursor {
            tri!(self.app.store.set("github/cursor/event", &newest));
        }
        Ok(())
    }

    /// The sorted logins of the user's `followers` or `following`, stored when the set changed.
    async fn follows(&self, now: DateTime<Utc>, kind: &str) -> Result<Stage> {
        let path = format!("/users/{}/{kind}", self.user);
        let mut logins = Vec::new();
        for page in 1_u32.. {
            let url = tri!(Url::parse_with_params(
                &format!("{}{path}", self.api),
                [("per_page", "100"), ("page", page.to_string().as_str())],
            ));
            let text = tri!(self.get(&url).await);
            let accounts: Vec<Account> =
                tri!(serde_json::from_str(&text).with_context(|| format!("parse {kind}")));
            let full = accounts.len() == 100;
            logins.extend(accounts.into_iter().map(|account| account.login));
            if !full {
                break;
            }
        }
        logins.sort_unstable();
        let text = tri!(serde_json::to_string(&logins));
        let key = format!("github/sha/{kind}");
        let Some(sha) = changed(self.app, &key, &text) else {
            return Ok(Stage::default());
        };
        let file = File {
            path: storage::path(SOURCE, kind, now, None, "json"),
            content: tri!(storage::envelope(now, json!({"path": path}), &text)),
        };
        Ok((vec![file], vec![(key, json!(sha))]))
    }

    /// The commits of a search page not seen before, redacted where needed; move `newest` to the newest author date.
    fn fresh_commits(
        &self,
        items: Vec<Box<RawValue>>,
        seen: &mut HashMap<String, DateTime<Utc>>,
        newest: &mut DateTime<Utc>,
    ) -> Result<Vec<Box<RawValue>>> {
        let mut fresh = Vec::new();
        for item in items {
            let meta: CommitMeta = tri!(serde_json::from_str(item.get()).context("parse commit"));
            let at = meta.commit.author.date.with_timezone(&Utc);
            *newest = (*newest).max(at);
            // The seen SHAs stay in the collector's state and are never committed.
            if seen.insert(meta.sha, at).is_none() {
                fresh.push(tri!(self.keep_commit(&meta.repository, item)));
            }
        }
        Ok(fresh)
    }

    /// Fetch a URL as GitHub JSON.
    async fn get(&self, url: &Url) -> Result<String> {
        self.get_as(url, JSON).await
    }

    /// Fetch a URL with the given `Accept` media type.
    async fn get_as(&self, url: &Url, accept: &str) -> Result<String> {
        self.send(&self.app.http.get(url.clone()), accept, url.path())
            .await
    }

    /// Metadata of the gists updated since the newest stored `updated_at`. The list has no file contents. Gists
    /// belong to the user, so none are redacted.
    async fn gists(&self, now: DateTime<Utc>) -> Result<Stage> {
        let cursor: Option<DateTime<Utc>> = self.app.store.get("github/cursor/gist");
        let since = cursor.map(|at| at.to_rfc3339_opts(SecondsFormat::Secs, true));
        let mut newest = cursor;
        let mut fresh = Vec::new();
        let path = format!("/users/{}/gists", self.user);
        for page in 1_u32.. {
            let mut params = vec![("per_page", "100".to_owned()), ("page", page.to_string())];
            if let Some(stamp) = since.as_deref() {
                params.push(("since", stamp.to_owned()));
            }
            let url = tri!(Url::parse_with_params(
                &format!("{}{path}", self.api),
                &params
            ));
            let text = tri!(self.get(&url).await);
            let gists: Vec<Value> = tri!(serde_json::from_str(&text).context("parse gists"));
            let full = gists.len() == 100;
            for gist in gists {
                let updated = tri!(
                    gist.get("updated_at")
                        .and_then(Value::as_str)
                        .context("updated_at")
                );
                let at: DateTime<Utc> = tri!(updated.parse());
                // `since` is inclusive, so the newest stored gist comes back.
                if cursor.is_some_and(|stored| at <= stored) {
                    continue;
                }
                newest = newest.max(Some(at));
                fresh.push(gist);
            }
            if !full {
                break;
            }
        }
        let files = if fresh.is_empty() {
            Vec::new()
        } else {
            vec![File {
                path: storage::path(SOURCE, "gist", now, None, "json"),
                content: tri!(storage::envelope(
                    now,
                    json!({"path": path, "since": since, "gists": fresh.len()}),
                    &tri!(serde_json::to_string(&fresh)),
                )),
            }]
        };
        let state = if newest == cursor {
            Vec::new()
        } else {
            vec![("github/cursor/gist".to_owned(), json!(newest))]
        };
        Ok((files, state))
    }

    /// Run a GraphQL query and return the reply verbatim. Partial data with errors, such as SAML-protected repos, is kept.
    async fn graphql(&self, query: &str, variables: &Value) -> Result<String> {
        let req = self
            .app
            .http
            .post(format!("{}/graphql", self.api))
            .json(&json!({"query": query, "variables": variables}));
        let text = tri!(self.send(&req, JSON, "/graphql").await);
        let reply: Value = tri!(serde_json::from_str(&text).context("parse GraphQL reply"));
        if reply.get("data").is_none_or(Value::is_null) {
            bail!(
                "GraphQL failed: {}",
                reply.get("errors").unwrap_or(&Value::Null)
            );
        }
        if let Some(errors) = reply.get("errors") {
            warn!("github: GraphQL errors: {errors}");
        }
        Ok(text)
    }

    /// A commit verbatim when its repo is visible, else its author and committer dates only, with their offsets.
    fn keep_commit(&self, repo: &SearchRepo, item: Box<RawValue>) -> Result<Box<RawValue>> {
        if self.visible(&repo.full_name, repo.private.unwrap_or(true)) {
            return Ok(item);
        }
        let commit: Value = tri!(serde_json::from_str(item.get()));
        let redacted = json!({
            "commit": {"author": {"date": commit.pointer("/commit/author/date")}, "committer": {"date": commit.pointer("/commit/committer/date")}},
            "repository": {"private": true}, "redacted": true,
        });
        Ok(tri!(to_raw_value(&redacted)))
    }

    /// An event verbatim when its repo is visible, else its kind and time only.
    fn keep_event(
        &self,
        repo: &RepoMeta,
        public: bool,
        event: Box<RawValue>,
    ) -> Result<Box<RawValue>> {
        if self.visible(&repo.name, !public) {
            return Ok(event);
        }
        let value: Value = tri!(serde_json::from_str(event.get()));
        let redacted = json!({
            "id": value.get("id"), "type": value.get("type"), "public": false, "created_at": value.get("created_at"),
            "payload": {"action": value.pointer("/payload/action")}, "redacted": true,
        });
        Ok(tri!(to_raw_value(&redacted)))
    }

    /// A star verbatim when its repo is visible, else its time only.
    fn keep_star(&self, star: Value) -> Value {
        let name = star
            .pointer("/repo/full_name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let private = star
            .pointer("/repo/private")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        if self.visible(name, private) {
            return star;
        }
        json!({"starred_at": star.get("starred_at"), "repo": {"private": true}, "redacted": true})
    }

    /// The client, or `None` without a token and user.
    fn new(app: &'app App) -> Option<Self> {
        let (Some(token), Some(user)) = (
            app.cfg.github_activity_token.as_deref(),
            app.cfg.github_user.as_deref(),
        ) else {
            return None;
        };
        Some(Self {
            api: &app.cfg.github_api,
            app,
            exclude: app.github.repo(),
            token,
            user,
        })
    }

    /// Redact connection nodes in a foreign private repo, or in no visible repo at all, such as one GitHub hides from
    /// the token.
    fn redact_nodes(&self, items: &mut [Value]) {
        for item in items {
            if foreign(item, self.user) || !mentions(item, &|_name, _private| true) {
                redact_node(item);
            }
        }
    }

    /// Send a request, retrying GitHub's occasional 502s and 504s on large GraphQL queries.
    async fn send(&self, req: &RequestBuilder, accept: &str, what: &str) -> Result<String> {
        let mut attempt = 0_u64;
        loop {
            attempt = attempt.saturating_add(1);
            let cloned = tri!(req.try_clone().context("request body cannot be retried"));
            let resp = tri!(
                cloned
                    .bearer_auth(self.token)
                    .header("Accept", accept)
                    .header("X-GitHub-Api-Version", "2022-11-28")
                    .send()
                    .await
                    .with_context(|| format!("request {what}"))
            );
            let status = resp.status();
            let text = tri!(resp.text().await);
            if status.is_server_error() && attempt < RETRIES {
                warn!("github: {what} {status}, retrying");
                sleep(Pause::from_millis(RETRY_PAUSE_MS.saturating_mul(attempt))).await;
                continue;
            }
            if !status.is_success() {
                bail!(
                    "{what} failed: {status} {}",
                    text.chars().take(300).collect::<String>()
                );
            }
            return Ok(text);
        }
    }

    /// Stars newest first, down to the newest `starred_at` stored.
    async fn stars(&self, now: DateTime<Utc>) -> Result<Stage> {
        let cursor: Option<DateTime<Utc>> = self.app.store.get("github/cursor/star");
        let mut newest = cursor;
        let mut fresh = Vec::new();
        let path = format!("/users/{}/starred", self.user);
        for page in 1_u32.. {
            let url = tri!(Url::parse_with_params(
                &format!("{}{path}", self.api),
                [
                    ("sort", "created"),
                    ("direction", "desc"),
                    ("per_page", "100"),
                    ("page", page.to_string().as_str()),
                ],
            ));
            let text = tri!(self.get_as(&url, STAR_JSON).await);
            let stars: Vec<Value> = tri!(serde_json::from_str(&text).context("parse stars"));
            let full = stars.len() == 100;
            let mut reached = false;
            for star in stars {
                let starred = tri!(
                    star.get("starred_at")
                        .and_then(Value::as_str)
                        .context("starred_at")
                );
                let at: DateTime<Utc> = tri!(starred.parse());
                if cursor.is_some_and(|stored| at <= stored) {
                    reached = true;
                    break;
                }
                newest = newest.max(Some(at));
                fresh.push(self.keep_star(star));
            }
            if reached || !full {
                break;
            }
        }
        let files = if fresh.is_empty() {
            Vec::new()
        } else {
            vec![File {
                path: storage::path(SOURCE, "star", now, None, "json"),
                content: tri!(storage::envelope(
                    now,
                    json!({"path": path, "after": cursor, "stars": fresh.len()}),
                    &tri!(serde_json::to_string(&fresh)),
                )),
            }]
        };
        let state = if newest == cursor {
            Vec::new()
        } else {
            vec![("github/cursor/star".to_owned(), json!(newest))]
        };
        Ok((files, state))
    }

    /// Whether activity in this repo may be stored in full: it is public or the user owns it.
    fn visible(&self, full_name: &str, private: bool) -> bool {
        !private || owned_by(full_name, self.user)
    }
}

#[async_trait]
impl Source for GitHubActivity {
    async fn daily(&self, app: &App) -> Result<()> {
        let Some(gh) = Gh::new(app) else {
            return Ok(());
        };
        let now = Utc::now();
        // Each kind commits its files and stores its cursor on its own, so one failure keeps the others' progress.
        let mut errors = Vec::new();
        save(
            app,
            "contributions",
            gh.contributions(now).await,
            &mut errors,
        )
        .await;
        save(app, "commit", gh.commits(now).await, &mut errors).await;
        for (kind, field, nodes) in CONNECTIONS {
            let stage = gh.connection(now, kind, field, nodes).await;
            save(app, kind, stage, &mut errors).await;
        }
        save(
            app,
            "discussion_comment",
            gh.discussion_comments(now).await,
            &mut errors,
        )
        .await;
        save(app, "star", gh.stars(now).await, &mut errors).await;
        save(app, "gist", gh.gists(now).await, &mut errors).await;
        for kind in ["followers", "following"] {
            let stage = gh.follows(now, kind).await;
            save(app, kind, stage, &mut errors).await;
        }
        if !errors.is_empty() {
            bail!("{}", errors.join("; "));
        }
        Ok(())
    }

    async fn hourly(&self, app: &App) -> Result<()> {
        let Some(gh) = Gh::new(app) else {
            info!("github: not configured, skipping");
            return Ok(());
        };
        gh.events().await
    }

    fn name(&self) -> &'static str {
        SOURCE
    }

    fn router(&self) -> Router<Arc<App>> {
        Router::new()
    }
}

/// Whether a GraphQL node mentions a private repo the user does not own.
fn foreign(node: &Value, user: &str) -> bool {
    mentions(node, &|name, private| private && !owned_by(name, user))
}

/// Whether a GraphQL node mentions a repo for which `hit(nameWithOwner, isPrivate)` holds. A missing `isPrivate`
/// counts as private.
fn mentions(node: &Value, hit: &dyn Fn(&str, bool) -> bool) -> bool {
    if let Some(map) = node.as_object() {
        let private = map
            .get("isPrivate")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        return map
            .get("nameWithOwner")
            .and_then(Value::as_str)
            .is_some_and(|name| hit(name, private))
            || map.values().any(|child| mentions(child, hit));
    }
    node.as_array()
        .is_some_and(|items| items.iter().any(|child| mentions(child, hit)))
}

/// Whether `user` owns the repo `full_name`.
fn owned_by(full_name: &str, user: &str) -> bool {
    full_name
        .split('/')
        .next()
        .is_some_and(|owner| owner.eq_ignore_ascii_case(user))
}

/// Every contribution is an element of a list, so scrub each element that touches a foreign private repo.
fn redact_contributions(node: &mut Value, user: &str) {
    if let Some(items) = node.as_array_mut() {
        for item in items {
            if foreign(item, user) {
                redact_node(item);
            } else {
                redact_contributions(item, user);
            }
        }
    }
    if let Some(map) = node.as_object_mut() {
        for child in map.values_mut() {
            redact_contributions(child, user);
        }
    }
}

/// Strip what identifies a node and mark it redacted.
fn redact_node(item: &mut Value) {
    scrub(item);
    if let Some(map) = item.as_object_mut() {
        drop(map.insert("redacted".to_owned(), Value::Bool(true)));
    }
}

/// Commit a kind's files, then store its cursors; on failure, log and record the error.
async fn save(app: &App, what: &str, stage: Result<Stage>, errors: &mut Vec<String>) {
    let result = async {
        let (files, cursors) = tri!(stage);
        drop(tri!(
            app.save(&files, &format!("github: {what} ({} files)", files.len()))
                .await
        ));
        for (key, value) in cursors {
            tri!(app.store.set(&key, &value));
        }
        anyhow::Ok(())
    }
    .await;
    if let Err(error) = result {
        warn!("github: {what} failed: {error:#}");
        errors.push(format!("{what}: {error:#}"));
    }
}

/// Remove every identifying field, at any depth.
fn scrub(node: &mut Value) {
    if let Some(map) = node.as_object_mut() {
        for key in IDENTIFYING {
            drop(map.remove(key));
        }
        for child in map.values_mut() {
            scrub(child);
        }
    }
    if let Some(items) = node.as_array_mut() {
        for item in items {
            scrub(item);
        }
    }
}
