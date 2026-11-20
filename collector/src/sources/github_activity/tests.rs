//! Run the GitHub jobs against a fake API.

use alloc::sync::Arc;
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Duration, Utc};
use serde_json::value::to_raw_value;
use serde_json::{Value, json};

use super::{Gh, GitHubActivity, RepoMeta, STAR_JSON, SearchRepo, redact_contributions};
use crate::app::App;
use crate::sources::Source as _;
use crate::testing;
use crate::tri;

/// Where a committed connection page keeps its issue comments.
const COMMENTS: &str = "/response/data/user/issueComments/nodes";

/// The fake behind the handlers.
type Fake = Arc<Mutex<Seen>>;

/// What the fake GitHub API saw and serves.
#[derive(Clone, Default)]
struct Seen {
    /// How many of the next event requests answer 502.
    bad_gateways: u32,
    /// `after` of each issue comment page asked for.
    comment_afters: Vec<Value>,
    /// Issue comments in the connection, in `updatedAt` order; the cursor is the index.
    comments: u64,
    /// Pages of the event feed asked for.
    event_pages: Vec<String>,
    /// Logins following the user.
    followers: Vec<&'static str>,
    /// Gists, with their `updated_at`.
    gists: Vec<Value>,
    /// Variables of each contributions query.
    graphql_vars: Vec<Value>,
    /// Commit search queries.
    search_queries: Vec<HashMap<String, String>>,
    /// Stars, newest first.
    stars: Vec<Value>,
    /// Newest event id in the feed.
    top: u64,
}

impl Seen {
    /// The page of the issue comment connection after `after`. Comment 0 is in a work repo, comment 1 in the data
    /// repo.
    fn comment_page(&mut self, after: &Value) -> Value {
        self.comment_afters.push(after.clone());
        let start = after
            .as_str()
            .and_then(|cursor| cursor.parse::<u64>().ok())
            .map_or(0, |cursor| cursor.saturating_add(1));
        let end = self.comments.min(start.saturating_add(100));
        let cursor = (end > start).then(|| end.saturating_sub(1).to_string());
        let nodes: Vec<Value> = (start..end)
            .map(|index| {
                let (repo, private) = match index {
                    0 => ("work/secret", true),
                    1 => ("owner/repo", true),
                    _ => ("knyghty/x", false),
                };
                json!({
                    "databaseId": index, "createdAt": "2026-10-01T10:00:00Z", "url": format!("https://github.com/{repo}/issues/1#c{index}"),
                    "body": "secret plan", "issue": {"number": 1_u64, "title": "secret"}, "pullRequest": null,
                    "repository": {"nameWithOwner": repo, "isPrivate": private},
                })
            })
            .collect();
        json!({"data": {"user": {"issueComments": {
            "totalCount": self.comments,
            "pageInfo": {"endCursor": cursor, "hasNextPage": end < self.comments},
            "nodes": nodes,
        }}}})
    }

    /// Answer the event feed: ids `top` down to 151 in pages of 100, every third in the data repo; 502 while
    /// `bad_gateways` lasts.
    fn events(
        &mut self,
        user: &str,
        query: &HashMap<String, String>,
    ) -> Result<Json<Value>, StatusCode> {
        if user != "knyghty" {
            return Err(StatusCode::BAD_REQUEST);
        }
        if self.bad_gateways > 0 {
            self.bad_gateways = self.bad_gateways.saturating_sub(1);
            return Err(StatusCode::BAD_GATEWAY);
        }
        let Some(page) = query.get("page") else {
            return Err(StatusCode::BAD_REQUEST);
        };
        self.event_pages.push(page.clone());
        let Ok(number) = page.parse::<u64>() else {
            return Err(StatusCode::BAD_REQUEST);
        };
        let high = self
            .top
            .saturating_sub(number.saturating_sub(1).saturating_mul(100));
        let events = (151_u64..=high)
            .rev()
            .take(100)
            .map(|id| {
                let repo = if id.is_multiple_of(3) {
                    "owner/repo"
                } else {
                    "knyghty/x"
                };
                json!({"id": id.to_string(), "type": "PushEvent", "repo": {"name": repo}, "created_at": "2026-10-14T10:00:00Z"})
            })
            .collect();
        Ok(Json(Value::Array(events)))
    }

    /// Answer the followers list.
    fn follower_logins(&self) -> Json<Value> {
        let logins = self
            .followers
            .iter()
            .map(|login| json!({"login": login}))
            .collect();
        Json(Value::Array(logins))
    }

    /// Answer the gists list: those updated at or after `since`.
    fn gists_since(&self, since: &str) -> Json<Value> {
        let gists = self
            .gists
            .iter()
            .filter(|gist| {
                gist.get("updated_at")
                    .and_then(Value::as_str)
                    .is_some_and(|updated| updated >= since)
            })
            .cloned()
            .collect();
        Json(Value::Array(gists))
    }

    /// Answer a GraphQL query by what it asks for.
    fn graphql(&mut self, body: &Value) -> Json<Value> {
        let query = body
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if query.contains("createdAt }") {
            let created = Utc::now().checked_sub_signed(Duration::days(65));
            return Json(json!({"data": {"user": {"createdAt": created}}}));
        }
        if query.contains("issueComments(") {
            let after = body
                .pointer("/variables/after")
                .cloned()
                .unwrap_or(Value::Null);
            return Json(self.comment_page(&after));
        }
        if query.contains("repositoryDiscussionComments") {
            return Json(
                json!({"data": {"user": {"repositoryDiscussionComments": {"totalCount": 1_u64, "nodes": [
                    {"databaseId": 7_u64, "body": "hi", "discussion": {"repository": {"nameWithOwner": "django/django", "isPrivate": false}}},
                ]}}}}),
            );
        }
        if query.contains("orderBy") {
            return Json(json!({"data": {"user": {}}}));
        }
        self.graphql_vars
            .push(body.get("variables").cloned().unwrap_or(Value::Null));
        Json(
            json!({"data": {"user": {"contributionsCollection": {"restrictedContributionsCount": 0_u64}}}}),
        )
    }

    /// Answer a commit search: 103 commits, one a minute from 2026-10-01 at +02:00, in pages of 100.
    fn search(&mut self, query: HashMap<String, String>) -> Json<Value> {
        let page = query
            .get("page")
            .and_then(|number| number.parse::<u64>().ok())
            .unwrap_or(1);
        self.search_queries.push(query);
        let start = page.saturating_sub(1).saturating_mul(100);
        let items: Vec<Value> = (start..103_u64.min(start.saturating_add(100)))
            .map(|index| {
                let date = format!(
                    "2026-10-01T{:02}:{:02}:00.000+02:00",
                    10_u64.saturating_add(index.div_euclid(60)),
                    index.rem_euclid(60)
                );
                json!({"sha": format!("s{index}"), "commit": {"author": {"date": date}, "committer": {"date": date}}, "repository": {"full_name": "knyghty/x"}})
            })
            .collect();
        Json(json!({"total_count": 103_u64, "incomplete_results": false, "items": items}))
    }

    /// Answer the starred list, when asked with the star media type.
    fn starred(&self, headers: &HeaderMap) -> Result<Json<Value>, StatusCode> {
        let accept = headers.get("accept").and_then(|value| value.to_str().ok());
        if accept != Some(STAR_JSON) {
            return Err(StatusCode::BAD_REQUEST);
        }
        Ok(Json(Value::Array(self.stars.clone())))
    }
}

/// Paths and parsed contents of every committed file.
fn committed(recorded: &testing::Shared) -> Result<Vec<(String, Value)>> {
    let guard = recorded.lock().unwrap_or_else(PoisonError::into_inner);
    let paths: Vec<String> = guard.trees.iter().flatten().cloned().collect();
    let blobs = guard.blobs.clone();
    drop(guard);
    let mut files = Vec::new();
    for (path, blob) in paths.into_iter().zip(blobs) {
        files.push((path, tri!(serde_json::from_str(&blob))));
    }
    Ok(files)
}

/// The stored cursor of `kind`.
fn cursor(app: &App, kind: &str) -> Option<String> {
    app.store.get(&format!("github/cursor/{kind}"))
}

/// Backfill contributions by 30-day window and store new commits by author date.
#[tokio::test]
async fn daily_backfills_contributions_and_new_commits() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(GitHubActivity.daily(&app).await);
    let files = tri!(of(&recorded, &["contributions", "commit"]));
    let kinds: Vec<&str> = files.iter().map(|file| kind_of(&file.0)).collect();
    let expected = [
        "contributions",
        "contributions",
        "contributions",
        "commit",
        "commit",
    ];
    ensure!(kinds == expected, "three windows, then two commit pages");
    let first_page = tri!(files.get(3).context("a first commit page"));
    ensure!(
        len_at(&first_page.1, "/response") == Some(100),
        "a full first page"
    );
    let second_page = tri!(files.get(4).context("a second commit page"));
    ensure!(
        len_at(&second_page.1, "/response") == Some(3),
        "three commits on the second page"
    );
    ensure!(
        text_at(&second_page.1, "/response/0/commit/author/date")
            == Some("2026-10-01T11:40:00.000+02:00"),
        "commits are kept verbatim"
    );
    let state = snapshot(&fake);
    ensure!(state.graphql_vars.len() == 3, "three contribution queries");
    ensure!(
        state
            .graphql_vars
            .first()
            .and_then(|vars| text_at(vars, "/login"))
            == Some("knyghty"),
        "queries name the user"
    );
    ensure!(state.search_queries.len() == 2, "two search pages");
    let first_query = tri!(state.search_queries.first().context("a search query"));
    ensure!(
        first_query
            .get("q")
            .is_some_and(|query| query
                == "author:knyghty -repo:owner/repo author-date:>=2007-12-18T00:00:00Z"),
        "the search starts two weeks before 2008"
    );
    ensure!(
        first_query
            .get("sort")
            .is_some_and(|sort| sort == "author-date"),
        "sorted by author date"
    );
    let newest: DateTime<Utc> = tri!("2026-10-01T09:42:00Z".parse());
    ensure!(
        app.store.get("github/cursor/commit") == Some(newest),
        "the commit cursor is the newest author date"
    );
    ensure!(
        app.store
            .get::<DateTime<Utc>>("github/cursor/contributions")
            .is_some(),
        "a contributions cursor"
    );

    tri!(GitHubActivity.daily(&app).await);
    let later = tri!(of(&recorded, &["contributions", "commit"]));
    ensure!(later.len() == 6, "one refetched week, no repeated commits");
    ensure!(
        later
            .get(5)
            .is_some_and(|file| file.0.starts_with("raw/github/contributions/")),
        "the refetched week"
    );
    let third_query = tri!(
        snapshot(&fake)
            .search_queries
            .get(2)
            .cloned()
            .context("a third search query")
    );
    ensure!(
        third_query
            .get("q")
            .is_some_and(|query| query
                == "author:knyghty -repo:owner/repo author-date:>=2026-09-17T09:42:00Z"),
        "the search resumes two weeks before the newest commit"
    );
    Ok(())
}

/// Store new and changed items only: redacted in foreign private repos, skipped in the data repo.
#[tokio::test]
async fn daily_stores_only_new_and_changed_items() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    with(&fake, |state| {
        state.comments = 150;
        state.stars = vec![
            star("2026-10-02T00:00:00Z", "django/django", false),
            star("2026-10-01T00:00:00Z", "work/secret", true),
        ];
        state.gists = vec![gist("g1", "2026-10-01T00:00:00Z")];
        state.followers = vec!["b", "a"];
    });
    tri!(GitHubActivity.daily(&app).await);
    let pages = tri!(of(&recorded, &["issue_comment"]));
    let nodes: Vec<&Value> = pages
        .iter()
        .filter_map(|page| page.1.pointer(COMMENTS))
        .collect();
    ensure!(nodes.len() == 2, "two comment pages");
    let first_page = tri!(nodes.first().context("a first comment page"));
    ensure!(len_at(first_page, "") == Some(99), "no data repo comment");
    let second = nodes.get(1).and_then(|page| len_at(page, ""));
    ensure!(second == Some(50), "the rest on page two");
    let work = tri!(first_page.pointer("/0").context("a work comment"));
    ensure!(redacted(work), "work comment redacted");
    let created = text_at(work, "/createdAt");
    ensure!(created == Some("2026-10-01T10:00:00Z"), "time kept");
    ensure!(!work.to_string().contains("secret"), "scrubbed");
    let body = text_at(first_page, "/1/body");
    ensure!(body == Some("secret plan"), "public comment in full");
    let first_cursor = cursor(&app, "issue_comment");
    ensure!(first_cursor.as_deref() == Some("149"), "at last comment");
    ensure!(cursor(&app, "issue").is_none(), "no issue cursor");
    let star_files = tri!(of(&recorded, &["star"]));
    let stars = tri!(star_files.first().context("a star file"));
    let public_star = text_at(&stars.1, "/response/0/repo/full_name");
    ensure!(public_star == Some("django/django"), "public star in full");
    let expected =
        json!({"starred_at": "2026-10-01T00:00:00Z", "repo": {"private": true}, "redacted": true});
    let private_star = stars.1.pointer("/response/1");
    ensure!(private_star == Some(&expected), "time only");
    let follower_files = tri!(of(&recorded, &["followers"]));
    let followers = tri!(follower_files.first().context("a followers file"));
    let response = followers.1.get("response");
    ensure!(response == Some(&json!(["a", "b"])), "sorted");
    let others = tri!(of(&recorded, &["gist", "discussion_comment", "following"]));
    ensure!(others.len() == 3, "gists, discussion comments, following");

    with(&fake, |state| {
        state.comments = 155;
        state.comment_afters.clear();
        let newest_star = star("2026-10-03T00:00:00Z", "knyghty/x", true);
        state.stars.insert(0, newest_star);
        state.gists.push(gist("g2", "2026-10-03T00:00:00Z"));
    });
    let before = tri!(committed(&recorded)).len();
    tri!(GitHubActivity.daily(&app).await);
    let afters = snapshot(&fake).comment_afters;
    ensure!(afters == [json!("149")], "paging starts at the cursor");
    let all = tri!(committed(&recorded));
    let files: Vec<_> = all.into_iter().skip(before).collect();
    let kinds: Vec<&str> = files.iter().map(|file| kind_of(&file.0)).collect();
    let expected_kinds = ["contributions", "issue_comment", "star", "gist"];
    ensure!(kinds == expected_kinds, "only what changed");
    let new_comments = files.get(1).and_then(|file| len_at(&file.1, COMMENTS));
    ensure!(new_comments == Some(5), "five new comments");
    let new_stars = files.get(2).and_then(|file| len_at(&file.1, "/response"));
    ensure!(new_stars == Some(1), "one new star");
    let gist_file = tri!(files.get(3).context("a gist file"));
    let new_gist = text_at(&gist_file.1, "/response/0/id");
    ensure!(new_gist == Some("g2"), "the new gist");
    let second_cursor = cursor(&app, "issue_comment");
    ensure!(second_cursor.as_deref() == Some("154"), "cursor moves");
    Ok(())
}

/// The ids of the events in a committed event file.
fn event_ids(file: &Value) -> Vec<&str> {
    file.get("response")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|event| text_at(event, "/id"))
        .collect()
}

/// A gist `id` updated at `at`.
fn gist(id: &str, at: &str) -> Value {
    json!({"id": id, "updated_at": at, "public": false, "files": {"a.py": {"filename": "a.py"}}})
}

/// Commit the new events, except those in the data repo, and move the cursor.
#[tokio::test]
async fn hourly_commits_new_events_outside_the_data_repo() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(GitHubActivity.hourly(&app).await);
    let files = tri!(committed(&recorded));
    ensure!(files.len() == 1, "one event file");
    let first = tri!(files.first().context("an event file"));
    ensure!(
        first.0.starts_with("raw/github/event/"),
        "stored under event"
    );
    let ids = event_ids(&first.1);
    ensure!(ids.len() == 100, "150 events, a third in the data repo");
    ensure!(
        ids.iter().all(|id| id
            .parse::<u64>()
            .is_ok_and(|number| !number.is_multiple_of(3))),
        "no event in the data repo"
    );
    ensure!(snapshot(&fake).event_pages == ["1", "2"], "two pages read");
    ensure!(
        app.store.get::<u64>("github/cursor/event") == Some(300),
        "the cursor is the newest id"
    );

    with(&fake, |state| {
        state.top = 305;
        state.event_pages.clear();
    });
    tri!(GitHubActivity.hourly(&app).await);
    let later = tri!(committed(&recorded));
    ensure!(later.len() == 2, "a second event file");
    let second = tri!(later.get(1).context("a second event file"));
    ensure!(
        event_ids(&second.1) == ["305", "304", "302", "301"],
        "only the new events outside the data repo"
    );
    ensure!(snapshot(&fake).event_pages == ["1"], "one page read");
    ensure!(
        app.store.get::<u64>("github/cursor/event") == Some(305),
        "the cursor moves"
    );
    Ok(())
}

/// The kind segment of a committed path, `raw/github/<kind>/...`.
fn kind_of(path: &str) -> &str {
    path.split('/').nth(2).unwrap_or_default()
}

/// The length of the array at `pointer`.
fn len_at(value: &Value, pointer: &str) -> Option<usize> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .map(Vec::len)
}

/// Committed files of the given kinds.
fn of(recorded: &testing::Shared, kinds: &[&str]) -> Result<Vec<(String, Value)>> {
    Ok(tri!(committed(recorded))
        .into_iter()
        .filter(|file| kinds.contains(&kind_of(&file.0)))
        .collect())
}

/// Redact nodes in other people's private repos, and keep public and own repos in full.
#[tokio::test]
async fn redacts_only_other_peoples_private_repos() -> Result<()> {
    let (app, ..) = setup().await;
    let gh = tri!(Gh::new(&app).context("a configured client"));
    let mut reply = json!({"data": {"user": {"contributionsCollection": {
        "commitContributionsByRepository": [
            {"repository": {"nameWithOwner": "work/secret", "isPrivate": true},
             "contributions": {"nodes": [{"occurredAt": "t", "commitCount": 3_u64}]}},
            {"repository": {"nameWithOwner": "django/django", "isPrivate": false}, "contributions": {"nodes": []}},
            {"repository": {"nameWithOwner": "knyghty/private", "isPrivate": true}, "contributions": {"nodes": []}},
        ],
        "pullRequestReviewContributions": {"nodes": [{
            "occurredAt": "t",
            "pullRequestReview": {"url": "https://github.com/work/secret/pull/1#r", "state": "APPROVED"},
            "pullRequest": {"number": 1_u64, "url": "u", "repository": {"nameWithOwner": "work/secret", "isPrivate": true}},
        }]},
    }}}});
    redact_contributions(&mut reply, "knyghty");
    let text = reply.to_string();
    let scrubbed = !text.contains("work/secret") && !text.contains("pull/1");
    ensure!(scrubbed, "no work repo");
    let kept = text.contains("django/django") && text.contains("knyghty/private");
    ensure!(kept, "public and own repos stay");
    let collection = tri!(value_at(&reply, "/data/user/contributionsCollection"));
    let commits = tri!(value_at(collection, "/commitContributionsByRepository/0"));
    let count = commits.pointer("/contributions/nodes/0/commitCount");
    ensure!(count == Some(&json!(3_u64)), "counts stay");
    let reviews = tri!(value_at(collection, "/pullRequestReviewContributions"));
    let state = text_at(reviews, "/nodes/0/pullRequestReview/state");
    ensure!(state == Some("APPROVED"), "state stays");
    let review = tri!(value_at(reviews, "/nodes/0"));
    ensure!(redacted(review), "the review is marked redacted");

    let work = SearchRepo {
        full_name: "work/secret".to_owned(),
        private: None,
    };
    let raw_commit = tri!(to_raw_value(&json!({
        "sha": "abc123", "commit": {"message": "secret", "author": {"date": "d1", "email": "e"}, "committer": {"date": "d2"}},
        "repository": {"full_name": "work/secret"},
    })));
    let commit = tri!(gh.keep_commit(&work, raw_commit));
    let commit_text = commit.get();
    let commit_scrubbed = !commit_text.contains("secret") && !commit_text.contains("abc123");
    ensure!(commit_scrubbed, "no message or SHA");
    let commit_value: Value = tri!(serde_json::from_str(commit_text));
    let date = text_at(&commit_value, "/commit/author/date");
    ensure!(date == Some("d1"), "the author date stays");

    let raw_event = tri!(to_raw_value(&json!({
        "id": "1", "type": "PullRequestEvent", "created_at": "t", "repo": {"name": "work/secret"},
        "payload": {"action": "opened", "pull_request": {"title": "secret"}},
    })));
    let repo = RepoMeta {
        name: "work/secret".to_owned(),
    };
    let event = tri!(gh.keep_event(&repo, false, raw_event));
    ensure!(!event.get().contains("secret"), "no title");
    let event_value: Value = tri!(serde_json::from_str(event.get()));
    let action = text_at(&event_value, "/payload/action");
    ensure!(action == Some("opened"), "the action stays");

    let mut nodes = vec![
        json!({"databaseId": 1_u64, "createdAt": "t", "body": "secret", "url": "u", "discussion": {"title": "secret", "category": {"name": "secret"},
               "repository": {"nameWithOwner": "work/secret", "isPrivate": true}}}),
        json!({"databaseId": 2_u64, "createdAt": "t", "body": "secret", "repository": null}),
        json!({"databaseId": 3_u64, "body": "public", "repository": {"nameWithOwner": "django/django", "isPrivate": false}}),
    ];
    gh.redact_nodes(&mut nodes);
    for node in nodes.iter().take(2) {
        let node_text = node.to_string();
        let clean = !node_text.contains("secret") && !node_text.contains("\"u\"");
        ensure!(clean, "scrubbed");
        ensure!(text_at(node, "/createdAt") == Some("t"), "the time stays");
        ensure!(redacted(node), "marked redacted");
    }
    let public = tri!(nodes.get(2).context("a public node"));
    ensure!(text_at(public, "/body") == Some("public"), "kept in full");
    ensure!(public.get("redacted").is_none(), "not marked");
    Ok(())
}

/// Whether a node is marked redacted.
fn redacted(node: &Value) -> bool {
    node.get("redacted") == Some(&Value::Bool(true))
}

/// Retry 5xx answers twice, then fail.
#[tokio::test]
async fn retries_server_errors_a_few_times() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    with(&fake, |state| state.bad_gateways = 2);
    tri!(GitHubActivity.hourly(&app).await);
    ensure!(
        tri!(of(&recorded, &["event"])).len() == 1,
        "two 502s are retried"
    );
    with(&fake, |state| state.bad_gateways = 3);
    ensure!(
        GitHubActivity.hourly(&app).await.is_err(),
        "three 502s fail the run"
    );
    Ok(())
}

/// An app pointed at the fake API, the fake GitHub's recorder and the fake's state.
async fn setup() -> (Arc<App>, testing::Shared, Fake) {
    let (github, recorded) = testing::fake_github().await;
    let fake: Fake = Arc::new(Mutex::new(Seen {
        top: 300,
        ..Seen::default()
    }));
    let router = Router::new()
        .route(
            "/users/{user}/events",
            get(
                async |State(shared): State<Fake>,
                       Path(user): Path<String>,
                       Query(query): Query<HashMap<String, String>>| {
                    with(&shared, |state| state.events(&user, &query))
                },
            ),
        )
        .route(
            "/search/commits",
            get(
                async |State(shared): State<Fake>, Query(query): Query<HashMap<String, String>>| {
                    with(&shared, |state| state.search(query))
                },
            ),
        )
        .route(
            "/graphql",
            post(
                async |State(shared): State<Fake>, Json(body): Json<Value>| {
                    with(&shared, |state| state.graphql(&body))
                },
            ),
        )
        .route(
            "/users/{user}/starred",
            get(async |State(shared): State<Fake>, headers: HeaderMap| {
                with(&shared, |state| state.starred(&headers))
            }),
        )
        .route(
            "/users/{user}/gists",
            get(
                async |State(shared): State<Fake>, Query(query): Query<HashMap<String, String>>| {
                    let since = query.get("since").map_or("", String::as_str);
                    with(&shared, |state| state.gists_since(since))
                },
            ),
        )
        .route(
            "/users/{user}/followers",
            get(async |State(shared): State<Fake>| with(&shared, |state| state.follower_logins())),
        )
        .route("/users/{user}/following", get(async || Json(json!([]))))
        .with_state(Arc::clone(&fake));
    let api = testing::serve(router).await;
    let app = testing::app_with(&github, |cfg| {
        cfg.github_api = api;
        cfg.github_activity_token = Some("activity-token".to_owned());
        cfg.github_user = Some("knyghty".to_owned());
    });
    (app, recorded, fake)
}

/// Do nothing without a token and user.
#[tokio::test]
async fn skips_when_not_configured() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    let app = testing::app(&github);
    tri!(GitHubActivity.hourly(&app).await);
    tri!(GitHubActivity.daily(&app).await);
    ensure!(
        recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .commit_messages
            .is_empty(),
        "no commits"
    );
    Ok(())
}

/// A copy of the fake's state.
fn snapshot(fake: &Fake) -> Seen {
    with(fake, |state| state.clone())
}

/// A star of `repo` at `at`.
fn star(at: &str, repo: &str, private: bool) -> Value {
    json!({"starred_at": at, "repo": {"full_name": repo, "private": private, "description": "secret plan"}})
}

/// The string at `pointer`.
fn text_at<'value>(value: &'value Value, pointer: &str) -> Option<&'value str> {
    value.pointer(pointer).and_then(Value::as_str)
}

/// The value at `pointer`.
fn value_at<'value>(value: &'value Value, pointer: &str) -> Result<&'value Value> {
    value
        .pointer(pointer)
        .with_context(|| format!("nothing at {pointer}"))
}

/// Run `action` on the fake's state.
fn with<Out, Action>(fake: &Fake, action: Action) -> Out
where
    Action: FnOnce(&mut Seen) -> Out,
{
    let mut guard = fake.lock().unwrap_or_else(PoisonError::into_inner);
    action(&mut guard)
}
