//! Run the Gmail source against a fake mailbox.

use alloc::sync::Arc;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use anyhow::{Context as _, Result, bail, ensure};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse as _, Response};
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use chrono::{TimeDelta, Utc};
use serde_json::{Value, json};

use super::{AuthState, Cursor, Gmail, Grant, Mailbox, SCOPE, key};
use crate::app::App;
use crate::sources::Source as _;
use crate::testing;
use crate::tri;

/// The fake mailbox: ids newest first, who sent each, and what the fake was asked.
#[derive(Default)]
struct Fake {
    /// Every request, as `list <page>`, `history <id>` or `get <id> <format>`.
    asked: Vec<String>,
    /// 404 on history, as when Google forgot the start id.
    forget_history: bool,
    /// Records of the history API, each adding one message id.
    history: Vec<(u64, &'static str)>,
    /// The mailbox's current history id.
    history_id: u64,
    /// The messages, newest first, with their From header.
    messages: Vec<(&'static str, &'static str)>,
    /// Message gets still to refuse with Google's 403 for an exceeded per-minute quota.
    quota_refusals: usize,
}

/// The fake behind the HTTP handlers.
type Shared = Arc<Mutex<Fake>>;

impl Fake {
    /// Answer the history API: the records after the start id, or 404 when told to forget.
    fn history(&mut self, query: &HashMap<String, String>) -> Response {
        let start = query.get("startHistoryId").cloned().unwrap_or_default();
        self.asked.push(format!("history {start}"));
        assert_eq!(
            query.get("historyTypes").map(String::as_str),
            Some("messageAdded"),
            "history type"
        );
        if self.forget_history {
            return (StatusCode::NOT_FOUND, "{\"error\": {\"code\": 404}}").into_response();
        }
        let since: u64 = start.parse().unwrap_or(0);
        let records: Vec<Value> = self
            .history
            .iter()
            .filter(|&&(id, _)| id > since)
            .map(|&(id, message)| {
                json!({"id": id.to_string(), "messagesAdded": [{"message": {"id": message, "threadId": message}}]})
            })
            .collect();
        Json(json!({"history": records, "historyId": self.history_id.to_string()})).into_response()
    }

    /// Answer the list API: two ids a page, to test paging.
    fn list(&mut self, query: &HashMap<String, String>) -> Json<Value> {
        let page_token = query.get("pageToken").cloned().unwrap_or_default();
        self.asked.push(format!("list {page_token}"));
        assert_eq!(
            query.get("maxResults").map(String::as_str),
            Some("500"),
            "page size"
        );
        let start: usize = page_token.parse().unwrap_or(0);
        let end = start.saturating_add(2).min(self.messages.len());
        let ids: Vec<Value> = self
            .messages
            .get(start..end)
            .into_iter()
            .flatten()
            .map(|&(id, _)| json!({"id": id, "threadId": id}))
            .collect();
        let mut page = json!({"messages": ids, "resultSizeEstimate": self.messages.len()});
        if end < self.messages.len()
            && let Some(fields) = page.as_object_mut()
        {
            drop(fields.insert("nextPageToken".to_owned(), json!(end.to_string())));
        }
        Json(page)
    }

    /// Answer a message get: 403 while refusals remain, 404 for an unknown id, else the message.
    fn message(&mut self, id: &str, query: &HashMap<String, String>) -> Response {
        let format = query.get("format").cloned().unwrap_or_default();
        self.asked.push(format!("get {id} {format}"));
        if self.quota_refusals > 0 {
            self.quota_refusals = self.quota_refusals.saturating_sub(1);
            return (
                StatusCode::FORBIDDEN,
                r#"{"error": {"code": 403, "message": "Quota exceeded for quota metric 'Total Query Cost' and limit 'Units per minute per user' of service 'gmail.googleapis.com'", "errors": [{"reason": "rateLimitExceeded"}]}}"#,
            )
                .into_response();
        }
        let Some(&(_, from)) = self.messages.iter().find(|&&(message, _)| message == id) else {
            return (StatusCode::NOT_FOUND, "gone").into_response();
        };
        let mut payload = json!({"headers": [
            {"name": "From", "value": from}, {"name": "Subject", "value": format!("Hello {id}")},
            {"name": "To", "value": "Alex <alex@example.com>"},
        ]});
        if format == "full"
            && let Some(fields) = payload.as_object_mut()
        {
            drop(fields.insert(
                "body".to_owned(),
                json!({"size": 5_u32, "data": "aGVsbG8="}),
            ));
            drop(fields.insert("mimeType".to_owned(), json!("text/plain")));
        }
        Json(json!({"id": id, "threadId": id, "labelIds": ["INBOX"], "snippet": "the first words of the body",
                    "internalDate": "1761994800000", "sizeEstimate": 1234_u32, "historyId": "9", "payload": payload}))
        .into_response()
    }

    /// Answer the profile API with the current history id.
    fn profile(&self) -> Json<Value> {
        Json(
            json!({"emailAddress": "alex@example.com", "messagesTotal": self.messages.len(),
                    "threadsTotal": 3_u32, "historyId": self.history_id.to_string()}),
        )
    }
}

/// Lock a test mutex, ignoring poisoning.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The parsed blob of the `index`th committed file.
fn committed(recorded: &testing::Shared, index: usize) -> Result<Value> {
    let found = lock(recorded).blobs.get(index).cloned();
    let blob = tri!(found.with_context(|| format!("no blob {index}")));
    Ok(tri!(serde_json::from_str(&blob)))
}

/// Start the fake Gmail and an app with one approved account, `personal`.
async fn setup(full_senders: &[&str]) -> Result<(Arc<App>, testing::Shared, Shared)> {
    let (github, recorded) = testing::fake_github().await;
    let fake: Shared = Arc::new(Mutex::new(Fake {
        history_id: 100,
        messages: vec![
            ("m5", "Thuisbezorgd <noreply@mail.thuisbezorgd.nl>"),
            ("m4", "a friend <friend@example.org>"),
            ("m3", "NS <no-reply@ns.nl>"),
            ("m2", "colleague@work.example"),
            ("m1", "old@example.org"),
        ],
        ..Fake::default()
    }));
    let base = testing::serve(
        Router::new()
            .route(
                "/token",
                post(async |Form(form): Form<HashMap<String, String>>| {
                    assert_eq!(
                        form.get("client_id").map(String::as_str),
                        Some("cid"),
                        "client id"
                    );
                    if form.get("grant_type").map(String::as_str) == Some("authorization_code") {
                        return Json(json!({
                            "access_token": "at", "refresh_token": "rt", "refresh_token_expires_in": 604_800_u32,
                            "scope": format!("{SCOPE} openid"),
                        }));
                    }
                    assert_eq!(
                        form.get("refresh_token").map(String::as_str),
                        Some("rt"),
                        "refresh token"
                    );
                    Json(json!({"access_token": "at"}))
                }),
            )
            .route(
                "/users/me/profile",
                get(async |State(shared): State<Shared>| lock(&shared).profile()),
            )
            .route(
                "/users/me/labels",
                get(async || {
                    Json(json!({"labels": [{"id": "INBOX", "name": "INBOX", "type": "system"}]}))
                }),
            )
            .route(
                "/users/me/messages",
                get(
                    async |State(shared): State<Shared>, Query(query): Query<HashMap<String, String>>| {
                        lock(&shared).list(&query)
                    },
                ),
            )
            .route(
                "/users/me/messages/{id}",
                get(
                    async |State(shared): State<Shared>,
                           Path(id): Path<String>,
                           Query(query): Query<HashMap<String, String>>| {
                        lock(&shared).message(&id, &query)
                    },
                ),
            )
            .route(
                "/users/me/history",
                get(
                    async |State(shared): State<Shared>, Query(query): Query<HashMap<String, String>>| {
                        lock(&shared).history(&query)
                    },
                ),
            )
            .with_state(Arc::clone(&fake)),
    )
    .await;
    let senders: Vec<String> = full_senders
        .iter()
        .map(|sender| (*sender).to_owned())
        .collect();
    let app = testing::app_with(&github, |cfg| {
        cfg.google_client_id = Some("cid".into());
        cfg.google_client_secret = Some("secret".into());
        cfg.google_token_url = format!("{base}/token");
        cfg.gmail_api_url = base.clone();
        cfg.gmail_full_senders = senders;
    });
    let pending = AuthState {
        account: "personal".to_owned(),
        state: "state".to_owned(),
    };
    let approved = pending.finish(&app, "code").await;
    ensure!(approved.status() == StatusCode::OK, "approve the account");
    Ok((app, recorded, fake))
}

/// Two 403 quota refusals pause the run, then the gets are repeated.
#[tokio::test]
async fn an_exceeded_quota_is_retried_after_a_pause() -> Result<()> {
    let (app, recorded, fake) = tri!(setup(&[]).await);
    lock(&fake).quota_refusals = 2;
    let mailbox = tri!(Mailbox::open(&app, "personal").await);
    tri!(mailbox.run().await);
    ensure!(
        lock(&recorded).commit_messages == ["gmail: personal (5)"],
        "one commit"
    );
    let asked = lock(&fake).asked.clone();
    let gets = asked.iter().filter(|ask| ask.starts_with("get ")).count();
    ensure!(
        gets == 7,
        "two refused gets were repeated: {}",
        asked.join(", ")
    );
    Ok(())
}

/// The first runs page through the mailbox; later runs follow the history API.
#[tokio::test]
async fn walks_the_mailbox_then_follows_history() -> Result<()> {
    let (app, recorded, fake) = tri!(setup(&["thuisbezorgd.nl", "no-reply@ns.nl"]).await);
    tri!(Gmail.hourly(&app).await);
    let found = lock(&recorded)
        .trees
        .first()
        .and_then(|tree| tree.first())
        .cloned();
    let first = tri!(found.context("no file"));
    ensure!(
        lock(&recorded).commit_messages == ["gmail: personal (5)"],
        "one commit"
    );
    ensure!(first.starts_with("raw/gmail/personal/message/"), "{first}");
    let file = tri!(committed(&recorded, 0));
    ensure!(
        file.pointer("/request/history_id") == Some(&json!("100")),
        "history id"
    );
    ensure!(
        file.pointer("/request/count") == Some(&json!(5_u64)),
        "count"
    );
    let messages = tri!(
        file.get("response")
            .and_then(Value::as_array)
            .context("response")
    );
    ensure!(messages.len() == 5, "five messages");
    for message in messages {
        let from = tri!(
            message
                .pointer("/payload/headers/0/value")
                .and_then(Value::as_str)
                .context("From")
        );
        let whole = from.contains("thuisbezorgd") || from.contains("ns.nl");
        ensure!(
            message
                .pointer("/payload/body")
                .is_some_and(Value::is_object)
                == whole,
            "{from}"
        );
        ensure!(
            message.get("snippet").is_some() == whole,
            "snippet is body text: {from}"
        );
    }
    let asked = lock(&fake).asked.clone();
    ensure!(
        asked.iter().filter(|ask| ask.starts_with("list")).count() == 3,
        "three pages of two"
    );
    ensure!(asked.contains(&"get m5 full".to_owned()), "m5 whole");
    ensure!(asked.contains(&"get m3 full".to_owned()), "m3 whole");
    ensure!(
        !asked.contains(&"get m4 full".to_owned()),
        "m4 metadata only"
    );
    let cursor: Option<Cursor> = app.store.get(&key("personal", "cursor"));
    let expected = Cursor {
        backfilled: true,
        history_id: Some("100".into()),
        page_token: None,
    };
    ensure!(cursor == Some(expected), "cursor after the walk");

    // Nothing new: no history records, no commit, but the cursor keeps the mailbox's current id.
    tri!(Gmail.hourly(&app).await);
    ensure!(lock(&recorded).trees.len() == 1, "no new commit");

    // Two messages arrive, one of them deleted again before the run.
    lock(&fake).messages.insert(0, ("m6", "shop@amazon.nl"));
    lock(&fake).history = vec![(101, "m6"), (102, "m7")];
    lock(&fake).history_id = 102;
    tri!(Gmail.hourly(&app).await);
    let second = tri!(committed(&recorded, 1));
    ensure!(
        second.pointer("/request/from_history_id") == Some(&json!("100")),
        "from history id"
    );
    ensure!(
        second.pointer("/request/history_id") == Some(&json!("102")),
        "history id"
    );
    let ids: Vec<&str> = second
        .get("response")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|message| message.get("id").and_then(Value::as_str))
        .collect();
    ensure!(ids == ["m6"], "the deleted m7 is skipped");
    let moved: Option<Cursor> = app.store.get(&key("personal", "cursor"));
    ensure!(
        moved.and_then(|state| state.history_id) == Some("102".to_owned()),
        "cursor follows history"
    );
    ensure!(
        lock(&fake).asked.contains(&"history 100".to_owned()),
        "history asked from 100"
    );
    Ok(())
}

/// A 404 from the history API lists the newest page and restarts from the mailbox's current id.
#[tokio::test]
async fn lists_again_when_google_forgot_the_history_id() -> Result<()> {
    let (app, recorded, fake) = tri!(setup(&[]).await);
    tri!(app.store.set(
        &key("personal", "cursor"),
        &Cursor {
            backfilled: true,
            history_id: Some("5".into()),
            page_token: None,
        },
    ));
    lock(&fake).forget_history = true;
    tri!(Gmail.hourly(&app).await);
    let file = tri!(committed(&recorded, 0));
    ensure!(
        file.get("response").and_then(Value::as_array).map(Vec::len) == Some(2),
        "one page of the newest ids"
    );
    ensure!(
        file.pointer("/response/0/snippet").is_none(),
        "no allowlist: metadata only"
    );
    let cursor: Option<Cursor> = app.store.get(&key("personal", "cursor"));
    let expected = Cursor {
        backfilled: true,
        history_id: Some("100".into()),
        page_token: None,
    };
    ensure!(
        cursor == Some(expected),
        "cursor restarts from the current id"
    );
    Ok(())
}

/// The daily job commits labels and profile once, then only what changed.
#[tokio::test]
async fn stores_labels_and_profile_when_changed() -> Result<()> {
    let (app, recorded, fake) = tri!(setup(&[]).await);
    tri!(Gmail.daily(&app).await);
    tri!(Gmail.daily(&app).await);
    let found = lock(&recorded).trees.first().cloned();
    let first = tri!(found.context("no commit"));
    ensure!(
        lock(&recorded).commit_messages == ["gmail: personal labels, profile"],
        "one commit"
    );
    ensure!(
        first
            .iter()
            .any(|path| path.starts_with("raw/gmail/personal/labels/")),
        "labels stored"
    );
    ensure!(
        first
            .iter()
            .any(|path| path.starts_with("raw/gmail/personal/profile/")),
        "profile stored"
    );
    lock(&fake).history_id = 101;
    tri!(Gmail.daily(&app).await);
    let trees = lock(&recorded).trees.clone();
    ensure!(trees.len() == 2, "a second commit");
    ensure!(
        trees.get(1).map(Vec::len) == Some(1),
        "only the changed profile"
    );
    Ok(())
}

/// A grant that ends within a day fails the run with the approval link, without the secret.
#[tokio::test]
async fn asks_to_approve_again_before_access_ends() -> Result<()> {
    let (app, _, _) = tri!(setup(&[]).await);
    let stored: Option<Grant> = app.store.get(&key("personal", "oauth"));
    let mut grant = tri!(stored.context("no grant"));
    grant.expires_at = Utc::now().checked_add_signed(TimeDelta::hours(3));
    tri!(app.store.set(&key("personal", "oauth"), &grant));
    let Err(error) = Gmail.hourly(&app).await else {
        bail!("the run passed");
    };
    let text = error.to_string();
    ensure!(
        text.contains("personal: access ends")
            && text.contains("/gmail/auth?account=personal&key="),
        "{text}"
    );
    ensure!(
        !text.contains(&app.cfg.admin_secret),
        "the secret is not shown"
    );
    Ok(())
}

/// The allowlist matches by address or domain, case-insensitively, with or without a display name.
#[test]
fn matches_senders_by_address_and_domain() -> Result<()> {
    let app = testing::app_with("http://localhost", |cfg| {
        cfg.gmail_full_senders = vec!["thuisbezorgd.nl".to_owned(), "No-Reply@NS.nl".to_owned()];
    });
    let mailbox = Mailbox {
        account: "personal",
        app: &app,
        expires_at: None,
        token: String::new(),
    };
    let from = |value: &str| json!({"payload": {"headers": [{"name": "From", "value": value}]}});
    ensure!(
        mailbox.stores_body(&from("Thuisbezorgd <noreply@mail.thuisbezorgd.nl>")),
        "subdomain"
    );
    ensure!(
        mailbox.stores_body(&from("orders@thuisbezorgd.nl")),
        "bare address on the domain"
    );
    ensure!(mailbox.stores_body(&from("NS <no-reply@ns.nl>")), "address");
    ensure!(
        mailbox.stores_body(&from("A B <Orders@Thuisbezorgd.NL>")),
        "lower-cased address between angle brackets"
    );
    ensure!(
        !mailbox.stores_body(&from("someone@ns.nl")),
        "another address on the domain"
    );
    ensure!(
        !mailbox.stores_body(&from("evil@notthuisbezorgd.nl")),
        "domain suffix only"
    );
    ensure!(!mailbox.stores_body(&json!({"payload": {}})), "no headers");
    ensure!(!mailbox.stores_body(&from("no address here")), "no address");
    let unlisted = testing::app_with("http://localhost", |_| {});
    let none = Mailbox {
        account: "personal",
        app: &unlisted,
        expires_at: None,
        token: String::new(),
    };
    ensure!(
        !none.stores_body(&from("orders@thuisbezorgd.nl")),
        "empty allowlist"
    );
    Ok(())
}
