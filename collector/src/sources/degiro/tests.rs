//! Tests for the DEGIRO sources against a fake web trader.

use alloc::collections::BTreeSet;
use alloc::sync::Arc;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{Duration, NaiveDate, NaiveTime, Utc};
use serde_json::{Value, json};

use super::{Api, HISTORY_START, Login, OVERLAP_DAYS, PENSION, PERSONAL};
use crate::app::App;
use crate::sources::Source as _;
use crate::testing::{self, Recorded};
use crate::tri;

/// The cash movements of the window starting 1 January 2024.
const CASH_MOVEMENTS: &str = r#"{"data": {"cashMovements": [
    {"date": "2024-02-28T09:00:00+01:00", "valueDate": "2024-02-28T09:00:00+01:00", "id": 10, "description": "iDEAL storting",
     "currency": "EUR", "change": 1000.0, "balance": {"unsettledCash": 0.0, "total": 1000.0}, "type": "CASH_TRANSACTION"},
    {"date": "2024-03-01T10:00:00+01:00", "valueDate": "2024-03-01T10:00:00+01:00", "id": 11, "productId": 789,
     "description": "Koop 10 @ 95 EUR", "currency": "EUR", "change": -950.0, "balance": {"total": 50.0}, "type": "TRANSACTION"}]}}"#;
/// The trading update: one position, the cash row, the totals and no cash funds.
const PORTFOLIO: &str = r#"{
    "portfolio": {"lastUpdated": 1, "name": "portfolio", "value": [
        {"id": "456", "name": "positionrow", "value": [
            {"name": "id", "value": "456"}, {"name": "positionType", "value": "PRODUCT"},
            {"name": "size", "value": 10}, {"name": "price", "value": 100.5}, {"name": "value", "value": 1005.0},
            {"name": "plBase", "value": {"EUR": -50.0}}, {"name": "todayPlBase", "value": {"EUR": 1.0}},
            {"name": "breakEvenPrice", "value": 105.5}]},
        {"id": "FLATEX_EUR", "name": "positionrow", "value": [
            {"name": "id", "value": "FLATEX_EUR"}, {"name": "positionType", "value": "CASH"},
            {"name": "size", "value": 200.0}, {"name": "price", "value": 1}, {"name": "value", "value": 200.0}]}]},
    "totalPortfolio": {"lastUpdated": 1, "name": "totalPortfolio", "value": [
        {"name": "totalCash", "value": 200.0}, {"name": "reportNetliq", "value": 1205.0},
        {"name": "totalDepositWithdrawal", "value": 1000.0}, {"name": "freeSpaceNew", "value": {"EUR": 200.0}}]},
    "cashFunds": {"lastUpdated": 1, "name": "cashFunds", "value": []}
}"#;
/// The descriptions of the two products the fixtures mention.
const PRODUCTS: &str = r#"{"data": {
    "456": {"id": "456", "name": "Vanguard FTSE All-World", "isin": "IE00B3RBWM25", "symbol": "VWRL", "currency": "EUR",
            "productType": "ETF", "productTypeId": 131, "exchangeId": "200", "closePrice": 100.5, "closePriceDate": "2024-03-01",
            "tradable": true, "active": true},
    "789": {"id": "789", "name": "Some Stock", "isin": "NL0000000001", "symbol": "SMS", "currency": "EUR",
            "productType": "STOCK", "productTypeId": 1, "exchangeId": "200", "tradable": true, "active": true}}}"#;
/// The transactions of the window starting 1 January 2024.
const TRANSACTIONS: &str = r#"{"data": [{"id": 1, "productId": 456, "date": "2024-03-01T10:00:00+01:00", "buysell": "B",
    "price": 95.0, "quantity": 10, "total": -950.0, "orderTypeId": 0, "counterParty": "MK", "transfered": false,
    "fxRate": 0, "totalInBaseCurrency": -950.0, "feeInBaseCurrency": -2.0, "totalPlusFeeInBaseCurrency": -952.0,
    "totalPlusAllFeesInBaseCurrency": -952.0, "transactionTypeId": 0, "tradingVenue": "XAMS"}]}"#;

/// What the fake web trader received, shared with its handlers.
type Fake = Arc<Mutex<Seen>>;

/// What the fake web trader received.
#[derive(Clone, Default)]
struct Seen {
    /// The body of every login.
    logins: Vec<Value>,
    /// Number of logouts.
    logouts: usize,
    /// `fromDate` of every account overview request.
    overview_windows: Vec<String>,
    /// The body of every product description request.
    product_requests: Vec<Value>,
    /// Reject logins with this status code and body, when set.
    reject_login: Option<(StatusCode, Value)>,
    /// `fromDate` of every transactions request.
    transaction_windows: Vec<String>,
}

/// Lock what the fake received, poisoned or not.
fn seen(fake: &Fake) -> MutexGuard<'_, Seen> {
    fake.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Lock the fake GitHub's record, poisoned or not.
fn recorded(record: &testing::Shared) -> MutexGuard<'_, Recorded> {
    record.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Check the session query parameters of a call after login.
fn needs_session(query: &HashMap<String, String>) {
    assert_eq!(
        query.get("sessionId").map(String::as_str),
        Some("sess1"),
        "session id"
    );
    assert_eq!(
        query.get("intAccount").map(String::as_str),
        Some("123"),
        "account"
    );
}

/// The login call: record the body, and reject when `reject_login` is set.
async fn login(
    State(fake): State<Fake>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert!(
        headers
            .get("user-agent")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|agent| agent.starts_with("Mozilla/5.0")),
        "browser user agent"
    );
    let mut log = seen(&fake);
    log.logins.push(body);
    let (status, reply) = log.reject_login.clone().unwrap_or_else(|| {
        (
            StatusCode::OK,
            json!({"isPassCodeEnabled": false, "locale": "nl_NL", "sessionId": "sess1", "status": 0_u8, "statusText": "success"}),
        )
    });
    drop(log);
    (status, Json(reply))
}

/// A transactions or account overview report: the window starting 1 January 2024 holds items, the rest are empty.
async fn report(
    State(fake): State<Fake>,
    Path(kind): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> &'static str {
    needs_session(&query);
    let from = query.get("fromDate").cloned().unwrap_or_default();
    let mut log = seen(&fake);
    let (windows, items, empty) = if kind == "transactions" {
        assert_eq!(
            query.get("groupTransactionsByOrder").map(String::as_str),
            Some("false"),
            "transactions are not grouped"
        );
        (
            &mut log.transaction_windows,
            TRANSACTIONS,
            r#"{"data": []}"#,
        )
    } else {
        (
            &mut log.overview_windows,
            CASH_MOVEMENTS,
            r#"{"data": {"cashMovements": []}}"#,
        )
    };
    windows.push(from.clone());
    drop(log);
    if from == "01/01/2024" { items } else { empty }
}

/// Serve a fake web trader and an app with both accounts configured, `totp_secret` on each.
async fn setup(totp_secret: Option<&str>) -> (Arc<App>, testing::Shared, Fake) {
    let (github, record) = testing::fake_github().await;
    let fake: Fake = Arc::default();
    let client = async |Query(query): Query<HashMap<String, String>>| -> &'static str {
        assert_eq!(
            query.get("sessionId").map(String::as_str),
            Some("sess1"),
            "session id"
        );
        assert!(
            !query.contains_key("intAccount"),
            "DEGIRO answers 404 to client details with an intAccount"
        );
        r#"{"data": {"id": 1, "intAccount": 123, "username": "alex", "email": "alex@example.com"}}"#
    };
    let update = async |Path(rest): Path<String>,
                        Query(query): Query<HashMap<String, String>>|
           -> &'static str {
        assert_eq!(
            rest, "123;jsessionid=sess1",
            "account and session in the path"
        );
        needs_session(&query);
        assert_eq!(
            query.get("portfolio").map(String::as_str),
            Some("0"),
            "portfolio asked"
        );
        assert_eq!(
            query.get("totalPortfolio").map(String::as_str),
            Some("0"),
            "totals asked"
        );
        PORTFOLIO
    };
    let products = async |State(shared): State<Fake>,
                          Query(query): Query<HashMap<String, String>>,
                          Json(body): Json<Value>|
           -> &'static str {
        needs_session(&query);
        seen(&shared).product_requests.push(body);
        PRODUCTS
    };
    let logout = async |State(shared): State<Fake>,
                        Path(rest): Path<String>,
                        Query(query): Query<HashMap<String, String>>|
           -> StatusCode {
        assert_eq!(rest, "logout;jsessionid=sess1", "session in the path");
        needs_session(&query);
        let mut log = seen(&shared);
        log.logouts = log.logouts.saturating_add(1);
        drop(log);
        StatusCode::OK
    };
    let base = testing::serve(
        Router::new()
            .route("/login/secure/login", post(login))
            .route("/login/secure/login/totp", post(login))
            .route("/pa/secure/client", get(client))
            .route("/trading/secure/v5/update/{rest}", get(update))
            .route("/portfolio-reports/secure/v4/{report}", get(report))
            .route("/portfolio-reports/secure/v6/{report}", get(report))
            .route("/product_search/secure/v5/products/info", post(products))
            .route("/trading/secure/{rest}", put(logout))
            .with_state(Arc::clone(&fake)),
    )
    .await;
    let credentials = Login {
        password: "hunter2".to_owned(),
        totp_secret: totp_secret.map(String::from),
        username: "alex".to_owned(),
    };
    let app = testing::app_with(&github, move |cfg| {
        cfg.degiro_url = base;
        cfg.degiro = Some(credentials.clone());
        cfg.degiro_pension = Some(Login {
            username: "tomp".to_owned(),
            ..credentials
        });
    });
    (app, record, fake)
}

/// Without a configured login the daily run does nothing.
#[tokio::test]
async fn without_credentials_nothing_happens() -> Result<()> {
    let (github, record) = testing::fake_github().await;
    let app = testing::app(&github);
    tri!(PERSONAL.daily(&app).await);
    tri!(PENSION.daily(&app).await);
    ensure!(recorded(&record).commit_messages.is_empty(), "no commit");
    Ok(())
}

/// The pension account logs in with its own username and keeps its own files and cursors.
#[tokio::test]
async fn the_pension_account_has_its_own_login_directory_and_cursors() -> Result<()> {
    let (app, record, fake) = setup(None).await;
    tri!(PENSION.daily(&app).await);
    let first_login = seen(&fake).logins.first().cloned();
    ensure!(
        first_login.as_ref().and_then(|login| login.get("username")) == Some(&json!("tomp")),
        "pension username"
    );
    let message = recorded(&record).commit_messages.first().cloned();
    ensure!(
        message
            .as_deref()
            .is_some_and(|text| text.starts_with("degiro_pension: portfolio")),
        "pension commit"
    );
    let tree = recorded(&record).trees.first().cloned();
    ensure!(
        tree.is_some_and(|paths| paths
            .iter()
            .all(|path| path.starts_with("raw/degiro_pension/"))),
        "pension directory"
    );
    ensure!(
        app.store.get::<NaiveDate>(&PENSION.history_key()).is_some(),
        "pension cursor"
    );
    ensure!(
        app.store
            .get::<NaiveDate>(&PERSONAL.history_key())
            .is_none(),
        "no personal cursor"
    );
    Ok(())
}

/// The first run walks yearly windows from 2010 and describes the products once; the next run fetches again from a
/// week before the last run only.
#[tokio::test]
async fn first_run_walks_the_history_then_only_the_last_week() -> Result<()> {
    let (app, record, fake) = setup(Some("JBSWY3DPEHPK3PXP")).await;
    tri!(PERSONAL.daily(&app).await);
    let first = seen(&fake).clone();
    let login = tri!(first.logins.first().context("login"));
    ensure!(login.get("username") == Some(&json!("alex")), "username");
    ensure!(login.get("password") == Some(&json!("hunter2")), "password");
    let otp = tri!(
        login
            .get("oneTimePassword")
            .and_then(Value::as_str)
            .context("one-time password")
    );
    ensure!(
        otp.len() == 6 && otp.bytes().all(|byte| byte.is_ascii_digit()),
        "{otp}"
    );
    let first_window = first.transaction_windows.first().map(String::as_str);
    ensure!(first_window == Some("01/01/2010"), "history from 2010");
    let windows = first.transaction_windows.len();
    ensure!(
        first.overview_windows.len() == windows,
        "both reports per window"
    );
    ensure!(windows >= 16, "{windows} windows");
    // Products come from the portfolio (456) and the cash movements (789), each once.
    ensure!(
        first.product_requests == [json!(["456", "789"])],
        "products asked once"
    );
    ensure!(first.logouts == 1, "logged out");
    let first_tree = recorded(&record).trees.first().cloned();
    let tree = tri!(first_tree.context("first tree"));
    let kinds: Vec<&str> = tree
        .iter()
        .filter_map(|path| path.split('/').nth(2))
        .collect();
    let count = |kind: &str| kinds.iter().filter(|&&candidate| candidate == kind).count();
    ensure!(
        [
            count("portfolio"),
            count("transaction"),
            count("account_overview"),
            count("product")
        ] == [1_usize; 4],
        "one file per kind: {}",
        kinds.join(",")
    );
    let message = recorded(&record).commit_messages.first().cloned();
    ensure!(
        message.as_deref() == Some("degiro: portfolio, transaction, account_overview, product"),
        "{message:?}"
    );
    ensure!(
        tree.iter()
            .any(|path| path.contains("/transaction/") && path.ends_with("-2024-01-01.json")),
        "window start as suffix"
    );
    // No stored request carries the session id.
    let blobs = recorded(&record).blobs.clone();
    for blob in &blobs {
        let stored: Value = tri!(serde_json::from_str(blob));
        let request = tri!(serde_json::to_string(
            stored.get("request").unwrap_or(&Value::Null)
        ));
        ensure!(!request.contains("sess1"), "{request}");
    }
    let from: NaiveDate = tri!(app.store.get(&PERSONAL.history_key()).context("cursor"));
    let week_ago = tri!(
        Utc::now()
            .date_naive()
            .checked_sub_signed(Duration::days(OVERLAP_DAYS))
            .context("date")
    );
    ensure!(from == week_ago, "cursor a week back");
    let known: BTreeSet<String> = tri!(app.store.get(&PERSONAL.products_key()).context("products"));
    ensure!(known.len() == 2, "two products seen");

    tri!(PERSONAL.daily(&app).await);
    let second = seen(&fake).clone();
    let last_window = from.format("%d/%m/%Y").to_string();
    ensure!(
        second.transaction_windows.last() == Some(&last_window),
        "second run from the cursor"
    );
    ensure!(
        second.product_requests.len() == 1,
        "seen products are not asked again"
    );
    ensure!(second.logouts == 2, "logged out again");
    let second_message = recorded(&record).commit_messages.get(1).cloned();
    ensure!(
        second_message.as_deref() == Some("degiro: portfolio"),
        "{second_message:?}"
    );
    Ok(())
}

/// Without a TOTP secret the plain login is used, and a `totpNeeded` rejection names the missing variable.
#[tokio::test]
async fn without_a_totp_secret_the_plain_login_is_used_and_2fa_is_explained() -> Result<()> {
    let (app, _record, fake) = setup(None).await;
    seen(&fake).reject_login = Some((
        StatusCode::BAD_REQUEST,
        json!({"status": 6_u8, "statusText": "totpNeeded"}),
    ));
    let error = tri!(PERSONAL.daily(&app).await.err().context("login accepted")).to_string();
    ensure!(error.contains("DEGIRO_TOTP_SECRET"), "{error}");
    ensure!(!error.contains("hunter2"), "password in the error");
    let log = seen(&fake).clone();
    ensure!(
        log.logins
            .first()
            .is_some_and(|login| login.get("oneTimePassword").is_none()),
        "no one-time password sent"
    );
    ensure!(log.logouts == 0, "no logout without a session");
    Ok(())
}

/// A 202 asking for in-app approval is explained, without the token it carries.
#[tokio::test]
async fn an_in_app_approval_request_is_explained_without_its_token() -> Result<()> {
    let (app, _record, fake) = setup(None).await;
    seen(&fake).reject_login = Some((
        StatusCode::ACCEPTED,
        json!({"captchaRequired": false, "inAppToken": "tok-secret", "status": 12_u8, "statusText": "inAppTOTPNeeded"}),
    ));
    let error = tri!(PERSONAL.daily(&app).await.err().context("login accepted")).to_string();
    ensure!(error.contains("authenticator app"), "{error}");
    ensure!(error.contains("inAppTOTPNeeded"), "{error}");
    ensure!(!error.contains("tok-secret"), "{error}");
    Ok(())
}

/// A 200 without a session id names the reply's keys, not its values.
#[tokio::test]
async fn a_login_reply_without_a_session_id_names_its_keys() -> Result<()> {
    let (app, _record, fake) = setup(None).await;
    seen(&fake).reject_login = Some((
        StatusCode::OK,
        json!({"statusText": "odd", "secret": "hunter3"}),
    ));
    let error = tri!(PERSONAL.daily(&app).await.err().context("login accepted")).to_string();
    ensure!(error.contains("no sessionId"), "{error}");
    ensure!(error.contains("keys: [statusText,secret]"), "{error}");
    ensure!(!error.contains("hunter3"), "{error}");
    Ok(())
}

/// The one-time password matches the RFC 6238 test vector, in either case and with spaces.
#[test]
fn totp_matches_the_rfc_6238_vector() -> Result<()> {
    // RFC 6238 appendix B, SHA-1, secret "12345678901234567890", T = 59: 94287082.
    let totp = |secret: &str, unix: u64| {
        let login = Login {
            password: String::new(),
            totp_secret: Some(secret.to_owned()),
            username: String::new(),
        };
        login.one_time_password(unix)
    };
    let secret = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ";
    ensure!(
        tri!(totp(secret, 59)).as_deref() == Some("287082"),
        "T = 59"
    );
    ensure!(
        tri!(totp(
            "gezd gnbv gy3t qojq gezd gnbv gy3t qojq",
            1_111_111_109
        ))
        .as_deref()
            == Some("081804"),
        "T = 1111111109, lower case with spaces"
    );
    ensure!(totp("not base32!", 59).is_err(), "bad secret refused");
    Ok(())
}

/// Windows are a year each from the start, and the last one ends today.
#[tokio::test]
async fn windows_are_yearly_and_end_today() -> Result<()> {
    let (github, _record) = testing::fake_github().await;
    let app = testing::app(&github);
    let today = tri!(NaiveDate::from_ymd_opt(2026, 11, 1).context("date"));
    let api = Api {
        app: &app,
        base: "",
        int_account: 0,
        now: today.and_time(NaiveTime::MIN).and_utc(),
        session_id: String::new(),
        source: &PERSONAL,
    };
    let windows = api.date_windows(HISTORY_START);
    let end_2010 = tri!(NaiveDate::from_ymd_opt(2010, 12, 31).context("date"));
    ensure!(
        windows.first() == Some(&(HISTORY_START, end_2010)),
        "first window is 2010"
    );
    let start_2011 = tri!(NaiveDate::from_ymd_opt(2011, 1, 1).context("date"));
    ensure!(
        windows.get(1).map(|window| window.0) == Some(start_2011),
        "second window starts 2011"
    );
    ensure!(
        windows.last().map(|window| window.1) == Some(today),
        "last window ends today"
    );
    ensure!(windows.len() == 17, "one window per year");
    let week_ago = tri!(today.checked_sub_signed(Duration::days(7)).context("date"));
    ensure!(
        api.date_windows(week_ago) == [(week_ago, today)],
        "a short range is one window"
    );
    let future = tri!(today.checked_add_signed(Duration::days(3)).context("date"));
    ensure!(
        api.date_windows(future) == [(today, today)],
        "a start after today is today"
    );
    Ok(())
}
