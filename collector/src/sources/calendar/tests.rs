//! Tests of the ICS redaction and parsing and of the hourly commits.

use alloc::collections::BTreeMap;
use alloc::sync::Arc;
use std::sync::{Mutex, PoisonError};

use anyhow::{Context as _, Result, ensure};
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use super::{Calendar, Ics, Property, Source as _};
use crate::app::App;
use crate::testing;
use crate::tri;

/// A calendar with a zoned recurring event with an alarm, an all-day event and a to-do.
const ICS: &str = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Acme Corp//EN\r\nX-WR-CALNAME:Acme\r\n\
BEGIN:VTIMEZONE\r\nTZID:Europe/Amsterdam\r\nBEGIN:STANDARD\r\nDTSTART:19701025T030000\r\nTZOFFSETFROM:+0200\r\nTZOFFSETTO:+0100\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n\
BEGIN:VEVENT\r\nUID:abc@acme.example\r\nDTSTAMP:20261016T120000Z\r\nDTSTART;TZID=Europe/Amsterdam:20261020T090000\r\nDURATION:PT1H30M\r\n\
RRULE:FREQ=WEEKLY;BYDAY=TU\r\nEXDATE;TZID=Europe/Amsterdam:20261027T090000,20261103T090000\r\nSUMMARY:Board meeting\\, Q4\r\n\
LOCATION:Room 1\r\nATTENDEE;CN=\"Doe; Jane\":mailto:jane@acme.example\r\nDESCRIPTION:Line one\\nline two that is folded\r\n  across lines\r\n\
STATUS:CONFIRMED\r\nX-MICROSOFT-CDO-BUSYSTATUS:BUSY\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Reminder\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\nEND:VEVENT\r\n\
BEGIN:VEVENT\r\nUID:day@acme.example\r\nDTSTART;VALUE=DATE:20261024\r\nDTEND;VALUE=DATE:20261025\r\nSUMMARY:Holiday\r\nTRANSP:TRANSPARENT\r\nEND:VEVENT\r\n\
BEGIN:VTODO\r\nUID:todo\r\nSUMMARY:Secret task\r\nEND:VTODO\r\nEND:VCALENDAR\r\n";

/// The ICS each fake calendar serves, by path. A missing path answers 404.
type Fake = Arc<Mutex<BTreeMap<String, String>>>;

/// The value at a JSON pointer, or null when missing.
fn field<'value>(value: &'value Value, pointer: &str) -> &'value Value {
    value.pointer(pointer).unwrap_or(&Value::Null)
}

/// The commit messages, trees and blobs the fake GitHub received so far.
fn received(recorded: &testing::Shared) -> (Vec<String>, Vec<Vec<String>>, Vec<String>) {
    let seen = recorded.lock().unwrap_or_else(PoisonError::into_inner);
    (
        seen.commit_messages.clone(),
        seen.trees.clone(),
        seen.blobs.clone(),
    )
}

/// An app with two fake calendars, `personal` as is and `work` redacted.
async fn setup() -> (Arc<App>, testing::Shared, Fake) {
    let (github, recorded) = testing::fake_github().await;
    let fake: Fake = Arc::new(Mutex::new(BTreeMap::from([
        ("s3cret-personal".to_owned(), ICS.to_owned()),
        ("s3cret-work".to_owned(), ICS.to_owned()),
    ])));
    let serve_ics =
        async |State(calendars): State<Fake>, Path(name): Path<String>| -> (StatusCode, String) {
            let ics = calendars
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&name)
                .cloned();
            // A new DTSTAMP on every download, as Google does.
            ics.map_or_else(
                || (StatusCode::NOT_FOUND, "no".to_owned()),
                |text| {
                    (
                        StatusCode::OK,
                        text.replace(
                            "DTSTAMP:20261016T120000Z",
                            &format!("DTSTAMP:{}", Utc::now().format("%Y%m%dT%H%M%S%.fZ")),
                        ),
                    )
                },
            )
        };
    let router = Router::new()
        .route("/{name}", get(serve_ics))
        .with_state(Arc::clone(&fake));
    let base = testing::serve(router).await;
    let config = json!({
        "personal": {"url": format!("{base}/s3cret-personal"), "redact": false},
        "work": {"url": format!("{base}/s3cret-work"), "redact": true},
    });
    let app = testing::app_with(&github, |cfg| cfg.calendars = Some(config.to_string()));
    (app, recorded, fake)
}

/// The hex SHA-256 of a UID, as the redaction writes it.
fn sha(text: &str) -> String {
    format!("{:x}", Sha256::digest(text))
}

/// Redaction keeps only time zones, times, recurrence and status, and hashes UIDs.
#[test]
fn redacts_to_times_status_and_time_zones() -> Result<()> {
    let redacted = Ics(ICS).redact();
    for gone in [
        "Acme", "Board", "Room", "jane", "Line one", "Reminder", "TRIGGER", "Secret", "DTSTAMP",
        "abc@",
    ] {
        ensure!(!redacted.contains(gone), "{gone} in {redacted}");
    }
    for kept in [
        "TZID:Europe/Amsterdam\r\nBEGIN:STANDARD\r\nDTSTART:19701025T030000\r\n",
        "DTSTART;TZID=Europe/Amsterdam:20261020T090000\r\nDURATION:PT1H30M\r\nRRULE:FREQ=WEEKLY;BYDAY=TU\r\n",
        "STATUS:CONFIRMED\r\nX-MICROSOFT-CDO-BUSYSTATUS:BUSY\r\nEND:VEVENT\r\n",
        "DTSTART;VALUE=DATE:20261024\r\n",
        "TRANSP:TRANSPARENT\r\n",
    ] {
        ensure!(redacted.contains(kept), "{kept} missing from {redacted}");
    }
    ensure!(
        redacted.contains(&format!("UID:{}\r\n", sha("abc@acme.example"))),
        "hashed uid"
    );
    ensure!(
        redacted.starts_with("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTIMEZONE"),
        "header"
    );
    ensure!(
        redacted.ends_with("END:VEVENT\r\nEND:VCALENDAR\r\n"),
        "footer"
    );
    Ok(())
}

/// Events parse into lower-case properties with time zones, lists and durations; nested components are skipped.
#[test]
fn parses_events() -> Result<()> {
    let events = Ics(ICS).events();
    ensure!(events.len() == 2, "two events");
    let first = tri!(events.first().context("first event"));
    ensure!(field(first, "/uid") == "abc@acme.example", "uid");
    ensure!(field(first, "/dtstart") == "20261020T090000", "dtstart");
    ensure!(
        field(first, "/dtstart_tzid") == "Europe/Amsterdam",
        "dtstart_tzid"
    );
    ensure!(*field(first, "/duration_s") == 5400_i64, "duration_s");
    ensure!(field(first, "/rrule") == "FREQ=WEEKLY;BYDAY=TU", "rrule");
    ensure!(
        *field(first, "/exdate") == json!(["20261027T090000", "20261103T090000"]),
        "exdate"
    );
    ensure!(
        *field(first, "/exdate_tzid") == Value::Null,
        "exdate has no tzid"
    );
    ensure!(field(first, "/summary") == "Board meeting, Q4", "summary");
    ensure!(
        field(first, "/description") == "Line one\nline two that is folded across lines",
        "description"
    );
    ensure!(
        field(first, "/x_microsoft_cdo_busystatus") == "BUSY",
        "busy status"
    );
    ensure!(
        first.get("attendee").is_none() && first.get("action").is_none(),
        "attendee and alarm action are skipped"
    );
    let second = tri!(events.get(1).context("second event"));
    ensure!(field(second, "/dtstart") == "20261024", "all-day dtstart");
    ensure!(field(second, "/transp") == "TRANSPARENT", "transp");
    let redacted = Ics(&Ics(ICS).redact()).events();
    let first_redacted = tri!(redacted.first().context("first redacted event"));
    ensure!(
        *field(first_redacted, "/uid") == sha("abc@acme.example"),
        "redacted uid"
    );
    ensure!(first_redacted.get("summary").is_none(), "redacted summary");
    Ok(())
}

/// Durations with weeks, days, hours and minutes, a sign, and without the `P`.
#[test]
fn parses_durations() -> Result<()> {
    let seconds = |duration: &str| Property::parse(&format!("DURATION:{duration}")).seconds();
    ensure!(seconds("PT45M") == Some(2700), "minutes");
    ensure!(seconds("P1DT2H") == Some(93_600), "a day and hours");
    ensure!(seconds("-P1W") == Some(-604_800), "negative week");
    ensure!(seconds("1H").is_none(), "no P");
    Ok(())
}

/// A calendar's first file is whole: the header, the parsed events and their text, redacted where configured.
#[tokio::test]
async fn hourly_stores_each_calendar_whole_first() -> Result<()> {
    let (app, recorded, _) = setup().await;
    tri!(Calendar.hourly(&app).await);
    let (messages, trees, blobs) = received(&recorded);
    ensure!(messages == ["calendar: personal, work"], "commit message");
    let tree = tri!(trees.first().context("one tree"));
    ensure!(
        tree.first()
            .is_some_and(|path| path.starts_with("raw/calendar/personal/")),
        "personal path"
    );
    ensure!(
        tree.get(1)
            .is_some_and(|path| path.starts_with("raw/calendar/work/")),
        "work path"
    );
    ensure!(
        blobs.iter().all(|blob| !blob.contains("s3cret")),
        "no URL in the blobs"
    );
    let personal: Value = tri!(serde_json::from_str(tri!(
        blobs.first().context("personal blob")
    )));
    ensure!(
        *field(&personal, "/request") == json!({"calendar": "personal", "redacted": false}),
        "personal request"
    );
    ensure!(
        *field(&personal, "/response/full") == json!(true),
        "the first file is whole"
    );
    ensure!(
        field(&personal, "/response/header")
            .as_str()
            .is_some_and(|header| header
                .starts_with("BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:-//Acme")
                && header.contains("TZID:Europe/Amsterdam")),
        "personal header with the time zone"
    );
    ensure!(
        field(&personal, "/response/events/0/summary") == "Board meeting, Q4",
        "personal summary"
    );
    ensure!(
        field(&personal, "/response/vevents/0")
            .as_str()
            .is_some_and(|text| text.contains("ATTENDEE;CN=")
                && text.contains("folded across lines")
                && !text.contains("DTSTAMP")),
        "event text keeps every property but DTSTAMP"
    );
    let work: Value = tri!(serde_json::from_str(tri!(
        blobs.get(1).context("work blob")
    )));
    ensure!(
        field(&work, "/response/vevents")
            .as_array()
            .is_some_and(|texts| texts.iter().all(|text| !text.to_string().contains("Board"))),
        "work events are redacted"
    );
    ensure!(
        field(&work, "/response/events/0/dtstart") == "20261020T090000",
        "work dtstart"
    );

    Ok(())
}

/// After the first file, only changed and removed events are stored, and a new `DTSTAMP` alone stores nothing.
#[tokio::test]
async fn hourly_stores_only_changed_and_removed_events() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    tri!(Calendar.hourly(&app).await);
    tri!(Calendar.hourly(&app).await);
    ensure!(received(&recorded).1.len() == 1, "no new file");

    let changed = ICS.replace("Room 1", "Room 2");
    fake.lock().unwrap_or_else(PoisonError::into_inner).extend([
        ("s3cret-personal".to_owned(), changed.clone()),
        ("s3cret-work".to_owned(), changed.clone()),
    ]);
    tri!(Calendar.hourly(&app).await);
    // The redacted work calendar does not hold the location, so it did not change.
    let (messages, _, blobs) = received(&recorded);
    ensure!(
        messages.get(1) == Some(&"calendar: personal".to_owned()),
        "only personal changed"
    );
    let delta: Value = tri!(serde_json::from_str(tri!(
        blobs.get(2).context("delta blob")
    )));
    ensure!(
        *field(&delta, "/response/full") == json!(false)
            && field(&delta, "/response/events")
                .as_array()
                .is_some_and(|events| events.len() == 1)
            && field(&delta, "/response/events/0/location") == "Room 2"
            && *field(&delta, "/response/removed") == json!([])
            && field(&delta, "/response/header").is_null(),
        "the delta holds only the changed event: {delta}"
    );

    // Drop the all-day event: the next file names it as removed.
    let without = changed.replace(
        "BEGIN:VEVENT\r\nUID:day@acme.example\r\nDTSTART;VALUE=DATE:20261024\r\nDTEND;VALUE=DATE:20261025\r\nSUMMARY:Holiday\r\nTRANSP:TRANSPARENT\r\nEND:VEVENT\r\n",
        "",
    );
    fake.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .extend([("s3cret-personal".to_owned(), without)]);
    tri!(Calendar.hourly(&app).await);
    let (_, _, after) = received(&recorded);
    let gone: Value = tri!(serde_json::from_str(tri!(
        after.get(3).context("removal blob")
    )));
    ensure!(
        *field(&gone, "/response/removed") == json!(["day@acme.example|"])
            && *field(&gone, "/response/events") == json!([]),
        "the removal names the event's key: {gone}"
    );
    Ok(())
}

/// One calendar's failure is reported after the others are committed, without its URL.
#[tokio::test]
async fn a_failing_calendar_keeps_the_others_and_hides_its_url() -> Result<()> {
    let (app, recorded, fake) = setup().await;
    drop(
        fake.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove("s3cret-work"),
    );
    let Err(error) = Calendar.hourly(&app).await else {
        anyhow::bail!("hourly succeeded without the work calendar");
    };
    ensure!(
        error.to_string() == "calendar work 404 Not Found",
        "error: {error}"
    );
    ensure!(
        received(&recorded).0 == ["calendar: personal"],
        "personal is committed"
    );
    Ok(())
}

/// A malformed `CALENDARS` is reported by position, not by content.
#[tokio::test]
async fn bad_config_does_not_echo_it() -> Result<()> {
    let (github, _) = testing::fake_github().await;
    let app = testing::app_with(&github, |cfg| {
        cfg.calendars = Some(r#"{"work": "https://example.com/s3cret"}"#.into());
    });
    let Err(error) = Calendar.hourly(&app).await else {
        anyhow::bail!("hourly accepted a bad CALENDARS");
    };
    let message = error.to_string();
    ensure!(!message.contains("s3cret"), "{message}");
    Ok(())
}

/// Without `CALENDARS` the hourly run does nothing.
#[tokio::test]
async fn unconfigured_is_skipped() -> Result<()> {
    let (github, recorded) = testing::fake_github().await;
    let app = testing::app(&github);
    tri!(Calendar.hourly(&app).await);
    ensure!(received(&recorded).1.is_empty(), "no commits");
    Ok(())
}
