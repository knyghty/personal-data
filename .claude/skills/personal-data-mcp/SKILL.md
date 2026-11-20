---
name: personal-data
description: Answer questions about the user's own health, sleep, fitness, home, weather, air quality, whereabouts, calendar, money, gaming, music, screen time and coding activity with the personal-data MCP connector (tools `tables` and `query`). Use whenever the user asks about his sleep, weight, heart rate, steps, workouts, mood, home sensors, the weather or air he was in, where he was, his meetings and appointments, spending, groceries, Steam playtime, the music he listened to, app and phone use, Claude Code sessions or GitHub activity, or how any of these relate to each other.
---

# Personal data

The `personal-data` connector serves DuckDB tables built from the user's raw data: Withings, Apple Health (which also holds Oura and Withings readings), Home Assistant (including the phone's GPS fixes), Open-Meteo weather and air quality, Luchtmeetnet measured air quality, calendars, bunq, Albert Heijn receipts, Steam, Last.fm, GitHub, and screen time, browser history and Claude Code activity from the Mac.

## How to work

1. Call `tables` once per conversation. It lists every table with a description, its columns and when the data was last built. Read the descriptions: they carry the caveats.
2. Call `query` with DuckDB SQL. Aggregate in SQL (per day, week or month) instead of pulling raw rows: results stop at 200 rows by default (`max_rows` raises it), and long ranges of minute or hourly data are millions of rows.
3. Check the date range you are about to analyse exists (`min(ts)`, `max(ts)`) before drawing conclusions; sources started at different times.
4. Answer with the numbers, then the caveats that matter. For trends or comparisons, make a chart when the client can.

## Time

- Timestamps are `TIMESTAMPTZ` in UTC. The user lives in Amsterdam: use `ts AT TIME ZONE 'Europe/Amsterdam'` for local hours, and `(ts AT TIME ZONE 'Europe/Amsterdam')::DATE` for local days.
- Nights: `withings_sleep_night.wake_date` is the morning the night ended. Use it to line sleep up with the day after.
- `weather_day` days are UTC days; group `weather_hour` for local days.

## Pitfalls

- `apple_sample` is long form: filter on `name` (e.g. `step_count`, `active_energy` in kJ, `heart_rate_variability`, `respiratory_rate`, `time_in_daylight`, `blood_oxygen_saturation`). Energy is in kJ; divide by 4.184 for kcal.
- `apple_sample.source`: most metrics, including steps and energy, have an empty source because Apple Health already merged the devices, so sum them as they are. The rest carry device or app names such as `<name>’s Apple Watch`, `<name>’s iPhone`, `Withings`, `Oura`, `eufy Life`, `Dexcom ONE+`, sometimes several joined with ` | `. The apostrophe is a curly `’`, so match with `source ILIKE '%watch%'`, and look at `SELECT DISTINCT source` for a metric before filtering. Where a metric has several sources, pick one or average; never sum across them.
- Sleep exists twice: `withings_sleep_night` (Sleep Analyzer under the mattress, with stages, HR, breathing and a score) and `apple_sleep_night` (Watch and other sources). Say which one you used.
- `bunq_payment`: negative amounts are money out. Card purchases are `type = 'MASTERCARD'`; `SAVINGS` and transfers between the user's own accounts are not spending, so exclude them (compare `counterparty_iban` with `bunq_account.iban`).
- `steam_playtime_hour`: rows with `games_that_hour > 1` are usually automated card idling, not real play.
- Home Assistant: `ha_history` covers only recent days; `ha_statistics` has hourly long-term history per `statistic_id` (e.g. `sensor.living_room_homepod_temperature`). Join `ha_history.entity_id` to `ha_entity`, `ha_device` and `ha_area` for names and rooms.
- Weather follows the user's phone only since collection started (October 2026). Earlier hours (`from_archive`) are an ERA5 backfill for home in Amsterdam, so on days the user travelled they describe Amsterdam, not where he was. Check `lat`/`lon` before relating weather to a trip.
- Air quality exists twice: `weather_air_quality_hour` is modelled (CAMS) at the phone's position, `air_quality_measured_hour` is measured at a fixed Luchtmeetnet station (NL49017, a busy Amsterdam road about 3.5 km from home) since 2019. The measured table is long form: filter on `component` (`NO2`, `PM10`, `PM25`, `LKI` for the 1 to 11 index), and `hour` is the start of the averaged hour. Say which one you used.
- `location` covers only the time since collection started (October 2026) and is sparse: Home Assistant records a fix only when the position changes significantly. For where the user was at a moment, take the last fix before it; for workout routes use `apple_workout_route`.
- `calendar_event` is the current state of each calendar. Recurring events are one row with `rrule`, not expanded, so count occurrences with care. Rows with `redacted = true` (work) have no summary, location or description: use them for busy time only and never guess what the meetings were.
- GitHub: rows with `redacted = true` are the user's work in other people's private repos, kept only as times, kinds, states and line counts, with no repo, title or body. Use them for working hours and volume; never try to work out which repo or project they belong to. Public and personal repos are complete, including comment and description text. `github_issue_comment` lacks inline review comments, `github_star` misses unstars, and `github_follow.first_seen` is when collection first saw a follow.
- Albert Heijn: `ah_receipt` and `ah_receipt_item` come from AH's unofficial app API, so they can stop updating. They hold only purchases with the user's bonus card. Amounts are in EUR. The same purchases are in `bunq_payment`, so never add the two up.
- Last.fm: `lastfm_scrobble` holds only what the scrobbler app caught, so plays on other devices or apps may be missing. `ts` is when the track finished or passed half, not when it started. Apple Music plays appear only once the scrobbler app has run, so the latest hours can be incomplete. `loved` is as of the fetch; `lastfm_loved` is current.
- Screen time: `screen_time_app` starts about four weeks before the helper was installed (Biome and knowledgeC keep no more), so there is no older history. iPhone and iPad rows exist only with Share Across Devices on. iPhone intervals are stitched from focus changes and capped at an hour, so a long single interval is an upper bound. Sum per device; Mac and iPhone time overlap.
- Browser history: `browser_visit` is Safari and Firefox on the Mac. Visits from the iPhone or other machines are there only if synced (iCloud, Firefox Sync), so never treat it as complete phone browsing. In Safari `origin_or_source` 1 is a visit from the iPhone or iPad; a synced Firefox visit looks like a local one. `url` is the full URL and may hold tokens or session ids: never print URLs in answers unless the user asks; use `host`.
- Claude Code: `claude_code_event` and `claude_code_session` are metadata only (times, types, models, tool names, tokens), with no prompts or outputs. Never claim to know what a session was about beyond its `project`.
- A table this skill mentions but `tables` does not list is a source that is not collected yet (GitHub needs a token). Say so rather than guessing.

## Raw files

When a table lacks a field, `query` can read the raw JSON: `read_json('raw/<source>/<type>/**/*.json', union_by_name = true)`. Files are append-only and overlap, so dedupe (latest file wins). Prefer the tables: raw scans are slow.

## Examples

Average sleep and score per month:

```sql
SELECT date_trunc('month', wake_date) AS month, round(avg(asleep_h), 2) AS sleep_h, round(avg(score)) AS score
FROM withings_sleep_night GROUP BY 1 ORDER BY 1;
```

Daily steps against sleep the night after:

```sql
WITH steps AS (
  SELECT (ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, sum(qty) AS steps
  FROM apple_sample WHERE name = 'step_count' GROUP BY 1
)
SELECT s.day, s.steps, n.asleep_h, n.score
FROM steps s JOIN withings_sleep_night n ON n.wake_date = s.day + 1
ORDER BY s.day DESC LIMIT 30;
```

Spending per month by merchant:

```sql
SELECT date_trunc('month', ts) AS month, counterparty, -sum(amount) AS spent
FROM bunq_payment WHERE type = 'MASTERCARD' AND amount < 0
GROUP BY ALL ORDER BY month DESC, spent DESC;
```
