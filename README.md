# personal-data

Raw personal data, append-only. A small Rust service (`collector/`) pulls or receives data from each source. It stores every batch as a new gzipped file under `raw/` in the Tigris bucket `personal-data-raw`. The query server (`duckdb/`) builds clean tables from the files into DuckLake and serves them to Claude over MCP.

## Layout

```
raw/<source>/<type>/YYYY/MM/DD/<timestamp>[-suffix].json.gz   in the bucket
collector/   the service (Rust, runs on Fly.io)
duckdb/      the query server and the table definitions (Python, runs on Fly.io)
mac/         the Mac helper (Rust, runs on the Mac)
```

The repository holds code only. Files are never rewritten. Pulled data is wrapped as `{"fetched_at", "request", "response"}`, where `response` is the API reply verbatim. Apple Health and screen time pushes are stored exactly as received, with no wrapper. Windowed pulls (Home Assistant history, Withings sleep) are named by the start of the window. Pushes and snapshots are named by the time of receipt.

Sources and types:

| source | types | cadence |
|---|---|---|
| withings | `measure`, `activity`, `workouts`, `sleep_summary`, `intraday_activity`, `sleep`, `heart`, `heart_signal`, `notification`, and daily `user_devices`, `user_goals` | hourly poll plus webhook |
| apple_health | one type per Health Auto Export automation, named after the automation | pushed by the phone |
| screen_time | `push`: Mac app usage, iPhone and iPad app focus, Biome stream names and media records, Claude Code activity, Safari, Firefox and Orion history, Photos metadata and labels, Podcasts play state, Obsidian note metadata, shell command history | pushed by the Mac every 15 minutes |
| network | `push`: UniFi clients with signal and rates, the access point's radios and neighbouring networks, AdGuard DNS queries counted per client and domain | pushed by the Home Assistant Pi every hour |
| home_assistant | `history`, `logbook`, `statistics`, and daily `states`, `config`, `registry_entity`, `registry_device`, `registry_area`, `registry_floor`, `registry_label`, `statistic_ids` | hourly |
| weather | `open_meteo`, `open_meteo_air_quality`, daily `open_meteo_daily`, and one-off `open_meteo_archive`, for the phone's last known position | hourly |
| luchtmeetnet | per station `measurement`, `lki`, and daily `station` | hourly, `station` only on change |
| calendar | one type per calendar in `CALENDARS`, named after it | hourly, only on change |
| bunq, bunq_business | per account `payment`, `mastercard_action`, `request_inquiry`, `request_response`, per user `invoice`, `notification`, and daily `monetary_account`, `card` | hourly poll plus webhook |
| steam | `summary`, `recently_played`, per game `achievements`, and daily `owned_games`, `badges`, `badge_progress`, `friends`, `bans`, `groups`, `wishlist` | hourly, all but `summary` only on change |
| lastfm | `recent_tracks` (backfill), `scrobble`, and daily `user_info`, `loved_tracks` | hourly, only new scrobbles, daily types only on change |
| github | `event`, and daily `commit`, `contributions`, `issue`, `pull_request`, `issue_comment`, `discussion`, `discussion_comment`, `star`, `gist`, `followers`, `following` | hourly |
| albert_heijn | `receipt` | hourly, only new receipts |
| youtube | `video`: Data API details of each video in browser history | hourly, each video once |
| twitch | `user`, `video`: Helix lookups of channels and past broadcasts in browser history, and daily `follows`, `subscriptions` | hourly, each once; daily only on change |
| knmi | `hourly`, `daily` measured station weather (Schiphol, IJmuiden, Wijk aan Zee) | daily, last 10 days; hourly backfill from 2019 |
| knmi_edr | `ten_minute` per station, `grid` (daily rain, temperature, evaporation at home) | daily; grids backfilled from 1951 hourly |
| knmi_radar | `precipitation`: every 5-minute nowcast run at home, radar rain plus 2 hours of forecast | hourly, from the last stored run |
| earthquakes | `catalogue` (once), `recent` (last 60 days), FDSN text | daily |
| water | `temperature` per Rijkswaterstaat station, daily `swimming_spots` from zwemwater.nl | hourly, only new measurements; spots daily, only on change |
| kindle | daily `library` (XML), `whispersync` (read states, Vocabulary Builder), `sidecars` (positions, highlights, notes per book) | daily, only on change |
| audible | daily `library`, `stats` (listening time per day and month), `positions` | daily, only on change |
| degiro, degiro_pension | daily `portfolio` (positions, totals, cash funds), `transaction` and `account_overview` (cash movements) per date window, `product` (each product once) | daily, history backfilled from 2010 |
| google | per account, every file of the Data Portability archives: My Activity (YouTube, Maps, Search), YouTube subscriptions, comments, playlists, Maps places | daily, snapshots only on change |
| gmail | per account `message`: headers, labels and size of every mail, whole messages only from `GMAIL_FULL_SENDERS`; daily `labels`, `profile` | hourly, new mail only; daily only on change |
| macrofactor | `food-log`, `workout-log`: the app's CSV exports, every nutrient per food entry and every set; `workbook`: the full XLSX export, daily totals, weight, expenditure, steps, training stats, targets and settings | by hand, named by the export time |
| apple_media_services | `export`: the privacy.apple.com export in Apple's folders, nested zips unpacked: Apple Music plays and library, podcasts, Apple TV, purchases, store activity, TestFlight, Game Center | by hand, dated by the last day covered |
| moneybird | per administration `sales_invoice`, `purchase_invoice`, `receipt`, `journal_document`, `contact`, `financial_mutation`, `time_entry`, and daily `administration`, `ledger_account`, `project`, `product`, `tax_rate`, `financial_account`, `recurring_sales_invoice` | hourly, only new or changed items; daily only on change |
| offers | per store `prijsprofeet` (supermarket offers from the PrijsProfeet API) and `folderz` (drugstore offers the Pi parses from folderz.nl) | daily pull and daily push, only offers not stored before |

## How it runs

The collector is one Fly.io app (`glimmering-voice-610`) with a 1 GB volume for cursors and tokens. When the machine is idle, Fly stops it. When a request arrives, Fly starts it. A second, tiny scheduled machine calls `POST /admin/run` about once an hour. That run pulls every source and takes the daily snapshots once a day. Cursors move only after a successful upload, so the next run pulls a missed hour. After each upload, the collector calls the query server's `POST /<token>/ingest` with the new paths (`INGEST_URL`).

Endpoints:

| route | purpose |
|---|---|
| `GET /health` | liveness |
| `POST /admin/run` | run all sources now (`?job=daily` forces the daily part). If any source failed, the status is 500. Bearer `ADMIN_SECRET` |
| `GET /admin/state` | cursors and last runs, secrets redacted. Bearer `ADMIN_SECRET` |
| `PUT /admin/state` | merge a JSON object into the state, for example to move a cursor back. A null value removes the key. Bearer `ADMIN_SECRET` |
| `PUT /admin/raw/<path>` | store the body as `raw/<path>`, for files added by hand, such as lab reports and app exports. Bearer `ADMIN_SECRET` |
| `GET /withings/auth?key=ADMIN_SECRET` | start the Withings OAuth flow |
| `GET /withings/callback` | Withings redirects here |
| `POST /withings/notify` | Withings webhook |
| `POST /apple-health/ingest` | Health Auto Export target. Bearer `APPLE_HEALTH_SECRET` |
| `POST /screen-time/ingest` | `personal-data-mac` target. Bearer `SCREEN_TIME_SECRET` |
| `POST /network/ingest` | `pi/push_network.py` target. Bearer `NETWORK_SECRET` |
| `POST /bunq/notify/<token>`, `POST /bunq_business/notify/<token>` | bunq webhooks. The token is generated by the collector |
| `GET /albert_heijn/auth?key=ADMIN_SECRET&code=CODE` | exchange the code of a hand login to Albert Heijn for tokens |

## Setup

Install `flyctl`, `gh` and the Rust toolchain. Then:

1. Create the app and volume. The app name is generic on purpose.

   ```sh
   cd collector
   fly apps create --generate-name -o personal
   fly volumes create data --size 1 -r ams -a <app>
   ```

   Put the app name and `https://<app>.fly.dev` in `fly.toml` (`app`, `PUBLIC_URL`).

2. Create the bucket. This sets `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL_S3`, `AWS_REGION` and `BUCKET_NAME` on the app:

   ```sh
   fly storage create -n personal-data-raw -a <app>
   ```

   Create a fine-grained GitHub token for this repo only, with Contents read and Issues read and write. The collector reads `duckdb/watchlist.json` and opens issues for failures with it. Generate random secrets for the admin and push endpoints. Set all secrets:

   ```sh
   fly secrets set GITHUB_TOKEN=... ADMIN_SECRET=... APPLE_HEALTH_SECRET=... SCREEN_TIME_SECRET=... NETWORK_SECRET=... \
     HA_TOKEN=... WITHINGS_CLIENT_ID=... WITHINGS_CLIENT_SECRET=... BUNQ_API_KEY=... \
     STEAM_API_KEY=... LASTFM_API_KEY=... GITHUB_ACTIVITY_TOKEN=...
   ```

3. Deploy:

   ```sh
   fly deploy --ha=false
   ```

4. Create the hourly trigger once. It is a separate machine that only calls the app:

   ```sh
   fly machine run curlimages/curl -a <app> --schedule hourly --vm-memory 256 --restart no \
     -- curl -fsS --max-time 3000 -X POST -H "Authorization: Bearer <ADMIN_SECRET>" https://<app>.fly.dev/admin/run
   ```

   Later `fly deploy` runs update only the app machine. The trigger keeps working.

5. Check:

   ```sh
   curl https://<app>.fly.dev/health
   curl -X POST -H "Authorization: Bearer <ADMIN_SECRET>" https://<app>.fly.dev/admin/run
   fly logs -a <app>
   ```

Run the service locally with the same environment variables and `cargo run` inside `collector/`. Set `STATE_PATH` to a local file. Local runs store files in the real bucket.

### Deploy from GitHub

Every push to `main` that touches `collector/` runs `.github/workflows/collector.yml`. It runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo doc` with `RUSTDOCFLAGS=-D warnings` and `cargo test`, then deploys with `flyctl deploy --remote-only`. `collector/Cargo.toml` turns on every stable rustc, clippy and rustdoc lint, the restriction group included, and forbids `unsafe`. Nothing is allowed back, so any warning fails the build. The `?` operator is one of the banned items, so the code uses the `tri!` macro from `src/tri.rs` instead. `mac/` has the same lints, and a push that touches it runs `.github/workflows/mac.yml` on a macOS runner with the same checks and no deploy. After the deploy it calls `POST /admin/run`. If any source failed, the workflow fails. Pull requests run only the tests. Data commits under `raw/` do not start the workflow.

The workflow needs two repository secrets. Create them once:

```sh
fly tokens create deploy -a <app> -x 8760h | gh secret set FLY_API_TOKEN -R knyghty/personal-data
gh secret set ADMIN_SECRET -R knyghty/personal-data
```

The deploy token expires after a year. When the deploy job starts failing with an authentication error, create a new one.

### Withings

1. Create an application at https://developer.withings.com (Public API). Set the callback URL to `https://<app>.fly.dev/withings/callback`. The notification callback is registered by the collector itself.
2. Put the client id and secret in `fly secrets`.
3. Open `https://<app>.fly.dev/withings/auth?key=<ADMIN_SECRET>` in a browser and approve all scopes. The callback stores the tokens on the volume and subscribes to notifications.
4. The next run pulls the full history of measures, activity, workouts and sleep summaries. Per-minute sleep, intraday activity and ECG are pulled from `WITHINGS_BACKFILL_FROM` (`YYYY-MM-DD`, default one year back). Each run pulls 200 days, in commits of 20 days.

Refresh tokens rotate and the collector saves each new one at once. If the refresh token expires (one year without a run) or you revoke the app, open the auth URL from step 3 again. Nothing else changes.

### Apple Health

Install Health Auto Export on the iPhone (automations need the paid tier). Create REST API automations:

- URL `https://<app>.fly.dev/apple-health/ingest`, method POST, format JSON, Export Version 2.
- Header `Authorization` with value `Bearer <APPLE_HEALTH_SECRET>`.
- Date range Since Last Sync. Sync every 1 hour. Batch Requests on.
- One automation with data type Health Metrics and all metrics selected. The app sends single samples, with heart rate as Avg, Min and Max per reading. The automation name becomes the folder under `raw/apple_health/`.
- One automation named `personal-data-sync-nutrition` with data type Health Metrics, the dietary metrics selected, date range Previous 7 Days and sync every day. MacroFactor writes food at the time eaten, not the time logged, so Since Last Sync misses late entries; the overlap dedupes.
- One automation with data type Workouts, with Include Route Data and Include Workout Metrics on.
- One automation each for ECG, State of Mind and Medications. Symptoms and Heart Rate Notifications have no data and are not set up.
- Paste the header value from the Mac clipboard. A typed value is easy to get wrong, and the collector then logs `apple_health: rejected push`.

If a metrics push is over 16 MB, the collector splits it into files named `-part01.json` and on. A backlog after a gap then fits the collector's memory and DuckDB's JSON object limit.

The phone runs automations only while unlocked, so hourly means roughly hourly. Since Last Sync covers the gaps.

To backfill, run a manual export per data type from the app with the same URL and header. The app sends one request per day and runs only while open, so a metrics export over several years takes hours.

### Screen time

`mac/` builds `personal-data-mac`, a helper that launchd runs on the Mac every 15 minutes. Each run reads:

- `/app/usage` rows of `~/Library/Application Support/Knowledge/knowledgeC.db` without a source device, which is the Mac's own app usage.
- `App.InFocus` in `~/Library/Biome/streams/restricted/`, for the Mac (`local`) and every device under `remote/`: focus gained and lost, with time and bundle id. Device names come from `DevicePeer` in `~/Library/Biome/sync/sync.db`. iPhones and iPads appear only with Share Across Devices on under Screen Time, on them and on the Mac.
- The names of all Biome streams. Records of a stream whose name contains `NowPlaying` or `Media`, and of the context streams in `RAW_STREAMS` in `mac/src/apple.rs` (location, motion, lock, notifications, Focus, Wi-Fi, Bluetooth, audio route, web usage and others), are sent as raw protobuf fields (field number to value), to map once we see them, at most 5,000 per stream and device per push. Streams holding messages, mail, notes, calls or the pasteboard are not sent.
- `~/.claude/projects/*/*.jsonl`, the Claude Code session logs, as metadata only: session, project, time, record type, model, tool name and token counts. Message text, tool inputs, outputs and file contents never leave the Mac.
- Browser history: `history_visits` of `~/Library/Safari/History.db` and `moz_historyvisits` of every `~/Library/Application Support/Firefox/Profiles/*/places.sqlite`, one row per visit with time, full URL (query string kept), title and the profile directory. Both hold visits from other devices only when synced to the Mac, inserted on arrival with their original time. Safari's `origin` and Firefox's `source` are sent raw. Firefox's `source` is 0 organic, 2 bookmarked or 3 searched, and a visit from Firefox Sync is 0 like a local one. In Safari 0 is local and 1 synced over iCloud. Safari also sends any `history_visits` column whose name contains `device`; none is known. The cursor is the visit id, not the time, so late synced visits are still sent. Each push carries at most 20,000 visits, so the first runs send the whole history over several pushes.
- Photos metadata: every asset in `~/Pictures/Photos Library.photoslibrary/database/Photos.sqlite`, with times, location, camera, and favourite, hidden and trashed flags, but no image data. An asset is sent again when Photos changes it, ordered by modification time and uuid, at most 20,000 per push.
- Photos labels: what Photos' on-device analysis found per asset, from its search index `database/search/psi.sqlite` (scenes, plants and animals, landmarks, places, events, words read from the image), and the people of each detected face. An asset's labels are sent again when they change, compared by CRC in the state file, at most 20,000 per push.
- Podcasts: every episode in `~/Library/Group Containers/243LU875E5.groups.com.apple.podcasts/Documents/MTLibrary.sqlite` that was played, saved, bookmarked or downloaded, with its podcast, title, duration, play head, play count, play state and last played time. iCloud keeps this library in step with the iPhone, so it holds plays from every device. An episode is sent again when any of its fields change, compared by CRC in the state file, at most 5,000 per push.
- Obsidian: every file in the iCloud vault `~/Library/Mobile Documents/iCloud~md~obsidian/Documents`, as metadata only. Each file is sent as its path, created and modified times, size, word and line counts, tags and wiki link targets. The counts skip the front matter. Tags are `#tag` in the body and `tags:` in the front matter. The text never leaves the Mac. Dot files and directories (`.obsidian`, `.trash`, iCloud placeholders) are skipped. A file is sent again when any field changes, compared by CRC in the state file. It is sent once more as `deleted` when it is gone. At most 5,000 files go in one push.
- Shell history: `~/.zsh_history` and `~/.local/share/fish/fish_history`, as the time, the shell and the command's first word only, past `sudo` and `VAR=value` prefixes. Arguments never leave the Mac. Both shells rewrite their files, so the cursor is the newest time sent and the hashes of the entries at that second, at most 20,000 per shell and push.

The databases are live, so the helper copies them with their `-wal` and `-shm` to a temporary directory and reads the copy. knowledgeC writes a row when the interval ends, sometimes minutes later, so the helper reads it an hour back from its cursor and sends only rows it has not sent. It sends one JSON batch and moves its cursors in `~/Library/Application Support/personal-data-mac/state.json` only after a 2xx. Devices and stream names are sent only when they change. A run with nothing new sends nothing. Biome and knowledgeC keep about four weeks, so the first push reaches four weeks back and nothing earlier exists.

The helper needs Full Disk Access for knowledgeC, Biome and Safari. Without it the run fails with `Operation not permitted` and names the binary to allow. macOS ties the grant to the binary's signature: an ad-hoc signature changes with every build, so the binary is signed with a self-signed certificate, whose grant survives rebuilds.

Install:

1. Generate a secret and set it on the collector:

   ```sh
   SCREEN_TIME_SECRET=$(openssl rand -hex 32)
   fly secrets set -a glimmering-voice-610 SCREEN_TIME_SECRET=$SCREEN_TIME_SECRET
   ```

2. Create the signing certificate once. OpenSSL 3 needs `-legacy` for the keychain to read the bundle:

   ```sh
   openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 3650 -subj "/CN=personal-data-mac" \
     -addext "keyUsage=critical,digitalSignature" -addext "extendedKeyUsage=critical,codeSigning"
   openssl pkcs12 -export -legacy -inkey key.pem -in cert.pem -out id.p12 -passout pass:pdm -name personal-data-mac
   security import id.p12 -k ~/Library/Keychains/login.keychain-db -P pdm -T /usr/bin/codesign
   rm key.pem cert.pem id.p12
   ```

3. Build, copy and sign the binary. `codesign` lists the certificate as not trusted, which does not matter here:

   ```sh
   cd mac && cargo build --release
   mkdir -p ~/.local/bin && cp target/release/personal-data-mac ~/.local/bin/
   codesign -f -s personal-data-mac --identifier com.knyghty.personal-data-mac ~/.local/bin/personal-data-mac
   ```

4. Write the config. Without one, the first run writes a template and fails.

   ```sh
   mkdir -p ~/Library/Application\ Support/personal-data-mac
   printf '{"url": "https://glimmering-voice-610.fly.dev/screen-time/ingest", "secret": "%s"}\n' "$SCREEN_TIME_SECRET" \
     > ~/Library/Application\ Support/personal-data-mac/config.json
   ```

5. Open System Settings, Privacy & Security, Full Disk Access. Click +, press Cmd-Shift-G, enter `~/.local/bin/personal-data-mac` and turn it on.

6. Install the agent and start it:

   ```sh
   sed "s|HOME_DIR|$HOME|g" com.knyghty.personal-data-mac.plist > ~/Library/LaunchAgents/com.knyghty.personal-data-mac.plist
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.knyghty.personal-data-mac.plist
   ```

7. Check the log. A good run ends with `sent ... rows`:

   ```sh
   tail -f ~/Library/Logs/personal-data-mac.log
   ```

After a rebuild, copy and sign the binary as in step 3, then run `launchctl kickstart -k gui/$(id -u)/com.knyghty.personal-data-mac`. The grant stays.

Flights: with `"flights": {"lat": ..., "lon": ..., "radius_nm": 3}` in the config, a second agent runs `personal-data-mac --flights` every minute. It asks [adsb.lol](https://api.adsb.lol) for the aircraft within the radius and appends the reply, verbatim, to `flights.jsonl` next to the config. The 15-minute run sends those polls with its push under `flights` and deletes them only after a 2xx. Install the agent like the main one, from `com.knyghty.personal-data-mac.flights.plist`. While the Mac sleeps there are no polls, so a missing minute is not a minute without aircraft.

To see what an app keeps in a container only Full Disk Access can read, `--probe <dir>` lists its files and the tables, row counts and columns of each SQLite database, `--dump <db>` prints the first rows of each table, and `--segb <file>` shows what the SEGB reader makes of a Biome segment. Run them through launchd, which applies the grant; from a terminal they fail like any other process:

```sh
launchctl submit -l probe -o /tmp/probe.txt -- ~/.local/bin/personal-data-mac --probe ~/Library/Containers/<app>/Data
sleep 5; launchctl remove probe
```

The first run reads all Claude Code logs, 64 MB of them per run, and browser history, 20,000 visits (a few MB) per run, so a long history takes a few runs. Expected volume: a few KB per push and up to 96 pushes a day, so roughly 100 MB per year.

### Home Assistant

The collector talks to Home Assistant over Tailscale Funnel with a long-lived access token.

1. Install the Tailscale add-on. In the Tailscale admin console enable MagicDNS and HTTPS certificates, and add the `funnel` node attribute to the policy. In the add-on set `share_homeassistant: true`. Home Assistant becomes reachable at `https://homeassistant.<tailnet>.ts.net`. Put that in `HA_URL` in `fly.toml`.
2. Under System, Network in the Home Assistant UI, enable Trust X-Forwarded-For and add `127.0.0.1` to trusted proxies. Restart Home Assistant.
3. Create a long-lived access token under Profile, Security and set it as `HA_TOKEN`.
4. Set `HA_LOCATION_ENTITY` to the phone's `device_tracker` entity. The weather module uses its coordinates.

Each hourly run pulls one file per hour of history: every state change of every entity, with full attributes (`significant_changes_only=0`). It also pulls the logbook per hour and the hourly long-term statistics for every statistic id. The entity list comes from `/api/states` on every run, so new entities are included automatically. The daily snapshot stores all current states and `/api/config`. It also stores the entity, device, area, floor and label registries and the statistic id list, so ids stay interpretable later.

Retention: the recorder keeps `purge_keep_days` of history (default 10). The collector backfills from `HA_BACKFILL_DAYS` (default 10) and pulls 48 hours per run, so an outage shorter than the retention loses nothing. An outage longer than retention loses history and logbook for the gap. Long-term statistics are never purged. If the database on the Pi has room, set `purge_keep_days: 30` under `recorder:` in `configuration.yaml`.

Measured volume with 163 entities: about 1 MB of history and 0.3 MB of logbook per day, so roughly 0.5 GB per year uncompressed.

### Network

`pi/push_network.py` runs on the Home Assistant Pi as a shell command every hour and pushes one JSON body to `/network/ingest`:

- `unifi.clients`: every client the UniFi controller knows, with band, channel, signal, link rates, byte counters and uptime, trimmed to the fields in `CLIENT_FIELDS`.
- `unifi.devices`: the access point with its radio statistics (channel, width, utilisation, clients), per-network counters and uplink.
- `unifi.neighbours_by_channel` and `unifi.neighbours_strongest` every hour, and `unifi.neighbours`, the full scan of the networks around the flat, once a day.
- `adguard.queries`: the hour's DNS queries from AdGuard Home, counted per client, domain, record type, status and reason. `raw_count` is the number of queries behind the rows. The script reads AdGuard through the Supervisor's ingress with Home Assistant's own token, so it needs no AdGuard login.
- `clients`: IP to name, from the UniFi client table, to name the DNS clients.

The script keeps its AdGuard cursor in `personal_data_network_state.json` next to itself and moves it only after a 2xx. A run that cannot reach one of the two sources pushes the other and lists the failure under `errors`.

Install:

1. Generate a secret and set it on the collector: `fly secrets set -a glimmering-voice-610 NETWORK_SECRET=$(openssl rand -hex 32)`.
2. In the UniFi controller create a local admin with the read-only role for the script.
3. Copy `pi/push_network.py` to `/config/custom_templates/` on the Pi (that folder is not served over HTTP, unlike `www/`) with `personal_data_network.json` next to it:

   ```json
   {"url": "https://glimmering-voice-610.fly.dev/network/ingest", "secret": "<NETWORK_SECRET>",
    "unifi": {"url": "https://192.168.2.28:8443", "username": "<read-only admin>", "password": "<its password>", "site": "default"},
    "adguard_slug": "a0d7b954_adguard", "ha_user_id": "<user_id attribute of your person entity>"}
   ```

   The Supervisor opens the AdGuard ingress session on behalf of a Home Assistant user, so `ha_user_id` must be a real one. The `user_id` attribute of `person.<you>` is it.

4. Add the shell command to `configuration.yaml` and reload it with `shell_command.reload`:

   ```yaml
   shell_command:
     push_network: python3 /config/custom_templates/push_network.py
   ```

5. Create an automation that calls `shell_command.push_network` every hour at minute 7. The script logs one line per run to `personal_data_network.log` next to itself.

Measured volume: the first push was 176 KB with the full neighbour list and 542 DNS rows for 1,829 queries. An hourly push without the list is about 80 KB. That is roughly 700 MB per year uncompressed, most of it DNS rows.

### Weather

No setup. Open-Meteo needs no key. All times are UTC.

- `open_meteo`: the forecast API. Each hourly file holds current conditions, the past three hours and the next hour, and the last eight 15-minute steps.
- `open_meteo_air_quality`: the air quality API (CAMS). Pollutants, the European AQI and pollen for the past three hours and the next hour.
- `open_meteo_daily`: once a day, daily aggregates for the past two days and today, including sunrise, sunset and daylight duration.

To backfill, set a start date in the state:

```sh
curl -X PUT -H "Authorization: Bearer <ADMIN_SECRET>" -H "Content-Type: application/json" -d '{"weather/backfill_from": "2019-01-01"}' https://<app>.fly.dev/admin/state
```

Each hourly run then pulls 12 months, one file per month: the archive API (ERA5 reanalysis, hourly and daily) into `open_meteo_archive`, and the air quality history into `open_meteo_air_quality`. It stops two days before today and removes the key. Air quality covers 2019 on, pollen 2021 on. The backfill uses the current position, so start it at home.

Measured volume: about 85 MB per year, and about 20 MB for a backfill from 2019.

### Luchtmeetnet

No setup. The Luchtmeetnet open API (RIVM) needs no key. It is its own source, not a `weather` type, because it measures at fixed stations while `weather` models the phone's position. `LUCHTMEETNET_STATIONS` lists the station numbers, comma-separated. The default is `NL49017`, Amsterdam-Stadhouderskade, a traffic station about 3.5 km from home. `https://api.luchtmeetnet.nl/open_api/stations` lists the others.

- `measurement`: `/measurements` for the last three hours, every component the station measures. NL49017 measures NO2, NO, PM10, PM2.5, soot (`FN`), benzene, toluene and xylene, but not O3. Each value is the mean of the hour that ends at `timestamp_measured`.
- `lki`: `/lki`, the Dutch air quality index from 1 to 11, for the same hours.
- `station`: once a day, `/stations/<id>/`, stored only when it changed.

Files carry the station number as suffix, with one file per page (`-2`, `-3`). The API serves at most seven days per request and redirects a longer range to its last seven days, so a redirect fails the run.

The first runs backfill from 2019-01-01, 26 weeks per run, so about 16 runs. When done, the collector sets `luchtmeetnet/backfill_done`. To backfill again, for example after adding a station:

```sh
curl -X PUT -H "Authorization: Bearer <ADMIN_SECRET>" -H "Content-Type: application/json" -d '{"luchtmeetnet/backfill_done": null, "luchtmeetnet/backfill_from": "2019-01-01"}' https://<app>.fly.dev/admin/state
```

The newest hours are not validated yet. Hourly runs do not fetch them again, so later corrections arrive only with a new backfill.

Expected volume: about 55 MB per year, and about 130 MB once for the backfill.

### bunq

1. In the bunq app open Settings, Developers, API keys and add a key. Choose Allow All IP Addresses, because Fly machines have no fixed address.
2. Set it as `BUNQ_API_KEY`.
3. A business account is a separate bunq user. Switch to the business profile, create a key there and set it as `BUNQ_BUSINESS_API_KEY`. It gets its own files under `raw/bunq_business/`, its own state and its own callback at `/bunq_business/notify/<token>`.

On the first run the collector generates an RSA key pair, registers it with bunq (`/installation`, `/device-server`) and opens a session. The key pair and the tokens live on the volume. The auto logout setting in the app sets the session length. When a session runs out, the collector opens a new one. If you create a new API key, set the secret again. The collector notices the change and registers a new installation.

Each run lists the monetary accounts and pulls four lists per account: `payment` (the ledger, including card settlements, savings transfers and interest), `mastercard-action` (card authorisations with merchant, status and local amount), `request-inquiry` and `request-response`. It also pulls the user's `invoice` list. Each list has its own id cursor per account. The first run walks back through the whole history. It fetches 200 items per page and 100 pages per run over all lists, so a long history takes a few runs. Later runs pull only items newer than the stored ids. The daily snapshot stores all monetary accounts with their balances, including savings, joint and linked external accounts, and the cards.

The user event feed is not used. It hides card settlements and savings transfers behind other event types, and its auto-save entries made one page of 200 events 20 MB.

The daily job also registers a callback for the `MUTATION` category at `https://<app>.fly.dev/bunq/notify/<token>`. bunq posts there on every balance change and the collector pulls the new events at once. If the machine is stopped, bunq retries up to five times a minute apart, which covers the start. Existing callbacks on the account are kept.

bunq allows 3 GET requests per 3 seconds, so the collector waits about a second between pages. The API key can move money, so it is never written to the repo or the logs.

To test against the sandbox without committing to the repo, create a sandbox user and run the ignored test:

```sh
curl -sS -X POST https://public-api.sandbox.bunq.com/v1/sandbox-user-person   # ApiKey.api_key
cd collector && BUNQ_SANDBOX_KEY=sandbox_... cargo test live_sandbox -- --ignored --nocapture
```

### Steam

1. Create a Web API key at https://steamcommunity.com/dev/apikey and set it as `STEAM_API_KEY`.
2. Put the 64-bit SteamID in `STEAM_ID` in `fly.toml`.
3. Under Edit Profile, Privacy Settings, set My profile, Game details and Friends List to Public. With game details private, playtime and achievements come back empty.

Each run stores the player summary (`personastate`, current `gameid`), so every file is one sample of what Steam showed as running. It stores `GetRecentlyPlayedGames` when it changed, so the difference in `playtime_forever` between two files is the playtime of that hour. It pulls `GetOwnedGames` and fetches achievements, with English names, for every game with stats whose playtime moved since its last pull. The first run backfills all played games, 100 per run. An achievement file is committed only when the reply changed, which means a new unlock. The daily job stores owned games with extended app info only when a game was added or removed, so playtime history comes from `recently_played`. It also stores badges with level and XP, community badge progress, friends, bans, groups and the wishlist, each only when changed.

Card idling (ArchiSteamFarm) also counts as playtime. An hour in which several games gain playtime at once is likely idling, and so is a game that gains playtime while its game badge in `badges` levels up. Card drops left are not in the Web API.

Expected growth: about 10 MB per year of summaries and a few MB of recent playtime, achievements and library changes, so under 20 MB per year.

### Last.fm

The `lastfm` source stores the music the owner plays in Apple Music, which a scrobbler app sends to Last.fm.

1. Create a Last.fm account and install a scrobbler for Apple Music on the iPhone or Mac, signed in to that account.
2. Under Settings, Privacy, turn off Hide recent listening information. With it on, the API returns no recent tracks.
3. Create an API account at https://www.last.fm/api/account/create and set its API key as `LASTFM_API_KEY`. The shared secret is not needed.
4. Put the user name in `LASTFM_USER` in `fly.toml`. Without either, the source is skipped.

The first runs backfill the whole history from `user.getRecentTracks` (`extended=1`, 200 per page) into `recent_tracks`, oldest page first, 50 requests per run, and resume from the cursor. Each page is stored verbatim and named by its oldest scrobble.

After the backfill, each hourly run reads again from 14 days before the newest stored scrobble, the oldest play Last.fm accepts. The Mac scrobbles live while the iPhone app sends its plays in batches with their original times, so older scrobbles often arrive after newer ones. The collector keeps the time, artist and track of every scrobble stored within the 14 days in its state. It commits only scrobbles not stored before, into `scrobble`. Those files are not verbatim replies but a list of the new tracks, with the request and their count in `request`.

The now playing track has no time and appears as a scrobble once played, so it is ignored. 5xx replies and Last.fm errors 8, 11 and 16 are retried twice. Error 29, the rate limit, ends the run and keeps what it committed. The daily job stores `user.getInfo` (play count, registration) and all pages of `user.getLovedTracks`, each only when changed.

Expected volume: about 1 KB per scrobble, so roughly 10 to 20 MB per year, plus the backfill once.

### GitHub

The `github` source records the activity of `GITHUB_USER` (set in `fly.toml`), as a rough log of working hours. It is not the `GITHUB_TOKEN` client that commits here.

1. Create a fine-grained token owned by the user, for all of the user's repos, with read access to contents, issues, pull requests and discussions, and to the account's starring, followers and gists.
2. Set it as `GITHUB_ACTIVITY_TOKEN`. Without it, or without `GITHUB_USER`, the source is skipped.

Each hourly run reads `/users/<user>/events` and stores the events newer than the last stored id. The feed holds only the last 300 events and 90 days, so an outage longer than about a day loses events. The daily job searches `/search/commits` for commits authored by the user, sorted by author date. Search sees only default branches. Each run goes back 14 days before the newest commit and stores only commits not seen before. The first runs walk back through the whole history, 3000 commits per run. The daily job also stores the GraphQL `contributionsCollection` per 30-day window: the contribution calendar, commits per repo and day, and pull requests, reviews and issues with their times. The first runs go back to the account's creation, 60 windows per run. Later runs fetch the last seven days again.

The daily job also pages the GraphQL `issues`, `pullRequests`, `issueComments` and `repositoryDiscussions` of the user in ascending `updatedAt`, 100 per page, from the stored cursor. The cursor holds the last item's `updatedAt` and id, so each run stores what was created or changed since, such as a pull request merged after it was opened. The first runs walk back through the whole history, 40 pages per type per run. `issueComments` covers issue and pull request conversations but not inline review comments. Discussion comments, and the logins of `followers` and `following`, are stored whole when they changed. Stars come from `/users/<user>/starred`, newest first, down to the newest stored `starred_at`. Gists come from `/users/<user>/gists` with `since`, as metadata without file contents.

The collector commits to the data repo as the token owner, so every type excludes it: the events, commits and GraphQL nodes are filtered, and the search query has `-repo:<GITHUB_REPO>`. Events, commits, stars and gists are therefore not verbatim replies but a list of the new items.

Activity in public repos and in the user's own repos is stored verbatim, with bodies. Activity in other people's private repos, such as an employer's, is redacted before it is committed: events keep their id, type, action and time; commits keep their author and committer dates; stars keep `starred_at`; GraphQL nodes lose every repo name, title, number, URL, name and body but keep their `databaseId`, times, state and line counts. A GraphQL node without a visible repo is redacted too. Such items carry `"redacted": true`. GraphQL replies also drop `errors`, since they can name the organisation.

The fine-grained token cannot read other people's private repos, so work there appears only as `restrictedContributionsCount`, and search and events show only public activity. Expected volume is about 45 MB per year, plus about 70 MB once for the commit, issue, pull request and comment history.

### Calendar

1. Copy the secret ICS link of each calendar. In Proton Calendar, open the calendar's settings, then Share with anyone, Full view. `webcal://` links work too.
2. Set them as one secret, a JSON object of calendar name to link and redaction:

   ```sh
   fly secrets set CALENDARS='{"personal": {"url": "https://calendar.proton.me/api/calendar/v1/url/...", "redact": false}, "work": {"url": "https://...", "redact": true}}'
   ```

   Without it the source is skipped. The name becomes the folder under `raw/calendar/`. `redact` defaults to false.

Each hourly run downloads every calendar and stores a file only when an event changed. It ignores `DTSTAMP`, which some servers set on every download. A calendar's first file has `full: true` and holds every event. Its `response` holds the calendar's own lines and time zones under `header`, the parsed events under `events`, and each event's text under `vevents`. Each later file holds only the new and changed events, and lists the `uid|recurrence-id` keys of the events that are gone under `removed`. It holds `header` only when that changed. Files from before 2026-11-19 are whole snapshots, with the ICS text under `ics`. The `request` names the calendar. The links give read access to the whole calendar, so they are never written to the repo, the logs or issues.

A redacted calendar keeps only the `VTIMEZONE` blocks and, per event, `DTSTART`, `DTEND`, `DURATION`, `RRULE`, `RDATE`, `EXDATE`, `RECURRENCE-ID`, `STATUS`, `TRANSP` and `X-MICROSOFT-CDO-BUSYSTATUS`, with `UID` replaced by its SHA-256. Titles, descriptions, locations, attendees, organisers, links, attachments, conference details, alarms and every other component are dropped before the commit.

Recurrences are not expanded: a recurring event is one event with its `RRULE`, plus one event per changed occurrence with `RECURRENCE-ID`.

Each change stores the whole calendar about one and a half times, so a 200 KB calendar that changes once a day adds about 100 MB per year.

### Albert Heijn

The `albert_heijn` source stores in-store receipts (kassabonnen) from the API of the Appie app. The API is unofficial and undocumented, so it can break without notice. The calls follow [appie-go](https://github.com/gwillem/appie-go). Only purchases with the bonus card appear.

AH's firewall blocks scripted logins, so the collector never logs in. Log in once by hand:

1. In the browser, open the developer tools on the Network tab and turn on Preserve log.
2. In the same tab, open `https://login.ah.nl/login?client_id=appie-ios&response_type=code&redirect_uri=appie://login-exit` and log in.
3. AH redirects to `appie://login-exit?code=...`, which the browser cannot open. Copy that URL from the address bar, or from the last request in the Network tab.
4. Straight away, open `https://<app>.fly.dev/albert_heijn/auth?key=<ADMIN_SECRET>&code=<the URL>`. The bare code works too. The reply is `Albert Heijn connected`, and the tokens are on the volume.

Without a login the source is skipped. The access token lasts about two hours. A run refreshes it when it expires within ten minutes and stores the new refresh token at once, because the old one stops working. If the refresh fails, the run fails and asks for a new login: repeat the steps above.

Each run lists the receipts newest first, 100 per page, down to the first page with a stored receipt. It fetches `posReceiptDetails` of each new receipt and commits the reply as `receipt/YYYY/MM/DD/<receipt time>-<id>.json`, with the list entry in `request`. A receipt time without an offset is Amsterdam time, filed as if UTC. The first runs backfill everything AH keeps, oldest first, 100 receipts per run. AH has introspection off, so a field it removes fails the whole query. The queries are constants in `collector/src/sources/albert_heijn.rs`: delete the field the error names.

Expected volume: a few KB per receipt, so a few MB per year.

### YouTube

The `youtube` source looks up every YouTube video in the browser history of screen time pushes with `videos.list`. The ingest queues new video ids in `youtube/pending`, and each hourly run asks for up to 1,000, 50 per request, with every free part. A video the API does not return, deleted or private, is still marked seen in `youtube/seen` and not asked again.

1. In Google Cloud, create a project, enable YouTube Data API v3 and create an API key. No billing account is needed.
2. `fly secrets set -a glimmering-voice-610 YOUTUBE_API_KEY=...`

The key is sent in the `x-goog-api-key` header, so it never appears in stored requests. The API allows 10,000 units a day and a request costs one. To look up videos pushed before the source existed, put their ids in `youtube/pending` through `PUT /admin/state`.

Expected volume: about 5 KB per video.

### Twitch

The `twitch` source says what the Twitch pages in browser history were, since Twitch has no watch history API. The screen time ingest queues the channel logins and past broadcast ids of new visits; each hourly run looks up to 2,000 of each up, 100 per Helix request, with an app token. With the owner's login, the daily job stores his follows and the subscriptions among them, each only when changed.

1. At dev.twitch.tv, register an application: OAuth redirect `https://<app>.fly.dev/twitch/callback`, client type Confidential. Set `TWITCH_CLIENT_ID` and `TWITCH_CLIENT_SECRET`.
2. Open `https://<app>.fly.dev/twitch/auth?key=<ADMIN_SECRET>` and approve. The refresh token is renewed on every daily run.

To look up visits pushed before the source existed, put their logins and ids in `twitch/user/pending` and `twitch/video/pending` through `PUT /admin/state`.

### Audible

The `audible` source uses the unofficial API of the Audible apps, as the [audible](https://github.com/mkb79/Audible) Python library does. Requests are signed with the key of a registered device, which works until the device is removed under Amazon's devices. The daily job stores the library, listening time for the last 30 days and 12 months, and the last position in each book, each only when changed.

1. Register a device with the library's external login: build the login URL with `audible.login.build_oauth_url` for the store (`de` for audible.de), log in, then pass the landing URL's `openid.oa2.authorization_code` to `audible.register.register`. Long URLs do not paste into a terminal prompt, so do it in a script.
2. Put the device's `adp_token` and `device_private_key`, and the store's domain, in the state: `PUT /admin/state` with `{"audible/oauth": {"adp_token": ..., "device_private_key": ..., "domain": "de"}}`.

### DEGIRO

The `degiro` source stores the owner's investment account through the API behind the DEGIRO web trader, following [degiro-connector](https://github.com/Chavithra/degiro-connector). The API is unofficial and undocumented, so it can break without notice.

1. Set `DEGIRO_USERNAME` and `DEGIRO_PASSWORD`, the web trader login. The pension account is the `degiro_pension` source, with `DEGIRO_PENSION_USERNAME`, `DEGIRO_PENSION_PASSWORD` and `DEGIRO_PENSION_TOTP_SECRET`. Its files go under `raw/degiro_pension/`, and the tables carry both accounts with an `account` column.
2. Turn two-step verification on with an authenticator app, on each account. Without it, DEGIRO answers the login with `inAppTOTPNeeded` and waits for a tap in its own app, which an unattended run cannot give. With it on, DEGIRO refuses the plain login (`totpNeeded`), and the collector needs the base32 secret behind the authenticator app as `DEGIRO_TOTP_SECRET`. DEGIRO shows it only while setting up the authenticator: under Profile, Security, turn two-factor authentication off and on again, pick the manual entry instead of the QR code, and copy the key. Add it to the authenticator too.

Each daily run logs in, reads the account number, and stores the trading update verbatim as `portfolio`: every position with size, price, value and unrealised result, the account totals and the money market funds, one snapshot a day. It then fetches the transaction history and the cash movements (`account_overview`) per date window. The first run walks from 2010 in yearly windows; later runs fetch again from a week before the last run, since bookings land days later, and the tables dedupe by id. Empty windows are not stored. Products seen in those files are described once in `product` (name, ISIN, symbol, currency, type) and remembered in `degiro/products_seen`. The run logs out at the end.

If DEGIRO asks for a captcha or an approval in its app, the run fails with that text: log in once in a browser. The password, the TOTP secret and the session id never reach the repo, the logs or the issues: stored requests name the endpoint and window only, and errors drop the URL.

Expected volume: a few KB per day, plus the history once.

### KNMI

The `knmi` source needs no setup. KNMI's [climatology service](https://www.daggegevens.knmi.nl/klimatologie/uurgegevens) returns validated hourly and daily station data as JSON without a key, about two days behind. The daily job stores the last 10 days of both, so corrections land; each hourly run backfills three months from 2019 until it reaches the recent window. The `knmi_edr` source uses the KNMI Data Platform's [EDR API](https://developer.dataplatform.knmi.nl/edr-api) with a registered key, `KNMI_EDR_KEY`. Daily, it stores each station's 10-minute observations since the last stored time (at most 7 days back), and KNMI's daily 1 km grids (`Rd1` rain, `Tg1`, `Tn1`, `Tx1` temperature, `EV24` evaporation) at the cell around `HOME_LAT`, `HOME_LON` for the last 60 days, as the grids are about three weeks behind. Hourly, it backfills the grids from 1951 in 99-day pieces, since a request may hold at most 100 time steps. Each 10-minute reply carries about 40 KB of parameter descriptions, so one request per station per day keeps this near 225 MB a year. KNMI sometimes answers a request with a 500 that works when repeated, so each is tried three times.

The `knmi_radar` source reads rain at home from KNMI's radar through the Data Platform WMS (`KNMI_WMS_KEY`, dataset `radar_forecast`, layer `precipitationfc`). Each 5-minute nowcast run forecasts 2 hours ahead, and its step at its own reference time is the radar analysis. The server keeps 7 days and refuses requests over 512 values, so each hourly run asks one run at a time (25 steps) with GetFeatureInfo, from the last stored run, at most 96 runs. A run still missing 30 minutes after its time is a gap and skipped. Expected volume: about 300 KB a day. The Open Data and notification keys (`KNMI_OPEN_DATA_KEY`, `KNMI_NOTIFICATION_KEY`) are set but not used yet.

### Water

The `water` source needs no setup. Hourly, each Rijkswaterstaat station that measures water temperature every 10 minutes (`LIVE` in `collector/src/sources/water.rs`) is asked through the [water data API](https://ddapi20-waterwebservices.rijkswaterstaat.nl) for its measurements since the last stored one, at most 7 days back, and the reply is committed verbatim. Rijkswaterstaat repeats verbose metadata per value, so this is about 160 MB a year. Hand-sampled stations (`SAMPLED`) are asked daily for the last 45 days, as samples are entered late. Daily, the swimming spot list in the `data-swimming-spots` attribute of the zwemwater.nl home page is committed when changed; zwemwater.nl has no API, so a page change breaks it. An empty `rws_url` turns the source off.

### Kindle

The `kindle` source uses the unofficial API of the Kindle apps, signed like Audible with a registered device, here "Kindle for iOS" (device type `AGZZ1ST63LKBW`, following [mkb79/kindle](https://github.com/mkb79/kindle)). The daily job stores the library sync (`todo-ta-g7g.amazon.com/FionaTodoListProxy/syncMetaData`), every Whispersync dataset with all its records (`api.amazon.com/whispersync/v2/data/<user>/datasets`), and the sidecar of every ebook (`cde-ta-g7g.amazon.com/FionaCDEServiceEngine/sidecar`), each only when changed. A book never opened has no sidecar and is skipped. Reading sessions and time are not in these APIs.

1. Register the device as for Audible, with login `assoc_handle` `amzn_kindle_for_ios_<cc>`, `pageId` `amzn_kindle_for_ios_branded`, a client id ending in `#AGZZ1ST63LKBW`, and `device_type` `AGZZ1ST63LKBW` and `app_name` `Kindle for iOS` in the `auth/register` body.
2. `PUT /admin/state` with `{"kindle/oauth": {"adp_token": ..., "device_private_key": ..., "user_id": <customer_info.user_id>}}`.

### Google

The `google` source exports the owner's Google data with the [Data Portability API](https://developers.google.com/data-portability), available in the EEA, Switzerland and the UK. Each account starts two exports a day, the most Google allows: `activity`, My Activity from an hour before the previous export ended, and `snapshot`, everything else. The first exports took 5 to 25 minutes, and Google allows up to seven days; hourly runs poll them. A finished export's zip archives are unpacked and every file committed as `raw/google/<account>/YYYY/MM/DD/<export end>-<path in the archive>.<ext>`, except the index `archive_browser.json`. A JSON array over 20 MB, too large for one blob through GitHub's API, is split into `-partN` files of whole elements. Snapshot files are committed only when changed. Uploaded videos and Maps photos are not exported.

1. In Google Cloud, enable the Data Portability API. Under Google Auth Platform, fill in Branding (name, support email, a logo, and any URL on the app for home page and privacy policy), then publish the app under Audience. It stays unverified, which Google allows for personal use; approving shows an "unverified app" warning to click through.
2. Create an OAuth client of type Web application with redirect URI `https://<app>.fly.dev/google/callback`, and set `GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET`.
3. Open `https://<app>.fly.dev/google/auth?key=<ADMIN_SECRET>&account=<label>` and approve. A YouTube brand account has its own history: approve it as a second account with `&products=youtube,search`, picking it in Google's account list.

Approve for 180 days, the longest Google offers; an app left in Testing gets 7 days whatever is picked. A day before access ends, the run fails with the link to approve again, which opens the usual issue. The link revokes the old grant first, because Google refuses scopes a live grant already covers.

Expected volume: the first export of an account used since 2007 is about 75 MB; daily exports are not measured yet.

### Gmail

The `gmail` source stores the mail of the owner's Gmail accounts as metadata: the From, To, Cc, Subject, Date, List-Id and Message-ID headers, the labels, the size and the thread, as the [Gmail API](https://developers.google.com/gmail/api) returns them with `format=metadata`. Google's snippet, the first words of the body, is removed before the commit. Whole messages, bodies and attachments included, are stored only when the sender is in `GMAIL_FULL_SENDERS`, a comma-separated list of addresses or domains (a domain matches its subdomains): shops, railways and parcel services whose mail is a receipt or a ticket. Mail from anyone else never leaves Google as more than its headers.

1. In the Google Cloud project of the `google` source, enable the Gmail API and add `https://<app>.fly.dev/gmail/callback` to the redirect URIs of the OAuth client. The same `GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET` serve both sources.
2. `fly secrets set GMAIL_FULL_SENDERS=thuisbezorgd.nl,amazon.nl,ns.nl`, or leave it unset for metadata only.
3. Open `https://<app>.fly.dev/gmail/auth?key=<ADMIN_SECRET>&account=<label>` and approve, once per account. The label becomes the folder under `raw/gmail/`.

The mail scope is one Google calls restricted. An unverified app in production may be refused it; then keep the app in Testing with the account as a test user, where access lasts 7 days. A day before access ends, the run fails with the link to approve again, which opens the usual issue. Approving again starts the mailbox walk over, so the first files after it repeat older mail; the tables keep one copy per message.

The first runs walk the whole mailbox, newest first, 500 ids a page and at most 2,000 messages a run, and store the mailbox's history id from before the walk. After that, each hourly run asks the history API for the messages added since the stored id and moves the id after the commit. Google forgets a history id after about a week: the run then lists the newest 500 messages once and continues from the mailbox's current id, so an outage longer than a week loses the mail in between. Messages deleted between the listing and the fetch are skipped. Each run commits one file per account with every message it fetched. The daily job stores the labels and the profile (message and thread counts) when they changed.

Expected volume: about 2 KB per message as metadata, so a few MB a month for a busy inbox, plus the whole messages, which can be large with attachments.

### Moneybird

The `moneybird` source stores the bookkeeping of the owner's business from the [Moneybird API](https://developer.moneybird.com), for every administration the token can see.

1. In Moneybird, open the user menu, Applications, and create a new application of the personal token type, read-only, with the sales invoices, documents, contacts, bank, time entries and settings scopes.
2. `fly secrets set MONEYBIRD_TOKEN=...`. Without it the source is skipped.

Sales invoices, purchase invoices, receipts, general journal documents, contacts and financial mutations have a synchronization endpoint that lists the id and version of every item in one request. The collector keeps the versions in its state and fetches only new or changed items, one request each, as one file per resource and run. An edited invoice is stored again, and a deleted one drops out of the state but keeps its files. The API also returns 100 items per request on a POST to the same endpoint, but a read-only token refuses every POST. Time entries have no synchronization, so each run pages the whole period and stores the entries whose `updated_at` changed, as a list. The period runs from the administration's first bookkeeping year, at most nine years back because the API allows ten. The daily job stores the administrations, ledger accounts, projects, products, tax rates, financial accounts and recurring invoices, each only when changed.

Moneybird allows 150 requests per 5 minutes. A run makes at most 120 and stops on a 429; versions move only after the commit, so the next run carries on. The first pull of a few thousand bank mutations takes a day or two. The token can read everything in the administration, so it is never written to the repo or the logs.

Expected volume: a few KB per invoice or mutation, so a few MB per year.

### Offers

The `offers` source stores the weekly offers of the shops the owner buys from. When an item on his watch list goes on offer, it sends a Home Assistant notification. Supermarkets come from the [PrijsProfeet API](https://github.com/PrijsProfeet/supermarkt-api), a free JSON API over the flyers of Albert Heijn, Jumbo, Dirk, Aldi, Plus and others. It is refreshed nightly. Each offer carries the webshop id, EAN, category, Nutri-Score and the mechanic. Drugstores are not in it. Kruidvat and Etos come from the offer list pages of folderz.nl: the offer id, name, prices, days left and folderz's own quality rating. Folderz answers requests from Fly with an AWS WAF bot challenge and an empty body, but serves the home connection. So `pi/push_offers.py` on the Home Assistant Pi reads and parses the pages. It POSTs one JSON list per store to `/offers/folderz?store=<store>`, with the network push's secret as bearer. The kruidvat.nl site refuses requests that do not come from a browser, so it is not used.

1. `OFFERS_PRIJSPROFEET` lists the PrijsProfeet retailer keys, default `albert_heijn,dirk`. Empty turns the supermarket pull off.
2. Optional: request a free key at `https://www.prijsprofeet.nl/api#gratis-key` (email, click the link) and `fly secrets set PRIJSPROFEET_KEY=...`. Without it the public limit of 30 requests a minute applies and a run takes about two minutes longer. The free tier asks for a "data by PrijsProfeet" link wherever the data is shown. The Offers dashboard carries one.
3. `fly secrets set HA_NOTIFY=mobile_app_toms_iphone`, the notify service that gets the alerts. Needs `HA_URL` and `HA_TOKEN`. Unset sends none.
4. Copy `pi/push_offers.py` to `/config/custom_templates/` on the Pi, next to `personal_data_network.json`, which gives it the collector URL and secret. The stores default to Kruidvat and Etos. A `personal_data_offers.json` next to it with `{"stores": ["kruidvat", "etos"]}` overrides them. Add `push_offers: python3 /config/custom_templates/push_offers.py --detach` under `shell_command`, reload it, and create an automation that calls it once a day. The `--detach` flag forks the script, because Home Assistant kills a shell command after 60 seconds and a run takes longer. The script logs one line per store to `personal_data_offers.log`.

The watch list is `duckdb/watchlist.json`, one entry per item. An entry has a `label` and a case-insensitive regex `pattern` over the offer name. An optional `exclude` regex rules a matching offer out, for example kids' vitamins. It names the `stores` it applies to (`null` for all) and a `till` regex. The `till` regex matches the Albert Heijn till name in receipts and gives the usual price in the `watched_offer` table. Edit the file and push. The collector reads it from the repo on every run that has new offers.

The daily run pages each retailer's live offers and files only those whose id it has not stored before. Each retailer gets one file, a JSON list of the API's products verbatim. A push does the same for the parsed folderz cards of one store. Ids are kept in the state with the offer's end date and dropped a week after it. PrijsProfeet lists next week's offers from the Monday before, so they arrive a week early. A retailer that fails leaves the others' files in place and fails the run with its error. After each commit, the new offers that match the watch list go out as one notification. It lists the price, the old price and the dates. A run with none sends nothing. So a day brings at most one notification per push and one for the supermarkets.

Expected volume: about 5 MB a week for Albert Heijn, under 1 MB for each other store.

### Monitoring

Sentry's free plan covers this: 5k errors a month, one cron monitor and email alerts.

1. Create a Sentry project of type Rust and copy its DSN. This repo reports to the project `personal-data`.
2. Set it as a secret: `fly secrets set SENTRY_DSN=...`

Every logged error becomes a Sentry issue, with the warnings before it as breadcrumbs. Each run sends a check-in to the cron monitor `hourly-run`, which the first run creates. If a run fails, is missed for 30 minutes or takes over 55 minutes, Sentry opens an issue and emails you. If the `personal-data-sync-health` automation pushed nothing for 24 hours after its first push, the Apple Health source fails. Without `SENTRY_DSN` all of this is off.

### Automated fixes

When a job fails on two runs in a row, the run reports it to Sentry as the issue `Collector: <source> <job> failed`, with the error text. Every later failed run of the same job adds an event to that issue. A single failed run only logs a warning and answers 500. Upstream APIs time out or answer 5xx for an hour now and then, and the next run picks up where the cursor stayed. The count is kept in the state under `failures/<source>/<job>` and forgotten on success. The cron check-in fails on the same rule. Failures do not go to GitHub, because the repo is public and error text can quote API replies.

On the second failure in a row, the collector also fires the cloud Claude routine `fix-collector-failure` through its API trigger. The payload holds the source, the job and the error. The routine reproduces the failure with `cargo test` where it can. It commits a fix to `main` and waits for the deploy workflow and its smoke run. If the cause is not in the code, for example a revoked Withings token, it changes nothing and sends a push notification with the diagnosis. It never touches secrets or `fly.toml`. Its prompt is in `collector/routine-prompt.md`.

To connect the routine, open it at https://claude.ai/code/routines, add an API trigger, copy the URL and generate a token, then set both as secrets:

```sh
fly secrets set ROUTINE_FIRE_URL=https://api.anthropic.com/v1/claude_code/routines/<id>/fire ROUTINE_TOKEN=...
```

Without these two secrets the collector only reports to Sentry.

## Overlap between sources

The raw files keep every copy, because the sources disagree in detail and timing. When you query, use the most direct source and drop the copies. Home Assistant holds the least detail, so it is always the copy to drop:

- Withings measurements: use `raw/withings/`. Exclude Home Assistant entities from the Withings integration (`sensor.withings_*`, or look up the integration in `registry_entity`). In Apple Health, exclude samples whose `source` is `Health Mate` or `Withings`.
- Apple Watch and iPhone data: use `raw/apple_health/`. Exclude Home Assistant entities from the companion app (`sensor.toms_iphone_*`).
- Home sensors: Home Assistant is the direct source.
- Food: use `macrofactor_food` for the days its exports cover. For later days, use the dietary samples in `apple_sample`.

## Add a source

1. Add `collector/src/sources/<name>.rs` with a struct that implements `Source`: `name()`, and any of `router()` for inbound endpoints, `hourly()` and `daily()`.
2. Build files with `storage::path(...)` and `storage::envelope(...)`, store them with `app.save(...)`, and store cursors with `app.store.set(...)` after the save. Store only records that are new or changed since the last file. Use a cursor, an id set or a hash of the response without its fetch time, as the calendar, Steam and Moneybird sources do.
3. Add the struct to `all()` in `collector/src/sources/mod.rs` and any secrets to `fly secrets`.

## Query with DuckDB

`duckdb/tables.sql` defines clean, deduplicated tables, each with a comment that describes it. `duckdb/build.py` builds them into DuckLake: a SQLite catalog and Parquet files, both on the query server's volume. With the Parquet files in the bucket, a query over a week of Home Assistant data took 3.8 s instead of 0.13 s. The build skips a table when its statement, its files and the tables it reads did not change. It reads the large Apple Health pushes incrementally, so a build reads only their new files. A table marked `-- partition: year(ts), month(ts)` is split into files by month, so a query for a recent period opens only recent files.

### Query server

A second Fly app (`duckdb/`) serves the tables as a remote MCP server, so Claude can query them from any device. It keeps a copy of the bucket's `raw/` and the DuckLake tables on a 20 GB volume. The raw files in the bucket can rebuild every table. Every route sits under a secret path, `/<MCP_TOKEN>/`:

| route | purpose |
|---|---|
| `/<token>/mcp` | MCP (streamable HTTP), tools `tables` and `query`. Queries are read-only and can read files only under `raw/` |
| `POST /<token>/ingest` | the collector names the files it stored. The server downloads them, runs `build.py` in the background and answers 202 at once |
| `POST /<token>/rebuild` | download every file the local copy lacks, run `build.py` and answer when it is done |

Each build fetches `tables.sql` and `watchlist.json` from `main`, so a push changes the tables without a deploy. The first build after each start, and one build a day, also list the bucket, to catch files whose ingest call was lost. After 5 minutes without requests or builds, the server exits and the machine stops. The next request starts it again.

Setup:

```sh
cd duckdb
fly apps create --generate-name -o personal   # put the name in fly.toml
fly volumes create data --size 20 -r ams -a <app>
fly secrets set -a <app> MCP_TOKEN=$(openssl rand -hex 24) GITHUB_TOKEN=<read-only token for this repo>
fly secrets set -a <app> AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev \
  AWS_REGION=auto BUCKET_NAME=personal-data-raw   # the keys fly storage create set on the collector
fly deploy --ha=false
curl -fsS -X POST https://<app>.fly.dev/<token>/rebuild   # first build: download and full build, a few minutes
fly secrets set -a <collector app> INGEST_URL=https://<app>.fly.dev/<token>/ingest
```

Then add `https://<app>.fly.dev/<token>/mcp` as a custom connector in Claude (Settings, Connectors). The URL is the credential: keep it private, and change `MCP_TOKEN` to revoke it.

### Grafana

A custom domain (`<domain>`) is the query server under its own name: the map at `/map` and Grafana at `/grafana`, behind a passkey login. Grafana (`grafana/`, app `personal-data-grafana`) has no public address. The query server proxies `/grafana` to it over the private network (`personal-data-grafana.flycast`) and sets `X-WEBAUTH-USER`, which Grafana trusts as its login. It reads the data through the query server's `POST /sql`: the body is one SQL query, the reply JSON rows, with `Authorization: Bearer <MCP_TOKEN>`. It runs all the time, on a 512 MB machine with a 1 GB volume, so its alert rules are evaluated. The data source and the Trends dashboard are provisioned from `grafana/provisioning`; dashboards changed in Grafana are kept on the volume.

In a panel, the query is the data source's URL `/sql`, method POST, body type raw, the SQL as the body, parser backend, with a `timestamp` column for time series. `${__from:date:YYYY-MM-DD}` and `${__to:date:YYYY-MM-DD}` in the SQL are the dashboard's time range.

Setup:

```sh
cd grafana
fly apps create personal-data-grafana -o personal
fly volumes create grafana_data -a personal-data-grafana -r ams -s 1
fly secrets set -a personal-data-grafana MCP_TOKEN=<the query server's> GF_SERVER_ROOT_URL=https://<domain>/grafana/
fly ips allocate-v6 --private -a personal-data-grafana
fly deploy --no-public-ips --ha=false
fly certs add <domain> -a dawn-meadowbrook-804   # then the A and AAAA records it names, at the DNS host
fly secrets set -a dawn-meadowbrook-804 PUBLIC_HOSTS=<domain>,dawn-meadowbrook-804.fly.dev
```

A passkey belongs to the host it was registered on. Signed in on one host, `/passkey/handoff?host=<other>` opens the other's passkey page with a 10-minute grant to add one there.

Weekly body fat percentage from Withings (measure type 6, value times ten to the power of `unit`):

```sql
WITH m AS (
  SELECT unnest(response.body.measuregrps) AS g
  FROM read_json('raw/withings/measure/**/*.json', union_by_name = true)
)
SELECT date_trunc('week', to_timestamp(g.date)) AS week,
       round(avg(x.value * power(10, x.unit)), 1) AS body_fat_pct
FROM m, unnest(g.measures) AS t(x)
WHERE x.type = 6
GROUP BY 1 ORDER BY 1;
```

Resting heart rate trend from Apple Health (one automation named "Resting Heart Rate"):

```sql
SELECT date_trunc('day', strptime(p.date, '%Y-%m-%d %H:%M:%S %z')) AS day,
       round(avg(p.qty), 1) AS resting_hr
FROM read_json('raw/apple_health/resting-heart-rate/**/*.json', union_by_name = true),
     unnest(data.metrics) AS t(m), unnest(m.data) AS u(p)
GROUP BY 1 ORDER BY 1;
```

Indoor temperature at night versus the Withings sleep score of that night:

```sql
WITH temp AS (
  SELECT date_trunc('day', s.last_updated::TIMESTAMPTZ + INTERVAL 12 HOUR) AS night,
         avg(try_cast(s.state AS DOUBLE)) AS bedroom_c
  FROM read_json('raw/home_assistant/history/**/*.json', union_by_name = true),
       unnest(response) AS t(entity_rows), unnest(entity_rows) AS u(s)
  WHERE s.entity_id = 'sensor.bedroom_temperature'
    AND hour(s.last_updated::TIMESTAMPTZ) BETWEEN 0 AND 6
  GROUP BY 1
), sleep AS (
  SELECT n.date::DATE AS night, n.data.sleep_score AS score
  FROM read_json('raw/withings/sleep_summary/**/*.json', union_by_name = true),
       unnest(response.body.series) AS t(n)
)
SELECT sleep.night, round(bedroom_c, 1) AS bedroom_c, score
FROM sleep JOIN temp USING (night)
ORDER BY 1;
```

Replace `sensor.bedroom_temperature` with the entity id from `raw/home_assistant/registry_entity/`.
