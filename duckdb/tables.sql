-- Build clean, deduplicated tables from raw/. Run with duckdb/build.sh.
-- Every table keeps the latest copy of a record and drops the copies that the
-- append-only raw files hold. Timestamps are TIMESTAMPTZ; set TimeZone to view them locally.

SET preserve_insertion_order = false;

CREATE OR REPLACE TABLE build_info AS SELECT now() AS built_at;

-- Withings ---------------------------------------------------------------------

CREATE OR REPLACE TABLE withings_measure AS
WITH grp AS (
  SELECT g
  FROM read_json('raw/withings/measure/**/*.json', union_by_name = true),
       unnest(response.body.measuregrps) AS t(g)
  -- Drop measurement groups that are someone else's, by grpid.
  WHERE g.grpid NOT IN (8792698782)
  QUALIFY row_number() OVER (PARTITION BY g.grpid ORDER BY g.modified DESC) = 1
)
SELECT to_timestamp(g.date) AS ts, g.grpid, g.category, g.model, g.deviceid,
       m.type, m.value * power(10, m.unit) AS value, m.position
FROM grp, unnest(g.measures) AS u(m);

CREATE OR REPLACE TABLE withings_body AS
SELECT ts, grpid, model,
       max(value) FILTER (WHERE type = 1)   AS weight_kg,
       max(value) FILTER (WHERE type = 6)   AS fat_pct,
       max(value) FILTER (WHERE type = 8)   AS fat_kg,
       max(value) FILTER (WHERE type = 5)   AS fat_free_kg,
       max(value) FILTER (WHERE type = 76)  AS muscle_kg,
       max(value) FILTER (WHERE type = 77)  AS hydration_kg,
       max(value) FILTER (WHERE type = 88)  AS bone_kg,
       max(value) FILTER (WHERE type = 170) AS visceral_fat,
       max(value) FILTER (WHERE type = 4)   AS height_m
FROM withings_measure
WHERE category = 1 AND type IN (1, 4, 5, 6, 8, 76, 77, 88, 170)
GROUP BY ALL;

CREATE OR REPLACE TABLE withings_segmental AS
SELECT ts, grpid, position,
       max(value) FILTER (WHERE type = 175) AS muscle_kg,
       max(value) FILTER (WHERE type = 174) AS fat_kg
FROM withings_measure
WHERE type IN (174, 175)
GROUP BY ALL;

CREATE OR REPLACE TABLE withings_blood_pressure AS
SELECT ts, grpid, model,
       max(value) FILTER (WHERE type = 10) AS systolic,
       max(value) FILTER (WHERE type = 9)  AS diastolic,
       max(value) FILTER (WHERE type = 11) AS pulse
FROM withings_measure
WHERE type IN (9, 10, 11)
GROUP BY ALL
HAVING systolic IS NOT NULL;

CREATE OR REPLACE TABLE withings_heart AS
SELECT DISTINCT to_timestamp(s.timestamp) AS ts, s.heart_rate,
       s.bloodpressure.systole AS systolic, s.bloodpressure.diastole AS diastolic,
       s.ecg.afib AS afib, s.ecg.signalid AS signalid, s.model, s.deviceid
FROM read_json('raw/withings/heart/**/*.json', union_by_name = true),
     unnest(response.body.series) AS t(s);

CREATE OR REPLACE TABLE withings_ecg AS
SELECT request.record.ecg.signalid AS signalid, to_timestamp(request.record.timestamp) AS ts,
       response.body.sampling_frequency AS hz, response.body.signal AS signal, response.body.wearposition AS wear_position
FROM read_json('raw/withings/heart_signal/**/*.json', union_by_name = true)
QUALIFY row_number() OVER (PARTITION BY signalid ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE withings_sleep_night AS
WITH n AS (
  SELECT unnest(response.body.series) AS n
  FROM read_json('raw/withings/sleep_summary/**/*.json', union_by_name = true)
)
SELECT n.id, n.date::DATE AS wake_date, to_timestamp(n.startdate) AS in_bed, to_timestamp(n.enddate) AS out_of_bed,
       n.model, n.data.total_timeinbed / 3600.0 AS in_bed_h, n.data.total_sleep_time / 3600.0 AS asleep_h,
       n.data.lightsleepduration / 60.0 AS light_min, n.data.deepsleepduration / 60.0 AS deep_min,
       n.data.remsleepduration / 60.0 AS rem_min, n.data.wakeupduration / 60.0 AS awake_min,
       n.data.sleep_latency / 60.0 AS latency_min, n.data.waso / 60.0 AS waso_min,
       n.data.sleep_efficiency AS efficiency, n.data.wakeupcount AS wakeups, n.data.out_of_bed_count AS out_of_bed_count,
       n.data.hr_average AS hr_avg, n.data.hr_min AS hr_min, n.data.hr_max AS hr_max,
       n.data.rr_average AS rr_avg, n.data.rmssd_start_avg AS rmssd_start, n.data.rmssd_end_avg AS rmssd_end,
       n.data.apnea_hypopnea_index AS ahi, n.data.breathing_disturbances_intensity AS breathing_disturbances,
       n.data.snoring / 60.0 AS snoring_min, n.data.snoringepisodecount AS snoring_episodes,
       n.data.sleep_score AS score, n.data.night_events AS night_events
FROM n
QUALIFY row_number() OVER (PARTITION BY n.id ORDER BY n.modified DESC) = 1;

CREATE OR REPLACE TABLE withings_sleep_stage AS
SELECT DISTINCT to_timestamp(s.startdate) AS start_ts, to_timestamp(s.enddate) AS end_ts,
       (s.enddate - s.startdate) / 60 AS minutes, s.state, s.model
FROM read_json('raw/withings/sleep/**/*.json', union_by_name = true),
     unnest(response.body.series) AS t(s);

CREATE OR REPLACE TABLE withings_sleep_minute AS
WITH s AS (
  SELECT unnest(response.body.series) AS s
  FROM read_json('raw/withings/sleep/**/*.json', union_by_name = true)
)
SELECT DISTINCT to_timestamp(e.key::BIGINT) AS ts, e.value AS hr,
       try_cast(s.rr[e.key] AS DOUBLE) AS rr, try_cast(s.snoring[e.key] AS DOUBLE) AS snoring,
       try_cast(s.rmssd[e.key] AS DOUBLE) AS rmssd, try_cast(s.sdnn_1[e.key] AS DOUBLE) AS sdnn,
       try_cast(s.mvt_score[e.key] AS DOUBLE) AS movement, s.model
FROM s, unnest(map_entries(s.hr)) AS t(e);

CREATE OR REPLACE TABLE withings_intraday AS
SELECT DISTINCT to_timestamp(e.key::BIGINT) AS ts, e.value.duration AS duration_s,
       e.value.heart_rate AS hr, e.value.steps AS steps, e.value.calories AS kcal,
       e.value.distance AS distance_m, e.value.elevation AS elevation_m,
       e.value.sdnn1 AS sdnn, e.value.rmssd AS rmssd, e.value.hrv_quality AS hrv_quality,
       e.value.spo2_auto AS spo2, e.value.model AS model
FROM read_json('raw/withings/intraday_activity/**/*.json', union_by_name = true),
     unnest(map_entries(response.body.series)) AS t(e);

CREATE OR REPLACE TABLE withings_activity_day AS
SELECT a.date::DATE AS day, a.model, a.steps, a.distance AS distance_m, a.elevation AS elevation_m,
       a.soft AS soft_s, a.moderate AS moderate_s, a.intense AS intense_s, a.active AS active_s,
       a.calories AS active_kcal, a.totalcalories AS total_kcal,
       a.hr_average AS hr_avg, a.hr_min, a.hr_max,
       a.hr_zone_0 AS hr_zone_0_s, a.hr_zone_1 AS hr_zone_1_s, a.hr_zone_2 AS hr_zone_2_s, a.hr_zone_3 AS hr_zone_3_s
FROM read_json('raw/withings/activity/**/*.json', union_by_name = true),
     unnest(response.body.activities) AS t(a)
QUALIFY row_number() OVER (PARTITION BY a.date, a.model ORDER BY a.modified DESC) = 1;

CREATE OR REPLACE TABLE withings_workout AS
SELECT w.id, w.category, to_timestamp(w.startdate) AS start_ts, to_timestamp(w.enddate) AS end_ts,
       (w.enddate - w.startdate) / 60 AS minutes, w.model,
       w.data.calories AS kcal, w.data.intensity, w.data.distance AS distance_m, w.data.steps,
       w.data.hr_average AS hr_avg, w.data.hr_min, w.data.hr_max,
       w.data.hr_zone_0 AS hr_zone_0_s, w.data.hr_zone_1 AS hr_zone_1_s, w.data.hr_zone_2 AS hr_zone_2_s, w.data.hr_zone_3 AS hr_zone_3_s,
       w.data.spo2_average AS spo2_avg
FROM read_json('raw/withings/workouts/**/*.json', union_by_name = true),
     unnest(response.body.series) AS t(w)
QUALIFY row_number() OVER (PARTITION BY w.id ORDER BY w.modified DESC) = 1;

-- Home Assistant -----------------------------------------------------------------

CREATE OR REPLACE TABLE ha_entity AS
WITH latest AS (
  SELECT max(filename) AS f
  FROM read_json('raw/home_assistant/registry_entity/**/*.json', filename = true, union_by_name = true)
)
SELECT e.entity_id, e.platform, coalesce(e.name, e.original_name) AS name, e.device_id, e.area_id,
       e.disabled_by, e.hidden_by, e.unique_id, e.entity_category
FROM read_json('raw/home_assistant/registry_entity/**/*.json', filename = true, union_by_name = true),
     unnest(response) AS t(e)
WHERE filename = (SELECT f FROM latest);

CREATE OR REPLACE TABLE ha_device AS
WITH latest AS (
  SELECT max(filename) AS f
  FROM read_json('raw/home_assistant/registry_device/**/*.json', filename = true, union_by_name = true)
)
SELECT d.id AS device_id, coalesce(d.name_by_user, d.name) AS name, d.manufacturer, d.model, d.area_id, d.sw_version
FROM read_json('raw/home_assistant/registry_device/**/*.json', filename = true, union_by_name = true),
     unnest(response) AS t(d)
WHERE filename = (SELECT f FROM latest);

CREATE OR REPLACE TABLE ha_area AS
WITH latest AS (
  SELECT max(filename) AS f
  FROM read_json('raw/home_assistant/registry_area/**/*.json', filename = true, union_by_name = true)
)
SELECT a.area_id, a.name, a.floor_id
FROM read_json('raw/home_assistant/registry_area/**/*.json', filename = true, union_by_name = true),
     unnest(response) AS t(a)
WHERE filename = (SELECT f FROM latest);

-- History and statistics windows overlap, so later files replace rows with the same key.
-- incremental: raw/home_assistant/history/**/*.json
-- merge: entity_id, ts
-- partition: year(ts), month(ts)
CREATE OR REPLACE TABLE ha_history AS
SELECT s.entity_id::VARCHAR AS entity_id, s.last_updated::TIMESTAMPTZ AS ts, s.last_changed::TIMESTAMPTZ AS changed_ts,
       s.state::VARCHAR AS state, try_cast(s.state AS DOUBLE) AS value, to_json(s.attributes) AS attributes
FROM read_json($files, union_by_name = true),
     unnest(response) AS t(entity_rows), unnest(entity_rows) AS u(s)
QUALIFY row_number() OVER (PARTITION BY s.entity_id, s.last_updated ORDER BY fetched_at DESC) = 1;

-- incremental: raw/home_assistant/statistics/**/*.json
-- merge: statistic_id, hour
CREATE OR REPLACE TABLE ha_statistics AS
SELECT s.key AS statistic_id, to_timestamp(r.start / 1000) AS hour,
       r.mean, r.min, r.max, r.state, r.sum, r.change
FROM read_json($files,
               columns = {fetched_at: 'VARCHAR', request: 'JSON',
                          response: 'MAP(VARCHAR, STRUCT("start" BIGINT, "end" BIGINT, mean DOUBLE, min DOUBLE, max DOUBLE, state DOUBLE, sum DOUBLE, change DOUBLE)[])'}),
     unnest(map_entries(response)) AS t(s), unnest(s.value) AS u(r)
QUALIFY row_number() OVER (PARTITION BY s.key, r.start ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE ha_logbook AS
SELECT DISTINCT l."when"::TIMESTAMPTZ AS ts, l.entity_id, l.name, l.state, l.message, l.source, l.domain,
       l.context_entity_id, l.context_name
FROM read_json('raw/home_assistant/logbook/**/*.json', union_by_name = true),
     unnest(response) AS t(l);

-- Every GPS tracker counts: the companion app's device tracker and OwnTracks once it reports.
-- A tracker repeats a fix whenever another attribute (battery) changes, so keep a row only when the fix moved.
CREATE OR REPLACE TABLE location AS
WITH f AS (
  SELECT ts, entity_id AS tracker, (attributes->>'latitude')::DOUBLE AS lat, (attributes->>'longitude')::DOUBLE AS lon,
         (attributes->>'gps_accuracy')::DOUBLE AS accuracy_m, (attributes->>'altitude')::DOUBLE AS altitude_m,
         (attributes->>'vertical_accuracy')::DOUBLE AS vertical_accuracy_m, (attributes->>'speed')::DOUBLE AS speed_ms,
         (attributes->>'course')::DOUBLE AS course_deg, state AS zone, from_json(attributes->'in_zones', '["VARCHAR"]') AS in_zones
  FROM ha_history
  WHERE entity_id LIKE 'device_tracker.%' AND (attributes->>'latitude') IS NOT NULL
)
SELECT *
FROM f
QUALIFY (lat, lon, accuracy_m, zone) IS DISTINCT FROM lag((lat, lon, accuracy_m, zone)) OVER (PARTITION BY tracker ORDER BY ts);

-- The Nintendo parental controls sensor counts the Switch's play minutes of the current day and starts over at
-- midnight, so a day's total is the highest value seen. The console is named awoo in Home Assistant.
CREATE OR REPLACE TABLE switch_day AS
SELECT (ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, max(value) AS minutes, max(ts) AS last_ts
FROM ha_history
WHERE entity_id = 'sensor.living_room_switch_awoo_used_screen_time' AND value IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- Network ------------------------------------------------------------------------

-- The Pi pushes one body an hour (pi/push_network.py); every table below is keyed by its sent_at.
-- Signal is in dBm; rssi is the controller's signal above the noise floor; rates are kbit/s.
CREATE OR REPLACE TABLE wifi_client AS
SELECT sent_at::TIMESTAMPTZ AS ts, c->>'mac' AS mac, coalesce(c->>'name', c->>'hostname') AS name, c->>'hostname' AS hostname,
       c->>'ip' AS ip, c->>'oui' AS oui, (c->>'is_wired')::BOOLEAN AS is_wired, c->>'essid' AS essid, c->>'radio' AS radio,
       c->>'radio_proto' AS radio_proto, (c->>'channel')::INTEGER AS channel, (c->>'signal')::INTEGER AS signal_dbm,
       (c->>'rssi')::INTEGER AS rssi, (c->>'noise')::INTEGER AS noise_dbm, (c->>'tx_rate')::BIGINT AS tx_rate_kbps,
       (c->>'rx_rate')::BIGINT AS rx_rate_kbps, (c->>'tx_bytes')::BIGINT AS tx_bytes, (c->>'rx_bytes')::BIGINT AS rx_bytes,
       (c->>'tx_retries')::BIGINT AS tx_retries, (c->>'satisfaction')::INTEGER AS satisfaction, (c->>'uptime')::BIGINT AS uptime_s,
       to_timestamp((c->>'assoc_time')::BIGINT) AS assoc_ts, to_timestamp((c->>'first_seen')::BIGINT) AS first_seen,
       (c->>'tx_power')::INTEGER AS tx_power, (c->>'powersave_enabled')::BOOLEAN AS powersave
FROM read_json('raw/network/push/**/*.json', columns = {sent_at: 'VARCHAR', unifi: 'STRUCT(clients JSON[])'},
               maximum_object_size = 67108864),
     unnest(unifi.clients) AS t(c)
QUALIFY row_number() OVER (PARTITION BY sent_at, mac ORDER BY ts) = 1;

-- One row per access point radio per push: the channel it was on and how busy the air was.
-- cu_total is the channel utilisation in percent, cu_self_tx and cu_self_rx the share that was the AP's own traffic.
CREATE OR REPLACE TABLE wifi_radio AS
WITH d AS (
  SELECT sent_at, unnest(unifi.devices) AS dev
  FROM read_json('raw/network/push/**/*.json', columns = {sent_at: 'VARCHAR', unifi: 'STRUCT(devices JSON[])'},
                 maximum_object_size = 67108864)
)
SELECT sent_at::TIMESTAMPTZ AS ts, dev->>'name' AS ap, s->>'radio' AS radio, (s->>'channel')::INTEGER AS channel,
       (cfg->>'ht')::INTEGER AS width_mhz, cfg->>'tx_power_mode' AS tx_power_mode, (s->>'tx_power')::INTEGER AS tx_power,
       (s->>'num_sta')::INTEGER AS clients, (s->>'cu_total')::INTEGER AS cu_total, (s->>'cu_self_tx')::INTEGER AS cu_self_tx,
       (s->>'cu_self_rx')::INTEGER AS cu_self_rx, (s->>'satisfaction')::INTEGER AS satisfaction,
       (s->>'tx_retries')::BIGINT AS tx_retries, (s->>'tx_packets')::BIGINT AS tx_packets, (s->>'rx_packets')::BIGINT AS rx_packets
FROM d, unnest(from_json(dev->'radio_table_stats', '["JSON"]')) AS t(s),
     LATERAL (SELECT r AS cfg FROM unnest(from_json(dev->'radio_table', '["JSON"]')) AS u(r)
              WHERE (r->>'radio') = (s->>'radio') LIMIT 1) AS l
QUALIFY row_number() OVER (PARTITION BY sent_at, ap, radio ORDER BY ts) = 1;

-- One row per access point per push: uptime, load and the wired uplink.
CREATE OR REPLACE TABLE wifi_ap AS
SELECT sent_at::TIMESTAMPTZ AS ts, dev->>'name' AS ap, dev->>'mac' AS mac, dev->>'model' AS model, dev->>'version' AS firmware,
       (dev->>'uptime')::BIGINT AS uptime_s, (dev->>'state')::INTEGER AS state, (dev->>'satisfaction')::INTEGER AS satisfaction,
       (dev->>'num_sta')::INTEGER AS clients, (dev->'system-stats'->>'cpu')::DOUBLE AS cpu_pct, (dev->'system-stats'->>'mem')::DOUBLE AS mem_pct,
       (dev->'uplink'->>'speed')::INTEGER AS uplink_mbps, (dev->'uplink'->>'full_duplex')::BOOLEAN AS uplink_full_duplex,
       (dev->'uplink'->>'rx_bytes')::BIGINT AS uplink_rx_bytes, (dev->'uplink'->>'tx_bytes')::BIGINT AS uplink_tx_bytes,
       (dev->'uplink'->>'rx_errors')::BIGINT AS uplink_rx_errors, (dev->'uplink'->>'tx_errors')::BIGINT AS uplink_tx_errors
FROM read_json('raw/network/push/**/*.json', columns = {sent_at: 'VARCHAR', unifi: 'STRUCT(devices JSON[])'},
               maximum_object_size = 67108864),
     unnest(unifi.devices) AS t(dev)
QUALIFY row_number() OVER (PARTITION BY sent_at, mac ORDER BY ts) = 1;

-- One row per wifi network (SSID) per radio per push, with the bytes the AP moved on it since it booted.
CREATE OR REPLACE TABLE wifi_network AS
SELECT sent_at::TIMESTAMPTZ AS ts, dev->>'name' AS ap, v->>'essid' AS essid, v->>'radio' AS radio, (v->>'channel')::INTEGER AS channel,
       (v->>'num_sta')::INTEGER AS clients, (v->>'tx_power')::INTEGER AS tx_power, (v->>'satisfaction')::INTEGER AS satisfaction,
       v->>'state' AS state, (v->>'rx_bytes')::BIGINT AS rx_bytes, (v->>'tx_bytes')::BIGINT AS tx_bytes,
       (v->>'tx_retries')::BIGINT AS tx_retries, (v->>'tx_errors')::BIGINT AS tx_errors, (v->>'rx_errors')::BIGINT AS rx_errors,
       (v->>'avg_client_signal')::INTEGER AS avg_client_signal_dbm
FROM read_json('raw/network/push/**/*.json', columns = {sent_at: 'VARCHAR', unifi: 'STRUCT(devices JSON[])'},
               maximum_object_size = 67108864),
     unnest(unifi.devices) AS t(dev), unnest(from_json(dev->'vap_table', '["JSON"]')) AS u(v)
QUALIFY row_number() OVER (PARTITION BY sent_at, ap, essid, radio ORDER BY ts) = 1;

-- The networks the AP hears around the flat, summarised per channel and width every hour. rssi is above the noise floor,
-- so 40 is a strong neighbour and 10 a faint one. radio ng is 2.4 GHz, na 5 GHz.
CREATE OR REPLACE TABLE wifi_neighbour_hour AS
SELECT sent_at::TIMESTAMPTZ AS ts, n.radio, n.channel, n.bw AS width_mhz, n.count, n.max_rssi, n.mean_rssi
FROM read_json('raw/network/push/**/*.json',
               columns = {sent_at: 'VARCHAR',
                          unifi: 'STRUCT(neighbours_by_channel STRUCT(radio VARCHAR, channel INTEGER, bw INTEGER, count INTEGER, max_rssi INTEGER, mean_rssi DOUBLE)[])'},
               maximum_object_size = 67108864),
     unnest(unifi.neighbours_by_channel) AS t(n)
QUALIFY row_number() OVER (PARTITION BY sent_at, n.radio, n.channel, n.bw ORDER BY ts) = 1;

-- Every neighbouring network the AP heard, once a day (the first push after midnight carries the full list).
CREATE OR REPLACE TABLE wifi_neighbour AS
SELECT sent_at::TIMESTAMPTZ AS ts, n->>'bssid' AS bssid, n->>'essid' AS essid, (n->>'channel')::INTEGER AS channel,
       (n->>'bw')::INTEGER AS width_mhz, (n->>'rssi')::INTEGER AS rssi, (n->>'signal')::INTEGER AS signal_dbm,
       (n->>'noise')::INTEGER AS noise_dbm, n->>'radio' AS radio, n->>'band' AS band, n->>'security' AS security,
       n->>'oui' AS oui, (n->>'is_ubnt')::BOOLEAN AS is_ubnt, (n->>'age')::INTEGER AS age_s
FROM read_json('raw/network/push/**/*.json', columns = {sent_at: 'VARCHAR', unifi: 'STRUCT(neighbours JSON[])'},
               maximum_object_size = 67108864),
     unnest(unifi.neighbours) AS t(n)
QUALIFY row_number() OVER (PARTITION BY sent_at, bssid, channel ORDER BY ts) = 1;

-- DNS queries from AdGuard, counted per hour, client, domain and record type. Each push covers [from_ts, to_ts).
-- reason starts with Filtered when AdGuard blocked the query; cached means it answered from its cache.
-- client_name comes from the UniFi client table at push time and is null for devices it did not know.
-- Keep the latest push per window before unnesting: a window over every query row ran out of memory in DuckDB 1.5.
CREATE OR REPLACE TABLE dns_query AS
WITH push AS (
  SELECT sent_at, clients, adguard
  FROM read_json('raw/network/push/**/*.json',
                 columns = {sent_at: 'VARCHAR', clients: 'MAP(VARCHAR, VARCHAR)',
                            adguard: 'STRUCT(since VARCHAR, "until" VARCHAR, raw_count INTEGER, truncated BOOLEAN, queries STRUCT(client VARCHAR, domain VARCHAR, type VARCHAR, status VARCHAR, reason VARCHAR, cached BOOLEAN, count INTEGER)[])'},
                 maximum_object_size = 67108864)
  QUALIFY row_number() OVER (PARTITION BY adguard.since ORDER BY sent_at DESC) = 1
)
SELECT adguard.since::TIMESTAMPTZ AS from_ts, adguard."until"::TIMESTAMPTZ AS to_ts, q.client AS client_ip,
       clients[q.client] AS client_name, q.domain, q.type AS record_type, q.status, q.reason, q.cached, q.count
FROM push, unnest(adguard.queries) AS t(q);

-- Weather ------------------------------------------------------------------------

-- Open-Meteo returns parallel arrays. Unnest them side by side in the select list: indexing with a
-- lateral range(i) copies the whole response into every row and runs out of memory on the monthly archive files.

-- The forecast files win over the ERA5 archive backfill where they overlap.
CREATE OR REPLACE TABLE weather_hour AS
WITH h AS (
  SELECT fetched_at, filename LIKE '%/open_meteo_archive/%' AS from_archive,
         request.location.latitude AS lat, request.location.longitude AS lon,
         unnest(response.hourly.time) AS time,
         unnest(response.hourly.temperature_2m) AS temp_c, unnest(response.hourly.apparent_temperature) AS feels_c,
         unnest(response.hourly.wet_bulb_temperature_2m) AS wet_bulb_c, unnest(response.hourly.boundary_layer_height) AS boundary_layer_m,
         unnest(response.hourly.relative_humidity_2m) AS humidity_pct, unnest(response.hourly.dew_point_2m) AS dew_point_c,
         unnest(response.hourly.precipitation) AS precip_mm, unnest(response.hourly.rain) AS rain_mm, unnest(response.hourly.snowfall) AS snow_cm,
         unnest(response.hourly.precipitation_probability) AS precip_prob_pct, unnest(response.hourly.weather_code) AS wmo_code,
         unnest(response.hourly.cloud_cover) AS cloud_pct, unnest(response.hourly.pressure_msl) AS pressure_hpa,
         unnest(response.hourly.wind_speed_10m) AS wind_kmh, unnest(response.hourly.wind_gusts_10m) AS gusts_kmh,
         unnest(response.hourly.wind_direction_10m) AS wind_deg,
         unnest(response.hourly.uv_index) AS uv_index, unnest(response.hourly.sunshine_duration) AS sunshine_s,
         unnest(response.hourly.shortwave_radiation) AS radiation_wm2,
         unnest(response.hourly.visibility) AS visibility_m, unnest(response.hourly.is_day) AS is_day
  FROM read_json(['raw/weather/open_meteo/**/*.json', 'raw/weather/open_meteo_archive/**/*.json'],
                 union_by_name = true, filename = true)
)
SELECT time::TIMESTAMP AT TIME ZONE 'UTC' AS hour, * EXCLUDE (time, fetched_at, from_archive, lat, lon), lat, lon, from_archive
FROM h
QUALIFY row_number() OVER (PARTITION BY hour ORDER BY from_archive, fetched_at DESC) = 1;

CREATE OR REPLACE TABLE weather_air_quality_hour AS
WITH h AS (
  SELECT fetched_at, unnest(response.hourly.time) AS time,
         unnest(response.hourly.pm2_5) AS pm2_5, unnest(response.hourly.pm10) AS pm10,
         unnest(response.hourly.nitrogen_dioxide) AS no2, unnest(response.hourly.nitrogen_monoxide) AS no,
         unnest(response.hourly.ozone) AS o3, unnest(response.hourly.sulphur_dioxide) AS so2,
         unnest(response.hourly.carbon_monoxide) AS co, unnest(response.hourly.ammonia) AS nh3, unnest(response.hourly.dust) AS dust,
         unnest(response.hourly.aerosol_optical_depth) AS aod, unnest(response.hourly.uv_index) AS uv_index,
         unnest(response.hourly.european_aqi) AS eaqi,
         unnest(response.hourly.alder_pollen) AS alder_pollen, unnest(response.hourly.birch_pollen) AS birch_pollen,
         unnest(response.hourly.grass_pollen) AS grass_pollen, unnest(response.hourly.mugwort_pollen) AS mugwort_pollen,
         unnest(response.hourly.olive_pollen) AS olive_pollen, unnest(response.hourly.ragweed_pollen) AS ragweed_pollen
  FROM read_json('raw/weather/open_meteo_air_quality/**/*.json', union_by_name = true)
)
SELECT time::TIMESTAMP AT TIME ZONE 'UTC' AS hour, * EXCLUDE (time, fetched_at)
FROM h
QUALIFY row_number() OVER (PARTITION BY hour ORDER BY fetched_at DESC) = 1;

-- Days are UTC days; for local days group weather_hour by (hour AT TIME ZONE 'Europe/Amsterdam')::DATE.
CREATE OR REPLACE TABLE weather_day AS
WITH d AS (
  SELECT fetched_at, filename LIKE '%/open_meteo_archive/%' AS from_archive,
         unnest(response.daily.time) AS time, unnest(response.daily.sunrise) AS sunrise, unnest(response.daily.sunset) AS sunset,
         unnest(response.daily.daylight_duration) AS daylight_s, unnest(response.daily.sunshine_duration) AS sunshine_s,
         unnest(response.daily.temperature_2m_max) AS temp_max_c, unnest(response.daily.temperature_2m_min) AS temp_min_c,
         unnest(response.daily.temperature_2m_mean) AS temp_mean_c,
         unnest(response.daily.precipitation_sum) AS precip_mm, unnest(response.daily.precipitation_hours) AS precip_h,
         unnest(response.daily.wind_gusts_10m_max) AS gusts_max_kmh, unnest(response.daily.shortwave_radiation_sum) AS radiation_mj_m2,
         unnest(response.daily.uv_index_max) AS uv_max, unnest(response.daily.weather_code) AS wmo_code
  FROM read_json(['raw/weather/open_meteo_daily/**/*.json', 'raw/weather/open_meteo_archive/**/*.json'],
                 union_by_name = true, filename = true)
)
SELECT time::DATE AS day_utc, sunrise::TIMESTAMP AT TIME ZONE 'UTC' AS sunrise, sunset::TIMESTAMP AT TIME ZONE 'UTC' AS sunset,
       daylight_s / 3600 AS daylight_h, sunshine_s / 3600 AS sunshine_h,
       temp_max_c, temp_min_c, temp_mean_c, precip_mm, precip_h, gusts_max_kmh, radiation_mj_m2, uv_max, wmo_code, from_archive
FROM d
QUALIFY row_number() OVER (PARTITION BY day_utc ORDER BY from_archive, fetched_at DESC) = 1;

CREATE OR REPLACE TABLE weather_current AS
SELECT response.current.time::TIMESTAMP AT TIME ZONE 'UTC' AS ts, response.current.temperature_2m AS temp_c,
       response.current.apparent_temperature AS feels_c, response.current.relative_humidity_2m AS humidity_pct,
       response.current.precipitation AS precip_mm, response.current.weather_code AS wmo_code,
       response.current.cloud_cover AS cloud_pct, response.current.pressure_msl AS pressure_hpa,
       response.current.wind_speed_10m AS wind_kmh, response.current.wind_gusts_10m AS gusts_kmh,
       response.current.wind_direction_10m AS wind_deg, response.current.is_day AS is_day,
       request.location.latitude AS lat, request.location.longitude AS lon
FROM read_json('raw/weather/open_meteo/**/*.json', union_by_name = true)
QUALIFY row_number() OVER (PARTITION BY ts ORDER BY fetched_at DESC) = 1;

-- Luchtmeetnet -------------------------------------------------------------------

-- timestamp_measured is the end of the hour a value averages; hour is its start. The API gives no units.
CREATE OR REPLACE TABLE air_quality_measured_hour AS
SELECT m.station_number AS station, m.formula AS component, m.timestamp_measured - INTERVAL 1 HOUR AS hour, m.value,
       CASE m.formula WHEN 'LKI' THEN 'index' WHEN 'PS' THEN 'particles/cm³' ELSE 'µg/m³' END AS unit
FROM read_json(['raw/luchtmeetnet/measurement/**/*.json', 'raw/luchtmeetnet/lki/**/*.json'],
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON',
                          response: 'STRUCT(data STRUCT(station_number VARCHAR, formula VARCHAR, value DOUBLE, timestamp_measured TIMESTAMPTZ)[])'}),
     unnest(response.data) AS t(m)
QUALIFY row_number() OVER (PARTITION BY station, component, hour ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE air_quality_station AS
SELECT regexp_extract(request->>'url', '/stations/([^/]+)', 1) AS station, response.data.location AS name,
       response.data.type AS type, response.data.municipality AS municipality, response.data.organisation AS organisation,
       response.data.geometry.coordinates[2] AS lat, response.data.geometry.coordinates[1] AS lon,
       response.data.components AS components
FROM read_json('raw/luchtmeetnet/station/**/*.json', union_by_name = true)
QUALIFY row_number() OVER (PARTITION BY station ORDER BY fetched_at DESC) = 1;

-- Apple Health -------------------------------------------------------------------

-- The metric and workout pushes are gigabytes of JSON, so build.py reads only files it has not read
-- before, in batches, into staging tables of scalars (with the file name); the tables below dedupe those.
-- A statement marked `-- incremental: <glob> ...` reads `$files`, the next batch of unread files.

CREATE SCHEMA IF NOT EXISTS staging;

-- incremental: raw/apple_health/new-automation/**/*.json raw/apple_health/personal-data-sync-health/**/*.json raw/apple_health/personal-data-sync-nutrition/**/*.json
CREATE OR REPLACE TABLE staging.apple_metric AS
SELECT m.name, m.units, filename, p->>'source' AS source,
       p->>'date' AS date, (p->>'qty')::DOUBLE AS qty,
       (p->>'Avg')::DOUBLE AS avg, (p->>'Min')::DOUBLE AS min, (p->>'Max')::DOUBLE AS max,
       (p->>'systolic')::DOUBLE AS systolic, (p->>'diastolic')::DOUBLE AS diastolic,
       p->>'sleepStart' AS sleep_start, p->>'sleepEnd' AS sleep_end, p->>'inBedStart' AS in_bed_start, p->>'inBedEnd' AS in_bed_end,
       (p->>'totalSleep')::DOUBLE AS asleep_h, (p->>'inBed')::DOUBLE AS in_bed_h, (p->>'core')::DOUBLE AS core_h,
       (p->>'deep')::DOUBLE AS deep_h, (p->>'rem')::DOUBLE AS rem_h, (p->>'awake')::DOUBLE AS awake_h
FROM read_json($files,
               columns = {data: 'STRUCT(metrics STRUCT(name VARCHAR, units VARCHAR, data JSON[])[])'},
               filename = true, maximum_object_size = 67108864),
     unnest(data.metrics) AS t(m), unnest(m.data) AS u(p);

-- incremental: raw/apple_health/personal-data-sync-workouts/**/*.json
-- Parse each workout once into the fields needed: every w->>'x' would parse the whole workout, route included.
CREATE OR REPLACE TABLE staging.apple_workout AS
WITH w AS (
  SELECT filename, from_json(w, '{"id": "VARCHAR", "name": "VARCHAR", "start": "VARCHAR", "end": "VARCHAR", "duration": "DOUBLE",
    "distance": {"qty": "DOUBLE"}, "activeEnergyBurned": {"qty": "DOUBLE"}, "avgSpeed": {"qty": "DOUBLE"},
    "elevationUp": {"qty": "DOUBLE"}, "temperature": {"qty": "DOUBLE"}, "humidity": {"qty": "DOUBLE"},
    "activities": [{"metrics": {"heartRateAverage": {"qty": "DOUBLE"}, "heartRateMinimum": {"qty": "DOUBLE"},
                                "heartRateMaximum": {"qty": "DOUBLE"}}}],
    "isIndoor": "BOOLEAN", "source": {"name": "VARCHAR"}}') AS w
  FROM read_json($files, columns = {data: 'STRUCT(workouts JSON[])'}, filename = true, maximum_object_size = 67108864),
       unnest(data.workouts) AS t(w)
)
SELECT w.id, filename, w.name,
       strptime(w.start, '%Y-%m-%d %H:%M:%S %z') AS start_ts, strptime(w."end", '%Y-%m-%d %H:%M:%S %z') AS end_ts,
       w.duration / 60 AS minutes, w.distance.qty AS distance_km, w.activeEnergyBurned.qty AS active_kj,
       w.avgSpeed.qty AS avg_speed_kmh, w.elevationUp.qty AS elevation_up_m,
       w.temperature.qty AS temperature_c, w.humidity.qty AS humidity_pct,
       w.activities[1].metrics.heartRateAverage.qty AS hr_avg, w.activities[1].metrics.heartRateMinimum.qty AS hr_min,
       w.activities[1].metrics.heartRateMaximum.qty AS hr_max, w.isIndoor AS indoor, w.source.name AS source
FROM w;

-- Drop the workout JSON before unnesting the route: a lateral unnest copies the whole workout into every point.
-- incremental: raw/apple_health/personal-data-sync-workouts/**/*.json
CREATE OR REPLACE TABLE staging.apple_workout_route AS
WITH w AS (
  SELECT w->>'id' AS workout_id, filename, from_json(w->'route', '["JSON"]') AS route
  FROM read_json($files, columns = {data: 'STRUCT(workouts JSON[])'}, filename = true, maximum_object_size = 67108864),
       unnest(data.workouts) AS t(w)
), r AS (
  SELECT workout_id, filename, unnest(route) AS r FROM w
)
SELECT workout_id, filename, strptime(r->>'timestamp', '%Y-%m-%d %H:%M:%S %z') AS ts,
       (r->>'latitude')::DOUBLE AS lat, (r->>'longitude')::DOUBLE AS lon, (r->>'altitude')::DOUBLE AS altitude_m,
       (r->>'speed')::DOUBLE AS speed_ms, (r->>'course')::DOUBLE AS course_deg, (r->>'horizontalAccuracy')::DOUBLE AS accuracy_m
FROM r;

-- Health Auto Export buckets food per minute from the time of the export, so two exports of a meal disagree
-- on its timestamp. The nutrition automation always sends whole days: for its metrics on a day it covers,
-- keep only its newest file of that day.
CREATE OR REPLACE TABLE apple_sample AS
WITH s AS (
  SELECT *, left(date, 10) AS day, filename LIKE '%/personal-data-sync-nutrition/%' AS whole_day
  FROM staging.apple_metric
  WHERE name NOT IN ('heart_rate', 'sleep_analysis', 'blood_pressure')
),
newest AS (SELECT day, max(filename) AS file FROM s WHERE whole_day GROUP BY day)
SELECT name, units, strptime(date, '%Y-%m-%d %H:%M:%S %z') AS ts, qty, source
FROM s LEFT JOIN newest USING (day)
WHERE newest.file IS NULL OR s.filename = newest.file OR name NOT IN (SELECT name FROM s WHERE whole_day)
QUALIFY row_number() OVER (PARTITION BY name, date, source ORDER BY filename DESC) = 1;

-- The newest file's reading per time. An aggregate rather than a window, so it can spill to disk.
CREATE OR REPLACE TABLE apple_heart_rate AS
SELECT strptime(date, '%Y-%m-%d %H:%M:%S %z') AS ts, r.avg, r.min, r.max, r.source
FROM (
  SELECT date, arg_max({'avg': avg, 'min': min, 'max': max, 'source': source}, filename) AS r
  FROM staging.apple_metric
  WHERE name = 'heart_rate'
  GROUP BY date
);

CREATE OR REPLACE TABLE apple_sleep_night AS
SELECT strptime(sleep_start, '%Y-%m-%d %H:%M:%S %z') AS sleep_start, strptime(sleep_end, '%Y-%m-%d %H:%M:%S %z') AS sleep_end,
       strptime(in_bed_start, '%Y-%m-%d %H:%M:%S %z') AS in_bed_start, strptime(in_bed_end, '%Y-%m-%d %H:%M:%S %z') AS in_bed_end,
       asleep_h, in_bed_h, core_h, deep_h, rem_h, awake_h, source
FROM staging.apple_metric
WHERE name = 'sleep_analysis'
QUALIFY row_number() OVER (PARTITION BY sleep_start, source ORDER BY filename DESC) = 1;

CREATE OR REPLACE TABLE apple_blood_pressure AS
SELECT strptime(date, '%Y-%m-%d %H:%M:%S %z') AS ts, systolic, diastolic, source
FROM staging.apple_metric
WHERE name = 'blood_pressure'
QUALIFY row_number() OVER (PARTITION BY date, source ORDER BY filename DESC) = 1;

-- partition: year(ts), month(ts)
CREATE OR REPLACE TABLE apple_workout_route AS
SELECT * EXCLUDE (filename)
FROM staging.apple_workout_route
QUALIFY filename = max(filename) OVER (PARTITION BY workout_id);

CREATE OR REPLACE TABLE apple_workout AS
SELECT w.* EXCLUDE (filename), coalesce(r.points, 0) AS route_points
FROM staging.apple_workout AS w
LEFT JOIN (SELECT workout_id, count(*) AS points FROM apple_workout_route GROUP BY 1) AS r ON r.workout_id = w.id
QUALIFY row_number() OVER (PARTITION BY w.id ORDER BY w.filename DESC) = 1;

CREATE OR REPLACE TABLE apple_ecg AS
SELECT strptime(e->>'start', '%Y-%m-%d %H:%M:%S %z') AS start_ts, strptime(e->>'end', '%Y-%m-%d %H:%M:%S %z') AS end_ts,
       e->>'classification' AS classification, (e->>'averageHeartRate')::DOUBLE AS hr_avg,
       (e->>'samplingFrequency')::DOUBLE AS hz, (e->>'numberOfVoltageMeasurements')::BIGINT AS samples,
       from_json(e->'voltageMeasurements', '["JSON"]') AS voltages, e->>'source' AS source
FROM read_json('raw/apple_health/personal-data-sync-ecg/**/*.json',
               columns = {data: 'STRUCT(ecg JSON[])'},
               filename = true, union_by_name = true, maximum_object_size = 67108864),
     unnest(data.ecg) AS t(e)
QUALIFY row_number() OVER (PARTITION BY e->>'start' ORDER BY filename DESC) = 1;

CREATE OR REPLACE TABLE apple_state_of_mind AS
SELECT s->>'id' AS id, (s->>'start')::TIMESTAMPTZ AS ts, s->>'kind' AS kind,
       (s->>'valence')::DOUBLE AS valence, s->>'valenceClassification' AS classification,
       s->'labels' AS labels, s->'associations' AS associations
FROM read_json('raw/apple_health/personal-data-sync-state-of-mind/**/*.json',
               columns = {data: 'STRUCT(stateOfMind JSON[])'}, filename = true, union_by_name = true),
     unnest(data.stateOfMind) AS t(s)
QUALIFY row_number() OVER (PARTITION BY s->>'id' ORDER BY filename DESC) = 1;

CREATE OR REPLACE TABLE apple_medication AS
SELECT m->>'displayText' AS medication, strptime(m->>'scheduledDate', '%Y-%m-%d %H:%M:%S %z') AS scheduled_ts,
       m->>'status' AS status, m->'scheduledDosage' AS dose, m->>'units' AS units
FROM read_json('raw/apple_health/personal-data-sync-medications/**/*.json',
               columns = {data: 'STRUCT(medications JSON[])'}, filename = true, union_by_name = true),
     unnest(data.medications) AS t(m)
QUALIFY row_number() OVER (PARTITION BY m->>'scheduledDate', m->>'displayText' ORDER BY filename DESC) = 1;

-- bunq ---------------------------------------------------------------------------
-- The business accounts are shared with the personal user and appear under both folders, so read both and dedupe by id.

CREATE OR REPLACE TABLE bunq_account AS
SELECT fetched_at::DATE AS day, json_keys(a)[1] AS kind, (a->json_keys(a)[1]->>'id')::BIGINT AS account_id,
       a->json_keys(a)[1]->>'description' AS description, a->json_keys(a)[1]->>'currency' AS currency,
       (a->json_keys(a)[1]->'balance'->>'value')::DECIMAL(14,2) AS balance, a->json_keys(a)[1]->>'status' AS status,
       (SELECT x->>'value' FROM unnest(from_json(a->json_keys(a)[1]->'alias', '["JSON"]')) AS t(x) WHERE x->>'type' = 'IBAN' LIMIT 1) AS iban,
       fetched_at
FROM read_json(['raw/bunq/monetary_account/**/*.json', 'raw/bunq_business/monetary_account/**/*.json'],
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT("Response" JSON[], "Pagination" JSON)'},
               filename = true),
     unnest(response.Response) AS t(a)
QUALIFY row_number() OVER (PARTITION BY day, account_id ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE bunq_payment AS
SELECT (p->>'id')::BIGINT AS id, (p->>'created')::TIMESTAMP AT TIME ZONE 'UTC' AS ts,
       (p->>'monetary_account_id')::BIGINT AS account_id,
       (p->'amount'->>'value')::DECIMAL(14,2) AS amount, p->'amount'->>'currency' AS currency,
       p->>'type' AS type, p->>'sub_type' AS sub_type, p->>'description' AS description,
       p->'counterparty_alias'->>'display_name' AS counterparty, p->'counterparty_alias'->>'iban' AS counterparty_iban,
       p->'alias'->>'iban' AS own_iban, (p->'balance_after_mutation'->>'value')::DECIMAL(14,2) AS balance_after,
       p->>'merchant_reference' AS merchant_reference, (p->>'scheduled_id')::BIGINT AS scheduled_id, (p->>'batch_id')::BIGINT AS batch_id
FROM read_json(['raw/bunq/payment/**/*.json', 'raw/bunq_business/payment/**/*.json'],
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT("Response" JSON[], "Pagination" JSON)'},
               filename = true),
     unnest(response.Response) AS t(item), (SELECT item->'Payment') AS u(p)
QUALIFY row_number() OVER (PARTITION BY p->>'id' ORDER BY filename DESC) = 1;

CREATE OR REPLACE TABLE bunq_card_action AS
SELECT (m->>'id')::BIGINT AS id, (m->>'created')::TIMESTAMP AT TIME ZONE 'UTC' AS ts,
       (m->>'monetary_account_id')::BIGINT AS account_id,
       (m->'amount_billing'->>'value')::DECIMAL(14,2) AS amount, m->'amount_billing'->>'currency' AS currency,
       (m->'amount_local'->>'value')::DECIMAL(14,2) AS local_amount, m->'amount_local'->>'currency' AS local_currency,
       m->>'authorisation_status' AS status, m->>'authorisation_type' AS authorisation_type,
       m->'counterparty_alias'->>'display_name' AS merchant, m->>'city' AS city,
       m->>'pan_entry_mode_user' AS entry_mode, m->'label_card'->>'second_line' AS card,
       m->>'decision' AS decision, m->>'decision_description' AS decision_description,
       (m->>'card_id')::BIGINT AS card_id,
       coalesce(m->>'merchant_category_code', m->'counterparty_alias'->>'merchant_category_code') AS mcc
FROM read_json(['raw/bunq/mastercard_action/**/*.json', 'raw/bunq_business/mastercard_action/**/*.json'],
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT("Response" JSON[], "Pagination" JSON)'},
               filename = true),
     unnest(response.Response) AS t(item), (SELECT item->'MasterCardAction') AS u(m)
QUALIFY row_number() OVER (PARTITION BY m->>'id' ORDER BY filename DESC) = 1;

-- Card purchases outside the business account, from the authorisations, which carry the merchant category code
-- (ISO 18245) that sets the category. A merchant name overrides its code where the code is wrong (bouldering gyms
-- registered as bands or camps, cafés as food shops). Refunds count as negative spending.
CREATE OR REPLACE TABLE card_purchase AS
WITH a AS (SELECT DISTINCT ON (account_id) account_id, description FROM bunq_account ORDER BY account_id, day DESC),
p AS (
  SELECT c.id, c.ts, a.description AS account, c.merchant, c.city, c.mcc, try_cast(c.mcc AS INT) AS code,
         CASE WHEN c.status = 'CLEARING_REFUND' THEN -c.amount ELSE c.amount END AS amount, c.currency,
         lower(c.merchant) AS name
  FROM bunq_card_action c LEFT JOIN a USING (account_id)
  WHERE c.status IN ('AUTHORISED', 'STAND_IN_AUTHORISED', 'PRE_AUTHORISATION_FINALISED', 'UNAUTHORISED_CLEARING', 'CLEARING_REFUND')
    AND a.description IS DISTINCT FROM 'business' AND c.amount <> 0
)
SELECT id, ts, account, merchant, city, mcc, amount, currency,
  CASE
    WHEN regexp_matches(name, 'boulder|klim|gym|fysio') THEN 'Health & fitness'
    WHEN regexp_matches(name, 'coffee|caff|koffie|guji|morning owl|good beans') THEN 'Eating & drinking'
    WHEN code IN (5411, 5412, 5422, 5441, 5451, 5462, 5499, 5300, 763, 5921) THEN 'Groceries'
    WHEN code IN (5811, 5812, 5813, 5814) THEN 'Eating & drinking'
    WHEN code BETWEEN 3000 AND 3999 OR code IN (4011, 4111, 4112, 4121, 4131, 4411, 4457, 4468, 4511, 4582, 4722, 4784,
         4789, 7011, 7012, 7512, 7513, 7519, 7523, 5541, 5542, 5983) THEN 'Travel & transport'
    WHEN code IN (4814, 4816, 4899, 5192, 5734, 5735, 5815, 5816, 5817, 5818, 5968, 7372, 7375) THEN 'Subscriptions & digital'
    WHEN code IN (742, 5122, 5912, 7032, 7298, 7941, 7997, 8011, 8021, 8031, 8041, 8042, 8043, 8049, 8050, 8062, 8071, 8099)
      THEN 'Health & fitness'
    WHEN code IN (8211, 8220, 8241, 8244, 8249, 8299) THEN 'Education'
    WHEN code IN (7832, 7922, 7929, 7933, 7991, 7996, 7998, 7999, 8699) THEN 'Going out & events'
    WHEN code IN (6010, 6011) THEN 'Cash'
    WHEN code IN (1520, 4900, 6300, 6513, 7210, 7230, 7299, 7399, 8111, 8999) OR code BETWEEN 9000 AND 9999
      THEN 'Services & bills'
    WHEN code BETWEEN 5000 AND 5999 THEN 'Shopping'
    ELSE 'Other' END AS category
FROM p;

CREATE OR REPLACE TABLE bunq_request AS
SELECT 'sent' AS direction, (r->>'id')::BIGINT AS id, (r->>'created')::TIMESTAMP AT TIME ZONE 'UTC' AS ts,
       (r->>'monetary_account_id')::BIGINT AS account_id,
       (r->'amount_inquired'->>'value')::DECIMAL(14,2) AS amount_requested, (r->'amount_responded'->>'value')::DECIMAL(14,2) AS amount_paid,
       r->'amount_inquired'->>'currency' AS currency, r->>'status' AS status, r->>'description' AS description,
       r->'counterparty_alias'->>'display_name' AS counterparty,
       try_cast(r->>'time_responded' AS TIMESTAMP) AT TIME ZONE 'UTC' AS responded_ts
FROM read_json('raw/bunq*/request_inquiry/**/*.json',
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT("Response" JSON[], "Pagination" JSON)'},
               filename = true),
     unnest(response.Response) AS t(item), (SELECT item->'RequestInquiry') AS u(r)
QUALIFY row_number() OVER (PARTITION BY r->>'id' ORDER BY filename DESC) = 1
UNION ALL
SELECT 'received', (r->>'id')::BIGINT, (r->>'created')::TIMESTAMP AT TIME ZONE 'UTC',
       (r->>'monetary_account_id')::BIGINT,
       (r->'amount_inquired'->>'value')::DECIMAL(14,2), (r->'amount_responded'->>'value')::DECIMAL(14,2),
       r->'amount_inquired'->>'currency', r->>'status', r->>'description',
       r->'counterparty_alias'->>'display_name',
       try_cast(r->>'time_responded' AS TIMESTAMP) AT TIME ZONE 'UTC'
FROM read_json(['raw/bunq/request_response/**/*.json', 'raw/bunq_business/request_response/**/*.json'],
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT("Response" JSON[], "Pagination" JSON)'},
               filename = true),
     unnest(response.Response) AS t(item), (SELECT item->'RequestResponse') AS u(r)
QUALIFY row_number() OVER (PARTITION BY r->>'id' ORDER BY filename DESC) = 1;

CREATE OR REPLACE TABLE bunq_invoice AS
SELECT (i->>'id')::BIGINT AS id, try_cast(i->>'invoice_date' AS DATE) AS invoice_date, i->>'invoice_number' AS invoice_number,
       i->>'status' AS status, (i->'total_vat_inclusive'->>'value')::DECIMAL(14,2) AS total, i->'total_vat_inclusive'->>'currency' AS currency,
       i->>'description' AS description, i->'group' AS items,
       CASE WHEN filename LIKE 'raw/bunq_business/%' THEN 'business' ELSE 'personal' END AS user
FROM read_json(['raw/bunq/invoice/**/*.json', 'raw/bunq_business/invoice/**/*.json'],
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT("Response" JSON[], "Pagination" JSON)'},
               filename = true),
     unnest(response.Response) AS t(item), (SELECT item->'Invoice') AS u(i)
QUALIFY row_number() OVER (PARTITION BY i->>'id' ORDER BY filename DESC) = 1;

-- DEGIRO --------------------------------------------------------------------------
-- Two accounts, personal (raw/degiro) and pension (raw/degiro_pension), told apart by account. The trading update
-- lists each position and each total as {name, value} pairs; these pivot them. Product ids are strings: DEGIRO sends
-- them as strings in the portfolio and as numbers in transactions and cash movements.

CREATE OR REPLACE MACRO degiro_account(filename) AS
  CASE WHEN filename LIKE '%/degiro_pension/%' THEN 'pension' ELSE 'personal' END;

CREATE OR REPLACE TABLE degiro_product AS
SELECT key AS product_id, p->>'name' AS name, p->>'symbol' AS symbol, p->>'isin' AS isin,
       p->>'currency' AS currency, p->>'productType' AS product_type, p->>'exchangeId' AS exchange_id,
       (p->>'closePrice')::DOUBLE AS close_price, try_cast(p->>'closePriceDate' AS DATE) AS close_price_date,
       (p->>'active')::BOOLEAN AS active, p AS raw, fetched_at
FROM read_json('raw/degiro*/product/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(data JSON)'}),
     unnest(json_keys(response.data)) AS t(key), (SELECT response.data->('$."' || key || '"')) AS u(p)
QUALIFY row_number() OVER (PARTITION BY product_id ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE degiro_position AS
WITH f AS (
  SELECT degiro_account(filename) AS account, fetched_at, r->>'id' AS position_id, e->>'name' AS name, e->'value' AS value
  FROM read_json('raw/degiro*/portfolio/**/*.json', filename = true,
                 columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(portfolio STRUCT(value JSON[]))'}),
       unnest(response.portfolio.value) AS t(r), unnest(from_json(r->'value', '["JSON"]')) AS u(e)
), p AS (
  SELECT account, fetched_at, fetched_at::DATE AS day, position_id,
         max(value->>'$') FILTER (WHERE name = 'positionType') AS position_type,
         max(try_cast(value AS DOUBLE)) FILTER (WHERE name = 'size') AS size,
         max(try_cast(value AS DOUBLE)) FILTER (WHERE name = 'price') AS price,
         max(try_cast(value AS DOUBLE)) FILTER (WHERE name = 'value') AS value_eur,
         max(try_cast(value AS DOUBLE)) FILTER (WHERE name = 'breakEvenPrice') AS break_even_price,
         max((value->>'EUR')::DOUBLE) FILTER (WHERE name = 'plBase') AS pl_eur,
         max((value->>'EUR')::DOUBLE) FILTER (WHERE name = 'todayPlBase') AS today_pl_eur,
         max(try_cast(value AS DOUBLE)) FILTER (WHERE name = 'realizedProductPl') AS realized_pl,
         max(try_cast(value AS DOUBLE)) FILTER (WHERE name = 'averageFxRate') AS average_fx_rate
  FROM f GROUP BY 1, 2, 3, 4
)
SELECT p.account, p.day, p.position_id, p.position_type, d.name, d.symbol, d.isin, d.currency, d.product_type,
       p.size, p.price, p.value_eur, p.break_even_price, p.pl_eur, p.today_pl_eur, p.realized_pl, p.average_fx_rate, p.fetched_at
FROM p LEFT JOIN degiro_product d ON d.product_id = p.position_id
QUALIFY row_number() OVER (PARTITION BY p.account, p.day, p.position_id ORDER BY p.fetched_at DESC) = 1
ORDER BY account, day, position_id;

CREATE OR REPLACE TABLE degiro_portfolio_day AS
WITH f AS (
  SELECT degiro_account(filename) AS account, fetched_at, e->>'name' AS name, e->'value' AS value
  FROM read_json('raw/degiro*/portfolio/**/*.json', filename = true,
                 columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(totalPortfolio STRUCT(value JSON[]))'}),
       unnest(response.totalPortfolio.value) AS t(e)
), t AS (
  SELECT account, fetched_at, fetched_at::DATE AS day,
         max(try_cast(value AS DOUBLE)) FILTER (WHERE name = 'totalCash') AS total_cash,
         max(try_cast(value AS DOUBLE)) FILTER (WHERE name = 'reportNetliq') AS net_liquidation,
         max(try_cast(value AS DOUBLE)) FILTER (WHERE name = 'reportPortfValue') AS report_portfolio_value,
         max(try_cast(value AS DOUBLE)) FILTER (WHERE name = 'totalDepositWithdrawal') AS deposits,
         max(try_cast(value AS DOUBLE)) FILTER (WHERE name = 'totalNonProductFees') AS non_product_fees,
         max((value->>'EUR')::DOUBLE) FILTER (WHERE name = 'freeSpaceNew') AS free_space,
         map_from_entries(list((name, value))) AS totals
  FROM f GROUP BY 1, 2, 3
  QUALIFY row_number() OVER (PARTITION BY account, day ORDER BY fetched_at DESC) = 1
), pos AS (
  SELECT account, day, sum(value_eur) FILTER (WHERE position_type = 'PRODUCT') AS positions_value,
         sum(value_eur) FILTER (WHERE position_type <> 'PRODUCT') AS cash_positions,
         sum(pl_eur) FILTER (WHERE position_type = 'PRODUCT') AS unrealized_pl,
         count(*) FILTER (WHERE position_type = 'PRODUCT' AND size <> 0) AS positions
  FROM degiro_position GROUP BY 1, 2
)
SELECT t.account, t.day, pos.positions_value, t.total_cash, coalesce(pos.positions_value, 0) + coalesce(t.total_cash, 0) AS total_value,
       t.net_liquidation, t.deposits, pos.unrealized_pl, pos.positions, pos.cash_positions, t.report_portfolio_value,
       t.non_product_fees, t.free_space, t.totals, t.fetched_at
FROM t LEFT JOIN pos USING (account, day)
ORDER BY account, day;

CREATE OR REPLACE TABLE degiro_transaction AS
WITH x AS (
  SELECT degiro_account(filename) AS account, fetched_at, (x->>'id')::BIGINT AS id, (x->>'date')::TIMESTAMPTZ AS ts, x->>'productId' AS product_id,
         x->>'buysell' AS buysell, (x->>'quantity')::DOUBLE AS quantity, (x->>'price')::DOUBLE AS price,
         (x->>'total')::DOUBLE AS total, (x->>'totalInBaseCurrency')::DOUBLE AS total_eur,
         (x->>'totalFeesInBaseCurrency')::DOUBLE AS fees_eur, (x->>'feeInBaseCurrency')::DOUBLE AS fee_eur,
         (x->>'autoFxFeeInBaseCurrency')::DOUBLE AS fx_fee_eur, (x->>'totalPlusAllFeesInBaseCurrency')::DOUBLE AS total_with_fees_eur,
         (x->>'fxRate')::DOUBLE AS fx_rate, (x->>'orderTypeId')::INT AS order_type_id, (x->>'transactionTypeId')::INT AS transaction_type_id,
         x->>'counterParty' AS counter_party, x->>'tradingVenue' AS trading_venue, (x->>'transfered')::BOOLEAN AS transferred, x AS raw
  FROM read_json('raw/degiro*/transaction/**/*.json', filename = true,
                 columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(data JSON[])'}),
       unnest(response.data) AS t(x)
)
SELECT x.account, x.id, x.ts, x.product_id, d.name, d.symbol, d.isin, x.buysell, x.quantity, x.price, x.total, x.total_eur, x.fees_eur,
       x.fee_eur, x.fx_fee_eur, x.total_with_fees_eur, x.fx_rate, x.order_type_id, x.transaction_type_id, x.counter_party,
       x.trading_venue, x.transferred, x.raw
FROM x LEFT JOIN degiro_product d ON d.product_id = x.product_id
QUALIFY row_number() OVER (PARTITION BY x.account, x.id ORDER BY x.fetched_at DESC) = 1
ORDER BY ts;

CREATE OR REPLACE TABLE degiro_cash_movement AS
WITH m AS (
  SELECT degiro_account(filename) AS account, fetched_at, (m->>'id')::BIGINT AS id, (m->>'date')::TIMESTAMPTZ AS ts,
         try_cast(m->>'valueDate' AS TIMESTAMPTZ) AS value_date,
         m->>'type' AS type, m->>'description' AS description, m->>'currency' AS currency, (m->>'change')::DOUBLE AS change,
         (m->'balance'->>'total')::DOUBLE AS balance, m->>'productId' AS product_id, m AS raw
  FROM read_json('raw/degiro*/account_overview/**/*.json', filename = true,
                 columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(data STRUCT(cashMovements JSON[]))'}),
       unnest(response.data.cashMovements) AS t(m)
)
SELECT m.account, m.id, m.ts, m.value_date, m.type, m.description, m.currency, m.change, m.balance, m.product_id, d.name, d.symbol, m.raw
FROM m LEFT JOIN degiro_product d ON d.product_id = m.product_id
QUALIFY row_number() OVER (PARTITION BY m.account, coalesce(m.id::VARCHAR, m.ts::VARCHAR || m.description || m.change::VARCHAR)
                           ORDER BY m.fetched_at DESC) = 1
ORDER BY ts;

-- Steam --------------------------------------------------------------------------
-- owned_games is stored only when the library changes, so its playtime is stale; playtime comes from recently_played.

CREATE OR REPLACE TABLE steam_game AS
SELECT (g->>'appid')::INT AS appid, g->>'name' AS name, (g->>'has_community_visible_stats')::BOOLEAN AS has_stats
FROM read_json('raw/steam/owned_games/**/*.json',
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(response STRUCT(games JSON[]))'},
               filename = true),
     unnest(response.response.games) AS t(g)
QUALIFY row_number() OVER (PARTITION BY appid ORDER BY filename DESC) = 1;

-- Each recently_played file is stored when playtime moved, so the diff lands in the hour before the fetch.
-- games_that_hour > 1 suggests card idling.
CREATE OR REPLACE TABLE steam_playtime_hour AS
WITH snap AS (
  SELECT fetched_at, (g->>'appid')::INT AS appid, g->>'name' AS name, (g->>'playtime_forever')::INT AS forever
  FROM read_json('raw/steam/recently_played/**/*.json',
                 columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(response STRUCT(games JSON[]))'},
                 filename = true),
       unnest(response.response.games) AS t(g)
), d AS (
  SELECT appid, name, date_trunc('hour', fetched_at) - INTERVAL 1 HOUR AS hour,
         forever - lag(forever) OVER (PARTITION BY appid ORDER BY fetched_at) AS minutes
  FROM snap
)
SELECT hour, appid, name, minutes, count(*) OVER (PARTITION BY hour) AS games_that_hour
FROM d
WHERE minutes > 0;

CREATE OR REPLACE TABLE steam_achievement AS
WITH a AS (
  SELECT (request->'params'->>'appid')::INT AS appid, response.playerstats.gameName AS game, filename,
         unnest(response.playerstats.achievements) AS a
  FROM read_json('raw/steam/achievements/**/*.json',
                 columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(playerstats STRUCT("gameName" VARCHAR, achievements JSON[]))'},
                 filename = true)
)
SELECT appid, game, a->>'apiname' AS apiname, a->>'name' AS name, a->>'description' AS description,
       (a->>'achieved')::INT = 1 AS achieved, to_timestamp(nullif((a->>'unlocktime')::BIGINT, 0)) AS unlocked_at
FROM a
QUALIFY row_number() OVER (PARTITION BY appid, apiname ORDER BY filename DESC) = 1;

CREATE OR REPLACE TABLE steam_presence AS
SELECT fetched_at AS ts, (p->>'personastate')::INT AS personastate, p->>'gameid' AS gameid, p->>'gameextrainfo' AS game
FROM read_json('raw/steam/summary/**/*.json',
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(response STRUCT(players JSON[]))'}),
     unnest(response.response.players) AS t(p);

-- Last.fm ------------------------------------------------------------------------
-- Every number is a string, names sit under #text, and a single track comes as an object instead of a list.
-- recent_tracks holds verbatim backfill pages (extended=1); scrobble holds the list of new tracks each hourly
-- lookback found. Both are read through one glob, since a glob without files fails. The now playing track has no date.

CREATE OR REPLACE TABLE lastfm_scrobble AS
WITH f AS (
  SELECT fetched_at, if(filename LIKE '%/recent_tracks/%', response->'recenttracks'->'track', response) AS t
  FROM read_json('raw/lastfm/**/*.json',
                 columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON'}, filename = true)
  WHERE filename LIKE '%/recent_tracks/%' OR filename LIKE '%/scrobble/%'
), s AS (
  SELECT fetched_at, unnest(if(json_type(t) = 'ARRAY', from_json(t, '["JSON"]'), [t])) AS s FROM f
)
SELECT to_timestamp((s->'date'->>'uts')::BIGINT) AS ts,
       coalesce(s->'artist'->>'name', s->'artist'->>'#text') AS artist, nullif(s->'album'->>'#text', '') AS album,
       s->>'name' AS track, nullif(s->>'mbid', '') AS track_mbid, nullif(s->'artist'->>'mbid', '') AS artist_mbid,
       nullif(s->'album'->>'mbid', '') AS album_mbid, s->>'loved' = '1' AS loved, s->>'url' AS url
FROM s
WHERE s->'date'->>'uts' IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY ts, artist, track ORDER BY fetched_at DESC) = 1;

-- loved_tracks is the whole list, one file per page, stored when it changed; the newest fetch is the current list.
CREATE OR REPLACE TABLE lastfm_loved AS
WITH f AS (
  SELECT fetched_at, response.lovedtracks.track AS t
  FROM read_json('raw/lastfm/loved_tracks/**/*.json',
                 columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(lovedtracks STRUCT(track JSON))'})
), s AS (
  SELECT unnest(if(json_type(t) = 'ARRAY', from_json(t, '["JSON"]'), [t])) AS s
  FROM f WHERE fetched_at = (SELECT max(fetched_at) FROM f)
)
SELECT to_timestamp((s->'date'->>'uts')::BIGINT) AS loved_at, s->'artist'->>'name' AS artist, s->>'name' AS track,
       nullif(s->>'mbid', '') AS track_mbid, nullif(s->'artist'->>'mbid', '') AS artist_mbid, s->>'url' AS url
FROM s
WHERE s IS NOT NULL;

-- Apple Media Services -----------------------------------------------------------
-- The privacy.apple.com export, saved by hand in Apple's folders under export/<last day covered>/, nested zips
-- unpacked. Each export holds the full history, so read only the newest.
SET VARIABLE ams = (SELECT regexp_replace(max(file), '/Apple Music Activity/.*', '')
                    FROM glob('raw/apple_media_services/export/*/*/*/Apple Music Activity/Apple Music Play Activity.csv'));

-- Plays have no artist: take it from the library by title and album, else from the play histories' "Artist - Title",
-- where the title may carry a suffix such as "(2017 Mix)" that the play's song name lacks.
CREATE OR REPLACE TABLE apple_music_play AS
WITH p AS (
  FROM read_csv(getvariable('ams') || '/Apple Music Activity/Apple Music Play Activity.csv')
  WHERE "Event Type" = 'PLAY_END' AND "Song Name" IS NOT NULL
), lib AS (
  SELECT "Title" AS track, "Album" AS album, any_value("Artist") AS artist
  FROM read_json(getvariable('ams') || '/Apple Music Activity/Apple Music Library Tracks.json')
  GROUP BY ALL
), history AS (
  SELECT "Track Description" AS description
  FROM read_csv(getvariable('ams') || '/Apple Music Activity/Apple Music - Play History Daily Tracks.csv')
  UNION ALL
  SELECT "Track Name" FROM read_csv(getvariable('ams') || '/Apple Music Activity/Apple Music - Track Play History.csv')
), guess AS (
  SELECT song, mode(left(description, strpos(description, ' - ' || song) - 1)) AS artist
  FROM (SELECT DISTINCT "Song Name" AS song FROM p) JOIN history ON contains(description, ' - ' || song)
  GROUP BY song
)
SELECT p."Event Start Timestamp" AS started_at, p."Event End Timestamp" AS ended_at,
       coalesce(lib.artist, guess.artist) AS artist, p."Album Name" AS album, p."Song Name" AS track,
       p."Play Duration Milliseconds" / 1000 AS played_s, p."Media Duration In Milliseconds" / 1000 AS duration_s,
       p."End Reason Type" AS end_reason, p."Device Type" AS device, p."Feature Name" AS feature, p."Offline" AS offline
FROM p
LEFT JOIN lib ON lib.track = p."Song Name" AND lib.album IS NOT DISTINCT FROM p."Album Name"
LEFT JOIN guess ON guess.song = p."Song Name";

-- Last.fm from the first scrobble, Apple Music before it, counted by Last.fm's rule: over 30 s, and played for
-- half the track or 4 minutes.
CREATE OR REPLACE TABLE music_play AS
SELECT ts, artist, album, track, 'lastfm' AS source FROM lastfm_scrobble
UNION ALL
SELECT started_at, artist, album, track, 'apple_music'
FROM apple_music_play
WHERE duration_s > 30 AND played_s >= least(duration_s / 2, 240)
  AND started_at < (SELECT coalesce(min(ts), 'infinity') FROM lastfm_scrobble);

CREATE OR REPLACE TABLE apple_tv_play AS
SELECT "UTC Start Time" AS started_at, "UTC End Time" AS ended_at, "Content Episode Name" AS series,
       "Content Season Name" AS season, "Content Title" AS title, "Play Duration" / 1000 AS played_s,
       "Content Length MS" / 1000 AS length_s, "Client Platform" AS platform, "Program Type" AS program_type
FROM read_csv(getvariable('ams') || '/Stores Activity/Other Activity/Video Play Activity.csv')
WHERE "Action Type" = 'stop';

CREATE OR REPLACE TABLE apple_podcast_episode AS
WITH shows AS (
  SELECT "Feed URL" AS feed, "Title" AS podcast, "Subscribed?" = 'Yes' AS subscribed
  FROM read_csv(getvariable('ams') || '/Stores Activity/Apple TV and Podcast Information/Your Podcasts.csv')
)
SELECT e."Feed URL" AS feed, shows.podcast, shows.subscribed, e."Episode ID" AS episode_id,
       e."Last played on"::TIMESTAMPTZ AS last_played, e."Marked as played on"::TIMESTAMPTZ AS marked_played,
       e."Playback position" AS position_s, e."Play count" AS plays, e."Has been played?" = 'Yes' AS played
FROM read_csv(getvariable('ams') || '/Stores Activity/Apple TV and Podcast Information/Podcasts Playstate.csv', all_varchar = true) AS e
LEFT JOIN shows ON shows.feed = e."Feed URL";

-- Amounts are text such as "€ 19,99" or "€16.52", and empty for free apps.
CREATE OR REPLACE TABLE apple_purchase AS
SELECT "Item Purchased Date"::TIMESTAMPTZ AS ts, "Content Type" AS type, "Item Description" AS item,
       "Container Description" AS app,
       coalesce(replace(regexp_replace("Invoice Item Total", '[^0-9,.]', '', 'g'), ',', '.')::DOUBLE, 0) AS amount,
       "Currency" AS currency, "Refund Amount" IS NOT NULL AS refunded, "Payment Type" AS payment
FROM read_csv(getvariable('ams') || '/Stores Activity/Account and Transaction History/Store Transaction Purchase and Free Apps History.csv',
              all_varchar = true);

-- Each subscription has a record per renewal period; keep the newest.
CREATE OR REPLACE TABLE apple_subscription AS
SELECT "Subscription ID" AS id, "Container Description" AS app, "Item Description" AS item,
       min("Subscription Service Start Date") OVER w AS started_at, max("Subscription Expiration Date") OVER w AS expires_at,
       "Automatic Renewal?" AS auto_renew
FROM read_csv(getvariable('ams') || '/Stores Activity/Account and Transaction History/Subscription History.csv')
WINDOW w AS (PARTITION BY "Subscription ID")
QUALIFY row_number() OVER (PARTITION BY "Subscription ID" ORDER BY "Subscription Purchase Record Last Modified Date" DESC) = 1;

-- Apple swaps the columns: Content Type holds the app and Item Description the platform.
CREATE OR REPLACE TABLE apple_app_update AS
SELECT "Activity Date" AS ts, "Content Type" AS app, "Item Description" AS platform, "Version Text" AS version,
       "Seller" AS seller, regexp_extract("Device Details", 'model/(\S+)', 1) AS model
FROM read_csv(getvariable('ams') || '/Update and Redownload History/iTunes and App-Book Re-download and Update History.csv');

CREATE OR REPLACE TABLE apple_app_store_search AS
SELECT DISTINCT "Event Date Time" AS ts, coalesce("Final Search Term", "Search Term") AS term
FROM read_csv(getvariable('ams') || '/Stores Activity/Other Activity/App Store Click Activity.csv')
WHERE coalesce("Final Search Term", "Search Term") IS NOT NULL;

CREATE OR REPLACE TABLE apple_music_library AS
SELECT "Title" AS track, "Artist" AS artist, "Album" AS album, "Genre" AS genre, "Track Year" AS year,
       "Date Added To Library" AS added_at, "Last Played Date" AS last_played, "Track Play Count" AS plays,
       "Skip Count" AS skips, "Favorite Status - Track" AS favourite
FROM read_json(getvariable('ams') || '/Apple Music Activity/Apple Music Library Tracks.json');

CREATE OR REPLACE TABLE apple_music_favourite AS
SELECT "Last Modified" AS ts, "Favorite Type" AS type, "Item Description" AS item, "Preference" AS preference
FROM read_csv(getvariable('ams') || '/Apple Music Activity/Apple Music - Favorites.csv');

CREATE OR REPLACE TABLE apple_game_achievement AS
SELECT strptime(a.last_update_utc, '%m/%d/%Y %H:%M:%S') AS ts, g.game_name AS game, trim(a.achievements_title) AS achievement,
       a.percentage_complete AS percent
FROM (SELECT unnest(game_state) AS g FROM read_json(getvariable('ams') || '/Game Center/Game Center Data.json')),
     unnest(g.achievements) AS t(a);

CREATE OR REPLACE TABLE apple_testflight_session AS
WITH apps AS (
  SELECT appAdamId, any_value(name) AS app
  FROM read_csv(getvariable('ams') || '/Testflight/Testflight Data History-betaApps.csv')
  GROUP BY ALL
)
SELECT s.periodStart::DATE AS day, apps.app, s.periodValue AS sessions
FROM read_csv(getvariable('ams') || '/Testflight/Testflight Data History-sessions.csv') AS s
LEFT JOIN apps USING (appAdamId)
WHERE s.periodInterval = 'DAILY';

-- GitHub -------------------------------------------------------------------------
-- The collector's own commits to the data repo are filtered out at collection time. Activity in other people's
-- private repos (work) is redacted at collection: only times, kinds and counts, so redacted rows have no repo, title,
-- URL or SHA and dedupe on their times instead.

CREATE OR REPLACE TABLE github_event AS
SELECT e->>'id' AS id, e->>'type' AS type, e->'repo'->>'name' AS repo, (e->>'public')::BOOLEAN AS public,
       (e->>'created_at')::TIMESTAMPTZ AS ts, e->'payload'->>'action' AS action, e->'payload' AS payload,
       coalesce((e->>'redacted')::BOOLEAN, false) AS redacted
FROM read_json('raw/github/event/**/*.json',
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON[]'}, filename = true),
     unnest(response) AS t(e)
QUALIFY row_number() OVER (PARTITION BY e->>'id' ORDER BY filename DESC) = 1;

-- Author dates carry the local offset, e.g. 2026-10-01T13:41:44.000+02:00; *_offset keeps it.
CREATE OR REPLACE TABLE github_commit AS
SELECT c->>'sha' AS sha, c->'repository'->>'full_name' AS repo, (c->'repository'->>'private')::BOOLEAN AS private,
       (c->'commit'->'author'->>'date')::TIMESTAMPTZ AS authored_at, substr(c->'commit'->'author'->>'date', 24) AS author_offset,
       (c->'commit'->'committer'->>'date')::TIMESTAMPTZ AS committed_at, substr(c->'commit'->'committer'->>'date', 24) AS committer_offset,
       c->'commit'->>'message' AS message, coalesce((c->>'redacted')::BOOLEAN, false) AS redacted
FROM read_json('raw/github/commit/**/*.json',
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON[]'}, filename = true),
     unnest(response) AS t(c)
QUALIFY row_number() OVER (
  PARTITION BY coalesce(c->>'sha', (c->'commit'->'author'->>'date') || (c->'commit'->'committer'->>'date'))
  ORDER BY filename DESC) = 1;

CREATE OR REPLACE TABLE github_contribution_day AS
SELECT d.date::DATE AS day, d.contributionCount AS contributions
FROM read_json('raw/github/contributions/**/*.json', union_by_name = true),
     unnest(response.data.user.contributionsCollection.contributionCalendar.weeks) AS w(wk),
     unnest(wk.contributionDays) AS u(d)
QUALIFY row_number() OVER (PARTITION BY d.date ORDER BY fetched_at DESC) = 1;

-- Read GraphQL nodes as JSON: redacted nodes lack url, title and repo, which breaks struct access when no file has them.
CREATE OR REPLACE TABLE github_review AS
WITH n AS (
  SELECT fetched_at, unnest(from_json(response->'data'->'user'->'contributionsCollection'->'pullRequestReviewContributions'->'nodes', '["JSON"]')) AS r
  FROM read_json('raw/github/contributions/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON'})
)
SELECT r->'pullRequestReview'->>'url' AS url, r->'pullRequest'->'repository'->>'nameWithOwner' AS repo,
       r->'pullRequestReview'->>'state' AS state, (r->>'occurredAt')::TIMESTAMPTZ AS ts,
       coalesce((r->>'redacted')::BOOLEAN, false) AS redacted
FROM n
QUALIFY row_number() OVER (PARTITION BY coalesce(url, r->>'occurredAt') ORDER BY fetched_at DESC) = 1;

-- Issues, pull requests, comments and discussions are paged by updatedAt, so an item is stored again whenever it
-- changes; the newest copy wins. Redacted nodes keep databaseId.
CREATE OR REPLACE TABLE github_issue_comment AS
WITH n AS (
  SELECT fetched_at, unnest(from_json(response->'data'->'user'->'issueComments'->'nodes', '["JSON"]')) AS c
  FROM read_json('raw/github/issue_comment/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON'})
)
SELECT (c->>'databaseId')::BIGINT AS id, c->'repository'->>'nameWithOwner' AS repo, (c->'repository'->>'isPrivate')::BOOLEAN AS private,
       json_type(c->'pullRequest') = 'OBJECT' AS on_pull_request, (c->'issue'->>'number')::INT AS number, c->'issue'->>'title' AS title,
       c->>'url' AS url, c->>'body' AS body, (c->>'createdAt')::TIMESTAMPTZ AS created_at, (c->>'updatedAt')::TIMESTAMPTZ AS updated_at,
       coalesce((c->>'redacted')::BOOLEAN, false) AS redacted
FROM n
QUALIFY row_number() OVER (PARTITION BY coalesce(c->>'databaseId', c->>'createdAt') ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE github_issue AS
WITH n AS (
  SELECT fetched_at, unnest(from_json(response->'data'->'user'->'issues'->'nodes', '["JSON"]')) AS i
  FROM read_json('raw/github/issue/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON'})
)
SELECT (i->>'databaseId')::BIGINT AS id, i->'repository'->>'nameWithOwner' AS repo, (i->'repository'->>'isPrivate')::BOOLEAN AS private,
       (i->>'number')::INT AS number, i->>'title' AS title, i->>'url' AS url, i->>'state' AS state, i->>'stateReason' AS state_reason,
       (i->>'createdAt')::TIMESTAMPTZ AS opened_at, (i->>'updatedAt')::TIMESTAMPTZ AS updated_at, (i->>'closedAt')::TIMESTAMPTZ AS closed_at,
       i->>'body' AS body, coalesce((i->>'redacted')::BOOLEAN, false) AS redacted
FROM n
QUALIFY row_number() OVER (PARTITION BY coalesce(i->>'databaseId', i->>'createdAt') ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE github_pull_request AS
WITH n AS (
  SELECT fetched_at, unnest(from_json(response->'data'->'user'->'pullRequests'->'nodes', '["JSON"]')) AS p
  FROM read_json('raw/github/pull_request/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON'})
)
SELECT (p->>'databaseId')::BIGINT AS id, p->'repository'->>'nameWithOwner' AS repo, (p->'repository'->>'isPrivate')::BOOLEAN AS private,
       (p->>'number')::INT AS number, p->>'title' AS title, p->>'url' AS url, p->>'state' AS state, (p->>'isDraft')::BOOLEAN AS draft,
       (p->>'createdAt')::TIMESTAMPTZ AS opened_at, (p->>'updatedAt')::TIMESTAMPTZ AS updated_at,
       (p->>'mergedAt')::TIMESTAMPTZ AS merged_at, (p->>'closedAt')::TIMESTAMPTZ AS closed_at,
       (p->>'additions')::INT AS additions, (p->>'deletions')::INT AS deletions, (p->>'changedFiles')::INT AS changed_files,
       p->>'body' AS body, coalesce((p->>'redacted')::BOOLEAN, false) AS redacted
FROM n
QUALIFY row_number() OVER (PARTITION BY coalesce(p->>'databaseId', p->>'createdAt') ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE github_discussion AS
WITH n AS (
  SELECT fetched_at, unnest(from_json(response->'data'->'user'->'repositoryDiscussions'->'nodes', '["JSON"]')) AS d
  FROM read_json('raw/github/discussion/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON'})
)
SELECT (d->>'databaseId')::BIGINT AS id, d->'repository'->>'nameWithOwner' AS repo, (d->'repository'->>'isPrivate')::BOOLEAN AS private,
       (d->>'number')::INT AS number, d->>'title' AS title, d->>'url' AS url, d->'category'->>'name' AS category,
       (d->>'createdAt')::TIMESTAMPTZ AS created_at, (d->>'updatedAt')::TIMESTAMPTZ AS updated_at,
       (d->>'closedAt')::TIMESTAMPTZ AS closed_at, (d->>'answerChosenAt')::TIMESTAMPTZ AS answer_chosen_at,
       d->>'body' AS body, coalesce((d->>'redacted')::BOOLEAN, false) AS redacted
FROM n
QUALIFY row_number() OVER (PARTITION BY coalesce(d->>'databaseId', d->>'createdAt') ORDER BY fetched_at DESC) = 1;

-- Stored whole whenever any comment changes, so a deleted comment stays with its last copy.
CREATE OR REPLACE TABLE github_discussion_comment AS
WITH n AS (
  SELECT fetched_at, unnest(from_json(response->'data'->'user'->'repositoryDiscussionComments'->'nodes', '["JSON"]')) AS c
  FROM read_json('raw/github/discussion_comment/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON'})
)
SELECT (c->>'databaseId')::BIGINT AS id, c->'discussion'->'repository'->>'nameWithOwner' AS repo,
       (c->'discussion'->'repository'->>'isPrivate')::BOOLEAN AS private, (c->'discussion'->>'number')::INT AS number,
       c->'discussion'->>'title' AS title, c->>'url' AS url, c->>'body' AS body, (c->>'isAnswer')::BOOLEAN AS is_answer,
       (c->>'createdAt')::TIMESTAMPTZ AS created_at, (c->>'updatedAt')::TIMESTAMPTZ AS updated_at,
       coalesce((c->>'redacted')::BOOLEAN, false) AS redacted
FROM n
QUALIFY row_number() OVER (PARTITION BY coalesce(c->>'databaseId', c->>'createdAt') ORDER BY fetched_at DESC) = 1;

-- Only new stars are fetched, so unstarring is not seen.
CREATE OR REPLACE TABLE github_star AS
SELECT s->'repo'->>'full_name' AS repo, (s->'repo'->>'private')::BOOLEAN AS private, s->'repo'->>'description' AS description,
       s->'repo'->>'language' AS language, (s->>'starred_at')::TIMESTAMPTZ AS starred_at,
       coalesce((s->>'redacted')::BOOLEAN, false) AS redacted
FROM read_json('raw/github/star/**/*.json',
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON[]'}, filename = true),
     unnest(response) AS t(s)
QUALIFY row_number() OVER (PARTITION BY coalesce(s->'repo'->>'id', s->>'starred_at') ORDER BY filename DESC) = 1;

CREATE OR REPLACE TABLE github_gist AS
SELECT g->>'id' AS id, g->>'description' AS description, (g->>'public')::BOOLEAN AS public, json_keys(g->'files') AS files,
       g->>'html_url' AS url, (g->>'created_at')::TIMESTAMPTZ AS created_at, (g->>'updated_at')::TIMESTAMPTZ AS updated_at
FROM read_json('raw/github/gist/**/*.json',
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON[]'}, filename = true),
     unnest(response) AS t(g)
QUALIFY row_number() OVER (PARTITION BY g->>'id' ORDER BY filename DESC) = 1;

-- Each file is the whole set, stored when it changed.
CREATE OR REPLACE TABLE github_follow AS
WITH f AS (
  SELECT split_part(filename, '/', 3) AS direction, fetched_at, from_json(response, '["VARCHAR"]') AS logins
  FROM read_json(['raw/github/followers/**/*.json', 'raw/github/following/**/*.json'],
                 columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON'}, filename = true)
)
SELECT direction, login, min(fetched_at) AS first_seen,
       max(fetched_at) = (SELECT max(fetched_at) FROM f AS l WHERE l.direction = f.direction) AS current
FROM f, unnest(logins) AS t(login)
GROUP BY direction, login;

-- Calendar -----------------------------------------------------------------------
-- Each file is a whole calendar, stored when it changed, so the newest file per calendar is its current state.
-- The collector parses the events into response.events next to the ICS text in response.ics.

-- An ICS time is UTC (ends in Z), local in its TZID, floating (no TZID) or a date (all day). Floating times,
-- dates and TZIDs DuckDB does not know (Windows names such as W. Europe Standard Time) are read as Amsterdam time.
CREATE OR REPLACE MACRO ics_ts(v, tz) AS CASE
  WHEN length(v) = 8 THEN strptime(v, '%Y%m%d') AT TIME ZONE 'Europe/Amsterdam'
  WHEN v LIKE '%Z' THEN strptime(v, '%Y%m%dT%H%M%SZ') AT TIME ZONE 'UTC'
  ELSE strptime(v, '%Y%m%dT%H%M%S') AT TIME ZONE coalesce((SELECT name FROM pg_timezone_names() WHERE name = tz), 'Europe/Amsterdam')
END;

-- A calendar's newest whole file (`full`, or an old snapshot with `ics`) holds every event; later files hold the
-- events that changed and the `uid|recurrence-id` keys of the ones that are gone. Each key keeps its newest version.
CREATE OR REPLACE TABLE calendar_event AS
WITH f AS (
  SELECT split_part(filename, '/', 3) AS kind, filename, request->>'calendar' AS calendar,
         (request->>'redacted')::BOOLEAN AS redacted, response->'events' AS events, response->'removed' AS removed,
         coalesce((response->>'full')::BOOLEAN, (response->'ics') IS NOT NULL) AS whole
  FROM read_json('raw/calendar/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON'}, filename = true)
), live AS (
  SELECT f.* FROM f JOIN (SELECT kind, max(filename) AS since FROM f WHERE whole GROUP BY kind) USING (kind)
  WHERE filename >= since
), versions AS (
  SELECT kind, calendar, redacted, filename, e, (e->>'uid') || '|' || coalesce(e->>'recurrence_id', '') AS key
  FROM (SELECT *, unnest(from_json(events, '["JSON"]')) AS e FROM live)
  QUALIFY filename = max(filename) OVER (PARTITION BY kind, key)
), gone AS (
  SELECT kind, filename, unnest(from_json(removed, '["VARCHAR"]')) AS key FROM live WHERE removed IS NOT NULL
), e AS (
  SELECT calendar, redacted, e FROM versions v
  WHERE NOT EXISTS (SELECT 1 FROM gone g WHERE g.kind = v.kind AND g.key = v.key AND g.filename > v.filename)
)
SELECT calendar, e->>'uid' AS uid, ics_ts(e->>'recurrence_id', e->>'recurrence_id_tzid') AS recurrence_id,
       ics_ts(e->>'dtstart', e->>'dtstart_tzid') AS start_ts,
       coalesce(ics_ts(e->>'dtend', e->>'dtend_tzid'),
                ics_ts(e->>'dtstart', e->>'dtstart_tzid')
                  + to_seconds(coalesce((e->>'duration_s')::BIGINT, CASE WHEN length(e->>'dtstart') = 8 THEN 86400 ELSE 0 END))) AS end_ts,
       length(e->>'dtstart') = 8 AS all_day, e->>'summary' AS summary, e->>'location' AS location, e->>'description' AS description,
       e->>'status' AS status, e->>'transp' AS transp, e->>'x_microsoft_cdo_busystatus' AS busy_status,
       e->>'rrule' AS rrule, from_json(e->'rdate', '["VARCHAR"]') AS rdate, from_json(e->'exdate', '["VARCHAR"]') AS exdate, redacted
FROM e;

-- Albert Heijn -------------------------------------------------------------------
-- One file per receipt: the posReceiptDetails reply, with the receipt's list entry in request.list_item.
-- A receipt fetched again lands under the same file name, so the newest fetched_at wins.
-- A dateTime without an offset is Amsterdam time.

CREATE OR REPLACE TABLE ah_receipt AS
WITH r AS (
  SELECT request->>'id' AS id, request->'list_item' AS li, response.data.posReceiptDetails AS d,
         coalesce(request->'list_item'->>'dateTime', response.data.posReceiptDetails->'transaction'->>'dateTime') AS dt
  FROM read_json('raw/albert_heijn/receipt/**/*.json',
                 columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(data STRUCT(posReceiptDetails JSON))'},
                 filename = true)
  QUALIFY row_number() OVER (PARTITION BY id ORDER BY fetched_at DESC) = 1
)
SELECT id,
       CASE WHEN regexp_matches(dt, '(Z|[+-]\d\d:?\d\d)$') THEN dt::TIMESTAMPTZ
            ELSE dt::TIMESTAMP AT TIME ZONE 'Europe/Amsterdam' END AS ts,
       nullif(concat_ws(', ', nullif(concat_ws(' ', a->>'street', a->>'houseNumber'), ''), a->>'city'), '') AS store,
       (d->'transaction'->>'store')::INT AS store_id,
       coalesce(d->'total'->>'amount', li->'totalAmount'->>'amount')::DECIMAL(10,2) AS total,
       (d->'discountTotal'->>'amount')::DECIMAL(10,2) AS discount_total,
       (SELECT list(p->>'method') FROM unnest(from_json(d->'payments', '["JSON"]')) AS t(p)) AS payment_methods,
       json_array_length(d->'products') AS items
FROM r, (SELECT if(json_type(d->'address') = 'OBJECT', d->'address', li->'storeAddress')) AS u(a);

CREATE OR REPLACE TABLE ah_receipt_item AS
WITH r AS (
  SELECT request->>'id' AS receipt_id, from_json(response.data.posReceiptDetails->'products', '["JSON"]') AS products
  FROM read_json('raw/albert_heijn/receipt/**/*.json',
                 columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'STRUCT(data STRUCT(posReceiptDetails JSON))'},
                 filename = true)
  QUALIFY row_number() OVER (PARTITION BY receipt_id ORDER BY fetched_at DESC) = 1
)
SELECT r.receipt_id, a.ts, (p->>'id')::BIGINT AS product_id, p->>'name' AS name,
       (p->>'quantity')::DOUBLE AS quantity, (p->'weight'->>'amount')::DOUBLE AS weight, p->'weight'->>'unit' AS unit,
       (p->'price'->>'amount')::DECIMAL(10,2) AS price, (p->'amount'->>'amount')::DECIMAL(10,2) AS amount,
       p->'indicators' AS indicators
FROM r JOIN ah_receipt a ON a.id = r.receipt_id, unnest(r.products) AS t(p);

-- Screen time --------------------------------------------------------------------
-- One file per push from personal-data-mac on the Mac, at most every 15 minutes. A failed push is sent again with
-- what came since, so every table dedupes. devices and streams are in a push only when they changed.

-- Biome has no names for most synced devices, so they are named here from what each one ran. The returned Watch's
-- activity ends on 2026-09-25 at 14:49 and the current one's starts at 15:22.
CREATE OR REPLACE TABLE screen_time_device AS
WITH named(device_uuid, name, platform) AS (VALUES
  ('D4AA0FB4-003D-4233-9C59-BCAFE5C83468', 'iPhone', 'ios'),
  ('40AC7128-D004-43FE-90CB-BFFF5CDDDB8F', 'iPad Pro', 'ipados'),
  ('9DE78878-F3DC-43FF-8F7F-8E0F62DB6010', 'Apple Watch', 'watchos'),
  ('684510F7-8548-441E-A956-01996E5A5F6C', 'Apple Watch (returned 2026-09-25)', 'watchos'),
  ('7A8B6C02-A39E-465C-A1E7-ED770AC38151', 'Apple TV', 'tvos')
), pushed AS (
  SELECT d->>'device_uuid' AS device_uuid, arg_max(d->>'name', sent_at) AS name, arg_max(d->>'platform', sent_at) AS platform,
         min(sent_at) AS first_seen
  FROM read_json('raw/screen_time/push/**/*.json', columns = {sent_at: 'TIMESTAMPTZ', devices: 'JSON[]'},
                 maximum_object_size = 67108864),
       unnest(devices) AS t(d)
  GROUP BY 1
)
SELECT device_uuid, coalesce(n.name, nullif(p.name, '')) AS name, coalesce(n.platform, p.platform) AS platform, p.first_seen
FROM pushed AS p FULL JOIN named AS n USING (device_uuid);

-- Biome focus transitions become intervals per device: a gain lasts until the next gain of any app or the next loss
-- of the same app, at most an hour. The Mac's own Biome stream (device_uuid local) is left out: knowledgeC covers it.
CREATE OR REPLACE TABLE screen_time_app AS
WITH mac AS (
  SELECT DISTINCT host AS device, 'macos' AS platform, u->>'bundle' AS bundle,
         (u->>'start')::TIMESTAMPTZ AS start_ts, (u->>'end')::TIMESTAMPTZ AS end_ts, 'knowledgec' AS source,
         false AS capped
  FROM read_json('raw/screen_time/push/**/*.json', columns = {host: 'VARCHAR', mac_usage: 'JSON[]'},
                 maximum_object_size = 67108864),
       unnest(mac_usage) AS t(u)
), focus AS (
  SELECT DISTINCT f->>'device_uuid' AS device_uuid, f->>'bundle' AS bundle, (f->>'time')::TIMESTAMPTZ AS ts,
         (f->>'gained')::BOOLEAN AS gained
  FROM read_json('raw/screen_time/push/**/*.json', columns = {focus: 'JSON[]'}, maximum_object_size = 67108864),
       unnest(focus) AS t(f)
  WHERE f->>'device_uuid' <> 'local'
), edges AS (
  SELECT *,
         min(ts) FILTER (WHERE gained) OVER (PARTITION BY device_uuid ORDER BY ts, bundle
                                             ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS next_gain,
         min(ts) FILTER (WHERE NOT gained) OVER (PARTITION BY device_uuid, bundle ORDER BY ts
                                                 ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS next_loss
  FROM focus
), biome AS (
  SELECT coalesce(nullif(d.name, ''), e.device_uuid) AS device, coalesce(d.platform, 'unknown') AS platform, e.bundle,
         e.ts AS start_ts, least(e.next_gain, e.next_loss, e.ts + INTERVAL 1 HOUR) AS end_ts, 'biome' AS source,
         coalesce(least(e.next_gain, e.next_loss) > e.ts + INTERVAL 1 HOUR, true) AS capped
  FROM edges AS e LEFT JOIN screen_time_device AS d USING (device_uuid)
  WHERE e.gained
)
SELECT device, platform, bundle, start_ts, end_ts, date_diff('millisecond', start_ts, end_ts) / 1000 AS seconds, source,
       capped
FROM (SELECT * FROM mac UNION ALL SELECT * FROM biome)
WHERE end_ts > start_ts;

-- A message of the API spans several lines with the same message_id and usage, so only its last line keeps the tokens.
CREATE OR REPLACE TABLE claude_code_event AS
WITH e AS (
  SELECT DISTINCT ON (coalesce(c->>'uuid', concat_ws('|', c->>'session_id', c->>'ts', c->>'type')))
         (c->>'ts')::TIMESTAMPTZ AS ts, c->>'session_id' AS session_id, c->>'project' AS project, c->>'type' AS type,
         c->>'model' AS model, c->>'tool' AS tool, c->>'message_id' AS message_id, c->>'uuid' AS uuid,
         coalesce((c->>'sidechain')::BOOLEAN, false) AS sidechain,
         (c->>'input_tokens')::BIGINT AS input_tokens, (c->>'output_tokens')::BIGINT AS output_tokens,
         (c->>'cache_read_tokens')::BIGINT AS cache_read_tokens, (c->>'cache_creation_tokens')::BIGINT AS cache_creation_tokens
  FROM read_json('raw/screen_time/push/**/*.json', columns = {claude: 'JSON[]'}, maximum_object_size = 67108864),
       unnest(claude) AS t(c)
), m AS (
  SELECT *, message_id IS NOT NULL AND row_number() OVER (PARTITION BY message_id ORDER BY ts DESC, uuid) = 1 AS last_line
  FROM e
)
SELECT ts, session_id, project, type, model, tool, sidechain, message_id,
       CASE WHEN last_line THEN max(input_tokens) OVER w END AS input_tokens,
       CASE WHEN last_line THEN max(output_tokens) OVER w END AS output_tokens,
       CASE WHEN last_line THEN max(cache_read_tokens) OVER w END AS cache_read_tokens,
       CASE WHEN last_line THEN max(cache_creation_tokens) OVER w END AS cache_creation_tokens
FROM m
WINDOW w AS (PARTITION BY message_id);

-- Biome Media.NowPlaying, one row per state change on each device. The field numbers are inferred from the data,
-- not from a published schema: 3 playback state, 4 album, 5 artist or show, 6 duration (s), 8 title, 10 media type,
-- 14.3 output route, 15 app.
CREATE OR REPLACE TABLE media_now_playing AS
SELECT DISTINCT m->>'device_uuid' AS device_uuid, coalesce(nullif(d.name, ''), m->>'device_uuid') AS device,
       CASE WHEN (m->>'device_uuid') = 'local' THEN 'macos' ELSE d.platform END AS platform,
       (m->>'time')::TIMESTAMPTZ AS ts, (m->'fields'->>'3')::INT AS state, nullif(m->'fields'->>'8', '') AS title,
       nullif(m->'fields'->>'5', '') AS artist, nullif(m->'fields'->>'4', '') AS album,
       (m->'fields'->>'6')::DOUBLE AS duration_s, regexp_replace(m->'fields'->>'10', '^(kMRMediaRemoteNowPlayingInfoType|MRMediaRemoteMediaType)', '') AS media_type,
       nullif(m->'fields'->>'15', '') AS app, m->'fields'->'14'->>'3' AS output, m->'fields' AS fields
FROM read_json('raw/screen_time/push/**/*.json', columns = {media: 'JSON[]'}, maximum_object_size = 67108864),
     unnest(media) AS t(m)
LEFT JOIN screen_time_device AS d ON d.device_uuid = (m->>'device_uuid')
WHERE (m->>'stream') = 'restricted/Media.NowPlaying';

-- A play is a run of events with the same title on one device; it lasts until the next event there that isn't the
-- same title, capped at the track's duration.
CREATE OR REPLACE TABLE media_play AS
WITH e AS (
  SELECT *, lead(ts) OVER (PARTITION BY device_uuid ORDER BY ts) AS next_ts,
         title IS DISTINCT FROM lag(title) OVER (PARTITION BY device_uuid ORDER BY ts) AS starts
  FROM media_now_playing
  WHERE title IS NOT NULL OR state <> 1
), g AS (
  SELECT *, sum(starts::INT) OVER (PARTITION BY device_uuid ORDER BY ts) AS play
  FROM e
)
SELECT device_uuid, any_value(device) AS device, any_value(platform) AS platform, min(ts) AS start_ts,
       any_value(title) AS title, any_value(artist) AS artist, any_value(album) AS album, any_value(media_type) AS media_type,
       any_value(app) AS app, any_value(output) AS output, max(duration_s) AS duration_s,
       least(sum(date_diff('millisecond', ts, coalesce(next_ts, ts)) / 1000) FILTER (WHERE state = 1), max(duration_s)) AS played_s
FROM g
WHERE title IS NOT NULL
GROUP BY device_uuid, play;

-- Biome context streams, sent raw like media. Field meanings are read off the records, not from a published schema.
CREATE OR REPLACE TABLE biome_event AS
SELECT DISTINCT replace(r->>'stream', 'restricted/', '') AS stream, r->>'device_uuid' AS device_uuid,
       coalesce(nullif(d.name, ''), r->>'device_uuid') AS device, (r->>'time')::TIMESTAMPTZ AS ts, r->'fields' AS fields
FROM read_json('raw/screen_time/push/**/*.json', columns = {media: 'JSON[]'}, maximum_object_size = 67108864),
     unnest(media) AS t(r)
LEFT JOIN screen_time_device AS d ON d.device_uuid = (r->>'device_uuid')
WHERE (r->>'stream') NOT ILIKE '%media%' AND (r->>'stream') NOT ILIKE '%nowplaying%';

CREATE OR REPLACE TABLE bluetooth_connection AS
SELECT ts, device, fields->>'2' AS name, fields->>'1' AS address, (fields->>'4')::INT = 1 AS connected,
       (fields->>'9')::INT = 1 AS is_headphones, fields
FROM biome_event WHERE stream = 'Device.Wireless.Bluetooth';

CREATE OR REPLACE TABLE focus_mode AS
SELECT ts, device, fields->>'6' AS mode, (fields->>'2')::INT = 1 AS active, fields
FROM biome_event WHERE stream = 'UserFocus.ComputedMode';

CREATE OR REPLACE TABLE wifi_network AS
SELECT ts, device, fields->>'1' AS ssid, (fields->>'2')::INT = 1 AS connected
FROM biome_event WHERE stream = 'Device.Wireless.WiFi';

CREATE OR REPLACE TABLE notification AS
SELECT ts, device, fields->>'4' AS app, (fields->>'3')::INT AS kind, fields->>'5' AS notification_id
FROM biome_event WHERE stream = 'Notification.Usage';

CREATE OR REPLACE TABLE browser_visit AS
SELECT DISTINCT ON (v->>'browser', v->>'profile', (v->>'visit_id')::BIGINT)
       (v->>'time')::TIMESTAMPTZ AS ts, v->>'browser' AS browser, v->>'profile' AS profile,
       (v->>'visit_id')::BIGINT AS visit_id, v->>'url' AS url,
       lower(nullif(regexp_extract(v->>'url', '^[A-Za-z][A-Za-z0-9+.-]*://(?:[^/?#@]*@)?([^/?#:]+)', 1), '')) AS host,
       v->>'title' AS title,
       (v->>'origin_or_source')::INT AS origin_or_source, (v->>'visit_type')::INT AS visit_type,
       (v->>'load_successful')::BOOLEAN AS load_successful, v->>'transition' AS transition
FROM read_json('raw/screen_time/push/**/*.json', columns = {browser: 'JSON[]'}, maximum_object_size = 67108864),
     unnest(browser) AS t(v);

-- Photos metadata from the Mac's Photos library, sent again when Photos changes an asset: the newest copy wins.
-- Times are seconds since 2001; -180 means no location.
CREATE OR REPLACE TABLE photo AS
WITH p AS (
  SELECT p, filename FROM read_json('raw/screen_time/push/**/*.json', columns = {photos: 'JSON[]'}, filename = true,
                                    maximum_object_size = 67108864), unnest(photos) AS t(p)
)
SELECT DISTINCT ON (p->>'uuid')
       p->>'uuid' AS uuid, to_timestamp(978307200 + (p->>'created')::DOUBLE) AS taken_at,
       to_timestamp(978307200 + (p->>'added')::DOUBLE) AS added_at, p->>'timezone' AS timezone,
       CASE (p->>'kind')::INT WHEN 0 THEN 'photo' WHEN 1 THEN 'video' END AS kind,
       nullif((p->>'lat')::DOUBLE, -180) AS lat, nullif((p->>'lon')::DOUBLE, -180) AS lon,
       nullif((p->>'gps_accuracy')::DOUBLE, -1) AS gps_accuracy_m,
       (p->>'screenshot')::INT = 1 AS screenshot, (p->>'favorite')::INT = 1 AS favorite,
       (p->>'hidden')::INT = 1 AS hidden, (p->>'trashed')::INT = 1 AS trashed,
       p->>'camera_make' AS camera_make, p->>'camera_model' AS camera_model, p->>'lens' AS lens,
       (p->>'width')::INT AS width, (p->>'height')::INT AS height, nullif((p->>'duration')::DOUBLE, 0) AS duration_s,
       (p->>'size_bytes')::BIGINT AS size_bytes, p->>'original_filename' AS original_filename,
       p->>'imported_by' AS imported_by, (p->>'views')::INT AS views, p AS raw
FROM p
ORDER BY p->>'uuid', filename DESC;

-- What Photos' on-device analysis found per asset, from its search index; each push carries an asset's whole set.
CREATE OR REPLACE TABLE photo_label AS
WITH l AS (
  SELECT DISTINCT ON (l->>'uuid') l->>'uuid' AS uuid, l->'labels' AS labels
  FROM read_json('raw/screen_time/push/**/*.json', columns = {photo_labels: 'JSON[]'}, filename = true,
                 maximum_object_size = 67108864), unnest(photo_labels) AS t(l)
  ORDER BY l->>'uuid', filename DESC
), u AS (
  SELECT uuid, unnest(from_json(labels, '["JSON"]')) AS x FROM l
)
SELECT uuid, (x->>0)::INT AS category,
       CASE WHEN (x->>0)::INT BETWEEN 0 AND 14 OR (x->>0)::INT IN (1007, 1008, 1010) THEN 'place'
            WHEN (x->>0)::INT IN (1003, 1004) THEN 'business'
            WHEN (x->>0)::INT = 1103 THEN 'holiday'
            WHEN (x->>0)::INT = 1202 THEN 'caption'
            WHEN (x->>0)::INT = 1203 THEN 'text'
            WHEN (x->>0)::INT IN (1500, 1501) THEN 'scene'
            WHEN (x->>0)::INT = 1510 THEN 'species'
            WHEN (x->>0)::INT IN (1520, 1700) THEN 'landmark'
            WHEN (x->>0)::INT = 1701 THEN 'landmark_type'
            WHEN (x->>0)::INT IN (1600, 1601) THEN 'meaning'
            WHEN (x->>0)::INT IN (1610, 1611) THEN 'trip'
            WHEN (x->>0)::INT BETWEEN 1800 AND 1803 THEN 'event'
            WHEN (x->>0)::INT = 2200 THEN 'app'
            WHEN (x->>0)::INT IN (2500, 2501) THEN 'sound'
            WHEN (x->>0)::INT IN (2600, 2601) THEN 'activity'
            ELSE 'other' END AS kind,
       x->>1 AS label
FROM u;

CREATE OR REPLACE TABLE photo_person AS
WITH l AS (
  SELECT DISTINCT ON (l->>'uuid') l->>'uuid' AS uuid, l->'people' AS people
  FROM read_json('raw/screen_time/push/**/*.json', columns = {photo_labels: 'JSON[]'}, filename = true,
                 maximum_object_size = 67108864), unnest(photo_labels) AS t(l)
  ORDER BY l->>'uuid', filename DESC
), u AS (
  SELECT uuid, unnest(from_json(people, '["JSON"]')) AS x FROM l
)
SELECT uuid, x->>0 AS person, x->>1 AS name FROM u;

-- Apple Podcasts episodes with any activity, newest copy per episode. Times are seconds since 2001.
CREATE OR REPLACE TABLE podcast_episode AS
WITH e AS (
  SELECT e, filename FROM read_json('raw/screen_time/push/**/*.json', columns = {podcasts: 'JSON[]'}, filename = true,
                                    maximum_object_size = 67108864), unnest(podcasts) AS t(e)
)
SELECT DISTINCT ON (e->>'uuid')
       e->>'uuid' AS uuid, e->>'podcast' AS podcast, e->>'podcast_author' AS podcast_author, e->>'title' AS title,
       to_timestamp(978307200 + (e->>'published')::DOUBLE) AS published_at, (e->>'duration')::DOUBLE AS duration_s,
       (e->>'playhead')::DOUBLE AS playhead_s, (e->>'play_count')::INT AS play_count, (e->>'play_state')::INT AS play_state,
       to_timestamp(978307200 + (e->>'last_played')::DOUBLE) AS last_played_at,
       to_timestamp(978307200 + (e->>'play_state_modified')::DOUBLE) AS play_state_modified_at,
       (e->>'saved')::INT = 1 AS saved, (e->>'bookmarked')::INT = 1 AS bookmarked,
       to_timestamp(978307200 + (e->>'downloaded')::DOUBLE) AS downloaded_at,
       (e->>'subscribed')::INT = 1 AS subscribed, (e->>'episode_number')::INT AS episode_number,
       (e->>'season_number')::INT AS season_number, e->>'episode_type' AS episode_type, e->>'web_url' AS web_url,
       e->>'feed_url' AS feed_url, e->>'podcast_uuid' AS podcast_uuid, e AS raw
FROM e
ORDER BY e->>'uuid', filename DESC;

-- Shell commands on the Mac: the time and the command word only.
CREATE OR REPLACE TABLE shell_command AS
SELECT DISTINCT ON (host, c->>'shell', (c->>'time')::TIMESTAMPTZ, c->>'command')
       (c->>'time')::TIMESTAMPTZ AS ts, host, c->>'shell' AS shell, c->>'command' AS command,
       (c->>'duration_s')::BIGINT AS duration_s
FROM read_json('raw/screen_time/push/**/*.json', columns = {host: 'VARCHAR', shell: 'JSON[]'},
               maximum_object_size = 67108864),
     unnest(shell) AS t(c);

-- Obsidian notes: one row per version the Mac saw, metadata only, plus a row per deletion.
CREATE OR REPLACE TABLE obsidian AS
SELECT DISTINCT ON (n->>'path', n->>'modified', n->>'bytes', n->>'deleted')
       n->>'path' AS path, n->>'folder' AS folder, n->>'name' AS name, n->>'ext' AS ext,
       (n->>'created')::TIMESTAMPTZ AS created_at, (n->>'modified')::TIMESTAMPTZ AS modified_at,
       (n->>'bytes')::BIGINT AS bytes, (n->>'words')::INT AS words, (n->>'lines')::INT AS lines,
       from_json(n->'tags', '["VARCHAR"]') AS tags, from_json(n->'links', '["VARCHAR"]') AS links,
       coalesce((n->>'deleted')::BOOLEAN, false) AS deleted, sent_at::TIMESTAMPTZ AS sent_at
FROM read_json('raw/screen_time/push/**/*.json', columns = {sent_at: 'VARCHAR', obsidian: 'JSON[]'},
               maximum_object_size = 67108864),
     unnest(obsidian) AS t(n)
ORDER BY n->>'path', n->>'modified', n->>'bytes', n->>'deleted', sent_at;

-- YouTube ------------------------------------------------------------------------
-- One videos.list response per 50 videos seen in browser history, each video looked up once.
CREATE OR REPLACE TABLE youtube_video AS
WITH v AS (
  SELECT fetched_at::TIMESTAMPTZ AS fetched_at, unnest(from_json(response->'items', '["JSON"]')) AS i
  FROM read_json('raw/youtube/video/**/*.json', columns = {fetched_at: 'VARCHAR', response: 'JSON'})
), d AS (
  SELECT *, regexp_extract(i->>'$.contentDetails.duration', '^P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$',
                           ['d', 'h', 'm', 's']) AS p
  FROM v
)
SELECT DISTINCT ON (i->>'id')
       i->>'id' AS video_id, i->>'$.snippet.title' AS title, i->>'$.snippet.channelId' AS channel_id,
       i->>'$.snippet.channelTitle' AS channel, (i->>'$.snippet.publishedAt')::TIMESTAMPTZ AS published_at,
       coalesce(nullif(p.d, '')::INT, 0) * 86400 + coalesce(nullif(p.h, '')::INT, 0) * 3600
         + coalesce(nullif(p.m, '')::INT, 0) * 60 + coalesce(nullif(p.s, '')::INT, 0) AS duration_s,
       (i->>'$.snippet.categoryId')::INT AS category_id, from_json(i->'$.snippet.tags', '["VARCHAR"]') AS tags,
       i->>'$.snippet.description' AS description,
       coalesce(i->>'$.snippet.defaultAudioLanguage', i->>'$.snippet.defaultLanguage') AS language,
       (i->>'$.statistics.viewCount')::BIGINT AS views, (i->>'$.statistics.likeCount')::BIGINT AS likes,
       (i->>'$.statistics.commentCount')::BIGINT AS comments,
       (i->>'$.liveStreamingDetails.actualStartTime')::TIMESTAMPTZ AS live_start,
       (i->>'$.liveStreamingDetails.actualEndTime')::TIMESTAMPTZ AS live_end,
       i->>'$.status.privacyStatus' AS privacy, (i->>'$.status.madeForKids')::BOOLEAN AS made_for_kids,
       from_json(i->'$.topicDetails.topicCategories', '["VARCHAR"]') AS topics, fetched_at, i AS raw
FROM d
ORDER BY i->>'id', fetched_at DESC;

CREATE OR REPLACE TABLE youtube_watch AS
WITH w AS (
  SELECT ts, browser, origin_or_source, url,
         coalesce(nullif(regexp_extract(url, '[?&]v=([A-Za-z0-9_-]{11})(?:[&#]|$)', 1), ''),
                  nullif(regexp_extract(url, '^[^?#]*(?:youtu\.be/|/shorts/|/live/|/embed/)([A-Za-z0-9_-]{11})(?:[/?#]|$)', 1), '')) AS video_id
  FROM browser_visit
  WHERE host = 'youtu.be' OR host = 'youtube.com' OR host LIKE '%.youtube.com' OR host LIKE '%youtube-nocookie.com'
)
SELECT w.ts, w.video_id, v.title, v.channel, v.duration_s, w.browser, w.origin_or_source
FROM w LEFT JOIN youtube_video v USING (video_id)
WHERE w.video_id IS NOT NULL;

-- KNMI ----------------------------------------------------------------------------
-- Measured station weather, validated, about two days behind. Values come in tenths (0.1 °C, 0.1 m/s, 0.1 mm,
-- 0.1 h, 0.1 hPa); -1 for rain or sunshine means less than 0.05. The newest copy of each hour or day wins.
-- Typed columns rather than JSON: generic JSON for a few hundred thousand hours runs the build out of memory.
CREATE OR REPLACE TABLE knmi_hourly AS
-- The alias is k: names ignore case, so r would clash with the field R.
WITH hours AS (
  SELECT unnest(response) AS k, filename
  FROM read_json('raw/knmi/hourly/**/*.json', filename = true,
                 columns = {response: 'STRUCT(station_code INT, date VARCHAR, hour INT, DD INT, FH INT, FF INT, FX INT,
                                              T INT, T10N INT, TD INT, SQ INT, Q INT, DR INT, RH INT, P INT, VV INT,
                                              N INT, U INT, WW INT, IX INT, M INT, R INT, S INT, O INT, Y INT)[]'})
)
SELECT DISTINCT ON (station, ts)
       k.station_code AS station,
       CASE k.station_code WHEN 240 THEN 'Schiphol' WHEN 225 THEN 'IJmuiden' WHEN 257 THEN 'Wijk aan Zee' END AS name,
       -- hour 1 is 00:00 to 01:00 UT; ts is the end of the hour.
       k.date::TIMESTAMPTZ + to_hours(k.hour) AS ts,
       k.T / 10 AS temp_c, k.TD / 10 AS dew_point_c, k.U AS humidity_pct,
       k.FH / 10 AS wind_ms, k.FF / 10 AS wind_last_10min_ms, k.FX / 10 AS gust_ms, nullif(k.DD, 990) AS wind_dir,
       -- greatest() would turn a missing value into 0.
       CASE WHEN k.RH < 0 THEN 0 ELSE k.RH / 10 END AS rain_mm, k.DR / 10 AS rain_duration_h,
       CASE WHEN k.SQ < 0 THEN 0 ELSE k.SQ / 10 END AS sunshine_h,
       k.Q AS radiation_j_cm2, k.P / 10 AS pressure_hpa, k.N AS cloud_octants, k.VV AS visibility_code,
       k.WW AS weather_code, k.T10N / 10 AS min_temp_10cm_c,
       k.M = 1 AS fog, k.R = 1 AS rain, k.S = 1 AS snow, k.O = 1 AS thunder, k.Y = 1 AS ice
FROM hours
ORDER BY station, ts, filename DESC;

CREATE OR REPLACE TABLE knmi_daily AS
WITH r AS (
  SELECT unnest(from_json(response, '["JSON"]')) AS r, filename
  FROM read_json('raw/knmi/daily/**/*.json', columns = {response: 'JSON'}, filename = true,
                 maximum_object_size = 67108864)
)
SELECT DISTINCT ON (station, day)
       (r->>'station_code')::INT AS station,
       CASE (r->>'station_code')::INT WHEN 240 THEN 'Schiphol' WHEN 225 THEN 'IJmuiden' WHEN 257 THEN 'Wijk aan Zee' END AS name,
       (r->>'date')::DATE AS day, (r->>'TG')::DOUBLE / 10 AS temp_mean_c, (r->>'TN')::DOUBLE / 10 AS temp_min_c,
       (r->>'TX')::DOUBLE / 10 AS temp_max_c,
       CASE WHEN (r->>'RH')::INT < 0 THEN 0 ELSE (r->>'RH')::DOUBLE / 10 END AS rain_mm,
       CASE WHEN (r->>'SQ')::INT < 0 THEN 0 ELSE (r->>'SQ')::DOUBLE / 10 END AS sunshine_h, (r->>'SP')::INT AS sunshine_pct,
       (r->>'FG')::DOUBLE / 10 AS wind_mean_ms, (r->>'FXX')::DOUBLE / 10 AS gust_max_ms,
       (r->>'UG')::INT AS humidity_pct, (r->>'PG')::DOUBLE / 10 AS pressure_hpa, (r->>'NG')::INT AS cloud_octants,
       (r->>'EV24')::DOUBLE / 10 AS evaporation_mm, r AS raw
FROM r
ORDER BY station, day, filename DESC;

-- KNMI EDR: 10-minute station data and daily grids at home, CoverageJSON with values aligned to the time axis.
-- Typed rather than JSON, so a year of 10-minute values fits the build's memory.
-- partition: year(ts), month(ts)
CREATE OR REPLACE TABLE knmi_10min AS
WITH c AS (
  SELECT regexp_extract(filename, '-(0-20000-0-\d+)\.json$', 1) AS station_id, unnest(response.coverages) AS c, filename
  FROM read_json('raw/knmi_edr/ten_minute/**/*.json', filename = true, maximum_object_size = 67108864,
                 columns = {response: 'STRUCT(coverages STRUCT(domain STRUCT(axes STRUCT(t STRUCT("values" TIMESTAMPTZ[]))),
                                                              ranges MAP(VARCHAR, STRUCT("values" DOUBLE[])))[])'})
), p AS (
  SELECT station_id, filename, c.domain.axes.t."values" AS times, unnest(map_entries(c.ranges)) AS r FROM c
)
SELECT DISTINCT ON (station_id, ts, parameter)
       right(station_id, 3)::INT AS station,
       CASE right(station_id, 3)::INT WHEN 240 THEN 'Schiphol' WHEN 225 THEN 'IJmuiden' WHEN 257 THEN 'Wijk aan Zee' END AS name,
       unnest(times) AS ts, r.key AS parameter, unnest(r.value."values") AS value
FROM p
ORDER BY station_id, ts, parameter, filename DESC;

-- Each station's files describe only its own parameters, so take the newest description of each over all files.
CREATE OR REPLACE TABLE knmi_10min_parameter AS
SELECT DISTINCT ON (p.key)
       p.key AS parameter, p.value->>'$.observedProperty.label.en' AS label,
       coalesce(p.value->>'$.unit.symbol.value', p.value->>'$.unit.label.en') AS unit,
       coalesce(p.value->>'$.description.en', p.value->>'description') AS description
FROM (SELECT unnest(map_entries(response.parameters::MAP(VARCHAR, JSON))) AS p, filename
      FROM read_json('raw/knmi_edr/ten_minute/**/*.json', columns = {response: 'STRUCT(parameters JSON)'}, filename = true,
                     maximum_object_size = 67108864))
ORDER BY p.key, filename DESC;

CREATE OR REPLACE TABLE knmi_grid_home AS
WITH c AS (
  SELECT regexp_extract(filename, '-([A-Za-z0-9]+)\.json$', 1) AS grid, response AS c, filename
  FROM read_json('raw/knmi_edr/grid/**/*.json', columns = {response: 'JSON'}, filename = true,
                 maximum_object_size = 67108864)
), p AS (
  SELECT grid, filename, c, unnest(json_keys(c->'ranges')) AS parameter FROM c
)
SELECT DISTINCT ON (grid, day, parameter)
       grid, (unnest(from_json(c->'$.domain.axes.t.values', '["TIMESTAMPTZ"]')) - INTERVAL 8 HOUR)::DATE AS day, parameter,
       unnest(from_json(c->'ranges'->parameter->'values', '["DOUBLE"]')) AS value
FROM p
-- The station-* values are only filled at station cells, never at home.
WHERE parameter NOT LIKE 'station-%'
ORDER BY grid, day, parameter, filename DESC;

-- KNMI radar: per 5-minute step, the WMS reply at home, the value under data.<reference time>.<time>.
CREATE OR REPLACE TABLE knmi_rain_radar AS
WITH s AS (
  SELECT unnest(from_json(j, '["JSON"]')) AS s
  FROM read_json('raw/knmi_radar/precipitation/**/*.json', format = 'unstructured', records = false,
                 maximum_object_size = 67108864) AS t(j)
), d AS (
  -- Key on the reply's own time: read_json reformats the step's "time" field as a timestamp.
  SELECT s, s->'$.reply[0].data' AS data, json_keys(s->'$.reply[0].data')[1] AS k FROM s
)
SELECT DISTINCT ON (ts)
       k::TIMESTAMPTZ AS ts, json_extract_string(data, '$."' || k || '"."' || k || '"')::DOUBLE AS rain_mm_h,
       s->'$.reply[0].point'->>'coords' AS cell
FROM d
WHERE k IS NOT NULL;

CREATE OR REPLACE TABLE knmi_rain_forecast AS
WITH s AS (
  SELECT unnest(from_json(j, '["JSON"]')) AS s
  FROM read_json('raw/knmi_radar/precipitation/**/*.json', format = 'unstructured', records = false,
                 maximum_object_size = 67108864) AS t(j)
), r AS (
  SELECT s->'$.reply[0].data' AS data, json_keys(s->'$.reply[0].data')[1] AS ref FROM s
), v AS (
  SELECT ref, unnest(json_keys(data->ref)) AS k, data->ref AS steps FROM r WHERE ref IS NOT NULL
)
SELECT DISTINCT ON (ref, k)
       ref::TIMESTAMPTZ AS run_at, k::TIMESTAMPTZ AS ts,
       date_diff('minute', ref::TIMESTAMPTZ, k::TIMESTAMPTZ) AS lead_min, (steps->>k)::DOUBLE AS rain_mm_h
FROM v
WHERE k <> ref;

-- Flights -------------------------------------------------------------------------
-- adsb.lol replies polled every minute by the Mac helper, sent with the screen time pushes under "flights".
CREATE OR REPLACE TABLE flight_position AS
WITH p AS (
  SELECT (f->>'polled_at')::TIMESTAMPTZ AS polled_at, unnest(from_json(f->'$.reply.ac', '["JSON"]')) AS a
  FROM read_json('raw/screen_time/push/**/*.json', columns = {flights: 'JSON[]'}, maximum_object_size = 67108864),
       unnest(flights) AS t(f)
)
SELECT DISTINCT ON (a->>'hex', polled_at)
       polled_at AS ts, a->>'hex' AS icao, trim(a->>'flight') AS callsign, a->>'r' AS registration, a->>'t' AS aircraft_type,
       a->>'category' AS category, (a->>'lat')::DOUBLE AS lat, (a->>'lon')::DOUBLE AS lon,
       try_cast(a->>'alt_baro' AS INT) AS altitude_ft, (a->>'alt_geom')::INT AS altitude_geom_ft, (a->>'gs')::DOUBLE AS speed_kt,
       (a->>'track')::DOUBLE AS track, (a->>'baro_rate')::INT AS climb_ft_min, (a->>'dst')::DOUBLE AS distance_nm,
       (a->>'dir')::DOUBLE AS bearing, a->>'squawk' AS squawk, a AS raw
FROM p;

CREATE OR REPLACE TABLE flight_poll AS
SELECT DISTINCT (f->>'polled_at')::TIMESTAMPTZ AS ts, json_array_length(f->'$.reply.ac') AS aircraft
FROM read_json('raw/screen_time/push/**/*.json', columns = {flights: 'JSON[]'}, maximum_object_size = 67108864),
     unnest(flights) AS t(f);

-- Earthquakes ---------------------------------------------------------------------
-- KNMI's FDSN catalogue: the whole catalogue once, then the last 60 days daily; the newest copy of each event wins.
CREATE OR REPLACE TABLE earthquake AS
SELECT DISTINCT ON ("#EventID")
       "#EventID" AS event_id, "Time"::TIMESTAMP AT TIME ZONE 'UTC' AS ts, Latitude AS lat, Longitude AS lon,
       "Depth/km" AS depth_km, nullif(MagType, '') AS magnitude_type, Magnitude AS magnitude,
       EventLocationName AS place, EventType AS event_type
FROM read_csv('raw/earthquakes/**/*.txt', delim = '|', header = true, filename = true,
              types = {'Time': 'VARCHAR', 'Magnitude': 'DOUBLE', 'Latitude': 'DOUBLE', 'Longitude': 'DOUBLE',
                       'Depth/km': 'DOUBLE', 'MagType': 'VARCHAR'})
ORDER BY "#EventID", filename DESC;

-- Water ---------------------------------------------------------------------------
-- Rijkswaterstaat water temperatures (each station from its last stored measurement, so little overlap) and the
-- zwemwater.nl swimming spot list, stored when changed.
-- Read as typed structs: unpacking each reply as generic JSON ran out of the build's 1 GB.
CREATE OR REPLACE TABLE water_temperature AS
WITH w AS (
  SELECT unnest(response.WaarnemingenLijst) AS w
  FROM read_json('raw/water/temperature/**/*.json', maximum_object_size = 67108864, columns = {response: 'STRUCT(
    WaarnemingenLijst STRUCT(
      Locatie STRUCT(Code VARCHAR, Naam VARCHAR, Lat DOUBLE, Lon DOUBLE),
      MetingenLijst STRUCT(Tijdstip VARCHAR, Meetwaarde STRUCT(Waarde_Numeriek DOUBLE),
                           WaarnemingMetadata STRUCT(Bemonsteringshoogte VARCHAR, Statuswaarde VARCHAR))[]
    )[])'})
), m AS (
  SELECT w.Locatie AS l, unnest(w.MetingenLijst) AS m FROM w
)
SELECT DISTINCT ON (l.Code, depth_cm, ts)
       l.Code AS station, l.Naam AS name, l.Lat AS lat, l.Lon AS lon, m.Tijdstip::TIMESTAMPTZ AS ts,
       -(m.WaarnemingMetadata.Bemonsteringshoogte::INT) AS depth_cm, m.Meetwaarde.Waarde_Numeriek AS celsius,
       m.WaarnemingMetadata.Statuswaarde AS status
FROM m
-- 9999.9 is Rijkswaterstaat's mark for a missing value.
WHERE m.Meetwaarde.Waarde_Numeriek IS NOT NULL AND m.Meetwaarde.Waarde_Numeriek < 9999
  AND m.WaarnemingMetadata.Bemonsteringshoogte::INT > -999999;

CREATE OR REPLACE TABLE swimming_spot AS
WITH f AS (
  SELECT fetched_at, unnest(response) AS s, filename
  FROM read_json('raw/water/swimming_spots/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', response: 'JSON[]'},
                 filename = true, maximum_object_size = 67108864)
)
SELECT s->>'spotid' AS spot_id, s->>'name' AS name, s->>'status' AS status, (s->>'$.coordinates[0]')::DOUBLE AS lat,
       (s->>'$.coordinates[1]')::DOUBLE AS lon, fetched_at AS status_since_check
FROM f
QUALIFY filename = max(filename) OVER ();

CREATE OR REPLACE TABLE swimming_spot_status AS
SELECT DISTINCT ON (s->>'spotid', fetched_at) fetched_at AS checked_at, s->>'spotid' AS spot_id, s->>'name' AS name,
       s->>'status' AS status
FROM read_json('raw/water/swimming_spots/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', response: 'JSON[]'},
               maximum_object_size = 67108864),
     unnest(response) AS t(s);

-- Noise maps ----------------------------------------------------------------------
-- RIVM modelled noise at a location, looked up by hand (raw/noise_map/<location>/...), newest lookup per location.
CREATE OR REPLACE TABLE noise_map AS
SELECT location, lat, lon, l.source, l.metric, l.year, l.db, l.layer, fetched_at::TIMESTAMPTZ AS fetched_at
FROM read_json('raw/noise_map/**/*.json', columns = {fetched_at: 'VARCHAR', location: 'VARCHAR', lat: 'DOUBLE',
               lon: 'DOUBLE', levels: 'STRUCT(layer VARCHAR, source VARCHAR, metric VARCHAR, "year" INT, db INT)[]'},
               filename = true),
     unnest(levels) AS t(l)
QUALIFY filename = max(filename) OVER (PARTITION BY location);

-- MacroFactor -------------------------------------------------------------------
-- Food log exports saved by hand, one row per food entry and nutrient. Each day comes from the newest export
-- that has it, so edits and deletions in a later export win. Times are local.
CREATE OR REPLACE TABLE macrofactor_food AS
WITH f AS (
  SELECT *, row_number() OVER () AS entry
  FROM read_csv('raw/macrofactor/food-log/**/*.csv', header = true, all_varchar = true, union_by_name = true, filename = true)
  QUALIFY filename = max(filename) OVER (PARTITION BY "Date")
)
SELECT entry, strptime("Date" || ' ' || "Time", '%Y-%m-%d %I:%M %p') AT TIME ZONE 'Europe/Amsterdam' AS eaten_at,
       "Food Name" AS food, "Serving Size" AS serving, "Serving Qty"::DOUBLE AS servings,
       "Serving Qty"::DOUBLE * "Serving Weight (g)"::DOUBLE AS grams,
       regexp_extract(nutrient, '^(.*) \(([^)]+)\)$', 1) AS nutrient, regexp_extract(nutrient, '\(([^)]+)\)$', 1) AS unit,
       amount::DOUBLE AS amount
FROM (UNPIVOT f ON COLUMNS(* EXCLUDE ("Date", "Time", "Food Name", "Serving Size", "Serving Qty", "Serving Weight (g)", filename, entry))
      INTO NAME nutrient VALUE amount);

-- One row per set, in the order done. The export has the day but not the time.
CREATE OR REPLACE TABLE macrofactor_set AS
WITH s AS (
  SELECT *, row_number() OVER () AS line
  FROM read_csv('raw/macrofactor/workout-log/**/*.csv', header = true, all_varchar = true, union_by_name = true, filename = true)
  QUALIFY filename = max(filename) OVER (PARTITION BY "Date")
)
SELECT "Date"::DATE AS day, row_number() OVER (PARTITION BY "Date" ORDER BY line) AS set_no,
       "Workout" AS workout, "Workout Duration"::INT AS workout_s, "Exercise" AS exercise,
       "Exercise Base Weight (kg)"::DOUBLE AS base_kg, "Set Type" AS set_type, "Weight (kg)"::DOUBLE AS kg,
       "Reps"::DOUBLE AS reps, "RIR"::INT AS rir, "Duration" AS duration,
       "Distance short (m)"::DOUBLE AS distance_m, "Distance long (km)"::DOUBLE AS distance_km
FROM s;

-- Workbook exports hold the full history, so read only the newest. Daily sheets are a date and one column per
-- metric with its unit in brackets; an empty sheet reads its date as a number, hence the cast through text.
SET VARIABLE mf_workbook = (SELECT max(file) FROM glob('raw/macrofactor/workbook/**/*.xlsx'));

CREATE OR REPLACE MACRO mf_sheet(s) AS TABLE
SELECT s AS sheet, "Date"::VARCHAR::DATE AS day, regexp_replace(name, ' \([^)]+\)$', '') AS metric,
       nullif(regexp_extract(name, '\(([^)]+)\)$', 1), '') AS unit, value::DOUBLE AS value
FROM (UNPIVOT (FROM read_xlsx(getvariable('mf_workbook'), sheet = s)) ON COLUMNS(* EXCLUDE "Date") INTO NAME name VALUE value);

CREATE OR REPLACE TABLE macrofactor_daily AS
FROM mf_sheet('Calories & Macros') UNION ALL FROM mf_sheet('Micronutrients')
UNION ALL FROM mf_sheet('Scale Weight') UNION ALL FROM mf_sheet('Weight Trend') UNION ALL FROM mf_sheet('Body Metrics')
UNION ALL FROM mf_sheet('Expenditure') UNION ALL FROM mf_sheet('Steps')
UNION ALL FROM mf_sheet('Muscle Groups - Sets') UNION ALL FROM mf_sheet('Muscle Groups - Volume')
UNION ALL FROM mf_sheet('Exercises - 1-RM') UNION ALL FROM mf_sheet('Exercises - 3-RM') UNION ALL FROM mf_sheet('Exercises - 10-RM')
UNION ALL FROM mf_sheet('Exercises - Total Volume') UNION ALL FROM mf_sheet('Exercises - Best Set Volume')
UNION ALL FROM mf_sheet('Exercises - Heaviest Weight') UNION ALL FROM mf_sheet('Exercises - Total Reps')
UNION ALL FROM mf_sheet('Exercises - Best Set Reps') UNION ALL FROM mf_sheet('Exercises - Total Sets');

-- Calorie and macro targets per weekday, one set per program update.
CREATE OR REPLACE TABLE macrofactor_program AS
SELECT strptime("Program Update Date", '%m/%d/%Y')::DATE AS updated, "Program Weekday" AS weekday,
       "Calories (kcal)" AS kcal, "Fat (g)" AS fat_g, "Protein (g)" AS protein_g, "Carbs (g)" AS carbs_g,
       "Expenditure (kcal)" AS expenditure_kcal, "Daily Average (kcal)" AS daily_average_kcal, "Weight (kg)" AS weight_kg,
       "Expenditure Calculation Mode" AS expenditure_mode
FROM read_xlsx(getvariable('mf_workbook'), sheet = 'Nutrition Program Settings');

CREATE OR REPLACE TABLE macrofactor_weight_goal AS
SELECT "Goal" AS goal, "Status" AS status, strptime("Start Date", '%m/%d/%Y')::DATE AS start,
       strptime("End Date", '%m/%d/%Y')::DATE AS "end", "Goal Weight (kg)" AS goal_kg, "Goal Rate per Week (%)" AS rate_pct_per_week,
       "Starting Scale Weight (kg)" AS start_kg, "Ending Scale Weight (kg)" AS end_kg
FROM read_xlsx(getvariable('mf_workbook'), sheet = 'Weight Goals');

-- Blood tests -------------------------------------------------------------------
-- Lab reports transcribed by hand from the PDF and checked by hand, one file per report, values as printed.
CREATE OR REPLACE TABLE blood_test AS
SELECT sampled_at::TIMESTAMPTZ AS sampled_at, lab, external_reference AS report, r->>'section' AS section,
       r->>'test' AS test, r->>'method' AS method, (r->>'value')::DOUBLE AS value, r->>'value_text' AS value_text, r->>'unit' AS unit,
       r->>'flag' AS flag, (r->>'ref_low')::DOUBLE AS ref_low, (r->>'ref_high')::DOUBLE AS ref_high,
       r->>'ref_text' AS ref_text
-- Read times as text: auto-detection would drop their offset.
FROM read_json('raw/blood_test/report/**/*.json',
               columns = {lab: 'VARCHAR', external_reference: 'VARCHAR', sampled_at: 'VARCHAR', results: 'JSON[]'}),
     unnest(results) AS t(r);

-- Twitch --------------------------------------------------------------------------
-- Helix lookups of the channels and past broadcasts in browser history, and daily follows and subscriptions.
CREATE OR REPLACE TABLE twitch_channel AS
SELECT DISTINCT ON (u->>'login')
       u->>'id' AS channel_id, u->>'login' AS login, u->>'display_name' AS name, u->>'broadcaster_type' AS broadcaster_type,
       u->>'description' AS description, (u->>'created_at')::TIMESTAMPTZ AS created_at
FROM read_json('raw/twitch/user/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', response: 'STRUCT(data JSON[])'}),
     unnest(response.data) AS t(u)
ORDER BY u->>'login', fetched_at DESC;

CREATE OR REPLACE TABLE twitch_video AS
SELECT DISTINCT ON (v->>'id')
       v->>'id' AS video_id, v->>'user_login' AS channel, v->>'title' AS title, v->>'type' AS type,
       (v->>'created_at')::TIMESTAMPTZ AS created_at,
       coalesce(nullif(regexp_extract(v->>'duration', '(\d+)h', 1), '')::INT, 0) * 3600
         + coalesce(nullif(regexp_extract(v->>'duration', '(\d+)m', 1), '')::INT, 0) * 60
         + coalesce(nullif(regexp_extract(v->>'duration', '(\d+)s', 1), '')::INT, 0) AS duration_s,
       (v->>'view_count')::BIGINT AS views, v->>'language' AS language, v->>'stream_id' AS stream_id
FROM read_json('raw/twitch/video/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', response: 'STRUCT(data JSON[])'}),
     unnest(response.data) AS t(v)
ORDER BY v->>'id', fetched_at DESC;

CREATE OR REPLACE TABLE twitch_watch AS
WITH w AS (
  SELECT ts, browser, origin_or_source,
         nullif(regexp_extract(url, '^[^?#]*twitch\.tv/videos/(\d+)', 1), '') AS video_id,
         CASE WHEN url NOT LIKE '%twitch.tv/videos/%'
              THEN lower(nullif(regexp_extract(url, '^[A-Za-z]+://[^/]+/([A-Za-z0-9_]{3,25})(?:[/?#]|$)', 1), '')) END AS login
  FROM browser_visit
  WHERE host IN ('twitch.tv', 'www.twitch.tv', 'm.twitch.tv')
)
SELECT w.ts, coalesce(v.channel, c.login) AS channel, w.video_id, v.title AS video_title,
       w.video_id IS NULL AS live, w.browser, w.origin_or_source
FROM w
LEFT JOIN twitch_video v USING (video_id)
LEFT JOIN twitch_channel c ON c.login = w.login
WHERE v.video_id IS NOT NULL OR c.login IS NOT NULL;

CREATE OR REPLACE TABLE twitch_follow AS
SELECT f->>'broadcaster_login' AS channel, f->>'broadcaster_name' AS name, (f->>'followed_at')::TIMESTAMPTZ AS followed_at
FROM read_json('raw/twitch/follows/**/*.json', columns = {response: 'STRUCT(data JSON[])'}, filename = true),
     unnest(response.data) AS t(f)
QUALIFY filename = max(filename) OVER ();

CREATE OR REPLACE TABLE twitch_subscription AS
SELECT s->>'broadcaster_login' AS channel, s->>'tier' AS tier, (s->>'is_gift')::BOOLEAN AS is_gift,
       s->>'gifter_login' AS gifter
FROM read_json('raw/twitch/subscriptions/**/*.json', columns = {response: 'STRUCT(data JSON[])'}, filename = true),
     unnest(response.data) AS t(s)
QUALIFY filename = max(filename) OVER ();

-- Audible -------------------------------------------------------------------------
-- Daily pulls from the API of the Audible apps, each kind stored only when changed: the newest file is current.
CREATE OR REPLACE TABLE audible_book AS
SELECT b->>'asin' AS asin, b->>'title' AS title, b->>'subtitle' AS subtitle,
       list_transform(from_json(b->'authors', '[{"name": "VARCHAR"}]'), lambda a: a.name) AS authors,
       list_transform(from_json(b->'narrators', '[{"name": "VARCHAR"}]'), lambda a: a.name) AS narrators,
       b->>'$.series[0].title' AS series, b->>'$.series[0].sequence' AS series_part,
       (b->>'runtime_length_min')::INT AS minutes, (b->>'percent_complete')::DOUBLE AS percent_complete,
       (b->>'is_finished')::BOOLEAN AS finished, (b->>'purchase_date')::TIMESTAMPTZ AS purchased_at,
       (b->>'release_date')::DATE AS released, b->>'language' AS language, b AS raw
FROM read_json('raw/audible/library/**/*.json', columns = {response: 'STRUCT(items JSON[])'}, filename = true),
     unnest(response.items) AS t(b)
QUALIFY filename = max(filename) OVER ();

CREATE OR REPLACE TABLE audible_listening AS
WITH s AS (
  SELECT 'day' AS period, unnest(response.aggregated_daily_listening_stats) AS x, filename
  FROM read_json('raw/audible/stats/**/*.json', filename = true,
                 columns = {response: 'STRUCT(aggregated_daily_listening_stats JSON[], aggregated_monthly_listening_stats JSON[])'})
  UNION ALL
  SELECT 'month', unnest(response.aggregated_monthly_listening_stats), filename
  FROM read_json('raw/audible/stats/**/*.json', filename = true,
                 columns = {response: 'STRUCT(aggregated_daily_listening_stats JSON[], aggregated_monthly_listening_stats JSON[])'})
)
SELECT DISTINCT ON (period, x->>'interval_identifier')
       period, x->>'interval_identifier' AS interval, (x->>'aggregated_sum')::DOUBLE / 60000 AS minutes
FROM s
ORDER BY period, x->>'interval_identifier', filename DESC;

CREATE OR REPLACE TABLE audible_position AS
SELECT p->>'asin' AS asin, (p->>'$.last_position_heard.position_ms')::BIGINT / 60000.0 AS position_min,
       (p->>'$.last_position_heard.last_updated')::TIMESTAMP AS updated_at
FROM read_json('raw/audible/positions/**/*.json', columns = {response: 'STRUCT(asin_last_position_heard_annots JSON[])'},
               filename = true),
     unnest(response.asin_last_position_heard_annots) AS t(p)
QUALIFY filename = max(filename) OVER ();

-- Kindle --------------------------------------------------------------------------
-- Daily pulls from the API of the Kindle apps, each kind stored only when changed: the newest file is current.
CREATE OR REPLACE TABLE kindle_book AS
WITH f AS (SELECT content FROM read_text('raw/kindle/library/**/*.xml') ORDER BY filename DESC LIMIT 1),
i AS (SELECT unnest(regexp_extract_all(content, '(?s)<meta_data>(.*?)</meta_data>', 1)) AS m FROM f)
SELECT regexp_extract(m, '<ASIN>([^<]*)', 1) AS asin,
       replace(replace(replace(regexp_extract(m, '<title[^>]*>([^<]*)', 1), '&amp;', '&'), '&apos;', ''''), '&quot;', '"') AS title,
       list_transform(regexp_extract_all(m, '<author[^>]*>([^<]*)</author>', 1), lambda a: replace(a, '&amp;', '&')) AS authors,
       replace(regexp_extract(m, '<publisher>([^<]*)', 1), '&amp;', '&') AS publisher,
       nullif(regexp_extract(m, '<publication_date>([^<]*)', 1), '')::TIMESTAMPTZ::DATE AS published,
       nullif(regexp_extract(m, '<purchase_date>([^<]*)', 1), '')::TIMESTAMPTZ AS purchased_at,
       regexp_extract(m, '<cde_contenttype>([^<]*)', 1) AS content_type,
       regexp_extract(m, '<origin><type>([^<]*)', 1) AS origin
FROM i;

CREATE OR REPLACE TABLE kindle_annotation AS
WITH f AS (
  SELECT j FROM read_json('raw/kindle/sidecars/**/*.json', format = 'unstructured', records = false, filename = true,
                          maximum_object_size = 67108864) AS t(j)
  QUALIFY filename = max(filename) OVER ()
), b AS (SELECT unnest(map_entries(j::MAP(VARCHAR, JSON))) AS e FROM f),
a AS (SELECT e.key AS asin, unnest(from_json(e.value->'records', '["JSON"]')) AS r FROM b)
SELECT a.asin, k.title, r->>'type' AS type,
       coalesce(r->>'location', r->>'startPosition')::BIGINT AS position, (r->>'endPosition')::BIGINT AS end_position,
       (r->>'creationTime')::TIMESTAMP AS created_at, (r->>'lastModificationTime')::TIMESTAMP AS modified_at, r AS raw
FROM a LEFT JOIN kindle_book AS k ON k.asin = a.asin;

CREATE OR REPLACE TABLE kindle_read_state AS
WITH f AS (
  SELECT datasets FROM read_json('raw/kindle/whispersync/**/*.json', columns = {datasets: 'JSON[]'}, filename = true,
                                 maximum_object_size = 67108864)
  QUALIFY filename = max(filename) OVER ()
), r AS (
  SELECT unnest(map_entries((d->'records')::MAP(VARCHAR, JSON))) AS e
  FROM f, unnest(datasets) AS t(d) WHERE (d->>'name') = 'CurrentStates'
)
SELECT regexp_extract(e.key, 'amzn://([^/]+)/', 1) AS asin, k.title, (e.value->>'value')::JSON->>'state' AS state,
       (e.value->>'value')::JSON->>'origin' AS origin, (e.value->>'lastUpdatedTime')::TIMESTAMPTZ AS marked_at,
       (e.value->>'uploadSource')::JSON->>'$.us.ds.dt' AS device_type
FROM r LEFT JOIN kindle_book AS k ON k.asin = regexp_extract(e.key, 'amzn://([^/]+)/', 1);

CREATE OR REPLACE TABLE kindle_lookup AS
WITH f AS (
  SELECT datasets FROM read_json('raw/kindle/whispersync/**/*.json', columns = {datasets: 'JSON[]'}, filename = true,
                                 maximum_object_size = 67108864)
  QUALIFY filename = max(filename) OVER ()
), r0 AS (
  SELECT d->>'name' AS dataset, unnest(map_entries((d->'records')::MAP(VARCHAR, JSON))).value->>'value' AS raw
  FROM f, unnest(datasets) AS t(d) WHERE (d->>'name') IN ('LOOKUPS', 'WORDS', 'BOOK_INFO')
), r AS (
  -- A few values are a bare word instead of JSON.
  SELECT dataset, raw::JSON AS v FROM r0 WHERE json_valid(raw)
)
SELECT to_timestamp((l.v->>'TSTP')::BIGINT / 1000) AS ts, w.v->>'WORD' AS word, w.v->>'STEM' AS stem,
       l.v->>'USAG' AS sentence, b.v->>'TITL' AS book, b.v->>'AUTHOR' AS author, b.v->>'ASIN' AS asin
FROM r AS l
LEFT JOIN r AS w ON w.dataset = 'WORDS'
                 AND split_part(l.v->>'WKEY', ':', 2) = (w.v->>'WORD') AND split_part(l.v->>'WKEY', ':', 1) = (w.v->>'LANG')
LEFT JOIN r AS b ON b.dataset = 'BOOK_INFO' AND (b.v->>'GUID') = (l.v->>'BKEY')
WHERE l.dataset = 'LOOKUPS';

-- Google --------------------------------------------------------------------------
-- Data Portability exports per account: raw/google/<account>/YYYY/MM/DD/<export end>-<file in the archive>.<ext>.
-- Activity files hold what happened since the previous export, with an hour of overlap; snapshot files are the
-- whole list, stored only when changed, so the newest file per account is current.
CREATE OR REPLACE TABLE google_activity AS
SELECT DISTINCT ON (account, header, time, title)
       time::TIMESTAMPTZ AS ts, regexp_extract(filename, 'raw/google/([^/]+)/', 1) AS account, header AS product,
       split_part(title, ' ', 1) AS action, title, titleUrl AS url,
       subtitles->>'$[0].name' AS subtitle, subtitles->>'$[0].url' AS subtitle_url,
       list_transform(from_json(details, '[{"name": "VARCHAR"}]'), lambda d: d.name) AS details, description,
       locationInfos AS locations, products, activityControls AS activity_controls
FROM read_json('raw/google/*/**/*-my-activity-*-myactivity*.json', format = 'array', filename = true,
               columns = {header: 'VARCHAR', title: 'VARCHAR', titleUrl: 'VARCHAR', subtitles: 'JSON',
                          description: 'VARCHAR', details: 'JSON', locationInfos: 'JSON', time: 'VARCHAR',
                          products: 'VARCHAR[]', activityControls: 'VARCHAR[]'});

CREATE OR REPLACE TABLE youtube_history AS
SELECT ts, account, regexp_extract(url, '[?&]v=([A-Za-z0-9_-]{11})', 1) AS video_id,
       regexp_replace(title, '^Watched ', '') AS title, subtitle AS channel, subtitle_url AS channel_url,
       coalesce(list_contains(details, 'From Google Ads'), false) AS is_ad
FROM google_activity
WHERE product IN ('YouTube', 'YouTube Music') AND action = 'Watched';

CREATE OR REPLACE TABLE youtube_subscription AS
WITH f AS (
  SELECT *, regexp_extract(filename, 'raw/google/([^/]+)/', 1) AS account
  FROM read_csv('raw/google/*/**/*-youtube-and-youtube-music-subscriptions-subscriptions.csv', header = true,
                all_varchar = true, filename = true)
)
SELECT account, "Channel Id" AS channel_id, "Channel Title" AS channel, "Channel Url" AS channel_url
FROM f
QUALIFY filename = max(filename) OVER (PARTITION BY account);

CREATE OR REPLACE TABLE youtube_comment AS
WITH f AS (
  SELECT 'comment' AS kind, "Comment ID" AS id, "Comment Create Timestamp" AS ts, "Video ID" AS video_id,
         "Comment Text" AS text, "Parent Comment ID" AS parent_id, "Post ID" AS post_id, filename
  FROM read_csv('raw/google/*/**/*-youtube-and-youtube-music-comments-comments.csv', header = true,
                all_varchar = true, filename = true)
  UNION ALL BY NAME
  SELECT 'live_chat', "Live Chat ID", "Live Chat Create Timestamp", "Video ID", "Live Chat Text", NULL, "Post ID",
         filename
  FROM read_csv('raw/google/*/**/*-youtube-and-youtube-music-live-chats-live-chats.csv', header = true,
                all_varchar = true, filename = true)
)
-- The text is a list of segments ({"text": ..., "videoLink": ...}) separated by commas, not one JSON value.
SELECT DISTINCT ON (kind, id)
       kind, id, ts::TIMESTAMPTZ AS ts, regexp_extract(filename, 'raw/google/([^/]+)/', 1) AS account, video_id,
       nullif(parent_id, '') AS parent_id, nullif(post_id, '') AS post_id,
       array_to_string(list_transform(from_json('[' || text || ']', '[{"text": "VARCHAR"}]'), lambda s: s.text), '') AS text
FROM f
ORDER BY kind, id, filename DESC;

CREATE OR REPLACE TABLE youtube_playlist_video AS
WITH f AS (
  SELECT *, regexp_extract(filename, 'raw/google/([^/]+)/', 1) AS account,
         regexp_extract(filename, '-youtube-and-youtube-music-playlists-(.+)-videos\.csv$', 1) AS playlist
  FROM read_csv('raw/google/*/**/*-youtube-and-youtube-music-playlists-*-videos.csv', header = true,
                all_varchar = true, filename = true, union_by_name = true)
)
SELECT account, playlist, "Video ID" AS video_id, "Playlist Video Creation Timestamp"::TIMESTAMPTZ AS added_at
FROM f
QUALIFY filename = max(filename) OVER (PARTITION BY account, playlist);

CREATE OR REPLACE TABLE maps_place AS
WITH f AS (
  SELECT 'saved' AS list, unnest(features) AS p, filename
  FROM read_json('raw/google/*/**/*-maps-your-places-saved-places.json', columns = {features: 'JSON[]'}, filename = true)
  UNION ALL
  SELECT 'labeled', unnest(features), filename
  FROM read_json('raw/google/*/**/*-maps-my-labeled-places-labeled-places.json', columns = {features: 'JSON[]'},
                 filename = true)
)
SELECT regexp_extract(filename, 'raw/google/([^/]+)/', 1) AS account, list,
       coalesce(p->>'$.properties.location.name', p->>'$.properties.name') AS name,
       coalesce(p->>'$.properties.location.address', p->>'$.properties.address') AS address,
       (p->>'$.geometry.coordinates[1]')::DOUBLE AS lat, (p->>'$.geometry.coordinates[0]')::DOUBLE AS lon,
       (p->>'$.properties.date')::TIMESTAMPTZ AS saved_at, p->>'$.properties.google_maps_url' AS url
FROM f
QUALIFY filename = max(filename) OVER (PARTITION BY account, list);

-- Gmail per account: raw/gmail/<account>/message/..., each file the messages one run fetched. A message fetched twice
-- (a new grant walks the mailbox again) keeps its newest copy, a whole one over metadata.
CREATE OR REPLACE TABLE gmail_message AS
WITH m AS (
  SELECT regexp_extract(filename, 'raw/gmail/([^/]+)/', 1) AS account, fetched_at, unnest(response) AS msg
  FROM read_json('raw/gmail/*/message/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', response: 'JSON[]'},
                 filename = true)
), h AS (
  SELECT account, fetched_at, msg,
         from_json(msg->'payload'->'headers', '[{"name": "VARCHAR", "value": "VARCHAR"}]') AS headers
  FROM m
), x AS (
  SELECT account, fetched_at, msg->>'id' AS id, msg->>'threadId' AS thread_id,
         to_timestamp((msg->>'internalDate')::BIGINT / 1000) AS ts,
         list_filter(headers, lambda e: lower(e.name) = 'from')[1].value AS "from",
         list_filter(headers, lambda e: lower(e.name) = 'to')[1].value AS "to",
         list_filter(headers, lambda e: lower(e.name) = 'cc')[1].value AS cc,
         list_filter(headers, lambda e: lower(e.name) = 'subject')[1].value AS subject,
         trim(list_filter(headers, lambda e: lower(e.name) = 'list-id')[1].value, '<>') AS list_id,
         list_filter(headers, lambda e: lower(e.name) = 'message-id')[1].value AS message_id,
         from_json(msg->'labelIds', '["VARCHAR"]') AS labels,
         (msg->>'sizeEstimate')::BIGINT AS size_bytes,
         msg->>'$.payload.mimeType' AS mime_type,
         (msg->'payload'->'body') IS NOT NULL OR (msg->'payload'->'parts') IS NOT NULL AS has_body
  FROM h
)
SELECT account, id, thread_id, ts,
       CASE WHEN "from" LIKE '%<%' THEN nullif(trim(regexp_replace("from", '<[^>]*>$', ''), ' "'), '') END AS from_name,
       lower(trim(coalesce(nullif(regexp_extract("from", '<([^>]+)>$', 1), ''), "from"))) AS from_address,
       split_part(lower(trim(coalesce(nullif(regexp_extract("from", '<([^>]+)>$', 1), ''), "from"))), '@', 2) AS from_domain,
       "to", cc, subject, list_id, message_id, labels, size_bytes, mime_type, has_body
FROM x
QUALIFY row_number() OVER (PARTITION BY account, id ORDER BY has_body DESC, fetched_at DESC) = 1;

CREATE OR REPLACE TABLE claude_code_session AS
SELECT session_id, any_value(project) AS project, min(ts) AS start_ts, max(ts) AS end_ts, count(*) AS events,
       count(*) FILTER (WHERE type = 'user') AS user_messages, count(*) FILTER (WHERE type = 'tool_use') AS tool_uses,
       list(DISTINCT model ORDER BY model) FILTER (WHERE model IS NOT NULL) AS models,
       sum(input_tokens) AS input_tokens, sum(output_tokens) AS output_tokens,
       sum(cache_read_tokens) AS cache_read_tokens, sum(cache_creation_tokens) AS cache_creation_tokens
FROM claude_code_event
GROUP BY session_id;

-- Seasonal produce ----------------------------------------------------------------
-- The Voedingscentrum calendar of Dutch fruit and vegetables in season, parsed from its PDF by hand.
CREATE OR REPLACE TABLE seasonal_produce AS
WITH c AS (
  SELECT unnest(map_entries(months)) AS e
  FROM read_json('raw/seasonal_produce/calendar/**/*.json',
                 columns = {months: 'MAP(VARCHAR, STRUCT(vegetables VARCHAR[], fruit VARCHAR[]))'})
), m AS (
  SELECT list_position(['januari', 'februari', 'maart', 'april', 'mei', 'juni', 'juli', 'augustus', 'september',
                        'oktober', 'november', 'december'], lower(e.key)) AS month, e.value AS v FROM c
)
SELECT month, 'vegetable' AS kind, lower(unnest(v.vegetables)) AS name FROM m
UNION ALL
SELECT month, 'fruit' AS kind, lower(unnest(v.fruit)) AS name FROM m;

-- AH receipt lines that are a single fruit or vegetable, matched to the calendar by the till's abbreviated name.
-- Patterns are tried in order; imported ones name produce that never grows here in season.
CREATE OR REPLACE TABLE ah_produce AS
WITH p(prio, pattern, produce, imported) AS (VALUES
  (1, 'snoepgroente|wokgroente|zomerfruit|sw j |jam|yogh|skyr|smoothie|sap|chips|puree|passata|polpa|saus|soep', NULL, NULL),
  (2, 'sinaasappel', 'sinaasappel', true), (2, 'granaatappel', 'granaatappel', true), (2, 'mandarijn', 'mandarijn', true),
  (2, 'banaan|bananen|chiquita', 'banaan', true), (2, 'kiwibes', 'kiwibes', false), (2, 'kiwi', 'kiwi', true),
  (2, 'ananas', 'ananas', true), (2, 'avocado', 'avocado', true), (2, 'mango', 'mango', true),
  (2, 'citroen', 'citroen', true), (2, 'limoen', 'limoen', true), (2, 'gember', 'gember', true),
  (2, 'grapefruit', 'grapefruit', true), (2, 'lychee', 'lychee', true), (2, 'zoete aardappel', 'zoete aardappel', true),
  (3, 'blauwe bes', 'blauwe bes', false), (3, 'rode bes', 'rode bes', false), (3, 'framboz', 'framboos', false),
  (3, 'bramen', 'braam', false), (3, 'druif|druiven', 'druif', false), (3, 'kersen', 'kers', false),
  (3, 'perzik', 'perzik', false), (3, 'nectarin', 'nectarine', false), (3, 'pruimen', 'pruim', false),
  (3, 'abrikoz', 'abrikoos', false), (3, 'aardbei', 'aardbei', false), (3, 'meloen', 'meloen', false),
  (3, 'pink lady|elstar|jonagold|kanzi|granny|braeburn|goudreinet|^ah appels|^appels', 'appel', false),
  (3, 'peren|conference|doyenne', 'peer', false),
  (3, 'radijs', 'radijs', false), (3, 'spinazie', 'spinazie', false), (3, 'prei', 'prei', false),
  (3, 'paprika|papika', 'paprika', false), (3, 'spruit', 'spruiten', false), (3, 'knolselder', 'knolselderij', false),
  (3, 'bleekselder', 'bleekselderij', false), (3, 'boerenkool', 'boerenkool', false), (3, 'pastina', 'pastinaak', false),
  (3, 'uien', 'ui', false), (3, 'champignon|paddenst|oesterzwam', 'paddenstoelen', false),
  (3, 'wortel|winterpeen|peen', 'wortel', false), (3, 'bieten|rode biet', 'rode biet', false),
  (3, 'broccoli', 'broccoli', false), (3, 'bloemkool', 'bloemkool', false), (3, 'courgette', 'courgette', false),
  (3, 'komkommer', 'komkommer', false), (3, 'tomaat|tomaten', 'tomaat', false), (3, 'sperzie', 'sperzieboon', false),
  (3, 'pompoen', 'pompoen', false), (3, 'andijvie', 'andijvie', false), (3, 'aubergine', 'aubergine', false),
  (3, 'venkel', 'venkel', false), (3, 'witlof', 'witlof', false), (3, 'spitskool', 'spitskool', false),
  (3, 'rodekool|rode kool', 'rodekool', false), (3, 'savooi', 'savooikool', false), (3, 'veldsla', 'veldsla', false),
  (3, 'asperge', 'asperge', false), (3, 'peultje', 'peultjes', false), (3, 'doperwt', 'doperwt', false)
), m AS (
  SELECT i.receipt_id, i.ts, i.name AS item, i.amount, p.produce, p.imported
  FROM ah_receipt_item i JOIN p ON regexp_matches(lower(i.name), p.pattern)
  QUALIFY row_number() OVER (PARTITION BY i.receipt_id, i.name, i.amount ORDER BY p.prio) = 1
)
SELECT m.receipt_id, m.ts, m.item, m.amount, m.produce,
       coalesce(any_value(s.kind), CASE WHEN m.produce IN ('paprika', 'tomaat', 'komkommer', 'avocado', 'gember', 'zoete aardappel') THEN 'vegetable' ELSE 'fruit' END) AS kind,
       CASE WHEN m.imported THEN 'imported'
            WHEN bool_or(s.month = month(m.ts AT TIME ZONE 'Europe/Amsterdam')) THEN 'in season'
            ELSE 'out of season' END AS season
FROM m LEFT JOIN seasonal_produce s ON s.name = m.produce
WHERE m.produce IS NOT NULL
GROUP BY m.receipt_id, m.ts, m.item, m.amount, m.produce, m.imported;

-- Moneybird -----------------------------------------------------------------------
-- Each file holds the items the API returned for a batch of new or changed ids; the newest copy of an id wins.
CREATE OR REPLACE TABLE moneybird_sales_invoice AS
SELECT (i->>'id')::BIGINT AS id, (i->>'administration_id')::BIGINT AS administration_id, i->>'invoice_id' AS invoice_number,
       i->>'state' AS state, try_cast(i->>'invoice_date' AS DATE) AS invoice_date, try_cast(i->>'due_date' AS DATE) AS due_date,
       try_cast(i->>'paid_at' AS DATE) AS paid_at, try_cast(i->>'contact_id' AS BIGINT) AS contact_id,
       coalesce(nullif(i->'contact'->>'company_name', ''),
                nullif(trim(concat_ws(' ', i->'contact'->>'firstname', i->'contact'->>'lastname')), '')) AS contact,
       i->>'reference' AS reference, i->>'currency' AS currency,
       try_cast(i->>'total_price_excl_tax' AS DECIMAL(14,2)) AS total_excl_tax,
       try_cast(i->>'total_price_incl_tax' AS DECIMAL(14,2)) AS total_incl_tax,
       try_cast(i->>'total_paid' AS DECIMAL(14,2)) AS total_paid, try_cast(i->>'total_unpaid' AS DECIMAL(14,2)) AS total_unpaid,
       i->'details' AS details, try_cast(i->>'version' AS BIGINT) AS version, try_cast(i->>'updated_at' AS TIMESTAMPTZ) AS updated_at
FROM read_json('raw/moneybird/sales_invoice/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON[]'}),
     unnest(response) AS t(i)
QUALIFY row_number() OVER (PARTITION BY i->>'id' ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE moneybird_purchase_invoice AS
SELECT CASE WHEN filename LIKE '%/receipt/%' THEN 'receipt' ELSE 'purchase_invoice' END AS kind,
       (i->>'id')::BIGINT AS id, (i->>'administration_id')::BIGINT AS administration_id, i->>'reference' AS reference,
       i->>'state' AS state, try_cast(i->>'date' AS DATE) AS date, try_cast(i->>'due_date' AS DATE) AS due_date,
       try_cast(i->>'paid_at' AS DATE) AS paid_at, try_cast(i->>'contact_id' AS BIGINT) AS contact_id,
       coalesce(nullif(i->'contact'->>'company_name', ''),
                nullif(trim(concat_ws(' ', i->'contact'->>'firstname', i->'contact'->>'lastname')), '')) AS contact,
       i->>'currency' AS currency,
       try_cast(i->>'total_price_excl_tax' AS DECIMAL(14,2)) AS total_excl_tax,
       try_cast(i->>'total_price_incl_tax' AS DECIMAL(14,2)) AS total_incl_tax,
       i->'details' AS details, try_cast(i->>'version' AS BIGINT) AS version, try_cast(i->>'updated_at' AS TIMESTAMPTZ) AS updated_at
FROM read_json(['raw/moneybird/purchase_invoice/**/*.json', 'raw/moneybird/receipt/**/*.json'],
               columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON[]'}, filename = true),
     unnest(response) AS t(i)
QUALIFY row_number() OVER (PARTITION BY kind, i->>'id' ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE moneybird_financial_mutation AS
SELECT (m->>'id')::BIGINT AS id, (m->>'administration_id')::BIGINT AS administration_id, try_cast(m->>'date' AS DATE) AS date,
       try_cast(m->>'amount' AS DECIMAL(14,2)) AS amount, m->>'currency' AS currency, m->>'message' AS message,
       m->>'contra_account_name' AS contra_account_name, m->>'contra_account_number' AS contra_account_number,
       m->>'state' AS state, m->>'settlement_state' AS settlement_state, try_cast(m->>'amount_open' AS DECIMAL(14,2)) AS amount_open,
       try_cast(m->>'financial_account_id' AS BIGINT) AS financial_account_id, m->'payments' AS payments,
       m->'ledger_account_bookings' AS ledger_account_bookings, try_cast(m->>'version' AS BIGINT) AS version
FROM read_json('raw/moneybird/financial_mutation/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON[]'}),
     unnest(response) AS t(m)
QUALIFY row_number() OVER (PARTITION BY m->>'id' ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE moneybird_time_entry AS
SELECT (e->>'id')::BIGINT AS id, (e->>'administration_id')::BIGINT AS administration_id,
       try_cast(e->>'started_at' AS TIMESTAMPTZ) AS started_at, try_cast(e->>'ended_at' AS TIMESTAMPTZ) AS ended_at,
       (date_diff('second', try_cast(e->>'started_at' AS TIMESTAMPTZ), try_cast(e->>'ended_at' AS TIMESTAMPTZ))
        - coalesce(try_cast(e->>'paused_duration' AS BIGINT), 0)) / 3600.0 AS hours,
       e->>'description' AS description, try_cast(e->>'billable' AS BOOLEAN) AS billable,
       try_cast(e->>'contact_id' AS BIGINT) AS contact_id,
       coalesce(nullif(e->'contact'->>'company_name', ''),
                nullif(trim(concat_ws(' ', e->'contact'->>'firstname', e->'contact'->>'lastname')), '')) AS contact,
       try_cast(e->>'project_id' AS BIGINT) AS project_id, e->'project'->>'name' AS project,
       try_cast(e->>'sales_invoice_id' AS BIGINT) AS sales_invoice_id, try_cast(e->>'updated_at' AS TIMESTAMPTZ) AS updated_at
FROM read_json('raw/moneybird/time_entry/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON[]'}),
     unnest(response) AS t(e)
QUALIFY row_number() OVER (PARTITION BY e->>'id' ORDER BY fetched_at DESC) = 1;

CREATE OR REPLACE TABLE moneybird_contact AS
SELECT (c->>'id')::BIGINT AS id, (c->>'administration_id')::BIGINT AS administration_id, c->>'customer_id' AS customer_id,
       nullif(c->>'company_name', '') AS company_name, nullif(c->>'firstname', '') AS firstname, nullif(c->>'lastname', '') AS lastname,
       c->>'city' AS city, c->>'country' AS country, c->>'email' AS email, try_cast(c->>'archived' AS BOOLEAN) AS archived,
       try_cast(c->>'version' AS BIGINT) AS version, try_cast(c->>'updated_at' AS TIMESTAMPTZ) AS updated_at
FROM read_json('raw/moneybird/contact/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON[]'}),
     unnest(response) AS t(c)
QUALIFY row_number() OVER (PARTITION BY c->>'id' ORDER BY fetched_at DESC) = 1;

-- Analysis ------------------------------------------------------------------------
-- One row per Apple Watch night with what happened the day before, for questions about sleep.
CREATE OR REPLACE TABLE sleep_night AS
WITH n AS (
  SELECT (sleep_end AT TIME ZONE 'Europe/Amsterdam')::DATE AS wake_date, sleep_start, sleep_end,
         asleep_h, in_bed_h, core_h, deep_h, rem_h, awake_h,
         (date_part('hour', sleep_start AT TIME ZONE 'Europe/Amsterdam')
          + date_part('minute', sleep_start AT TIME ZONE 'Europe/Amsterdam') / 60 + 12) % 24 + 12 AS bedtime_h,
         date_part('hour', sleep_end AT TIME ZONE 'Europe/Amsterdam')
          + date_part('minute', sleep_end AT TIME ZONE 'Europe/Amsterdam') / 60 AS wake_h
  FROM apple_sleep_night WHERE source LIKE '%Watch' AND asleep_h BETWEEN 3 AND 13
  QUALIFY row_number() OVER (PARTITION BY wake_date ORDER BY asleep_h DESC) = 1
), day AS (
  SELECT (ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS d,
         sum(qty) FILTER (WHERE name = 'step_count') AS steps,
         sum(qty) FILTER (WHERE name = 'apple_exercise_time') AS exercise_min,
         sum(qty) FILTER (WHERE name = 'time_in_daylight') AS daylight_min
  FROM apple_sample WHERE name IN ('step_count', 'apple_exercise_time', 'time_in_daylight') GROUP BY 1
), hrv AS (
  SELECT n.wake_date, avg(s.qty) AS hrv_ms FROM n JOIN apple_sample s
    ON s.name = 'heart_rate_variability' AND s.ts BETWEEN n.sleep_start AND n.sleep_end GROUP BY 1
), temp AS (
  SELECT n.wake_date, avg(k.temp_c) AS night_temp_c FROM n JOIN knmi_hourly k
    ON k.station = 240 AND k.ts BETWEEN n.sleep_start AND n.sleep_end GROUP BY 1
), yt AS (
  SELECT n.wake_date, count(y.ts) AS youtube_2h_before FROM n LEFT JOIN youtube_history y
    ON y.ts BETWEEN n.sleep_start - INTERVAL 2 HOUR AND n.sleep_start AND NOT y.is_ad GROUP BY 1
), wo AS (
  SELECT n.wake_date, count(w.id) AS workouts,
         count(w.id) FILTER (WHERE w.end_ts > n.sleep_start - INTERVAL 3 HOUR) AS workouts_3h_before
  FROM n LEFT JOIN apple_workout w ON w.start_ts BETWEEN n.sleep_start - INTERVAL 18 HOUR AND n.sleep_start GROUP BY 1
)
SELECT n.*, asleep_h / nullif(in_bed_h, 0) AS efficiency, dayofweek(n.wake_date - 1) IN (5, 6) AS weekend_night,
       day.steps, day.exercise_min, day.daylight_min, hrv.hrv_ms, temp.night_temp_c, yt.youtube_2h_before,
       wo.workouts, wo.workouts_3h_before
FROM n LEFT JOIN day ON day.d = n.wake_date - 1 LEFT JOIN hrv USING (wake_date) LEFT JOIN temp USING (wake_date)
       LEFT JOIN yt USING (wake_date) LEFT JOIN wo USING (wake_date)
ORDER BY wake_date;

-- Great-circle distance in km from home: build.py sets home_lat and home_lon from HOME_LAT and HOME_LON.
CREATE OR REPLACE MACRO km_from_home(lat, lon) AS
  2 * 6371 * asin(sqrt(pow(sin(radians(lat - getvariable('home_lat')) / 2), 2)
                     + cos(radians(getvariable('home_lat'))) * cos(radians(lat))
                       * pow(sin(radians(lon - getvariable('home_lon')) / 2), 2)));

-- Days the owner was away from Amsterdam, from every source that places him: GPS of photos, workouts, the phone and the
-- weather fetches (over 40 km from home), in-person card purchases outside the Amsterdam area or in a foreign
-- currency, the time zone Apple Photos recorded, and the time zone offset of his git commits.
CREATE OR REPLACE TABLE away_day AS
WITH card AS (
  SELECT (ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, 'card' AS signal, city AS place,
         nullif(local_currency, 'EUR') AS currency, NULL::DOUBLE AS lat, NULL::DOUBLE AS lon,
         local_currency <> 'EUR' AS abroad
  FROM bunq_card_action
  WHERE status IN ('AUTHORISED', 'STAND_IN_AUTHORISED', 'PRE_AUTHORISATION_FINALISED', 'UNAUTHORISED_CLEARING')
    AND entry_mode IN ('ICC', 'UNSAFE_CHIP', 'UNSAFE_MAGNETIC_STRIPE', 'ATM')
    AND (local_currency <> 'EUR'
         OR NOT regexp_matches(lower(coalesce(city, '')), 'amsterdam|amstelveen|oud-zuid|diemen|duivendrecht|schiphol|ovpay|^$'))
), gps AS (
  SELECT (taken_at AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, 'photo' AS signal, lat, lon FROM photo
  WHERE lat IS NOT NULL AND NOT screenshot AND NOT trashed
  UNION ALL
  SELECT (w.start_ts AT TIME ZONE 'Europe/Amsterdam')::DATE, 'workout', r.lat, r.lon
  FROM apple_workout w JOIN apple_workout_route r ON r.workout_id = w.id
  QUALIFY row_number() OVER (PARTITION BY w.id ORDER BY r.ts) = 1
  UNION ALL
  SELECT (ts AT TIME ZONE 'Europe/Amsterdam')::DATE, 'phone', lat, lon FROM location
  UNION ALL
  SELECT (hour AT TIME ZONE 'Europe/Amsterdam')::DATE, 'weather', lat, lon FROM weather_hour WHERE NOT from_archive
), far AS (
  SELECT day, signal, NULL::VARCHAR AS place, NULL::VARCHAR AS currency, lat, lon,
         NOT (lat BETWEEN 50.7 AND 53.6 AND lon BETWEEN 3.3 AND 7.3) AS abroad
  FROM gps WHERE km_from_home(lat, lon) > 40
), photo_tz AS (
  -- Photos sets the zone from the location. Amsterdam's own zones and the bogus Pacific default of old imports say nothing.
  SELECT (taken_at AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, 'photo_tz' AS signal, timezone AS place,
         NULL::VARCHAR AS currency, NULL::DOUBLE AS lat, NULL::DOUBLE AS lon, true AS abroad
  FROM photo
  WHERE NOT screenshot AND NOT trashed AND timezone IS NOT NULL
    AND timezone NOT IN ('Europe/Amsterdam', 'Europe/Brussels', 'Europe/Berlin', 'Europe/Paris', 'GMT', 'GMT+0100', 'GMT+0200',
                         'America/Los_Angeles', 'US/Pacific')
), commit AS (
  -- A commit offset other than Amsterdam's offset at that moment. Z is git's default for CI and the web editor, not a place.
  SELECT (authored_at AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, 'commit' AS signal, 'UTC' || author_offset AS place,
         NULL::VARCHAR AS currency, NULL::DOUBLE AS lat, NULL::DOUBLE AS lon, true AS abroad
  FROM github_commit
  WHERE author_offset <> 'Z'
    AND author_offset <> '+0' || date_diff('hour', authored_at AT TIME ZONE 'UTC', authored_at AT TIME ZONE 'Europe/Amsterdam') || ':00'
), signals AS (
  SELECT * FROM card UNION ALL SELECT * FROM far UNION ALL SELECT * FROM photo_tz UNION ALL SELECT * FROM commit
)
SELECT day, bool_or(abroad) AS abroad, list_sort(list(DISTINCT signal)) AS signals, count(*) AS evidence,
       string_agg(DISTINCT place, ', ' ORDER BY place) AS places,
       string_agg(DISTINCT currency, ', ' ORDER BY currency) AS currencies,
       median(lat) AS lat, median(lon) AS lon, round(max(km_from_home(lat, lon))) AS max_km
FROM signals
GROUP BY day
ORDER BY day;

-- Consecutive away days, bridging gaps of up to two days without evidence, as one trip.
CREATE OR REPLACE TABLE trip AS
WITH g AS (
  SELECT *, coalesce(day - lag(day) OVER (ORDER BY day) > 3, true) AS starts FROM away_day
), t AS (
  SELECT *, sum(starts::INT) OVER (ORDER BY day) AS trip_id FROM g
), spent AS (
  SELECT t.trip_id, sum(p.amount) AS spent_eur
  FROM t JOIN card_purchase p ON (p.ts AT TIME ZONE 'Europe/Amsterdam')::DATE = t.day AND p.amount > 0
  GROUP BY 1
), photos AS (
  SELECT t.trip_id, count(*) AS photos
  FROM t JOIN photo p ON (p.taken_at AT TIME ZONE 'Europe/Amsterdam')::DATE = t.day AND NOT p.screenshot AND NOT p.trashed
  GROUP BY 1
)
SELECT t.trip_id, min(day) AS start_day, max(day) AS end_day, max(day) - min(day) + 1 AS days, count(*) AS days_with_evidence,
       bool_or(abroad) AS abroad,
       array_to_string(list_sort(list_distinct(flatten(list(string_split(places, ', '))))), ', ') AS places,
       array_to_string(list_sort(list_distinct(flatten(list(string_split(currencies, ', '))))), ', ') AS currencies,
       list_sort(list_distinct(flatten(list(signals)))) AS signals,
       median(lat) AS lat, median(lon) AS lon, max(max_km) AS max_km, any_value(spent.spent_eur) AS spent_eur,
       coalesce(any_value(photos.photos), 0) AS photos
FROM t LEFT JOIN spent USING (trip_id) LEFT JOIN photos USING (trip_id)
GROUP BY t.trip_id
ORDER BY start_day;

-- One row per local day with the day's totals from every source, for correlations and dashboards.
CREATE OR REPLACE TABLE day AS
WITH span AS (
  SELECT unnest(generate_series(DATE '2019-01-01', current_date, INTERVAL 1 DAY))::DATE AS day
), sample AS (
  SELECT (ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day,
         sum(qty) FILTER (WHERE name = 'step_count') AS steps,
         sum(qty) FILTER (WHERE name = 'walking_running_distance') AS walk_km,
         sum(qty) FILTER (WHERE name = 'cycling_distance') AS cycle_km,
         sum(qty) FILTER (WHERE name = 'apple_exercise_time') AS exercise_min,
         sum(qty) FILTER (WHERE name = 'time_in_daylight') AS daylight_min,
         sum(qty) FILTER (WHERE name = 'active_energy') / 4.184 AS active_kcal,
         sum(qty) FILTER (WHERE name = 'flights_climbed') AS floors,
         avg(qty) FILTER (WHERE name = 'resting_heart_rate') AS resting_hr,
         avg(qty) FILTER (WHERE name = 'heart_rate_variability') AS hrv_ms,
         avg(qty) FILTER (WHERE name = 'vo2_max') AS vo2_max,
         avg(qty) FILTER (WHERE name = 'weight_body_mass') AS apple_weight_kg,
         sum(qty) FILTER (WHERE name = 'dietary_energy') / 4.184 AS food_kcal,
         sum(qty) FILTER (WHERE name = 'protein') AS protein_g,
         sum(qty) FILTER (WHERE name = 'carbohydrates') AS carbs_g,
         sum(qty) FILTER (WHERE name = 'total_fat') AS fat_g,
         sum(qty) FILTER (WHERE name = 'caffeine') AS caffeine_mg
  FROM apple_sample
  WHERE name IN ('step_count', 'walking_running_distance', 'cycling_distance', 'apple_exercise_time', 'time_in_daylight',
                 'active_energy', 'flights_climbed', 'resting_heart_rate', 'heart_rate_variability', 'vo2_max',
                 'weight_body_mass', 'dietary_energy', 'protein', 'carbohydrates', 'total_fat', 'caffeine')
  GROUP BY 1
), body AS (
  SELECT (ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, avg(weight_kg) AS weight_kg, avg(fat_pct) AS fat_pct
  FROM withings_body GROUP BY 1
), workout AS (
  SELECT (start_ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, count(*) AS workouts, sum(minutes) AS workout_min,
         string_agg(DISTINCT name, ', ' ORDER BY name) AS workout_types
  FROM apple_workout GROUP BY 1
), mood AS (
  SELECT (ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, avg(valence) AS mood_valence, count(*) AS mood_entries
  FROM apple_state_of_mind GROUP BY 1
), screen AS (
  SELECT (start_ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day,
         sum(seconds) FILTER (WHERE platform = 'macos') / 3600 AS mac_h,
         sum(seconds) FILTER (WHERE platform = 'ios') / 3600 AS iphone_h,
         sum(seconds) FILTER (WHERE platform = 'ipados') / 3600 AS ipad_h
  FROM screen_time_app WHERE NOT capped AND bundle NOT LIKE 'com.apple.carousel.%' GROUP BY 1
), yt AS (
  SELECT (ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, count(*) AS youtube_videos FROM youtube_history WHERE NOT is_ad GROUP BY 1
), steam AS (
  SELECT (hour AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, sum(minutes) AS steam_min FROM steam_playtime_hour GROUP BY 1
), switch AS (
  SELECT day, minutes AS switch_min FROM switch_day
), media AS (
  SELECT (start_ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day,
         sum(played_s) FILTER (WHERE media_type = 'Podcast') / 60 AS podcast_min,
         sum(played_s) FILTER (WHERE media_type = 'Music') / 60 AS music_min
  FROM media_play GROUP BY 1
), commits AS (
  SELECT (authored_at AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, count(*) AS commits FROM github_commit GROUP BY 1
), claude AS (
  SELECT (ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, count(DISTINCT session_id) AS claude_sessions,
         count(*) FILTER (WHERE type = 'user') AS claude_prompts
  FROM claude_code_event WHERE NOT coalesce(sidechain, false) GROUP BY 1
), money AS (
  SELECT (ts AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, count(*) AS card_purchases, sum(amount) AS spent_eur,
         sum(amount) FILTER (WHERE category = 'Groceries') AS groceries_eur,
         sum(amount) FILTER (WHERE category = 'Eating & drinking') AS eating_out_eur
  FROM card_purchase WHERE amount > 0 GROUP BY 1
), photos AS (
  SELECT (taken_at AT TIME ZONE 'Europe/Amsterdam')::DATE AS day, count(*) AS photos
  FROM photo WHERE NOT screenshot AND NOT trashed GROUP BY 1
), weather AS (
  SELECT day, temp_mean_c, temp_min_c, temp_max_c, rain_mm, sunshine_h, wind_mean_ms FROM knmi_daily WHERE station = 240
), home_rain AS (
  SELECT day, value AS home_rain_mm FROM knmi_grid_home WHERE grid = 'Rd1' AND parameter = 'precipitation'
), air AS (
  SELECT (hour AT TIME ZONE 'Europe/Amsterdam')::DATE AS day,
         avg(value) FILTER (WHERE component = 'PM25') AS pm25, avg(value) FILTER (WHERE component = 'NO2') AS no2
  FROM air_quality_measured_hour WHERE station = 'NL49017' GROUP BY 1
), away AS (
  SELECT a.day, true AS away, a.abroad, t.trip_id, a.places
  FROM away_day a JOIN trip t ON a.day BETWEEN t.start_day AND t.end_day
)
SELECT span.day, dayname(span.day) AS weekday, dayofweek(span.day) IN (0, 6) AS weekend,
       n.asleep_h, n.bedtime_h, n.wake_h, n.efficiency AS sleep_efficiency,
       coalesce(body.weight_kg, sample.apple_weight_kg) AS weight_kg, body.fat_pct,
       sample.steps, sample.walk_km, sample.cycle_km, sample.exercise_min, sample.daylight_min, sample.active_kcal, sample.floors,
       sample.resting_hr, sample.hrv_ms, sample.vo2_max,
       workout.workouts, workout.workout_min, workout.workout_types,
       mood.mood_valence, mood.mood_entries,
       sample.food_kcal, sample.protein_g, sample.carbs_g, sample.fat_g, sample.caffeine_mg,
       screen.mac_h, screen.iphone_h, screen.ipad_h, yt.youtube_videos, steam.steam_min, switch.switch_min, media.podcast_min, media.music_min,
       commits.commits, claude.claude_sessions, claude.claude_prompts,
       money.card_purchases, money.spent_eur, money.groceries_eur, money.eating_out_eur,
       photos.photos, coalesce(away.away, false) AS away, coalesce(away.abroad, false) AS abroad, away.trip_id, away.places,
       weather.temp_mean_c, weather.temp_min_c, weather.temp_max_c, weather.rain_mm, home_rain.home_rain_mm, weather.sunshine_h,
       weather.wind_mean_ms, air.pm25, air.no2
FROM span
LEFT JOIN sleep_night n ON n.wake_date = span.day
LEFT JOIN sample USING (day) LEFT JOIN body USING (day) LEFT JOIN workout USING (day) LEFT JOIN mood USING (day)
LEFT JOIN screen USING (day) LEFT JOIN yt USING (day) LEFT JOIN steam USING (day) LEFT JOIN switch USING (day) LEFT JOIN media USING (day)
LEFT JOIN commits USING (day) LEFT JOIN claude USING (day) LEFT JOIN money USING (day) LEFT JOIN photos USING (day)
LEFT JOIN weather USING (day) LEFT JOIN home_rain USING (day) LEFT JOIN air USING (day) LEFT JOIN away USING (day)
ORDER BY span.day;

-- Offers --------------------------------------------------------------------------
-- Each file holds the offers a store listed that the collector had not seen before, so an offer is stored once.
CREATE OR REPLACE TABLE offer AS
WITH u AS (
  SELECT 'prijsprofeet' AS source, o->>'retailer' AS store, o->>'product_id' AS id, o->>'name' AS name, o->>'brand' AS brand,
         try_cast(o->>'price' AS DECIMAL(10,2)) AS price, try_cast(o->>'original_price' AS DECIMAL(10,2)) AS original_price,
         o->>'promotion_type' AS mechanic, o->'promotional_keywords'->>0 AS promo,
         try_cast(o->>'valid_from' AS DATE) AS valid_from, try_cast(o->>'valid_until' AS DATE) AS valid_until,
         o->>'unified_category' AS category, o->>'nutriscore' AS nutriscore, o->>'ean' AS ean, o->>'product_url' AS url,
         NULL::VARCHAR AS quality, fetched_at AS first_seen
  FROM read_json('raw/offers/prijsprofeet/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON[]'}),
       unnest(response) AS t(o)
  UNION ALL
  SELECT 'folderz', o->>'store', o->>'id', o->>'name', NULL, try_cast(o->>'price' AS DECIMAL(10,2)),
         try_cast(o->>'original_price' AS DECIMAL(10,2)), NULL, NULL, (fetched_at AT TIME ZONE 'Europe/Amsterdam')::DATE,
         try_cast(o->>'valid_until' AS DATE), NULL, NULL, NULL, o->>'url', o->>'quality', fetched_at
  FROM read_json('raw/offers/folderz/**/*.json', columns = {fetched_at: 'TIMESTAMPTZ', request: 'JSON', response: 'JSON[]'}),
       unnest(response) AS t(o)
)
SELECT * FROM u
QUALIFY row_number() OVER (PARTITION BY source, id ORDER BY first_seen) = 1;

-- The hand-kept watch list the collector alerts on; the same file drives the notifications.
CREATE OR REPLACE TABLE watch AS
SELECT label, pattern, exclude, stores, till
FROM read_json('duckdb/watchlist.json',
               columns = {label: 'VARCHAR', pattern: 'VARCHAR', exclude: 'VARCHAR', stores: 'VARCHAR[]', till: 'VARCHAR'});

-- Offers on watched items, with what the owner usually pays for them at AH, from the last year of receipts.
CREATE OR REPLACE TABLE watched_offer AS
WITH usual AS (
  SELECT w.label, round(avg(i.price), 2) AS usual_price, count(DISTINCT i.receipt_id) AS receipts
  FROM watch w JOIN ah_receipt_item i ON w.till IS NOT NULL AND regexp_matches(i.name, w.till)
  WHERE i.ts >= now() - INTERVAL 365 DAY AND i.price IS NOT NULL
  GROUP BY w.label
)
SELECT w.label, o.store, o.name, o.price, o.original_price, u.usual_price, u.receipts,
       round(100 * (1 - o.price / coalesce(o.original_price, u.usual_price))) AS saving_pct,
       o.mechanic, o.promo, o.quality, o.valid_from, o.valid_until, o.first_seen, o.url, o.source, o.id
FROM offer o
JOIN watch w ON regexp_matches(o.name, w.pattern, 'i') AND (w.stores IS NULL OR list_contains(w.stores, o.store))
            AND (w.exclude IS NULL OR NOT regexp_matches(o.name, w.exclude, 'i'))
LEFT JOIN usual u USING (label)
QUALIFY row_number() OVER (PARTITION BY o.source, o.id ORDER BY w.label) = 1;

-- Descriptions --------------------------------------------------------------------
-- The MCP server's `tables` tool shows these, so they are the documentation Claude sees.

COMMENT ON TABLE build_info IS 'When these tables were last built from raw/.';
COMMENT ON TABLE seasonal_produce IS 'Dutch fruit and vegetables in season per month (1-12), from the Voedingscentrum seasonal calendar (based on Velt), which Milieu Centraal links to: one row per month, kind (vegetable or fruit) and Dutch name, lowercase. In season means grown outdoors or in an unheated greenhouse in the Netherlands, or stored without much energy (apples and pears all year). Produce never listed (bananas, citrus, avocado) never grows here in season.';
COMMENT ON TABLE offer IS 'Supermarket and drugstore offers, one row per offer: source prijsprofeet (Albert Heijn, Dirk and other supermarkets from the PrijsProfeet API, with brand, EAN, category, Nutri-Score and the mechanic such as one_plus_one) or folderz (Kruidvat, Etos and other stores from folderz.nl list pages, with folderz''s quality rating excellent, good, average, poor or none). Prices in EUR; price is the offer price, null for a 1+1 without a price. valid_from and valid_until bound the offer; PrijsProfeet lists next week''s offers a week early (valid_from after first_seen), folderz validity is the days left on the card. first_seen is when the collector first stored it, which for the first pull is not when the offer started. History starts 6 November 2026.';
COMMENT ON TABLE watch IS 'The hand-kept list in duckdb/watchlist.json of items the owner wants to know about when on offer: pattern is a case-insensitive regex over the offer name, exclude an optional regex that rules a matching offer out (kids'' vitamins), stores the store slugs it applies to (null is every store), till a regex over the Albert Heijn till name in ah_receipt_item for the usual price. The collector reads the same file and sends a Home Assistant notification when a new offer matches.';
COMMENT ON TABLE watched_offer IS 'Offers that match the watch list, one row per offer: usual_price is the average unit price the owner paid for the item at Albert Heijn in the last year (receipts is over how many receipts), saving_pct the discount against the original price or, without one, the usual price. Live offers are those with valid_until >= today; use first_seen to see when they came in.';
COMMENT ON TABLE ah_produce IS 'Albert Heijn receipt lines that are one fruit or vegetable, matched by the till''s abbreviated name to a produce name (Dutch, as in seasonal_produce): season is in season (on the calendar in the month bought), out of season, or imported (never in season in the Netherlands, e.g. banaan, mandarijn, kiwi). Mixes (snoepgroente, wokgroente) and processed products are left out, as are potatoes, which the calendar does not list.';
COMMENT ON TABLE sleep_night IS 'One row per night from the Apple Watch (3 to 13 hours asleep), keyed by wake_date (Amsterdam), with the day before: bedtime_h and wake_h are Amsterdam clock hours when sleep started and ended (bedtime past midnight is above 24, so 24.5 is 00:30), stages in hours, efficiency asleep/in bed, weekend_night for Friday and Saturday nights, that day''s steps, exercise and daylight minutes, average HRV while asleep (ms), KNMI Schiphol temperature while asleep, YouTube videos started in the 2 hours before sleep, and workouts in the 18 hours before (workouts_3h_before: ending within 3 hours of sleep). Start here for sleep questions; bedtime is by far the strongest factor in how long the owner sleeps.';
COMMENT ON TABLE withings_body IS 'One Withings scale weigh-in per row: weight, fat, muscle, hydration, bone, visceral fat.';
COMMENT ON TABLE withings_segmental IS 'Segmental muscle and fat per body part (position) per weigh-in.';
COMMENT ON TABLE withings_measure IS 'Every Withings measure in long form: type is the Withings measure type id, value already scaled.';
COMMENT ON TABLE withings_blood_pressure IS 'One Withings blood pressure reading per row.';
COMMENT ON TABLE withings_heart IS 'Withings heart readings (ScanWatch/BPM): heart rate, blood pressure, AFib result, ECG signal id.';
COMMENT ON TABLE withings_ecg IS 'Raw Withings ECG signals, one per recording.';
COMMENT ON TABLE withings_sleep_night IS 'One night per row from the Withings Sleep Analyzer under the mattress (wake_date is the morning): stages in minutes, HR, breathing, AHI, snoring, score.';
COMMENT ON TABLE withings_sleep_stage IS 'Withings sleep stage intervals: state 0 awake, 1 light, 2 deep, 3 REM.';
COMMENT ON TABLE withings_sleep_minute IS 'Per-minute heart rate, breathing rate, HRV, snoring and movement in bed from the Sleep Analyzer.';
COMMENT ON TABLE withings_activity_day IS 'Withings daily activity per device: steps, distance, active time by intensity, calories, HR zones.';
COMMENT ON TABLE withings_intraday IS 'Withings intraday samples: heart rate, steps, calories, HRV, SpO2.';
COMMENT ON TABLE withings_workout IS 'Workouts recorded by Withings devices.';
COMMENT ON TABLE apple_sample IS 'Every Apple Health metric except heart rate, sleep and blood pressure, in long form (name, units, ts, qty). An empty source means Apple Health already merged the devices (most metrics, e.g. step_count): sum as is. Otherwise source names a device or app, e.g. the watch''s name, which holds a curly apostrophe (match with ILIKE), and a metric with several sources needs one picked before summing.';
COMMENT ON TABLE apple_heart_rate IS 'Apple Health heart rate samples, mostly from the Apple Watch.';
COMMENT ON TABLE apple_sleep_night IS 'Apple Health sleep per night and source, hours per stage.';
COMMENT ON TABLE apple_blood_pressure IS 'Apple Health blood pressure readings.';
COMMENT ON TABLE apple_workout IS 'Apple Health workouts with distance, energy (kJ), heart rate and number of route points.';
COMMENT ON TABLE apple_workout_route IS 'GPS points of Apple Health workouts; join on apple_workout.id = workout_id.';
COMMENT ON TABLE apple_ecg IS 'Apple Watch ECG recordings with classification and voltages.';
COMMENT ON TABLE apple_state_of_mind IS 'Apple Health State of Mind entries: valence -1 to 1, labels, associations. Mostly logged in 2024.';
COMMENT ON TABLE apple_medication IS 'Apple Health medication doses: scheduled time and status.';
COMMENT ON TABLE ha_history IS 'Every Home Assistant state change; value is the state as a number when numeric. About 10 days back from when collection started.';
COMMENT ON TABLE ha_logbook IS 'Home Assistant logbook entries.';
COMMENT ON TABLE ha_statistics IS 'Home Assistant hourly long-term statistics per statistic_id (usually an entity id): mean/min/max, or state/sum for counters such as energy. Goes back further than ha_history.';
COMMENT ON TABLE ha_entity IS 'Home Assistant entity registry; join ha_history.entity_id here, then to ha_device and ha_area for names and rooms.';
COMMENT ON TABLE ha_device IS 'Home Assistant device registry.';
COMMENT ON TABLE ha_area IS 'Home Assistant areas (rooms).';
COMMENT ON TABLE wifi_client IS 'Every wifi and wired client the UniFi controller knew, one row per client per hourly push (ts): band (radio ng is 2.4 GHz, na 5 GHz), channel, signal_dbm, link rates in kbit/s, byte counters since association, uptime. Name the HomePods, phones and IoT devices here; join dns_query on client_ip = ip for what they talked to.';
COMMENT ON TABLE wifi_radio IS 'The access point''s two radios per hourly push: channel, width, tx power and channel utilisation (cu_total percent of air time busy, cu_self_tx and cu_self_rx the AP''s own share). High cu_total with low self share means the neighbours are the load.';
COMMENT ON TABLE wifi_ap IS 'The access point per hourly push: firmware, uptime, CPU and memory, uplink speed and byte counters.';
COMMENT ON TABLE wifi_network IS 'Each wifi network (Beep on 5 GHz, Beep_2G on 2.4 GHz) per hourly push: clients, bytes moved since the AP booted, retries, errors and the average client signal.';
COMMENT ON TABLE wifi_neighbour_hour IS 'Neighbouring networks the AP hears, per hour, channel and width: how many and how strong (rssi above the noise floor). The congestion picture around the flat over time.';
COMMENT ON TABLE wifi_neighbour IS 'Every neighbouring network the AP heard, once a day: BSSID, SSID, channel, width, signal and security.';
COMMENT ON TABLE dns_query IS 'DNS queries from AdGuard Home on the Pi, counted per hour ([from_ts, to_ts)), client and domain: what each device resolved, so which services it used, when. reason starting with Filtered means blocked. The Mac (client_name MacBookPro) makes most queries. Aggregate by registrable domain (the last two labels) for a readable picture.';
COMMENT ON TABLE switch_day IS 'Minutes the Nintendo Switch was played per Amsterdam day, from the Nintendo parental controls sensor in Home Assistant (since November 2026): the sensor counts the current day and starts over at midnight, so minutes is the highest value seen that day and last_ts when it was last reported. Games played are not known; Steam has steam_playtime_hour for the PC side.';
COMMENT ON TABLE location IS 'GPS fixes of the owner''s phone from every Home Assistant GPS tracker, named in tracker (the companion app has its own; an OwnTracks tracker reports far more often once set up): lat, lon, accuracy_m, altitude, speed (m/s) and course when the phone sent them, and zone (home, not_home or a zone name). Only since collection started (October 2026), and sparse before OwnTracks: the companion app records a fix only when the position changes significantly, so a gap means the owner stayed put or the phone did not report. Workout routes are in apple_workout_route.';
COMMENT ON TABLE weather_hour IS 'Hourly Open-Meteo weather at the phone''s location since October 2026. Earlier hours (from_archive) are an ERA5 backfill for home in Amsterdam, so they do not follow trips; check lat/lon.';
COMMENT ON TABLE weather_current IS 'Open-Meteo current conditions at each hourly fetch.';
COMMENT ON TABLE weather_day IS 'Daily Open-Meteo weather by UTC day: sunrise, sunset, daylight, temperature range, precipitation, UV.';
COMMENT ON TABLE weather_air_quality_hour IS 'Hourly Open-Meteo air quality (µg/m³) and pollen (grains/m³); pollen is null outside the season and before 2021.';
COMMENT ON TABLE air_quality_measured_hour IS 'Hourly air quality measured at Luchtmeetnet (RIVM) stations since 2019, long form: component is NO2, NO, PM10, PM25, FN (soot), C6H6, C7H8, C8H10 (benzene, toluene, xylene), O3 where measured, or LKI (Dutch air quality index, 1 good to 11 very bad). hour is the start of the hour the value averages. NL49017 Amsterdam-Stadhouderskade is a traffic station and has no O3. Recent hours are unvalidated and not fetched again once revised. weather_air_quality_hour has the modelled values at the phone''s position.';
COMMENT ON TABLE air_quality_station IS 'Luchtmeetnet stations: name, type (traffic, urban background), position and the components measured.';
COMMENT ON TABLE bunq_account IS 'Daily balance snapshot per bunq account (personal and business).';
COMMENT ON TABLE bunq_payment IS 'All bunq payments, deduplicated across the personal and business users. Negative amount is money out. A card purchase is a MASTERCARD payment once settled.';
COMMENT ON TABLE card_purchase IS 'The owner''sbunq card purchases, personal and joint accounts (not the business one), since 2019, from the card authorisations: amount is money spent in euros (a refund is negative), mcc the Mastercard merchant category code, city where the merchant is. category comes from the code: Groceries, Eating & drinking (restaurants, cafés, bars, takeaway), Travel & transport, Subscriptions & digital, Health & fitness, Education, Going out & events, Cash (ATM), Services & bills, Shopping (other retail), or Other (about 1%). iDEAL and bank transfers (rent, bills) are not card purchases: see bunq_payment.';
COMMENT ON TABLE bunq_card_action IS 'bunq card authorisations, including declined ones; a settled purchase also appears in bunq_payment.';
COMMENT ON TABLE bunq_request IS 'bunq payment requests sent and received.';
COMMENT ON TABLE bunq_invoice IS 'Invoices bunq charged (subscription fees).';
COMMENT ON TABLE degiro_portfolio_day IS 'The owner''sDEGIRO accounts per day, account personal or pension, from the daily snapshot of the web trader (since November 2026, one row per account and day collected): positions_value is the sum of the product positions in EUR, total_cash the cash balance (totalCash), total_value their sum, net_liquidation DEGIRO''s own net liquidation value (reportNetliq) when it sends one, deposits the net money paid in over the account''s life (totalDepositWithdrawal), unrealized_pl the sum of the positions'' profit and loss against cost. totals holds every totalPortfolio field as DEGIRO sent it. Start here for net worth questions, with bunq_account for the bank side.';
COMMENT ON TABLE degiro_position IS 'Each DEGIRO position per account (personal or pension) and snapshot day: position_id is the product id for position_type PRODUCT (name, symbol, isin, currency and product_type from degiro_product), or a cash line such as FLATEX_EUR for CASH. size is the number held (or the cash amount), price the last price in the product''s currency, value_eur the value in EUR, break_even_price the average cost, pl_eur the unrealised profit or loss in EUR. A position sold entirely drops out, or lingers with size 0 until DEGIRO drops it.';
COMMENT ON TABLE degiro_transaction IS 'Every DEGIRO trade, in account personal or pension: buysell B or S, quantity (negative for a sale), price in the product''s currency, total the trade value in that currency, total_eur in EUR, fee_eur and fx_fee_eur the costs, total_with_fees_eur what the cash balance changed by (negative for a buy). Joined to degiro_product for name, symbol and isin. From the account''s start.';
COMMENT ON TABLE degiro_cash_movement IS 'Every booking on a DEGIRO cash account, account personal or pension: type (CASH_TRANSACTION for deposits and withdrawals, TRANSACTION for trades, also dividends, fees, interest, FX), description as DEGIRO shows it (Dutch), change in currency, balance the account balance after it, product_id with name and symbol when a product is involved. Deposits and withdrawals also appear in bunq_payment as transfers to and from DEGIRO (flatex Bank), so do not add the two up.';
COMMENT ON TABLE degiro_product IS 'Every product that appeared in the owner''s DEGIRO positions, trades or cash movements, as DEGIRO describes it: name, symbol, isin, currency, product_type (ETF, STOCK, ...), exchange_id, and the close price on the day first seen. raw has every field.';
COMMENT ON TABLE steam_game IS 'Owned Steam games.';
COMMENT ON TABLE steam_playtime_hour IS 'Steam minutes played per game per hour. games_that_hour > 1 usually means card idling, not real play.';
COMMENT ON TABLE steam_achievement IS 'Steam achievements per game; unlocked_at is null when not achieved.';
COMMENT ON TABLE steam_presence IS 'Steam status at each hourly check: personastate 0 offline, 1 online, 3 away; game is set while in a game.';
COMMENT ON TABLE lastfm_scrobble IS 'The owner''sLast.fm scrobbles: Apple Music plays sent by a scrobbler app, so only what the scrobbler caught, and Apple Music plays appear only once the scrobbler app has run. ts is when the track finished or passed half, not when it started. album and the mbids (MusicBrainz ids) are null when Last.fm has none. loved is as of the fetch; lastfm_loved is current.';
COMMENT ON TABLE lastfm_loved IS 'Tracks the owner currently loves on Last.fm, with when he loved them.';
COMMENT ON TABLE apple_music_play IS 'Every Apple Music play from Apple''s data export, from April 2024 to the export date (17 Oct 2026), including skips: played_s is how long it played, duration_s the track length. started_at is when it started. artist comes from the library or Apple''s play history, as the export has none on plays; null for a few. device is IPHONE, APPLETV, WATCH and so on.';
COMMENT ON TABLE music_play IS 'All music the owner listened to, one row per counted play: Apple Music plays from the data export until the first Last.fm scrobble (16 Oct 2026), Last.fm scrobbles after, so nothing is counted twice. Apple plays count by Last.fm''s rule (over 30 s, played half or 4 minutes). source says which. Use this for listening history; apple_music_play has skips and devices.';
COMMENT ON TABLE apple_tv_play IS 'Viewing sessions in the Apple TV app from Apple''s data export: series, season and episode title, played_s watched in the session, length_s the episode length.';
COMMENT ON TABLE apple_podcast_episode IS 'Podcast episodes in Apple Podcasts from Apple''s data export, as of the export: the feed, podcast title (null for feeds no longer followed), last played and marked-played times, position_s in the episode and play count. Episode titles are not in the export.';
COMMENT ON TABLE apple_purchase IS 'App Store and other Apple purchases and free downloads from Apple''s data export: item, the app it belongs to for in-app purchases, amount (0 for free) in currency, refunded, payment method.';
COMMENT ON TABLE apple_subscription IS 'App Store subscriptions from Apple''s data export: app, item, first start, latest expiry (null while it renews), auto_renew as of the export. Amounts are in apple_purchase.';
COMMENT ON TABLE apple_app_update IS 'App downloads, re-downloads and updates from Apple''s data export: app, platform, version, seller and device model (e.g. iPhone16,1).';
COMMENT ON TABLE apple_app_store_search IS 'App Store search terms from Apple''s data export, one row per search event.';
COMMENT ON TABLE apple_music_library IS 'Tracks in the Apple Music library as of the export: when added, last played, play and skip counts.';
COMMENT ON TABLE apple_music_favourite IS 'Songs, albums and artists favourited in Apple Music, as of the export.';
COMMENT ON TABLE apple_game_achievement IS 'Game Center achievements from Apple''s data export: game, achievement, percent complete, when (UTC).';
COMMENT ON TABLE apple_testflight_session IS 'TestFlight beta app sessions per day from Apple''s data export.';
COMMENT ON TABLE github_commit IS 'The owner''sGitHub commits (default branches) with the local timezone offset of the author date. redacted rows are work in other people''s private repos: times only, no repo, SHA or message.';
COMMENT ON TABLE github_event IS 'The owner''sGitHub events: pushes, PRs, reviews, comments. redacted rows are work in private repos: type, action and time only.';
COMMENT ON TABLE github_pull_request IS 'Every pull request the owner opened, with state, line counts and description. redacted rows are work in other people''s private repos: times, state and line counts only.';
COMMENT ON TABLE github_review IS 'Pull request reviews the owner submitted. redacted rows are work in private repos: state and time only.';
COMMENT ON TABLE github_contribution_day IS 'GitHub contribution count per day.';
COMMENT ON TABLE github_issue_comment IS 'Every comment the owner wrote on an issue or pull request conversation (not inline review comments), with its text. redacted rows are work in other people''s private repos: times only.';
COMMENT ON TABLE github_issue IS 'Every issue the owner opened, with state and description. redacted rows are work in other people''s private repos: times and state only.';
COMMENT ON TABLE github_discussion IS 'GitHub Discussions the owner started. redacted rows: times only.';
COMMENT ON TABLE github_discussion_comment IS 'Comments the owner wrote in GitHub Discussions. redacted rows: times only.';
COMMENT ON TABLE github_star IS 'Repos the owner starred and when. Unstarring is not recorded.';
COMMENT ON TABLE github_gist IS 'The owner''sgists, public and secret: description and file names, no contents.';
COMMENT ON TABLE github_follow IS 'GitHub accounts following the owner (direction followers) and that the owner follows (following). first_seen is when collection first saw the follow, not when it happened; current is false once it ended.';
COMMENT ON TABLE calendar_event IS 'Current events of each of the owner''s calendars, from the newest copy of the ICS feed. Recurring events are one row with rrule, rdate and exdate kept as ICS text (not expanded); a changed occurrence is its own row with recurrence_id. All-day events start at midnight Amsterdam time and end_ts is exclusive. redacted calendars (work) hold only times, status, transp and busy_status, with summary, location and description null and uid hashed. Deleted events drop out; older copies are in raw/calendar/.';
COMMENT ON TABLE ah_receipt IS 'Albert Heijn in-store receipts (kassabonnen) from AH''s unofficial app API, only for purchases with the owner''s bonus card. Amounts in EUR: total paid, discount_total the bonus discount; payment_methods as AH names them (e.g. PIN); store is the address; items the number of receipt lines. Purchases are also in bunq_payment, so do not add the two up.';
COMMENT ON TABLE ah_receipt_item IS 'Lines of Albert Heijn receipts; join ah_receipt on receipt_id. product_id is AH''s till id, not the ah.nl webshop id. quantity is the count, or weight with unit for loose produce; price is the unit price and amount the line total in EUR, before receipt-wide discounts. indicators is the JSON list of bonus and discount markers (name, discount, percentage).';
COMMENT ON TABLE screen_time_device IS 'Devices in the owner''s screen time: his Mac (device_uuid local, named after the Mac) and the iPhones and iPads that share Screen Time with it, from the Biome sync peers. platform is macos, ios (iPhone and iPad alike) or tvos. first_seen is when the helper first saw the device.';
COMMENT ON TABLE screen_time_app IS 'App usage intervals per device: bundle is the app''s bundle id (e.g. com.apple.Safari), seconds the length. Mac rows (source knowledgec) are the Mac''s own intervals. iPhone and iPad rows (source biome) are stitched from focus changes: an app in focus counts until another app gains focus or it loses focus, at most one hour. capped is true when no end was recorded within the hour, usually because the device locked or slept with the app open: leave capped rows out of usage totals (WHERE NOT capped), since their real length is unknown. On the Apple Watch the watch face (com.apple.carousel.*) is in focus whenever the wrist is raised, so leave it out of Watch usage. History starts about four weeks before the helper was installed (October 2026), as far back as the Mac keeps it. iPhone data needs Share Across Devices in Screen Time. Mac and iPhone intervals overlap in time, so sum per device, not across devices.';
COMMENT ON TABLE claude_code_event IS 'One row per line of the owner''s Claude Code session logs on his Mac, metadata only (no prompts, outputs, tool inputs or file contents). type is user (a prompt), assistant (a reply), tool_use (tool names the tool, comma-separated when several), tool_result, or another log type such as system. project is the working directory with / as - (e.g. -Users-tom-work-personal-personal-data). Tokens are on the last line of each API message only, so sum them freely; input_tokens excludes cache reads and writes. sidechain rows are subagents.';
COMMENT ON TABLE browser_visit IS 'One row per page visit in Safari, Firefox (Developer Edition) and Orion (from 5 Nov 2026) on the owner''s Mac, from the browsers'' own history, as far back as each keeps it (Safari''s default is a year). profile is the Firefox profile directory, default for Safari and Orion. Orion visits have transition (link, typed, start_page, ...) and no origin_or_source; their title is the page''s latest. url is the full URL with its query string, which can hold tokens: do not quote URLs unless asked, prefer host. Both browsers also hold visits from his other devices, but only those synced to the Mac (iCloud for Safari, Firefox Sync), and a synced visit can arrive late with its original ts. A synced Firefox visit cannot be told from a local one. origin_or_source is the raw Firefox source (0 organic, 2 bookmarked, 3 searched; a synced visit is 0) or Safari origin (0 local, 1 synced over iCloud from the iPhone or iPad). visit_type is Firefox''s transition (1 link, 2 typed, 3 bookmark, 5 and 6 redirects, ...), null for Safari; load_successful is Safari only. Redirects and reloads are separate visits, so count visits per host with care.';
COMMENT ON TABLE youtube_video IS 'Every YouTube video the owner opened in Safari or Firefox on his Mac (including iPhone and iPad Safari visits synced to it), as the YouTube Data API described it when first seen: statistics are from that moment, not current. Deleted and private videos are missing. duration_s is the video''s length, not time watched: archived 24/7 streams (lofi radio, ambience) run for weeks or years, so never sum it as watch time. category_id is YouTube''s video category (10 Music, 20 Gaming, 24 Entertainment, 27 Education, 28 Science & Technology, ...). raw is the whole API item.';
COMMENT ON TABLE youtube_watch IS 'YouTube watch history from browser visits: one row per visit to a video page, joined to youtube_video (title is null until the video is looked up, within an hour, or when it is deleted). A visit means the page was opened, not watched to the end; several rows close together are one viewing through redirects and reloads. YouTube apps (iPhone, iPad, Apple TV) are not in here.';
COMMENT ON TABLE media_now_playing IS 'Raw now-playing changes from Apple Biome per device (Mac, iPhone, iPad, Watches): state 1 playing, 2 paused, with title, artist, album, duration and the app (bundle id). Video in Mac Safari appears as app com.apple.WebKit.GPU with no title: find what it was in browser_visit at the same time. Firefox reports the page title.';
COMMENT ON TABLE media_play IS 'Playback sessions built from media_now_playing: one row per title played on a device, with seconds actually playing (played_s). Records without a title are left out, so Mac Safari video (com.apple.WebKit.GPU) is only in media_now_playing.';
COMMENT ON TABLE biome_event IS 'Raw records of Apple Biome context streams read on the owner''s Mac since October 2026, as protobuf field number to value; Biome keeps about four weeks, so history starts mid-September 2026. Streams with records so far: App.WebUsage (Mac Safari page time: 4 url, 5 domain, 6 app, 3 state), Notification.Usage, Device.Wireless.Bluetooth (also from the iPhone and Watches), UserFocus.ComputedMode, Device.Wireless.WiFi, _DKEvent.Wifi.Connection, GameController.Connected, Device.Power.LowPowerMode (1 on), ScreenTime.AppUsage (duplicates screen_time_app). The typed tables below cover the clear ones; field meanings are inferred from the records.';
COMMENT ON TABLE bluetooth_connection IS 'Bluetooth audio devices connecting and disconnecting on the iPhone and Apple Watches (AirPods, speakers, headphones): headphone use. connected false is a disconnect. Other numeric fields in fields look like battery levels (unconfirmed).';
COMMENT ON TABLE focus_mode IS 'Focus modes turning on (active) and off on the Mac, synced from the iPhone schedule, e.g. com.apple.sleep.sleep-mode for the sleep schedule.';
COMMENT ON TABLE wifi_network IS 'Wi-Fi networks the Mac joined (connected) and left: a rough location signal for the Mac.';
COMMENT ON TABLE notification IS 'Notifications on the Mac per app (bundle id). The meaning of kind is not known yet; 1 is the most common.';
COMMENT ON TABLE photo IS 'Every photo and video in the owner''s Apple Photos library (iCloud Photos, all devices), metadata only: when and where it was taken (lat and lon are null without GPS), camera, and whether it is a screenshot, favourite, hidden or trashed. Photos taken on his phones with location are a location history of their own. imported_by is the app that saved it (com.apple.camera for the camera, others for saved or shared images). raw has every field the helper sends.';
COMMENT ON TABLE photo_label IS 'What Apple Photos'' on-device analysis found in each photo (join photo on uuid), from its search index: kind scene (objects and scenes, e.g. Beach, Bicycle, Food, Receipt), species (plants and animals, e.g. a cat breed), landmark, landmark_type, place (street up to country names from the location), business, event (concerts with performer and genre), meaning (e.g. Entertainment), trip, holiday, activity, sound, caption, app, and text: one row per word read from the image (OCR), in no order. category is Apple''s undocumented number behind kind; kind other for numbers not yet named. Search with ilike on label.';
COMMENT ON TABLE photo_person IS 'Faces Apple Photos detected in each photo (join photo on uuid): one row per face, person is Photos'' id for the person the face was grouped into (null when not grouped), name the name given to that person in Photos (null when unnamed; the owner has named nobody yet). Count distinct person per photo for how many people are in it, or group by person to find the same person across photos.';
COMMENT ON TABLE podcast_episode IS 'Apple Podcasts episodes the owner played, saved, bookmarked or downloaded, from the Podcasts library on his Mac, which iCloud keeps in step with the iPhone: one row per episode, the newest copy sent. playhead_s is where he is in the episode (0 after it is finished), play_count how often it was started, last_played_at when he last listened, play_state the app''s raw state (0 unplayed or finished, 1 and 2 appear for in-progress and played; not documented), subscribed whether he follows the podcast. Episodes never touched are not sent, and since the helper started in November 2026 only the current state is known, not each listening session; older copies are in raw/screen_time/push/. Spoken-word listening also shows up in media_play (app com.apple.podcasts).';
COMMENT ON TABLE shell_command IS 'Commands the owner ran in a terminal on his Mac: ts when the command started, shell zsh or fish, command its first word only (the program, e.g. git, cargo, claude), with wrappers like sudo and VAR=value assignments skipped; arguments are never sent. duration_s is how long it ran (zsh only). zsh history goes back to October 2023, fish to late 2023. Commands run inside Claude Code tool calls are not here, only what the owner typed; see claude_code_event for those.';
COMMENT ON TABLE obsidian IS 'The owner''sObsidian notes as metadata only, from the iCloud vault on his Mac: one row per version the helper saw, so a note appears again each time it changed between two 15-minute runs, and once with deleted = true when it was removed or renamed away (that row has only path and sent_at). path is the file within the vault, folder and name its parts, ext md for notes and cook for cooklang recipes; created_at and modified_at are the file times, words and lines count the body without front matter (null for files that are not text), tags the #tags and front matter tags, links the targets of [[wiki links]]. sent_at is when the Mac pushed the row. The text never leaves the Mac. Starts November 2026 with one row per note as it then was; older modified_at values are the file times, not observed edits. For what he was writing when, order by modified_at and look at the newest row per path for the current vault.';
COMMENT ON TABLE knmi_hourly IS 'Measured weather per hour from KNMI stations: Schiphol (240, the station for Amsterdam), Wijk aan Zee (257, the coast) and IJmuiden (225, wind only). Validated and about two days behind, from 2019. ts is the end of the hour. Unlike weather_hour (an Open-Meteo model at the phone''s location), these are real measurements at fixed stations: prefer them for what actually happened.';
COMMENT ON TABLE knmi_daily IS 'Measured weather per day from the same KNMI stations: mean, min and max temperature, rain, sunshine (hours and % of possible), wind, gusts, humidity, pressure, cloud cover (octants) and evaporation.';
COMMENT ON TABLE knmi_10min IS 'KNMI station measurements every 10 minutes, long form: one row per station, time and parameter. Schiphol has about 80 parameters, Wijk aan Zee about 30, IJmuiden about 19 (wind). Look parameters up in knmi_10min_parameter; common ones: ta air temperature °C, rh humidity %, ff wind speed m/s, fx gust m/s, dd wind direction °, rg rain intensity mm/h, R1H rain in the past hour mm, ss sunshine minutes, qg solar radiation W/m², pp pressure hPa. Not validated and near real time, unlike knmi_hourly; history starts mid-October 2026.';
COMMENT ON TABLE knmi_10min_parameter IS 'Label, unit and description of each knmi_10min parameter, as KNMI gives them.';
COMMENT ON TABLE knmi_grid_home IS 'KNMI''s daily 1 km grids at the cell of the owner''s home: grid Rd1 precipitation (mm, with its variance), Tg1, Tn1 and Tx1 mean, minimum and maximum temperature (°C), EV24 Makkink evaporation (mm). day is the day the measurement period ends at 08:00 UT minus that hour, i.e. 08:00 to 08:00. Rain goes back to 1951; about three weeks behind. Rain and temperature at home rather than at Schiphol.';
COMMENT ON TABLE knmi_rain_radar IS 'Rain at the owner''s home every 5 minutes from KNMI''s precipitation radar (the analysis of each 5-minute nowcast run, i.e. what the radar saw), in mm per hour, for the roughly 1 km radar cell around home (cell). Near real time, from mid-October 2026. The value 0.000365 recurs in dry weather and looks like the radar''s floor (inferred, not documented): treat below 0.1 mm/h as dry. For a day''s rain total use knmi_grid_home (Rd1) or knmi_daily.';
COMMENT ON TABLE knmi_rain_forecast IS 'KNMI''s rain nowcast for the owner''s home: every 5-minute run (run_at) forecasts each 5 minutes up to 2 hours ahead (ts, lead_min), in mm per hour, the forecast behind apps like Buienradar. Join ts to knmi_rain_radar.ts to compare forecast with what the radar then saw. From 20 October 2026; treat below 0.1 mm/h as dry.';
COMMENT ON TABLE flight_position IS 'Aircraft within 3 nautical miles (5.5 km) of the owner''s home, polled every minute by his Mac from adsb.lol: one row per aircraft per poll, with callsign, registration, type, position, altitude (ft; null on the ground), speed (kt), climb rate (ft/min, positive climbing), distance_nm and bearing from home. Schiphol is about 8 km south-west, so climbing traffic is departures and descending traffic arrivals. Only while the Mac is awake: check flight_poll for coverage before reading a gap as no flights.';
COMMENT ON TABLE flight_poll IS 'Every minute the Mac polled for aircraft near home and how many it saw: the coverage of flight_position.';
COMMENT ON TABLE earthquake IS 'Seismic events in and around the Netherlands (lat 50.5-54, lon 3-7.5, so also Belgium, the German border and the southern North Sea) from KNMI, back to the start of its catalogue. event_type tells them apart: earthquake (natural), induced or triggered event (mostly gas extraction in Groningen), controlled explosion, quarry blast, sonic boom and others. magnitude is null for events without one (explosions). Times are UTC.';
COMMENT ON TABLE water_temperature IS 'Water temperature (°C) every 10 minutes from Rijkswaterstaat stations near Amsterdam: amsterdam.ndsmwerf (the IJ), amsterdam.schellingwouderbrug (IJ east, towards IJmeer), diemen (Amsterdam-Rijnkanaal), spaarndam.zijkanaalc (North Sea Canal), ijmuiden.buitenhaven (sea side, nearest to the Zandvoort/Bloemendaal beaches) and markermeer.midden; amsterdam.km25 and pampus.oost are hand samples about monthly. depth_cm is how deep the sensor is (some stations measure at two depths: use the shallowest for swimming). status Ongecontroleerd means not yet validated. Nieuwe Meer, Sloterplas and the city canals have no Rijkswaterstaat sensor: use the nearest station as a guide.';
COMMENT ON TABLE swimming_spot IS 'Every official swimming spot in the Netherlands (zwemwater.nl) with its current status: goed (fine), WAARSCHUWING (warning, e.g. blue-green algae), NEGATIEF_ZWEMADVIES (advice not to swim), NADER_ONDERZOEK (being investigated), ZWEMVERBOD (swimming banned). The owner swims around Amsterdam (Nieuwe Meer, Amsterdamse bos), at the coast between Zandvoort and Bloemendaal aan Zee, and in lakes generally. Official monitoring runs in the bathing season (May to September); outside it statuses may be stale.';
COMMENT ON TABLE swimming_spot_status IS 'The status of every swimming spot at each daily check that changed anything: the history behind swimming_spot.';
COMMENT ON TABLE noise_map IS 'Modelled yearly noise at the owner''s home from the RIVM noise maps (EU noise directive), per source: all, road, aircraft, rail, industry, wind turbines. db is Lden (day-evening-night average over the year) or Lnight; 0 means below the map''s lowest class. These are models, renewed every few years; apple_sample environmental_audio_exposure is what his Watch actually measured. The raw file also holds RIVM survey estimates of noise annoyance for his neighbourhood and municipality.';
COMMENT ON TABLE blood_test IS 'Blood test results per lab report, one row per test with the lab''s own name, unit (Dutch/German conventions: mmol/l, µmol/l, /nl, g/l) and reference range: ref_low and ref_high are numbers (either may be null for "< x" or "> x"), ref_text the lab''s wording when the range has tiers or notes. flag is the lab''s own high/low marking. Values are exactly as printed; convert units in queries. A result below the detection limit has value null and value_text as printed (e.g. "<4.00"). "% FS ges." is percent of total fatty acids.';
COMMENT ON TABLE macrofactor_food IS 'Food the owner logged in MacroFactor, from its food log exports: one row per food entry (entry) and nutrient, with amount in unit. eaten_at is when he ate it. grams is the weight eaten. Prefer this to the dietary samples in apple_sample for the days it covers.';
COMMENT ON TABLE macrofactor_set IS 'Strength training sets the owner logged in MacroFactor, in order per day (set_no): exercise, set type, kg, reps and rir (reps in reserve). The export has the day, not the time.';
COMMENT ON TABLE macrofactor_daily IS 'Daily values from MacroFactor''s full export, one row per day, sheet and metric: intake totals (Calories & Macros, Micronutrients), Scale Weight, Weight Trend (MacroFactor''s smoothed weight), Expenditure (estimated kcal burned), Steps, Body Metrics, and training stats per muscle group and exercise. unit is null when the sheet gives none.';
COMMENT ON TABLE macrofactor_program IS 'MacroFactor''s calorie and macro targets per weekday, one set per program update (updated), with the expenditure and weight it was based on.';
COMMENT ON TABLE macrofactor_weight_goal IS 'The owner''sweight goals in MacroFactor: loss, gain or maintenance phases with start and end dates, goal weight and weekly rate.';
COMMENT ON TABLE twitch_channel IS 'Twitch channels the owner visited in a browser, as the Helix API described them when first seen.';
COMMENT ON TABLE twitch_video IS 'Twitch past broadcasts (VODs) the owner opened in a browser, with title and length; Twitch deletes VODs after weeks, so old ones are missing.';
COMMENT ON TABLE twitch_watch IS 'Twitch watch history from browser visits (Twitch has no watch history API): live is true for a channel page (watching live), false for a past broadcast. A visit means the page was opened, not how long it was watched; for that see media_play, where Firefox playback shows up.';
COMMENT ON TABLE twitch_follow IS 'Channels the owner follows on Twitch now, with when he followed.';
COMMENT ON TABLE twitch_subscription IS 'Channels the owner is subscribed to on Twitch now (tier 1000, 2000, 3000), and whether the sub was a gift.';
COMMENT ON TABLE audible_book IS 'Audiobooks in the owner''s Audible library (audible.de) now, with progress: percent_complete and finished.';
COMMENT ON TABLE audible_listening IS 'Audible listening time per day (period day, interval YYYY-MM-DD, last 30 days of each pull, kept from then on) and per month (period month, interval YYYY-MM), in minutes, all devices.';
COMMENT ON TABLE audible_position IS 'Where the owner last was in each Audible book (minutes in) and when that was last updated, in UTC without zone.';
COMMENT ON TABLE kindle_book IS 'Every book in the owner''s Kindle library now (Amazon account in the Netherlands), with authors and purchase date. He reads them on a Kindle e-reader.';
COMMENT ON TABLE kindle_annotation IS 'Per Kindle book, from Amazon''s annotation store: type kindle.lpr is the furthest position read and kindle.most_recent_read the latest one (created_at says when), kindle.highlight a highlight from position to end_position (the highlighted text is not available), kindle.bookmark a bookmark, kindle.note a note. Positions are Kindle locations in the book file, comparable only within a book.';
COMMENT ON TABLE kindle_read_state IS 'Kindle books marked READ, and when (marked_at): AUTOMATIC when the Kindle marked it on reaching the end, MANUAL when the owner marked it. A finished-book list for reading on the e-reader.';
COMMENT ON TABLE kindle_lookup IS 'Every word the owner looked up in the Kindle dictionary (Vocabulary Builder): the word, its stem, the full sentence he was reading, the book and when.';
COMMENT ON TABLE google_activity IS 'Google My Activity of the owner''s Google accounts, from Google''s Data Portability API: one row per event, back to 2007. account is knyght (his personal Google account) or clavicles (the YouTube brand account he has used since before Google bought YouTube; most of his YouTube history is there). product is the header: YouTube, Maps, Search and others; older Search results he visited carry the site instead (e.g. telegraph.co.uk), so filter Search history on activity_controls or products, not product alone. action is the first word of title (Watched, Searched, Visited, Viewed, Directions, ...). Google Search history mostly ends when he moved to DuckDuckGo; for current web browsing see browser_visit.';
COMMENT ON TABLE youtube_history IS 'YouTube watch history from Google itself, every device and app, both accounts: the authoritative watch history, unlike youtube_watch (browser visits). A row means YouTube counted a watch, not that it was watched to the end. is_ad marks ads he was shown (From Google Ads): leave them out unless asked. Join youtube_video on video_id for length and category, where the browser saw the video.';
COMMENT ON TABLE youtube_subscription IS 'Channels each YouTube account is subscribed to now.';
COMMENT ON TABLE youtube_comment IS 'Comments (kind comment) and live chat messages (kind live_chat) the owner wrote on YouTube, with the text.';
COMMENT ON TABLE youtube_playlist_video IS 'Videos in each YouTube playlist now, including Watch later, with when they were added.';
COMMENT ON TABLE maps_place IS 'Google Maps places the owner saved (list saved) and labelled, such as Home and Gym (list labeled), with coordinates.';
COMMENT ON TABLE gmail_message IS 'Every mail in the owner''s Gmail accounts since collection started (the first runs also walk the older mail), one row per message, metadata only: who sent it (from_name, from_address, from_domain), to and cc as written, subject, list_id for mailing lists, labels as Gmail''s ids (INBOX, SENT, CATEGORY_PROMOTIONS, ...; sent mail has SENT and from_address is the owner''s), size_bytes and ts when Gmail received it. Bodies are not stored, except for the senders on the GMAIL_FULL_SENDERS allowlist (shops, railways, parcel services): has_body is true for those, and the full message with its MIME parts is in raw/gmail/<account>/message/. Count mail by from_domain or label; do not expect message text.';
COMMENT ON TABLE claude_code_session IS 'Claude Code sessions: project, first and last event, counts of events, prompts (user_messages) and tool uses, models used and summed tokens. end_ts - start_ts includes idle time.';
COMMENT ON TABLE moneybird_sales_invoice IS 'Invoices the owner''s business sent, from Moneybird (the bookkeeping): one row per invoice in its newest version. invoice_number is null for a draft; state is draft, scheduled, open, pending_payment, reminded, late, paid or uncollectible. Amounts in currency (EUR unless said otherwise), tax excluded and included; details is the JSON list of lines (description, amount, price, total_price_excl_tax_with_discount). Payments received are in bunq_payment on the business account and in moneybird_financial_mutation. An invoice deleted in Moneybird stays here in its last version.';
COMMENT ON TABLE moneybird_purchase_invoice IS 'Costs booked in Moneybird: kind purchase_invoice (a supplier invoice) or receipt (a bon), newest version per id. date is the document date, state new, saved, open, late, paid or pending_payment; details the JSON lines with their ledger accounts. The same purchase is in bunq_payment or card_purchase when paid by card.';
COMMENT ON TABLE moneybird_financial_mutation IS 'Bank transactions as Moneybird imported them from the business bank account (financial_account_id): amount negative for money out, state unprocessed or processed, settlement_state settled or pending. payments is the JSON list of invoice payments booked on it (invoice_type, invoice_id, price) and ledger_account_bookings the direct ledger bookings. The same transactions are in bunq_payment; this table adds what they were booked against.';
COMMENT ON TABLE moneybird_time_entry IS 'Hours the owner logged in Moneybird: started_at and ended_at (UTC), hours net of pauses, description, billable, contact and project, and sales_invoice_id once invoiced. An entry edited later is here in its newest version.';
COMMENT ON TABLE moneybird_contact IS 'Customers and suppliers in Moneybird, newest version per id: company_name or firstname and lastname, city, country, email, archived. Join moneybird_sales_invoice, moneybird_purchase_invoice and moneybird_time_entry on contact_id.';
COMMENT ON TABLE day IS 'One row per Amsterdam calendar day since 2019 with that day''s totals from every source, for correlations and dashboards: the night that ended that morning (asleep_h, bedtime_h, wake_h, sleep_efficiency from sleep_night), weight_kg (Withings scale, else Apple Health) and fat_pct, Apple Health activity (steps, walk_km, cycle_km, exercise_min, daylight_min, active_kcal, floors, resting_hr, hrv_ms, vo2_max), workouts with workout_min and workout_types, State of Mind (mood_valence -1 to 1, mood_entries), food logged (food_kcal, protein_g, carbs_g, fat_g, caffeine_mg), screen time in hours per device class (mac_h, iphone_h, ipad_h; uncapped intervals only, Watch face excluded), youtube_videos watched, steam_min, switch_min (Nintendo Switch, from November 2026), podcast_min and music_min, commits, claude_sessions and claude_prompts, card spending (card_purchases, spent_eur, groceries_eur, eating_out_eur), photos taken, away and abroad with trip_id and places from away_day and trip, KNMI Schiphol weather (temp_mean_c, temp_min_c, temp_max_c, rain_mm, sunshine_h, wind_mean_ms), home_rain_mm from the KNMI grid at home, and Luchtmeetnet pm25 and no2 at Stadhouderskade. A null means that source has nothing for the day, not zero: each source starts on its own date (sleep and activity 2024, screen time and Claude Code October 2026, money and weather 2019). Start here for any question that relates two things per day; drill into the source tables for detail.';
COMMENT ON TABLE away_day IS 'Days the owner was away from Amsterdam, one row per day with evidence: signals is the list of sources that placed him away (photo, workout, phone and weather are GPS over 40 km from home; card is an in-person card purchase outside the Amsterdam area or in a foreign currency; photo_tz is a time zone Apple Photos recorded other than Amsterdam''s; commit is a git author offset that differs from Amsterdam''s at the time), evidence the number of such records, places the merchant cities and zone names seen, currencies the foreign ones, lat and lon the median GPS position and max_km the farthest. abroad is true when any signal points outside the Netherlands (GPS outside the Dutch bounding box, a foreign currency, zone or offset); false means a day trip or stay elsewhere in the Netherlands. Online purchases are excluded by entry mode, so merchant cities such as Cork or Luxembourg do not appear. A day without any signal is not here even if he was away; a commit from a machine set to the wrong zone can mark a day abroad by mistake.';
COMMENT ON TABLE trip IS 'Trips built from away_day: consecutive away days, allowing up to two days in between without evidence, with start_day, end_day, days, days_with_evidence, abroad (any day abroad), places, currencies, signals, the median lat and lon, max_km, spent_eur (card purchases on those days) and photos taken. A one-day trip with abroad false is usually a day trip inside the Netherlands. Use trip_id to join day. Covers 2012 on for photos and 2019 on for card purchases; the collector''s own location data starts in October 2026.';
