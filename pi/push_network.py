#!/usr/bin/env python3
"""Push the home network's last hour to the collector.

Home Assistant runs this script as a shell command every hour. It reads the UniFi controller (clients,
the access point's radios, the neighbouring networks) and AdGuard Home's query log, trims both to the
fields worth keeping, and POSTs one JSON body to the collector's `/network/ingest`.

`personal_data_network.json` next to the script holds the collector URL and secret, the UniFi login and the id of the
Home Assistant user the AdGuard ingress session is opened as.
`personal_data_network_state.json` holds the AdGuard cursor and the day of the last full neighbour list.
`personal_data_network.log` gets one line per run.
"""

import datetime as dt
import http.cookiejar
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from typing import Any

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "personal_data_network.json")
STATE_PATH = os.path.join(HERE, "personal_data_network_state.json")
LOG_PATH = os.path.join(HERE, "personal_data_network.log")
SUPERVISOR = "http://supervisor"
TIMEOUT = 60
# AdGuard query log pages per run; at 500 entries each this caps a run at 25,000 queries.
MAX_PAGES = 50
PAGE = 500

CLIENT_FIELDS = (
    "mac",
    "hostname",
    "name",
    "ip",
    "oui",
    "is_wired",
    "is_guest",
    "essid",
    "bssid",
    "radio",
    "radio_proto",
    "channel",
    "signal",
    "rssi",
    "noise",
    "tx_rate",
    "rx_rate",
    "tx_bytes",
    "rx_bytes",
    "tx_packets",
    "rx_packets",
    "tx_retries",
    "wifi_tx_attempts",
    "satisfaction",
    "uptime",
    "first_seen",
    "last_seen",
    "assoc_time",
    "latest_assoc_time",
    "idletime",
    "tx_power",
    "ccq",
    "powersave_enabled",
    "is_11r",
    "authorized",
    "network",
    "vlan",
    "dev_cat",
    "dev_family",
    "dev_vendor",
    "os_name",
)
DEVICE_FIELDS = (
    "mac",
    "name",
    "model",
    "version",
    "uptime",
    "state",
    "upgradable",
    "satisfaction",
    "num_sta",
    "user-num_sta",
    "guest-num_sta",
    "last_seen",
    "ip",
    "led_override",
    "cfgversion",
    "sys_stats",
    "system-stats",
)
RADIO_FIELDS = (
    "radio",
    "name",
    "channel",
    "ht",
    "tx_power",
    "tx_packets",
    "rx_packets",
    "tx_retries",
    "num_sta",
    "cu_total",
    "cu_self_tx",
    "cu_self_rx",
    "satisfaction",
    "ast_txto",
    "ast_cst",
    "ast_be_xmit",
    "extchannel",
    "gain",
)
VAP_FIELDS = (
    "essid",
    "radio",
    "channel",
    "num_sta",
    "tx_power",
    "satisfaction",
    "state",
    "up",
    "rx_bytes",
    "tx_bytes",
    "rx_packets",
    "tx_packets",
    "rx_errors",
    "tx_errors",
    "rx_dropped",
    "tx_dropped",
    "tx_retries",
    "ccq",
    "avg_client_signal",
    "rx_crypts",
    "rx_frags",
    "tx_combined_retries",
    "wifi_tx_latency_mov",
)
UPLINK_FIELDS = (
    "type",
    "speed",
    "max_speed",
    "full_duplex",
    "up",
    "rx_bytes",
    "tx_bytes",
    "rx_packets",
    "tx_packets",
    "rx_dropped",
    "tx_dropped",
    "rx_errors",
    "tx_errors",
)
NEIGHBOUR_FIELDS = (
    "bssid",
    "essid",
    "channel",
    "bw",
    "rssi",
    "signal",
    "noise",
    "radio",
    "band",
    "center_freq",
    "security",
    "oui",
    "is_ubnt",
    "is_adhoc",
    "last_seen",
    "age",
)


def log(message):
    """Append one line to the log and keep the log short."""
    line = f"{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')} {message}\n"
    try:
        if os.path.exists(LOG_PATH) and os.path.getsize(LOG_PATH) > 512 * 1024:
            with open(LOG_PATH, encoding="utf-8") as handle:
                tail = handle.readlines()[-200:]
            with open(LOG_PATH, "w", encoding="utf-8") as handle:
                handle.writelines(tail)
        with open(LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(line)
    except OSError:
        pass
    print(line, end="", file=sys.stderr)


def pick(record, fields):
    """Keep only `fields` of `record`, those it has."""
    return {field: record[field] for field in fields if field in record}


def parse_time(text):
    """Parse an RFC 3339 time, trimming fractions past microseconds, which AdGuard writes."""
    trimmed = re.sub(r"(\.\d{6})\d+", r"\1", text.replace("Z", "+00:00"))
    return dt.datetime.fromisoformat(trimmed)


def read_json(path, default):
    """Read a JSON file, or `default` when it is missing."""
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return default


def write_json(path, value):
    """Write `value` as JSON, atomically."""
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(value, handle)
    os.replace(tmp, path)


class Unifi:
    """A logged-in session on the UniFi Network Application's legacy API."""

    def __init__(self, cfg):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=context), urllib.request.HTTPCookieProcessor(self.jar)
        )
        self.base = cfg["url"].rstrip("/")
        self.site = cfg.get("site", "default")
        reply = self.call("POST", "/api/login", {"username": cfg["username"], "password": cfg["password"]})
        if reply.get("meta", {}).get("rc") != "ok":
            raise RuntimeError(f"unifi login failed: {reply.get('meta')}")

    def call(self, method, path, body=None):
        """Send one request and return the parsed JSON reply."""
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.base + path, data=data, method=method)
        request.add_header("Content-Type", "application/json")
        request.add_header("Accept", "application/json")
        for cookie in self.jar:
            if cookie.name == "csrf_token":
                request.add_header("X-Csrf-Token", cookie.value or "")
        with self.opener.open(request, timeout=TIMEOUT) as response:
            return json.loads(response.read())

    def stat(self, kind):
        """The `data` list of a site statistic."""
        reply = self.call("GET", f"/api/s/{self.site}/stat/{kind}")
        if reply.get("meta", {}).get("rc") != "ok":
            raise RuntimeError(f"unifi stat/{kind} failed: {reply.get('meta')}")
        return reply.get("data", [])


def unifi_part(cfg, full_neighbours):
    """The UniFi part of the push: clients, the access point, neighbours summarised by channel and, once a day, in full."""
    api = Unifi(cfg)
    clients = [pick(client, CLIENT_FIELDS) for client in api.stat("sta")]
    devices = []
    for device in api.stat("device"):
        trimmed = pick(device, DEVICE_FIELDS)
        trimmed["radio_table_stats"] = [pick(radio, RADIO_FIELDS) for radio in device.get("radio_table_stats") or []]
        trimmed["radio_table"] = [
            pick(radio, ("radio", "name", "channel", "ht", "tx_power_mode", "tx_power"))
            for radio in device.get("radio_table") or []
        ]
        trimmed["vap_table"] = [pick(vap, VAP_FIELDS) for vap in device.get("vap_table") or []]
        trimmed["uplink"] = pick(device.get("uplink") or {}, UPLINK_FIELDS)
        devices.append(trimmed)
    neighbours = [pick(ap, NEIGHBOUR_FIELDS) for ap in api.stat("rogueap")]
    by_channel = defaultdict(list)
    for ap in neighbours:
        by_channel[(ap.get("radio"), ap.get("channel"), ap.get("bw"))].append(ap.get("rssi") or 0)
    summary = [
        {
            "radio": radio,
            "channel": channel,
            "bw": bw,
            "count": len(rssis),
            "max_rssi": max(rssis),
            "mean_rssi": round(sum(rssis) / len(rssis), 1),
        }
        for (radio, channel, bw), rssis in sorted(
            by_channel.items(), key=lambda item: (str(item[0][0]), item[0][1] or 0)
        )
    ]
    strongest = sorted(neighbours, key=lambda ap: ap.get("rssi") or 0, reverse=True)[:15]
    part = {
        "clients": clients,
        "devices": devices,
        "neighbours_by_channel": summary,
        "neighbours_strongest": strongest,
        "neighbour_count": len(neighbours),
    }
    if full_neighbours:
        part["neighbours"] = neighbours
    return part


class Supervisor:
    """Calls to the Supervisor with Home Assistant's own token, including an add-on's ingress."""

    def __init__(self):
        self.token = os.environ.get("SUPERVISOR_TOKEN")
        if not self.token:
            raise RuntimeError("SUPERVISOR_TOKEN is not in the environment")

    def call(self, method, path, headers=None, body=None):
        """Send one request and return the parsed JSON reply."""
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(SUPERVISOR + path, data=data, method=method)
        request.add_header("Authorization", f"Bearer {self.token}")
        request.add_header("Accept", "application/json")
        if data is not None:
            request.add_header("Content-Type", "application/json")
        for name, value in (headers or {}).items():
            request.add_header(name, value)
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"{method} {path.split('?')[0]}: {error.code} {error.read()[:300]!r}") from error

    def ingress(self, slug, user_id):
        """Open an ingress session to an add-on as a Home Assistant user. Returns a function that GETs a path on it."""
        entry = self.call("GET", f"/addons/{slug}/info")["data"]["ingress_entry"]
        token = entry.rstrip("/").rsplit("/", 1)[-1]
        session = self.call("POST", "/ingress/session", body={"user_id": user_id})["data"]["session"]
        headers = {
            "Cookie": f"ingress_session={session}",
            "X-Hass-Source": "core.ingress",
            "X-Ingress-Path": f"/api/hassio_ingress/{token}",
        }
        return lambda path: self.call("GET", f"/ingress/{token}/{path.lstrip('/')}", headers)


def adguard_part(slug, user_id, since, until, names):
    """The hour's DNS queries from AdGuard, counted per client, domain, type and outcome."""
    get = Supervisor().ingress(slug, user_id)
    counts = defaultdict(int)
    older_than = None
    seen = 0
    pages = 0
    done = False
    while not done and pages < MAX_PAGES:
        query = f"control/querylog?limit={PAGE}" + (
            f"&older_than={urllib.parse.quote(older_than)}" if older_than else ""
        )
        page = get(query)
        entries = page.get("data") or []
        pages += 1
        if not entries:
            break
        for entry in entries:
            at = parse_time(entry["time"])
            if at >= until:
                continue
            if at < since:
                done = True
                break
            seen += 1
            question = entry.get("question") or {}
            key = (
                entry.get("client"),
                (question.get("name") or "").rstrip(".").lower(),
                question.get("type"),
                entry.get("status"),
                entry.get("reason"),
                bool(entry.get("cached")),
            )
            counts[key] += 1
            info = entry.get("client_info") or {}
            if info.get("name") and entry.get("client") not in names:
                names[entry["client"]] = info["name"]
        older_than = page.get("oldest") or entries[-1]["time"]
        if len(entries) < PAGE:
            break
    queries = [
        {
            "client": client,
            "domain": domain,
            "type": qtype,
            "status": status,
            "reason": reason,
            "cached": cached,
            "count": count,
        }
        for (client, domain, qtype, status, reason, cached), count in counts.items()
    ]
    queries.sort(key=lambda row: (row["client"] or "", row["domain"], row["type"] or ""))
    return {
        "since": since.isoformat(),
        "until": until.isoformat(),
        "raw_count": seen,
        "pages": pages,
        "truncated": pages >= MAX_PAGES and not done,
        "queries": queries,
    }


def push(cfg, body):
    """POST the body to the collector. Returns the status code."""
    data = json.dumps(body, separators=(",", ":")).encode()
    request = urllib.request.Request(cfg["url"], data=data, method="POST")
    request.add_header("Authorization", f"Bearer {cfg['secret']}")
    request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, timeout=180) as response:
        return response.status, len(data)


def main():
    """One run: gather, push, move the cursors."""
    cfg = read_json(CONFIG_PATH, None)
    if cfg is None:
        log(f"no config at {CONFIG_PATH}")
        return 1
    state = read_json(STATE_PATH, {})
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    today = dt.datetime.now().date().isoformat()
    full_neighbours = state.get("neighbours_full_date") != today
    body: dict[str, Any] = {"sent_at": now.isoformat(), "errors": []}
    names: dict[str, str | None] = {}
    try:
        body["unifi"] = unifi_part(cfg["unifi"], full_neighbours)
        for client in body["unifi"]["clients"]:
            if client.get("ip"):
                names[client["ip"]] = client.get("name") or client.get("hostname")
    except Exception as error:  # noqa: BLE001 - the push goes out with whatever succeeded.
        body["errors"].append(f"unifi: {error}")
    since_text = state.get("adguard_cursor")
    since = parse_time(since_text) if since_text else now - dt.timedelta(hours=1)
    try:
        body["adguard"] = adguard_part(
            cfg.get("adguard_slug", "a0d7b954_adguard"), cfg["ha_user_id"], since, now, names
        )
    except Exception as error:  # noqa: BLE001
        body["errors"].append(f"adguard: {error}")
    body["clients"] = {ip: name for ip, name in names.items() if name}
    try:
        status, size = push(cfg, body)
    except urllib.error.HTTPError as error:
        log(f"push failed: {error.code} {error.read()[:200]!r} errors={body['errors']}")
        return 1
    except Exception as error:  # noqa: BLE001
        log(f"push failed: {error} errors={body['errors']}")
        return 1
    if "adguard" in body:
        state["adguard_cursor"] = now.isoformat()
    if "unifi" in body and full_neighbours:
        state["neighbours_full_date"] = today
    write_json(STATE_PATH, state)
    unifi = body.get("unifi") or {}
    adguard = body.get("adguard") or {}
    log(
        f"sent {size} bytes, status {status}, clients {len(unifi.get('clients', []))}, "
        f"queries {adguard.get('raw_count', 0)} in {len(adguard.get('queries', []))} rows, errors {body['errors']}"
    )
    return 1 if body["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
