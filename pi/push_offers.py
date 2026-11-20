#!/usr/bin/env python3
"""Push the drugstore offers on folderz.nl to the collector.

Home Assistant runs this script as a shell command once a day. Folderz answers the collector's own requests
from Fly with a bot challenge, so the Pi reads the store list pages from the home connection, parses the offer
cards and POSTs one JSON list per store to the collector's `/offers/folderz?store=<store>`, which files the
offers it has not seen and sends the alerts.

The collector URL and secret come from `personal_data_network.json` next to the script, the network push's
config; `personal_data_offers.json` next to it holds `{"stores": [...]}` and replaces the default stores when
present. `personal_data_offers.log` gets one line per run.

Pass `--detach` to fork and finish in the background: a run takes a minute or two, and Home Assistant kills
a shell command after 60 seconds.
"""

import datetime as dt
import html
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
NETWORK_CONFIG_PATH = os.path.join(HERE, "personal_data_network.json")
CONFIG_PATH = os.path.join(HERE, "personal_data_offers.json")
LOG_PATH = os.path.join(HERE, "personal_data_offers.log")
BASE = "https://www.folderz.nl"
DEFAULT_STORES = ["kruidvat", "etos"]
# Pages per store, a cap against a list that never ends; Kruidvat has about 65.
MAX_PAGES = 120
# Seconds between page requests.
PACE = 1.0
TIMEOUT = 60
USER_AGENT = "personal-data-offers"
DAYS_PER_UNIT = {"dag": 1, "dagen": 1, "week": 7, "weken": 7, "maand": 30, "maanden": 30}


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


def read_json(path, default):
    """Read a JSON file, or `default` when it is missing."""
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return default


def between(text, start, end):
    """The text between the first `start` and the next `end`, or None."""
    head, found, rest = text.partition(start)
    if not found:
        return None
    value, found, _ = rest.partition(end)
    return value if found else None


def clean(text):
    """Trimmed text with HTML entities decoded, or None when empty."""
    if text is None:
        return None
    value = html.unescape(text).strip()
    return value or None


def price(chunk, css_class):
    """The amount in the element with `css_class`, e.g. `€ 9,98` as 9.98, or None."""
    element = between(chunk, css_class, "</div>")
    if element is None or "€" not in element:
        return None
    try:
        return float(element.split("€", 1)[1].strip().replace(",", "."))
    except ValueError:
        return None


def days_left(chunk):
    """Days the card says are left: `2 dagen`, `1 week`, `1 maand`; None when it says nothing known."""
    text = clean(between(chunk, 'product-date txt-xs">', "<"))
    if not text:
        return None
    count, _, unit = text.partition(" ")
    if not count.isdigit() or unit.strip() not in DAYS_PER_UNIT:
        return None
    return int(count) * DAYS_PER_UNIT[unit.strip()]


def card(chunk, store, today):
    """The offer of one card chunk in the collector's shape, or None without a link or name."""
    href = between(chunk, 'href="', '"')
    name = clean(between(chunk, 'product__name txt-title">', "<"))
    if not href or "aanbieding-" not in href or not name:
        return None
    picture = between(chunk, "product__image", "</picture>")
    image = between(picture, 'src="', '"') if picture else None
    days = days_left(chunk)
    return {
        "id": "folderz_" + href.rsplit("aanbieding-", 1)[1].rstrip("/"),
        "image": image,
        "name": name,
        "original_price": price(chunk, "product__price-normal"),
        "price": price(chunk, "product__price-offer"),
        "quality": clean(between(chunk, 'data-quality-type="', '"')),
        "store": store,
        "url": BASE + href,
        "valid_from": None,
        "valid_until": (today + dt.timedelta(days=days)).isoformat() if days is not None else None,
    }


def fetch(url):
    """GET a page. Returns the status and the body text; a 404 is a status, not an error."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html"})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return 404, ""
        raise


def offers(store, today):
    """Every offer card of a store's list pages, first page first, until a 404 or a page without cards."""
    found = {}
    for page in range(1, MAX_PAGES + 1):
        url = f"{BASE}/winkels/{store}/aanbiedingen?page={page}"
        status, body = fetch(url)
        if status == 404 and page > 1:
            break
        if status != 200:
            raise RuntimeError(f"{url}: {status}")
        cards = [card(chunk, store, today) for chunk in body.split("js-offer-link-item")[1:]]
        cards = [offer for offer in cards if offer]
        if not cards and page == 1:
            raise RuntimeError(f"{url}: no offer cards in {len(body)} bytes")
        if not cards:
            break
        for offer in cards:
            found.setdefault(offer["id"], offer)
        time.sleep(PACE)
    return list(found.values())


def push(cfg, store, body):
    """POST the store's offers to the collector. Returns the status code and the size."""
    data = json.dumps(body, separators=(",", ":")).encode()
    url = cfg["url"].replace("/network/ingest", "/offers/folderz") + "?" + urllib.parse.urlencode({"store": store})
    request = urllib.request.Request(url, data=data, method="POST")
    request.add_header("Authorization", f"Bearer {cfg['secret']}")
    request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, timeout=180) as response:
        return response.status, len(data), response.read().decode("utf-8", "replace")


def detach():
    """Fork, let the parent exit and cut the child loose from Home Assistant's pipes, so the run survives the timeout."""
    if os.fork():
        os._exit(0)
    os.setsid()
    null = os.open(os.devnull, os.O_RDWR)
    for descriptor in (0, 1, 2):
        os.dup2(null, descriptor)
    os.close(null)


def main():
    """One run: read every store's list, push each to the collector."""
    cfg = read_json(NETWORK_CONFIG_PATH, None)
    if cfg is None:
        log(f"no config at {NETWORK_CONFIG_PATH}")
        return 1
    stores = read_json(CONFIG_PATH, {}).get("stores", DEFAULT_STORES)
    today = dt.datetime.now().date()
    failed = 0
    for store in stores:
        try:
            found = offers(store, today)
            status, size, reply = push(cfg, store, found)
            log(f"{store}: {len(found)} offers, sent {size} bytes, status {status}, reply {reply[:200]}")
        except urllib.error.HTTPError as error:
            log(f"{store}: push failed: {error.code} {error.read()[:200]!r}")
            failed += 1
        except Exception as error:  # noqa: BLE001 - the other stores still go out.
            log(f"{store}: failed: {error}")
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    if "--detach" in sys.argv[1:]:
        detach()
    sys.exit(main())
