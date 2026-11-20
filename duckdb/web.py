"""The map: a Leaflet page over the personal data, behind a passkey login.

- /<MCP_TOKEN>/passkey registers a passkey; only someone with the token URL can.
- /login signs in with a registered passkey and sets a signed session cookie for 30 days.
- /map and /map/data/* need that cookie. /map/knmi-radar passes WMS requests to KNMI with the key kept here.
- /grafana/* needs it too and goes to the private Grafana app, which trusts X-WEBAUTH-USER from this proxy.
- Passkeys belong to the host they were registered on: each name in PUBLIC_HOSTS has its own.
  Signed in on one, /passkey/handoff?host=<other> opens the other's /passkey with a 10-minute grant to add one there.
"""

import asyncio
import base64
import decimal
import hashlib
import hmac
import json
import os
import secrets
import time
import urllib.parse
import urllib.request
from pathlib import Path

import httpx
import webauthn
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import (
    FileResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from starlette.routing import Route
from webauthn.helpers.structs import (
    AuthenticatorSelectionCriteria,
    PublicKeyCredentialDescriptor,
    ResidentKeyRequirement,
    UserVerificationRequirement,
)

# Each host has its own passkeys: a passkey belongs to the domain it was registered on.
HOSTS = os.environ.get("PUBLIC_HOSTS", "dawn-meadowbrook-804.fly.dev").split(",")
PASSKEYS = Path(os.environ.get("PASSKEYS_PATH", "/data/passkeys.json"))
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://personal-data-grafana.flycast")
PROXY_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
# Hop-by-hop and length headers the proxy sets itself.
HOP_HEADERS = {"host", "connection", "keep-alive", "transfer-encoding", "content-length", "upgrade", "x-webauth-user"}
SESSION_DAYS = 30
CHALLENGE_SECONDS = 300
KNMI_WMS = "https://api.dataplatform.knmi.nl/wms/adaguc-server"
HERE = Path(__file__).parent


def plain(v):
    """A DuckDB value as JSON: times as ISO text, decimals as floats."""
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v


def _rp_id(request: Request) -> str:
    host = request.url.hostname
    return host if host in HOSTS else HOSTS[0]


def _secret() -> bytes:
    return os.environ["SESSION_SECRET"].encode()


def _sign(value: str) -> str:
    mac = hmac.new(_secret(), value.encode(), hashlib.sha256).hexdigest()
    return f"{value}.{mac}"


def _unsign(token: str | None) -> str | None:
    """The value of a signed token, or None when it is missing, forged or expired (`value` ends in `:<expiry>`)."""
    if not token or "." not in token:
        return None
    value, mac = token.rsplit(".", 1)
    good = hmac.new(_secret(), value.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(mac, good):
        return None
    expiry = value.rsplit(":", 1)[-1]
    return value if expiry.isdigit() and int(expiry) > time.time() else None


def _cookie(response: Response, name: str, value: str, seconds: int, samesite: str = "strict") -> None:
    signed = _sign(f"{value}:{int(time.time()) + seconds}")
    response.set_cookie(name, signed, max_age=seconds, httponly=True, secure=True, samesite=samesite)


def signed_in(request: Request) -> bool:
    return _unsign(request.cookies.get("session")) is not None


def _key_rp(key: dict) -> str:
    """The domain a stored passkey belongs to; the first ones predate the custom domain."""
    return key.get("rp_id", "dawn-meadowbrook-804.fly.dev")


def _load() -> list[dict]:
    return json.loads(PASSKEYS.read_text()) if PASSKEYS.exists() else []


def _save(keys: list[dict]) -> None:
    tmp = PASSKEYS.with_suffix(".tmp")
    tmp.write_text(json.dumps(keys, indent=2))
    tmp.replace(PASSKEYS)


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def _challenge(request: Request) -> bytes | None:
    value = _unsign(request.cookies.get("challenge"))
    return _unb64(value.rsplit(":", 1)[0]) if value else None


def _with_challenge(options, challenge: bytes) -> Response:
    response = Response(webauthn.options_to_json(options), media_type="application/json")
    _cookie(response, "challenge", _b64(challenge), CHALLENGE_SECONDS)
    return response


async def register_page(request: Request) -> Response:
    return FileResponse(HERE / "passkey.html")


async def register_options(request: Request) -> Response:
    challenge = secrets.token_bytes(32)
    rp_id = _rp_id(request)
    options = webauthn.generate_registration_options(
        rp_id=rp_id,
        rp_name="personal-data",
        user_name="tom",
        user_id=b"tom",
        challenge=challenge,
        authenticator_selection=AuthenticatorSelectionCriteria(
            resident_key=ResidentKeyRequirement.REQUIRED, user_verification=UserVerificationRequirement.REQUIRED
        ),
        exclude_credentials=[PublicKeyCredentialDescriptor(id=_unb64(k["id"])) for k in _load() if _key_rp(k) == rp_id],
    )
    return _with_challenge(options, challenge)


async def register_verify(request: Request) -> Response:
    challenge = _challenge(request)
    if challenge is None:
        return JSONResponse({"error": "challenge expired, start again"}, status_code=400)
    rp_id = _rp_id(request)
    try:
        verified = webauthn.verify_registration_response(
            credential=await request.json(),
            expected_challenge=challenge,
            expected_rp_id=rp_id,
            expected_origin=f"https://{rp_id}",
            require_user_verification=True,
        )
    except Exception as e:  # the library raises several types for a bad credential
        return JSONResponse({"error": str(e)}, status_code=400)
    keys = _load()
    keys.append(
        {
            "id": _b64(verified.credential_id),
            "public_key": _b64(verified.credential_public_key),
            "sign_count": verified.sign_count,
            "rp_id": rp_id,
            "added": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    )
    _save(keys)
    return JSONResponse({"ok": True, "passkeys": len(keys)})


async def login_page(request: Request) -> Response:
    return FileResponse(HERE / "login.html")


async def login_options(request: Request) -> Response:
    challenge = secrets.token_bytes(32)
    options = webauthn.generate_authentication_options(
        rp_id=_rp_id(request), challenge=challenge, user_verification=UserVerificationRequirement.REQUIRED
    )
    return _with_challenge(options, challenge)


async def login_verify(request: Request) -> Response:
    challenge = _challenge(request)
    if challenge is None:
        return JSONResponse({"error": "challenge expired, start again"}, status_code=400)
    credential = await request.json()
    keys = _load()
    rp_id = _rp_id(request)
    key = next((k for k in keys if k["id"] == credential.get("id") and _key_rp(k) == rp_id), None)
    if key is None:
        return JSONResponse({"error": "unknown passkey"}, status_code=403)
    try:
        verified = webauthn.verify_authentication_response(
            credential=credential,
            expected_challenge=challenge,
            expected_rp_id=rp_id,
            expected_origin=f"https://{rp_id}",
            credential_public_key=_unb64(key["public_key"]),
            credential_current_sign_count=key["sign_count"],
            require_user_verification=True,
        )
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=403)
    key["sign_count"] = verified.new_sign_count
    _save(keys)
    response = JSONResponse({"ok": True})
    _cookie(response, "session", "tom", SESSION_DAYS * 86400)
    response.delete_cookie("challenge")
    return response


def map_routes(connect) -> list[Route]:
    """The map page and its data, each needing a session. `connect` opens the read-only database."""

    def guarded(handler):
        async def wrapper(request: Request) -> Response:
            if not signed_in(request):
                return RedirectResponse(f"/login?{urllib.parse.urlencode({'next': request.url.path})}", status_code=303)
            return await handler(request)

        return wrapper

    grafana_client = httpx.AsyncClient(base_url=GRAFANA_URL, timeout=httpx.Timeout(120, connect=30))

    async def grafana(request: Request) -> Response:
        """Pass the request to Grafana as the signed-in owner: Grafana trusts the header (auth proxy) and is not public."""
        headers = [(k, v) for k, v in request.headers.items() if k.lower() not in HOP_HEADERS]
        headers.append(("X-WEBAUTH-USER", "tom"))
        upstream = grafana_client.build_request(
            request.method, request.url.path, params=request.query_params, headers=headers, content=await request.body()
        )
        r = await grafana_client.send(upstream, stream=True)
        out = {k: v for k, v in r.headers.items() if k.lower() not in HOP_HEADERS}
        return StreamingResponse(r.aiter_raw(), status_code=r.status_code, headers=out, background=BackgroundTask(r.aclose))

    def query(sql: str, params: list) -> list[tuple]:
        with connect() as con:
            return con.execute(sql, params).fetchall()

    async def geojson(sql: str, params: list, props: list[str]) -> Response:
        rows = await asyncio.to_thread(query, sql, params)
        features = [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [r[1], r[0]]},
                "properties": {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in zip(props, r[2:])},
            }
            for r in rows
            if r[0] is not None and r[1] is not None
        ]
        return JSONResponse({"type": "FeatureCollection", "features": features})

    def span(request: Request) -> list[str]:
        q = request.query_params
        return [q.get("from", "1900-01-01"), q.get("to", "2100-01-01")]

    async def home(request: Request) -> Response:
        return FileResponse(HERE / "home.html")

    async def page(request: Request) -> Response:
        # The home position comes from HOME_LAT and HOME_LON, so it stays out of the code.
        home = f"[{float(os.environ.get('HOME_LAT', '52.3731'))}, {float(os.environ.get('HOME_LON', '4.8922'))}]"
        html = (HERE / "map.html").read_text().replace("const HOME = [0, 0];", f"const HOME = {home};")
        return Response(html, media_type="text/html")

    async def location(request: Request) -> Response:
        # At most about 20,000 points: thin evenly over the span.
        sql = """
            WITH s AS (SELECT * FROM location WHERE ts >= ?::TIMESTAMPTZ AND ts < ?::TIMESTAMPTZ),
                 n AS (SELECT greatest(count(*) // 20000, 1) AS step FROM s)
            SELECT lat, lon, ts, accuracy_m, zone
            FROM (SELECT *, row_number() OVER (ORDER BY ts) AS i FROM s), n
            WHERE i % step = 0 ORDER BY ts
        """
        return await geojson(sql, span(request), ["ts", "accuracy_m", "zone"])

    async def photos(request: Request) -> Response:
        sql = """SELECT p.lat, p.lon, p.taken_at, p.kind, p.camera_model,
                        (SELECT string_agg(DISTINCT l.label, ', ') FROM photo_label l
                         WHERE l.uuid = p.uuid AND l.kind IN ('scene', 'species', 'landmark', 'event')) AS labels
                 FROM photo p
                 WHERE p.lat IS NOT NULL AND NOT p.trashed AND p.taken_at >= ?::TIMESTAMPTZ AND p.taken_at < ?::TIMESTAMPTZ"""
        return await geojson(sql, span(request), ["taken_at", "kind", "camera", "labels"])

    async def flights(request: Request) -> Response:
        sql = """SELECT lat, lon, ts, callsign, aircraft_type, altitude_ft, climb_ft_min FROM flight_position
                 WHERE ts >= ?::TIMESTAMPTZ AND ts < ?::TIMESTAMPTZ ORDER BY ts"""
        return await geojson(sql, span(request), ["ts", "callsign", "type", "altitude_ft", "climb_ft_min"])

    async def earthquakes(request: Request) -> Response:
        sql = """SELECT lat, lon, ts, magnitude, depth_km, place, event_type FROM earthquake
                 WHERE ts >= ?::TIMESTAMPTZ AND ts < ?::TIMESTAMPTZ"""
        return await geojson(sql, span(request), ["ts", "magnitude", "depth_km", "place", "type"])

    async def swimming(request: Request) -> Response:
        sql = "SELECT lat, lon, name, status FROM swimming_spot"
        return await geojson(sql, [], ["name", "status"])

    async def water(request: Request) -> Response:
        sql = """SELECT lat, lon, name, ts, celsius, depth_cm FROM water_temperature
                 QUALIFY row_number() OVER (PARTITION BY station ORDER BY ts DESC, abs(depth_cm)) = 1"""
        return await geojson(sql, [], ["name", "ts", "celsius", "depth_cm"])

    async def radar(request: Request) -> Response:
        key = os.environ.get("KNMI_WMS_KEY")
        if not key:
            return Response("KNMI_WMS_KEY is not set", status_code=503)
        url = f"{KNMI_WMS}?{urllib.parse.urlencode(dict(request.query_params))}"

        def fetch() -> tuple[int, str, bytes]:
            req = urllib.request.Request(url, headers={"Authorization": key})
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    return r.status, r.headers.get("Content-Type", "image/png"), r.read()
            except urllib.error.HTTPError as e:
                return e.code, e.headers.get("Content-Type", "text/plain"), e.read()

        status, ctype, body = await asyncio.to_thread(fetch)
        return Response(body, status_code=status, media_type=ctype, headers={"Cache-Control": "private, max-age=300"})

    return [
        Route("/", guarded(home)),
        Route("/grafana", guarded(grafana), methods=PROXY_METHODS),
        Route("/grafana/{path:path}", guarded(grafana), methods=PROXY_METHODS),
        Route("/map", guarded(page)),
        Route("/map/data/location", guarded(location)),
        Route("/map/data/photos", guarded(photos)),
        Route("/map/data/flights", guarded(flights)),
        Route("/map/data/earthquakes", guarded(earthquakes)),
        Route("/map/data/swimming", guarded(swimming)),
        Route("/map/data/water", guarded(water)),
        Route("/map/knmi-radar", guarded(radar)),
    ]


def _granted(request: Request) -> bool:
    value = _unsign(request.cookies.get("grant"))
    return value is not None and value.startswith("grant:")


async def handoff(request: Request) -> Response:
    """Signed in on one host: open another host's passkey page with a 10-minute grant to register one there."""
    target = request.query_params.get("host", HOSTS[0])
    if not signed_in(request) or target not in HOSTS:
        return RedirectResponse("/login?next=/passkey/handoff", status_code=303)
    grant = _sign(f"grant:{int(time.time()) + CHALLENGE_SECONDS * 2}")
    return RedirectResponse(f"https://{target}/passkey/claim?{urllib.parse.urlencode({'grant': grant})}", status_code=303)


async def claim(request: Request) -> Response:
    value = _unsign(request.query_params.get("grant"))
    if value is None or not value.startswith("grant:"):
        return PlainTextResponse("This link has expired. Start again from the host you are signed in on.\n", status_code=403)
    response = RedirectResponse("/passkey", status_code=303)
    # Lax: this response ends a redirect chain from the other host, which a strict cookie would not survive.
    _cookie(response, "grant", "grant", CHALLENGE_SECONDS * 2, samesite="lax")
    return response


def _registration(handler):
    """Registration without the token path: for a session on this host or a grant from another."""

    async def wrapper(request: Request) -> Response:
        if not (signed_in(request) or _granted(request)):
            return RedirectResponse("/login", status_code=303)
        return await handler(request)

    return wrapper


def auth_routes(token: str) -> list[Route]:
    return [
        Route("/passkey/handoff", handoff),
        Route("/passkey/claim", claim),
        Route("/passkey", _registration(register_page)),
        Route("/passkey/options", _registration(register_options), methods=["POST"]),
        Route("/passkey/verify", _registration(register_verify), methods=["POST"]),
        Route(f"/{token}/passkey", register_page),
        Route(f"/{token}/passkey/options", register_options, methods=["POST"]),
        Route(f"/{token}/passkey/verify", register_verify, methods=["POST"]),
        Route("/login", login_page),
        Route("/login/options", login_options, methods=["POST"]),
        Route("/login/verify", login_verify, methods=["POST"]),
    ]
