#!/usr/bin/env python3
"""SocoLive IPTV scanner — Python port of namhau-iptv-tool.

Scans live rooms from json.vnres.co, renders an extended M3U playlist plus an
IPTV JSON file, and publishes both to a GitHub repository via the Contents
API. Change detection reads the fingerprint embedded in the last publish
commit message, so it works from ephemeral CI runners with no local state.

The JSON serialization and fingerprint are byte-compatible with the Go
implementation: a Python run right after a Go publish reports "no changes".
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

log = logging.getLogger("scanner")

DEFAULT_API = "https://json.vnres.co"
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


class APIError(Exception):
    """Raised for any upstream/API failure worth failing the run on."""


# ---------------------------------------------------------------------------
# Live API access
# ---------------------------------------------------------------------------

def api_url(base: str, path: str, callback: str) -> str:
    now = str(int(time.time()))
    qs = urllib.parse.urlencode({"callback": callback, "v": now, "_": now})
    return f"{base.rstrip('/')}{path}?{qs}"


def unwrap_jsonp(text: str) -> str:
    start_candidates = [i for i in (text.find("{"), text.find("[")) if i >= 0]
    end = max(text.rfind("}"), text.rfind("]"))
    if not start_candidates or end < min(start_candidates):
        raise APIError("response is not JSONP/JSON")
    return text[min(start_candidates):end + 1]


def get_jsonp(url: str, user_agent: str, timeout: int):
    req = urllib.request.Request(url, headers={
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(4 << 20)
    except urllib.error.HTTPError as exc:
        raise APIError(f"HTTP {exc.code} {exc.reason}") from None
    except urllib.error.URLError as exc:
        raise APIError(f"request failed: {exc.reason}") from None
    return json.loads(unwrap_jsonp(body.decode("utf-8", "replace")))


def fetch_rooms(api: str, user_agent: str, timeout: int):
    """Return (group_count, unique live rooms sorted by roomNum)."""
    payload = get_jsonp(api_url(api, "/all_live_rooms.json", "all_live_rooms"),
                        user_agent, timeout)
    if payload.get("code") != 200:
        raise APIError(f"live rooms API returned code {payload.get('code')}: "
                       f"{payload.get('msg')}")
    unique: dict[str, dict] = {}
    groups = 0
    for raw in (payload.get("data") or {}).values():
        if not isinstance(raw, list):
            continue  # all_live_rooms also carries non-room entries
        groups += 1
        for room in raw:
            num = str(room.get("roomNum") or "")
            if not num or room.get("liveStatus") != 1:
                continue
            unique.setdefault(num, room)  # dedupe rooms listed in many groups
    return groups, [unique[k] for k in sorted(unique)]


def pick_stream(stream: dict):
    """Mirror the Go preference: hdM3u8 -> m3u8 -> hdFlv -> flv."""
    hd_m3u8 = stream.get("hdM3u8") or ""
    m3u8 = stream.get("m3u8") or ""
    hd_flv = stream.get("hdFlv") or ""
    flv = stream.get("flv") or ""
    if hd_m3u8:
        return hd_m3u8, "hls"
    if m3u8:
        return m3u8, "hls"
    if hd_flv:
        return hd_flv, "flv"
    if flv:
        return flv, "flv"
    return "", ""


def fetch_channel(api: str, room: dict, user_agent: str, timeout: int) -> dict:
    num = str(room.get("roomNum") or "")
    endpoint = api_url(api, f"/room/{urllib.parse.quote(num, safe='')}/detail.json",
                       "detail")
    payload = get_jsonp(endpoint, user_agent, timeout)
    if payload.get("code") != 200:
        raise APIError(f"API returned code {payload.get('code')}: {payload.get('msg')}")
    data = payload.get("data") or {}
    detail_room = data.get("room") or {}

    url, fmt = pick_stream(data.get("stream") or {})
    if not url:
        raise APIError("no stream URL")

    title = room.get("title") or detail_room.get("title") or ""
    anchor = ((room.get("anchor") or {}).get("nickName")
              or (detail_room.get("anchor") or {}).get("nickName") or "")
    logo = room.get("cover") or detail_room.get("cover") or ""
    group = (room.get("liveTypeName") or detail_room.get("liveTypeName")
             or "SocoLive")
    return {
        "room_num": num, "title": title, "anchor": anchor,
        "logo": logo, "group": group, "url": url, "format": fmt,
    }


def scan_channels(api: str, user_agent: str, timeout: int, workers: int):
    groups, rooms = fetch_rooms(api, user_agent, timeout)
    log.info("live rooms: groups=%d unique_live=%d", groups, len(rooms))

    def work(room):
        try:
            return fetch_channel(api, room, user_agent, timeout), None
        except Exception as exc:  # noqa: BLE001 — collect per-room failures
            return None, f"room {room.get('roomNum')}: {exc}"

    errors: list[str] = []
    channels: list[dict] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for channel, err in pool.map(work, rooms):
            if err:
                errors.append(err)
            else:
                channels.append(channel)
    channels.sort(key=lambda c: c["room_num"])
    return channels, errors


# ---------------------------------------------------------------------------
# Match/sports schedule — Bóng đá, Bóng rổ, Lịch thi đấu
# ---------------------------------------------------------------------------

def fetch_matches(api: str, user_agent: str, timeout: int, days: int = 7):
    """Đọc lịch thi đấu: /match_recommend.json + /match/matches_YYYYMMDD.json.

    Mỗi trận map tới phòng live qua anchors[].anchor.roomNum. Trả list dict
    {room_num, title, anchor, logo, group, category, sub_cat, host, guest,
     host_icon, guest_icon, match_time}.
    """
    rooms: dict[str, dict] = {}

    def add_match(m: dict, cat_name: str):
        sub = m.get("subCateName") or ""
        host = m.get("hostName") or ""
        guest = m.get("guestName") or ""
        # trận đấu chỉ có ý nghĩa phát khi có ít nhất 1 BLV anchor phát trận
        anchors = m.get("anchors") or []
        room_nums = [a.get("anchor", {}).get("roomNum") for a in anchors if a.get("anchor")]
        suffix = f" ({sub})" if sub else ""
        title = f"{host} vs {guest}{suffix}"
        logo = (m.get("hostIcon") or "").strip()
        for rn in (room_nums or [None]):
            num = str(rn or "")
            key = num or f"match-{m.get('scheduleId')}"
            if key in rooms:
                continue
            rooms[key] = {
                "room_num": num,
                "title": title,
                "anchor": "",
                "logo": logo,
                # Televizo: group-title gọn (môn chính), không tách từng giải
                "group": "Bóng đá" if cat_name == "Bóng đá"
                         else ("Bóng rổ" if cat_name == "Bóng rổ" else "Lịch thi đấu"),
                "category": cat_name,
                "sub_cat": sub,
                "host": host, "guest": guest,
                "host_icon": m.get("hostIcon") or "",
                "guest_icon": m.get("guestIcon") or "",
                "match_time": m.get("matchTime") or 0,
            }

    # match_recommend
    try:
        rec = get_jsonp(api_url(api, "/match_recommend.json", "match_recommend"),
                        user_agent, timeout)
        for m in (rec.get("data") or {}).get("matches") or []:
            add_match(m, m.get("categoryName") or "Thể thao")
    except Exception as exc:  # noqa: BLE001 — non-fatal
        log.warning("match_recommend: %s", exc)

    # matches_YYYYMMDD (hôm nay + `days` ngày tới)
    for i in range(days):
        d = datetime.now(timezone.utc).date() + timedelta(days=i)
        dstr = d.strftime("%Y%m%d")
        try:
            payload = get_jsonp(api_url(api, f"/match/matches_{dstr}.json", "matches"),
                                user_agent, timeout)
            for m in (payload.get("data") or []):
                add_match(m, m.get("categoryName") or "Thể thao")
        except Exception as exc:  # noqa: BLE001 — non-fatal
            log.warning("matches_%s: %s", dstr, exc)

    log.info("sport matches: %d entries (%d có room)", len(rooms),
             sum(1 for r in rooms.values() if r["room_num"]))
    return [rooms[k] for k in sorted(rooms)]


def match_channels(api: str, matches: list[dict], user_agent: str, timeout: int,
                   workers: int) -> tuple[list[dict], list[str]]:
    """Lấy stream cho từng trận (room của BLV) → channel giống scan_channels.

    Chỉ giữ trận có stream phát được. group-title theo môn + giải.
    """
    def work(m):
        num = m["room_num"]
        if not num:
            return None, f"match {m.get('title')}: no room"
        try:
            det = get_jsonp(api_url(api, f"/room/{num}/detail.json", "detail"),
                            user_agent, timeout)
            if det.get("code") != 200:
                return None, f"room {num}: code {det.get('code')}"
            data = det.get("data") or {}
            url, fmt = pick_stream(data.get("stream") or {})
            if not url:
                return None, f"room {num}: no stream"
            return {
                "room_num": num, "title": m["title"], "anchor": m["anchor"],
                "logo": m["logo"] or (data.get("room") or {}).get("cover") or "",
                "group": m["group"], "url": url, "format": fmt,
            }, None
        except Exception as exc:  # noqa: BLE001
            return None, f"room {num}: {exc}"

    errors: list[str] = []
    channels: list[dict] = []
    alive = [m for m in matches if m["room_num"]]
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for channel, err in pool.map(work, alive):
            if err:
                errors.append(err)
            else:
                channels.append(channel)
    channels.sort(key=lambda c: c["room_num"])
    return channels, errors


# ---------------------------------------------------------------------------
# Renderers — kept byte-compatible with the Go implementation
# ---------------------------------------------------------------------------

def iptv_items(channels: list[dict], channel_logo: str = "") -> list[dict]:
    """Wire format shared by the published JSON and the fingerprint.

    channel_logo, when non-empty, replaces the per-room cover so players
    show a uniform channel mark instead of match thumbnails.
    """
    items = []
    for c in channels:
        name = c["title"] + (f" - {c['anchor']}" if c["anchor"] else "")
        item = {
            "id": c["room_num"],
            "tvg_id": "room-" + c["room_num"],
            "name": name,
        }
        logo = channel_logo or c["logo"]
        if logo:
            item["logo"] = logo
        item["group"] = c["group"] or "SocoLive"
        item["url"] = c["url"]
        if c["format"]:
            item["type"] = c["format"]
        items.append(item)
    return items


def iptv_json_bytes(channels: list[dict], channel_logo: str = "") -> bytes:
    # Go emits MarshalIndent(..., "", "  ") with raw UTF-8 and HTML-escapes
    # &, <, > inside strings; replicate all three so files are byte-equal.
    text = json.dumps(iptv_items(channels, channel_logo), indent=2,
                      ensure_ascii=False)
    text = (text.replace("&", "\\u0026")
                .replace("<", "\\u003c")
                .replace(">", "\\u003e"))
    return (text + "\n").encode("utf-8")


def strip_url_query(u: str) -> str:
    parts = urllib.parse.urlsplit(u)
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, parts.path, "", parts.fragment))


def fingerprint_items(items: list[dict]) -> str:
    h = hashlib.sha256()
    for it in items:
        line = "|".join([
            it["id"], it["tvg_id"], it["name"], it.get("logo", ""),
            it["group"], it.get("type", ""), strip_url_query(it["url"]),
        ])
        h.update((line + "\n").encode("utf-8"))
    return h.hexdigest()


def escape_m3u(s: str) -> str:
    return s.replace('"', "'").replace("\r", " ").replace("\n", " ")


def m3u_bytes(channels: list[dict], epg: str, channel_logo: str = "") -> bytes:
    # Televizo là parser nghiêm ngặt: header #EXTM3U phải NGẮN (không gộp nhiều
    # url-tvg EPG vào 1 dòng — 549 ký tự làm nó fail), và KHÔNG dùng attribute
    # group-logo (không chuẩn M3U). Chỉ giữ tvg-id / tvg-logo / group-title.
    epg = (epg or "").strip()
    if epg:
        epg = epg.split(",")[0].strip()  # chỉ lấy 1 URL XMLTV đầu tiên
    out = ["#EXTM3U"]
    if epg:
        out[0] += f' url-tvg="{escape_m3u(epg)}"'
    out += [
        "# SOCOLIVE LIVE PLAYLIST",
        f"# Channels: {len(channels)}",
        f"# Updated: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
        "",
    ]
    lines = "\n".join(out) + "\n"
    chunks = [lines]
    for c in channels:
        name = c["title"] or f"Room {c['room_num']}"
        if c["anchor"]:
            name += f" - {c['anchor']}"
        group = c["group"] or "SocoLive"
        logo = channel_logo or c["logo"]
        chunks.append(
            f'#EXTINF:-1 tvg-id="{escape_m3u("room-" + c["room_num"])}" '
            f'tvg-logo="{escape_m3u(logo)}" '
            f'group-title="{escape_m3u(group)}",{escape_m3u(name)}\n'
        )
        chunks.append(c["url"] + "\n")
    return "".join(chunks).encode("utf-8")


# ---------------------------------------------------------------------------
# GitHub publishing — state lives in the commit message
# ---------------------------------------------------------------------------

class GitHubClient:
    def __init__(self, token: str, repo: str, branch: str):
        self.token = token
        self.repo = repo
        self.branch = branch

    def _request(self, url: str, method: str = "GET", payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", "Bearer " + self.token)
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        req.add_header("User-Agent", "namhau-iptv-scanner/0.1")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read(8 << 20)
                return resp.status, json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            body = exc.read(4096).decode("utf-8", "replace")
            try:
                message = json.loads(body).get("message", body[:200])
            except ValueError:
                message = body[:200]
            return exc.code, {"message": message}

    def contents_url(self, path: str) -> str:
        quoted = urllib.parse.quote(path.lstrip("/"), safe="/")
        return (f"https://api.github.com/repos/{self.repo}/contents/{quoted}"
                f"?ref={urllib.parse.quote(self.branch, safe='')}")

    def fetch_remote_state(self, path: str):
        """(fingerprint, published_at) parsed from the last commit message."""
        api = (f"https://api.github.com/repos/{self.repo}/commits"
               f"?path={urllib.parse.quote(path, safe='')}&per_page=1")
        status, body = self._request(api)
        if status != 200 or not isinstance(body, list) or not body:
            return None, None
        message = (body[0].get("commit") or {}).get("message", "")
        fp_match = re.search(r"\[fp=([0-9a-f]+)\]", message)
        if not fp_match:
            return None, None
        fingerprint = fp_match.group(1)
        at_match = re.search(r"\[at=([^\]]+)\]", message)
        published_at = None
        if at_match:
            try:
                published_at = datetime.strptime(
                    at_match.group(1), "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        return fingerprint, published_at

    def publish(self, path: str, data: bytes, fingerprint: str) -> str:
        status, current = self._request(self.contents_url(path))
        sha = current.get("sha") if status == 200 else None
        payload = {
            "message": (f"update {os.path.basename(path)} "
                        f"[fp={fingerprint}] "
                        f"[at={datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ}]"),
            "content": base64.b64encode(data).decode(),
            "branch": self.branch,
        }
        if sha:
            payload["sha"] = sha
        status, res = self._request(self.contents_url(path).split("?")[0],
                                    method="PUT", payload=payload)
        if status not in (200, 201):
            raise APIError(f"commit {path}: HTTP {status}: {res.get('message')}")
        return (res.get("commit") or {}).get("html_url") or "(no url)"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def atomic_write(path: str, data: bytes) -> None:
    tmp = path + ".tmp"
    with open(tmp, "wb") as fh:
        fh.write(data)
    os.replace(tmp, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default=DEFAULT_API)
    parser.add_argument("--proxy", default=os.environ.get("NAMHAU_PROXY", ""),
                        help="HTTP/SOCKS5 proxy (e.g. socks5://127.0.0.1:7891)")
    parser.add_argument("--out", default="Socolive.json",
                        help="local IPTV JSON output path")
    parser.add_argument("--m3u-out", default="",
                        help="optional local M3U output path")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--match-days", type=int, default=7,
                        help="số ngày đọc lịch thi đấu (match/matches_*)" if False else "days de lich thi dau")
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--user-agent", default=DEFAULT_UA)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--github-token", default=os.environ.get("GITHUB_TOKEN", ""))
    parser.add_argument("--github-repo", default="hanhk112023-jpg/M3u")
    parser.add_argument("--github-branch", default="main")
    parser.add_argument("--github-path", default="Socolive.json")
    parser.add_argument("--epg", default="",
                        help="comma-separated XMLTV URLs for url-tvg")
    parser.add_argument("--tvg-logo", default="",
                        help="channel logo URL overriding per-room covers; empty keeps match covers")
    parser.add_argument("--force-publish-hours", type=float, default=4.0,
                        help="republish after idle hours to refresh tokens; 0 disables")
    args = parser.parse_args()

    # Route HTTP through a proxy when configured (e.g. GitHub Actions runner
    # hitting a residential exit node to bypass the CDN's datacenter IP block).
    if getattr(args, "proxy", ""):
        handler = urllib.request.ProxyHandler({
            "http": args.proxy,
            "https": args.proxy,
        })
        urllib.request.install_opener(urllib.request.build_opener(handler))
        logging.info("using proxy %s", args.proxy)


    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    try:
        live_channels, live_errors = scan_channels(args.api, args.user_agent,
                                                   args.timeout, args.workers)
        # Nguồn thể thao: Bóng đá / Bóng rổ / Lịch thi đấu từ match API.
        match_entries = fetch_matches(args.api, args.user_agent, args.timeout,
                                      getattr(args, "match_days", 7))
        match_chan, match_errors = match_channels(args.api, match_entries,
                                                  args.user_agent, args.timeout,
                                                  args.workers)
        channels = live_channels + match_chan
        errors = live_errors + match_errors
        # Gộp nhóm: tab "Bóng đá" / "Bóng rổ" / "Lịch thi đấu" lên đầu
        # Televizo: nhóm chính gọn (Bóng đá / Bóng rổ / Lịch thi đấu)
        def group_sort_key(c):
            g = c["group"]
            if g == "Bóng đá":
                return 0
            if g == "Bóng rổ":
                return 1
            if g == "Lịch thi đấu":
                return 2
            return 3
        channels.sort(key=lambda c: (group_sort_key(c), c["room_num"]))
        log.info("total channels: %d (live=%d + sport=%d)",
                 len(channels), len(live_channels), len(match_chan))
    except APIError as exc:
        log.error("%s", exc)
        return 1

    atomic_write(args.out, iptv_json_bytes(channels, args.tvg_logo))
    if args.m3u_out:
        atomic_write(args.m3u_out, m3u_bytes(channels, args.epg, args.tvg_logo))

    for err in errors[:5]:
        log.warning("%s", err)
    if len(errors) > 5:
        log.warning("... and %d more room errors", len(errors) - 5)
    log.info("channels with stream: %d", len(channels))

    if not args.publish:
        return 0

    token = (args.github_token or "").strip()
    if not token:
        log.error("--publish needs --github-token or $GITHUB_TOKEN")
        return 1

    items = iptv_items(channels, args.tvg_logo)
    fp_full = fingerprint_items(items)
    fp_short = fp_full[:16]
    gh = GitHubClient(token, args.github_repo.strip("/"), args.github_branch)

    remote_fp, published_at = gh.fetch_remote_state(args.github_path)

    if not items and remote_fp:
        log.error("scan returned no channels; keeping previous playlist")
        return 1

    force_seconds = max(0.0, args.force_publish_hours) * 3600
    changed = remote_fp is None or fp_short != remote_fp
    expired = (force_seconds > 0 and
               (published_at is None or
                (datetime.now(timezone.utc) - published_at).total_seconds()
                >= force_seconds))
    if not changed and not expired:
        age = ((datetime.now(timezone.utc) - published_at).total_seconds()
               if published_at else 0)
        log.info("no changes since last publish (%ds ago); skip GitHub commit",
                 int(age))
        return 0
    reason = "content changed" if changed else "token refresh"

    m3u_path = (args.github_path.rsplit(".", 1)[0] + ".m3u"
                if "." in args.github_path else args.github_path + ".m3u")

    url = gh.publish(args.github_path, iptv_json_bytes(channels, args.tvg_logo),
                     fp_short)
    log.info("published to GitHub (%s): %s", reason, url)
    url = gh.publish(m3u_path, m3u_bytes(channels, args.epg, args.tvg_logo),
                     fp_short)
    log.info("published to GitHub (%s): %s", reason, url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
