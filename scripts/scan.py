#!/usr/bin/env python3
"""PodPeek Scanner — Fetches YouTube transcripts and scans for keyword matches."""

import csv
import io
import json
import logging
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import WebshareProxyConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("podpeek")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
MATCHES_FILE = DATA_DIR / "matches.json"
PROCESSED_FILE = DATA_DIR / "processed.json"

MAX_MATCHES_PER_KEYWORD_PER_VIDEO = 5
MAX_RETRIES = 3
FETCH_DELAY = 1.5  # seconds between transcript fetches
SNIPPET_CONTEXT_CHARS = 150
YOUTUBE_RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={}"
GOOGLE_SHEET_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid}"
)


def load_config():
    """Load configuration from environment variables."""
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
    keywords_gid = os.environ.get("KEYWORDS_GID", "")
    rolling_days = int(os.environ.get("ROLLING_DAYS", "30"))
    webshare_user = os.environ.get("WEBSHARE_USER", "")
    webshare_pass = os.environ.get("WEBSHARE_PASS", "")

    if not sheet_id:
        log.error("GOOGLE_SHEET_ID environment variable is required")
        sys.exit(1)
    if not keywords_gid:
        log.error("KEYWORDS_GID environment variable is required")
        sys.exit(1)

    return {
        "sheet_id": sheet_id,
        "keywords_gid": keywords_gid,
        "rolling_days": rolling_days,
        "webshare_user": webshare_user,
        "webshare_pass": webshare_pass,
    }


def fetch_google_sheet(sheet_id, gid="0"):
    """Fetch a published Google Sheet tab as CSV and return list of dicts."""
    url = GOOGLE_SHEET_CSV_URL.format(sheet_id=sheet_id, gid=gid)
    log.info("Fetching Google Sheet gid=%s", gid)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    return list(reader)


def fetch_channel_videos(channel_id):
    """Fetch latest videos from a YouTube channel RSS feed."""
    url = YOUTUBE_RSS_URL.format(channel_id)
    log.info("Fetching RSS for channel %s", channel_id)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning("RSS fetch failed for channel %s: %s", channel_id, e)
        return []

    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
        "media": "http://search.yahoo.com/mrss/",
    }
    root = ET.fromstring(resp.text)
    videos = []
    for entry in root.findall("atom:entry", ns):
        video_id_el = entry.find("yt:videoId", ns)
        title_el = entry.find("atom:title", ns)
        published_el = entry.find("atom:published", ns)
        if video_id_el is not None:
            videos.append(
                {
                    "video_id": video_id_el.text,
                    "title": title_el.text if title_el is not None else "",
                    "published": published_el.text if published_el is not None else "",
                }
            )
    log.info("Found %d videos for channel %s", len(videos), channel_id)
    return videos


def build_ytt_api(config):
    """Build a YouTubeTranscriptApi instance, optionally with proxy."""
    if config["webshare_user"] and config["webshare_pass"]:
        log.info("Using Webshare proxy for transcript fetches")
        proxy = WebshareProxyConfig(
            username=config["webshare_user"],
            password=config["webshare_pass"],
        )
        return YouTubeTranscriptApi(proxy_config=proxy)
    return YouTubeTranscriptApi()


def fetch_transcript(ytt_api, video_id):
    """Fetch English transcript segments for a video. Returns (segments, status)."""
    from youtube_transcript_api import (
        TranscriptsDisabled,
        NoTranscriptFound,
        RequestBlocked,
        VideoUnavailable,
    )

    try:
        fetched = ytt_api.fetch(video_id, languages=["en"])
        segments = [
            {
                "text": snippet.text,
                "start": snippet.start,
                "duration": snippet.duration,
            }
            for snippet in fetched
        ]
        return segments, "scanned"
    except TranscriptsDisabled:
        log.info("Transcripts disabled for %s", video_id)
        return None, "no_transcript"
    except NoTranscriptFound:
        log.info("No English transcript for %s", video_id)
        return None, "no_transcript"
    except RequestBlocked:
        log.warning("Request blocked for %s", video_id)
        return None, "blocked"
    except VideoUnavailable:
        log.info("Video unavailable: %s", video_id)
        return None, "failed"
    except Exception as e:
        log.warning("Transcript fetch error for %s: %s", video_id, e)
        return None, "failed"


def extract_snippet(segments, target_idx, term):
    """Extract ~300 chars of context around a keyword match."""
    start = max(0, target_idx - 2)
    end = min(len(segments), target_idx + 3)
    combined = " ".join(seg["text"] for seg in segments[start:end])

    # Find the keyword position (case-insensitive)
    match = re.search(re.escape(term), combined, re.IGNORECASE)
    if match:
        center = match.start()
        snip_start = max(0, center - SNIPPET_CONTEXT_CHARS)
        snip_end = min(len(combined), center + len(term) + SNIPPET_CONTEXT_CHARS)
        snippet = combined[snip_start:snip_end]
        if snip_start > 0:
            snippet = "..." + snippet
        if snip_end < len(combined):
            snippet = snippet + "..."
        return snippet
    return combined[:300]


def make_keyword_slug(term):
    """Convert a keyword term to a URL-safe slug."""
    return re.sub(r"[^a-z0-9]+", "-", term.lower()).strip("-")


def scan_for_keywords(segments, keywords, video_id):
    """Scan transcript segments for keyword matches. Returns list of match dicts."""
    matches = []
    for kw in keywords:
        term = kw["term"].strip()
        if not term:
            continue
        match_type = kw.get("match_type", "phrase").strip().lower()
        category = kw.get("category", "").strip()

        if match_type == "word":
            pattern = re.compile(r"\b" + re.escape(term) + r"\b", re.IGNORECASE)
        else:
            pattern = re.compile(re.escape(term), re.IGNORECASE)

        kw_matches = 0
        for i, seg in enumerate(segments):
            if kw_matches >= MAX_MATCHES_PER_KEYWORD_PER_VIDEO:
                break
            if pattern.search(seg["text"]):
                snippet = extract_snippet(segments, i, term)
                ts = seg["start"]
                match_id = f"{video_id}_{make_keyword_slug(term)}_{int(ts)}"
                matches.append(
                    {
                        "id": match_id,
                        "keyword": term,
                        "category": category,
                        "timestamp_seconds": ts,
                        "snippet": snippet,
                        "segment_index": i,
                    }
                )
                kw_matches += 1
    return matches


def load_json(path, default):
    """Load a JSON file or return default if missing/invalid."""
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log.warning("Failed to load %s: %s — using default", path, e)
    return default


def save_json(path, data):
    """Write data to a JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log.info("Wrote %s", path)


def main():
    config = load_config()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=config["rolling_days"])

    # Load existing data
    matches_data = load_json(
        MATCHES_FILE,
        {"generated_at": "", "scan_version": 1, "matches": []},
    )
    processed_data = load_json(
        PROCESSED_FILE,
        {"last_updated": "", "videos": {}},
    )

    existing_matches = matches_data.get("matches", [])
    existing_ids = {m["id"] for m in existing_matches}
    processed_videos = processed_data.get("videos", {})

    # Fetch config from Google Sheet
    try:
        podcasts = fetch_google_sheet(config["sheet_id"], gid="0")
        keywords = fetch_google_sheet(config["sheet_id"], gid=config["keywords_gid"])
    except requests.RequestException as e:
        log.error("Failed to fetch Google Sheet: %s", e)
        sys.exit(1)

    active_podcasts = [
        p for p in podcasts if p.get("active", "").strip().upper() == "TRUE"
    ]
    log.info(
        "Loaded %d active podcasts and %d keywords",
        len(active_podcasts),
        len(keywords),
    )

    if not active_podcasts:
        log.warning("No active podcasts found in sheet")
    if not keywords:
        log.warning("No keywords found in sheet")

    # Build transcript API
    ytt_api = build_ytt_api(config)
    new_matches = []

    for podcast in active_podcasts:
        channel_id = podcast.get("channel_id", "").strip()
        podcast_name = podcast.get("name", "").strip()
        if not channel_id:
            log.warning("Skipping podcast with no channel_id: %s", podcast_name)
            continue

        videos = fetch_channel_videos(channel_id)

        for video in videos:
            vid = video["video_id"]
            prev = processed_videos.get(vid, {})
            prev_status = prev.get("status", "")
            prev_retries = prev.get("retry_count", 0)

            # Skip if already scanned successfully
            if prev_status == "scanned":
                continue
            # Skip permanently failed (exceeded retries, not blocked)
            if prev_status in ("no_transcript", "failed") and prev_retries >= MAX_RETRIES:
                continue
            # Always retry blocked status

            log.info("Processing: %s — %s", podcast_name, video["title"])
            time.sleep(FETCH_DELAY)

            segments, status = fetch_transcript(ytt_api, vid)

            if segments:
                video_matches = scan_for_keywords(segments, keywords, vid)
                for m in video_matches:
                    if m["id"] not in existing_ids:
                        m.update(
                            {
                                "video_id": vid,
                                "podcast_name": podcast_name,
                                "channel_id": channel_id,
                                "episode_title": video["title"],
                                "published": video["published"],
                                "youtube_url": f"https://www.youtube.com/watch?v={vid}&t={int(m['timestamp_seconds'])}",
                                "scanned_at": now.isoformat(),
                            }
                        )
                        del m["segment_index"]
                        new_matches.append(m)
                        existing_ids.add(m["id"])
                match_count = len(video_matches)
            else:
                match_count = 0

            processed_videos[vid] = {
                "channel_id": channel_id,
                "title": video["title"],
                "published": video["published"],
                "scanned_at": now.isoformat(),
                "status": status,
                "match_count": match_count,
                "retry_count": prev_retries + (0 if status == "scanned" else 1),
            }

    # Merge and prune
    all_matches = existing_matches + new_matches
    pruned = [
        m
        for m in all_matches
        if datetime.fromisoformat(m["published"].replace("Z", "+00:00")) > cutoff
    ]

    log.info(
        "Matches: %d existing + %d new = %d total, %d after pruning",
        len(existing_matches),
        len(new_matches),
        len(all_matches),
        len(pruned),
    )

    # Sort by published date descending
    pruned.sort(key=lambda m: m.get("published", ""), reverse=True)

    # Save
    matches_data["generated_at"] = now.isoformat()
    matches_data["matches"] = pruned
    save_json(MATCHES_FILE, matches_data)

    processed_data["last_updated"] = now.isoformat()
    processed_data["videos"] = processed_videos
    save_json(PROCESSED_FILE, processed_data)

    log.info("Scan complete. %d new matches found.", len(new_matches))


if __name__ == "__main__":
    main()
