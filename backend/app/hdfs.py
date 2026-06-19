"""
Minimal WebHDFS read client for the AirSight backend.

Reads JSON (one object per line, as written by Spark `write.json`) straight from
the HDFS gold layer over the WebHDFS REST API (port 9870) — no Hadoop client
library required, same approach as kafka/consumers/hdfs_consumer.py.
"""
import os
import json
import logging

import requests

log = logging.getLogger("backend.hdfs")

HDFS_HOST = os.getenv("HDFS_NAMENODE_HOST", "namenode")
HDFS_PORT = os.getenv("HDFS_NAMENODE_PORT", "9870")
WEBHDFS_BASE = f"http://{HDFS_HOST}:{HDFS_PORT}/webhdfs/v1"


def list_dir(path: str) -> list:
    """Return the list of FileStatus dicts under a directory (empty if missing)."""
    url = f"{WEBHDFS_BASE}{path}?op=LISTSTATUS"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return r.json().get("FileStatuses", {}).get("FileStatus", [])
    except Exception as e:
        log.warning("LISTSTATUS failed for %s: %s", path, e)
        return []


def read_file(path: str) -> str:
    """Read a file's full content as text via WebHDFS OPEN (follows datanode redirect)."""
    url = f"{WEBHDFS_BASE}{path}?op=OPEN"
    r = requests.get(url, timeout=30, allow_redirects=True)
    r.raise_for_status()
    return r.text


def read_json_dir(path: str) -> list:
    """
    Read every Spark `part-*.json` file in a directory and return all parsed
    JSON objects (one per line). Returns [] if the directory is missing/empty.
    """
    rows = []
    for status in list_dir(path):
        name = status.get("pathSuffix", "")
        if not name.startswith("part-") or not name.endswith(".json"):
            continue
        try:
            content = read_file(f"{path}/{name}")
        except Exception as e:
            log.warning("Failed reading %s/%s: %s", path, name, e)
            continue
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                log.warning("Skipping malformed JSON line in %s/%s", path, name)
    return rows
