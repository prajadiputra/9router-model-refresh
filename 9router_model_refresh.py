#!/usr/bin/env python3
"""9Router Model Refresh — auto-sync custom provider model lists.

Fetch /v1/models from every active openai-compatible custom provider in 9Router,
diff against the cached model list (kv table, scope customModels), then:
  - ADD newly available model IDs
  - REMOVE model IDs that no longer exist upstream
  - PRUNE stale model references from combos.models

Runs entirely OUTSIDE the 9Router app (no files under /usr/local/lib/node_modules/9router
are touched), so it survives 9Router updates. Only reads/writes the SQLite DB.

Pattern modeled after OmniRoute's auto-sync (github.com/diegosouzapw/OmniRoute#5444):
fetch upstream /v1/models → diff → apply adds/removes → prune combos → log changes.
Failed provider syncs are logged and skipped; they never block other providers.

Usage:
    python3 9router_model_refresh.py            # one sync pass
    # cron: 0 * * * * python3 .../9router_model_refresh.py >> /var/log/9router-sync.log 2>&1
"""

import os
import sys
import json
import sqlite3
import urllib.request
import urllib.error
import time
from datetime import datetime, timezone

# ── config ──────────────────────────────────────────────────────────────────
DB = os.path.expanduser(os.environ.get("NINEROUTER_DB", "~/.9router/db/data.sqlite"))
BACKUP_DIR = os.path.expanduser(os.environ.get("NINEROUTER_BACKUP_DIR", "~/.9router/db/backups"))
BACKUP_FILE = os.path.join(BACKUP_DIR, "data.sqlite.pre-sync")
# Some upstreams (Cloudflare-fronted) 403 curl's default UA. Browser UA avoids it.
USER_AGENT = os.environ.get(
    "NINEROUTER_SYNC_UA",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)",
)
FETCH_TIMEOUT = int(os.environ.get("NINEROUTER_SYNC_TIMEOUT", "30"))


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", file=sys.stderr)


def backup() -> None:
    """WAL-safe single-snapshot backup (overwrite same file).

    Uses SQLite's online backup API, not file copy — safe while 9Router is live
    and writing to the WAL. Captures the pre-change state even if another
    connection has uncommitted writes.
    """
    os.makedirs(BACKUP_DIR, exist_ok=True)
    src = sqlite3.connect(DB)
    out = sqlite3.connect(BACKUP_FILE)
    with out:
        src.backup(out)
    out.close()
    src.close()
    log(f"backup → {BACKUP_FILE}")


def get_custom_providers(db: sqlite3.Connection) -> list[dict]:
    """Active openai-compatible custom providers: node config + connection auth."""
    providers = []
    for r in db.execute(
        """
        SELECT pn.id AS node_id, pn.name, pn.data AS node_data,
               pc.id AS conn_id, pc.data AS conn_data, pc.isActive
        FROM providerNodes pn
        JOIN providerConnections pc ON pn.id = pc.provider
        WHERE pn.type = 'openai-compatible' AND pc.isActive = 1
        """
    ):
        node_data = json.loads(r["node_data"])
        conn_data = json.loads(r["conn_data"])
        base_url = node_data.get("baseUrl", "").rstrip("/")
        api_key = conn_data.get("apiKey", "")
        if not base_url or not api_key:
            log(f"SKIP {r['name']}: missing baseUrl or apiKey")
            continue
        providers.append(
            {
                "node_id": r["node_id"],
                "name": r["name"],
                "prefix": node_data.get("prefix", r["name"]),
                "baseUrl": base_url,
                "connection_id": r["conn_id"],
                "apiKey": api_key,
            }
        )
    return providers


def fetch_upstream_models(provider: dict) -> list[str] | None:
    """Fetch GET {baseUrl}/models → list of model IDs. None on failure."""
    url = f"{provider['baseUrl']}/models"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {provider['apiKey']}", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
            data = json.loads(resp.read())
            models = data.get("data", data)
            if not isinstance(models, list):
                log(f"  {provider['name']}: unexpected response shape")
                return None
            return [m["id"] for m in models if isinstance(m, dict) and "id" in m]
    except urllib.error.HTTPError as e:
        log(f"  {provider['name']}: HTTP {e.code} {e.reason}")
        return None
    except Exception as e:
        log(f"  {provider['name']}: fetch error: {e}")
        return None


def get_cached_models(db: sqlite3.Connection, node_id: str) -> set[str]:
    """Model IDs cached in kv (scope customModels) for this provider node."""
    rows = db.execute(
        "SELECT key FROM kv WHERE scope='customModels' AND key LIKE ?",
        (f"{node_id}|%",),
    ).fetchall()
    return {r[0].split("|", 2)[1] for r in rows if "|" in r[0]}


def sync_provider(
    db: sqlite3.Connection, provider: dict, upstream_ids: list[str]
) -> dict:
    """Diff upstream vs cached; add/remove kv rows. Returns stats + removed ids."""
    node_id = provider["node_id"]
    name = provider["name"]
    cached = get_cached_models(db, node_id)
    upstream_set = set(upstream_ids)
    to_add = upstream_set - cached
    to_remove = cached - upstream_set

    added = 0
    for mid in to_add:
        key = f"{node_id}|{mid}|llm"
        value = json.dumps(
            {"providerAlias": node_id, "id": mid, "type": "llm", "name": mid}
        )
        db.execute(
            "INSERT OR REPLACE INTO kv (scope, key, value) VALUES (?, ?, ?)",
            ("customModels", key, value),
        )
        added += 1

    removed = 0
    for mid in to_remove:
        key = f"{node_id}|{mid}|llm"
        db.execute("DELETE FROM kv WHERE scope='customModels' AND key=?", (key,))
        removed += 1

    if added or removed:
        log(f"  {name}: +{added} -{removed} (cached: {len(cached)} → {len(upstream_set)})")
    else:
        log(f"  {name}: no change ({len(upstream_set)} models)")

    return {
        "name": name,
        "added": added,
        "removed": removed,
        "total": len(upstream_set),
        "removed_ids": to_remove,
    }


def prune_stale_combo_models(
    db: sqlite3.Connection, providers: list[dict], removed_by_node: dict
) -> None:
    """Remove pruned model ids (prefix/model-id) from combos.models."""
    prefix_removed = {}
    for p in providers:
        if p["node_id"] in removed_by_node and removed_by_node[p["node_id"]]:
            prefix_removed[p["prefix"]] = removed_by_node[p["node_id"]]
    if not prefix_removed:
        return

    db.row_factory = sqlite3.Row
    for r in db.execute("SELECT id, name, models FROM combos WHERE models IS NOT NULL"):
        models = json.loads(r["models"]) if r["models"] else []
        new_models = []
        removed_in_combo = []
        for m in models:
            if isinstance(m, str) and "/" in m:
                pref, mid = m.split("/", 1)
                if pref in prefix_removed and mid in prefix_removed[pref]:
                    removed_in_combo.append(m)
                    continue
            new_models.append(m)
        if removed_in_combo:
            db.execute(
                "UPDATE combos SET models=?, updatedAt=datetime('now') WHERE id=?",
                (json.dumps(new_models), r["id"]),
            )
            log(f"  combo '{r['name']}': pruned {removed_in_combo}")


def main() -> int:
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row

    providers = get_custom_providers(db)
    if not providers:
        log("no active custom providers found")
        db.close()
        return 0

    log(f"syncing {len(providers)} custom providers")

    any_change = False
    removed_by_node = {}

    for p in providers:
        log(f"  {p['name']} ({p['baseUrl']})")
        upstream = fetch_upstream_models(p)
        if upstream is None:
            log("    SKIP: fetch failed")
            continue
        result = sync_provider(db, p, upstream)
        if result["added"] > 0 or result["removed"] > 0:
            any_change = True
        if result["removed_ids"]:
            removed_by_node[p["node_id"]] = result["removed_ids"]

    if any_change:
        backup()
        prune_stale_combo_models(db, providers, removed_by_node)
        db.commit()
        log("committed")
    else:
        log("no changes, no commit")

    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
