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
# JSONL event log consumed by 9router-live widget
EVENT_LOG = os.path.expanduser(os.environ.get("NINEROUTER_EVENT_LOG", "~/.9router/db/sync-events.jsonl"))
EVENT_MAX_LINES = int(os.environ.get("NINEROUTER_EVENT_MAX_LINES", "5000"))
# Some upstreams (Cloudflare-fronted) 403 curl's default UA. Browser UA avoids it.
USER_AGENT = os.environ.get(
    "NINEROUTER_SYNC_UA",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)",
)
FETCH_TIMEOUT = int(os.environ.get("NINEROUTER_SYNC_TIMEOUT", "30"))


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    ts = now_iso()
    print(f"[{ts}] {msg}", file=sys.stderr)


def emit_event(event: dict) -> None:
    """Append one JSONL event to the event log (read by 9router-live)."""
    try:
        event = dict(event)
        event.setdefault("ts", now_iso())
        with open(EVENT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
            f.flush()
        # trim old lines to EVENT_MAX_LINES
        if EVENT_MAX_LINES > 0:
            try:
                lines = open(EVENT_LOG, encoding="utf-8").readlines()
                if len(lines) > EVENT_MAX_LINES:
                    with open(EVENT_LOG, "w", encoding="utf-8") as f:
                        f.writelines(lines[-EVENT_MAX_LINES:])
            except OSError:
                pass
    except OSError as e:
        log(f"  event_log write failed: {e}")


def backup() -> None:
    """WAL-safe single-snapshot backup (overwrite same file).

    Uses SQLite's online backup API, not file copy — safe while 9Router is live
    and writing to the WAL. Captures the pre-change state even if another
    connection has uncommitted writes.
    """
    os.makedirs(BACKUP_DIR, exist_ok=True)
    # remove stale 0-byte file from a previous no-change run
    if os.path.exists(BACKUP_FILE) and os.path.getsize(BACKUP_FILE) == 0:
        os.remove(BACKUP_FILE)
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
    added_ids = []
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
        added_ids.append(mid)

    removed = 0
    removed_ids = []
    for mid in to_remove:
        key = f"{node_id}|{mid}|llm"
        db.execute("DELETE FROM kv WHERE scope='customModels' AND key=?", (key,))
        removed += 1
        removed_ids.append(mid)

    for mid in added_ids:
        emit_event({
            "type": "model_add",
            "provider": name,
            "prefix": provider["prefix"],
            "model": mid,
        })
    for mid in removed_ids:
        emit_event({
            "type": "model_remove",
            "provider": name,
            "prefix": provider["prefix"],
            "model": mid,
        })

    if added or removed:
        log(f"  {name}: +{added} -{removed} (cached: {len(cached)} → {len(upstream_set)})")
    else:
        log(f"  {name}: no change ({len(upstream_set)} models)")

    return {
        "name": name,
        "added": added,
        "removed": removed,
        "total": len(upstream_set),
        "added_ids": added_ids,
        "removed_ids": removed_ids,
    }


def reconcile_combo_orphans(db: sqlite3.Connection, providers: list[dict]) -> int:
    """Scan all combos — remove ANY model reference whose prefix is a known
    custom provider but whose model ID no longer exists in kv (scope customModels).

    This catches orphans from previous runs where the model was removed from kv
    but the combo reference survived (e.g. script updated mid-cycle, or manual
    kv edit).  Returns count of pruned references.
    """
    # Build prefix → set of valid model IDs from kv
    prefix_models: dict[str, set[str]] = {}
    for p in providers:
        prefix_models[p["prefix"]] = get_cached_models(db, p["node_id"])
    if not prefix_models:
        return 0

    db.row_factory = sqlite3.Row
    pruned_total = 0
    for r in db.execute("SELECT id, name, models FROM combos WHERE models IS NOT NULL"):
        models = json.loads(r["models"]) if r["models"] else []
        new_models = []
        removed_in_combo = []
        for m in models:
            if isinstance(m, str) and "/" in m:
                pref, mid = m.split("/", 1)
                if pref in prefix_models and mid not in prefix_models[pref]:
                    removed_in_combo.append(m)
                    pruned_total += 1
                    continue
            new_models.append(m)
        if removed_in_combo:
            db.execute(
                "UPDATE combos SET models=?, updatedAt=datetime('now') WHERE id=?",
                (json.dumps(new_models), r["id"]),
            )
            log(f"  combo '{r['name']}': pruned {removed_in_combo}")
    return pruned_total


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


def prune_deleted_provider_models(db: sqlite3.Connection) -> int:
    """Remove combo refs whose provider prefix is unknown everywhere.

    A prefix is "known" if it appears as:
      - a node prefix / provider slug in providerNodes / providerConnections, or
      - the first segment of ANY kv customModels key (covers aliases like oc, bzl).

    Only refs whose prefix matches NONE of those are true orphans of a deleted
    provider (e.g. `agentic/...` after the provider node was removed in the UI).
    Returns count of pruned references.
    """
    known = set()

    for r in db.execute("SELECT data, name FROM providerNodes"):
        try:
            d = json.loads(r["data"]) if r["data"] else {}
        except (ValueError, TypeError):
            d = {}
        known.add(d.get("prefix") or r["name"])
    for r in db.execute("SELECT provider, name FROM providerConnections"):
        known.add(r["provider"])
        known.add(r["name"])
    for r in db.execute("SELECT key FROM kv WHERE scope='customModels'"):
        first = r["key"].split("|", 1)[0]
        if first:
            known.add(first)

    if not known:
        return 0

    db.row_factory = sqlite3.Row
    pruned_total = 0
    for r in db.execute("SELECT id, name, models FROM combos WHERE models IS NOT NULL"):
        try:
            models = json.loads(r["models"]) if r["models"] else []
        except (ValueError, TypeError):
            continue
        new_models = []
        removed_in_combo = []
        for m in models:
            if isinstance(m, str) and "/" in m:
                pref, _mid = m.split("/", 1)
                if pref and pref not in known:
                    removed_in_combo.append(m)
                    pruned_total += 1
                    continue
            new_models.append(m)
        if removed_in_combo:
            db.execute(
                "UPDATE combos SET models=?, updatedAt=datetime('now') WHERE id=?",
                (json.dumps(new_models), r["id"]),
            )
            log(f"  combo '{r['name']}': pruned deleted-provider refs {removed_in_combo}")
    return pruned_total


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
            removed_by_node[p["node_id"]] = set(result["removed_ids"])

    if any_change:
        backup()
        prune_stale_combo_models(db, providers, removed_by_node)
        reconcile_combo_orphans(db, providers)
        prune_deleted_provider_models(db)
        db.commit()
        log("committed")
    else:
        # If nothing changed at provider level, still reconcile any combo orphans
        # that may have been left by a previous interrupted cycle.
        if reconcile_combo_orphans(db, providers) > 0 or prune_deleted_provider_models(db) > 0:
            db.commit()
            log("reconciled combo orphans (no provider change)")
        # clean up stale 0-byte backup file from a previous run
        if os.path.exists(BACKUP_FILE) and os.path.getsize(BACKUP_FILE) == 0:
            os.remove(BACKUP_FILE)
        log("no changes, no commit")

    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
