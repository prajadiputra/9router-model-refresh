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
import re
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


# Context whitelist per-provider (verified ≥ 1M from official pricing pages)
# Sources: limitrouter.com/pricing, commandcode.ai/docs/plans/goat, openrouter.ai/api/v1/models
CONTEXT_WHITELIST_BY_PREFIX = {
    # limitrouter: https://limitrouter.com/pricing — kolom "Konteks"
    'limit': [
        # >=1M verified di LimitRouter
        ('deepseek-v4-flash', 'deepseek-v4-flash-0731', 'deepseek-v4-pro', 'deepseek-v4-pro-0813',
         'glm-5.2', 'glm-5.3', 'glm-5.3-flash', 'glm-5.2-fast', 'glm-5.2-prio',
         'claude-opus-4.7', 'claude-opus-4.8', 'claude-opus-5', 'claude-sonnet-5',
         'kimi-k3', 'kimi-k3-prio', 'kimi-k3-fast', 'qwen3.8-max', 'gpt-5.4',
         'minimax-m3', 'grok-4.5', 'grok-4.6', 'gemini-3.5-flash', 'gemini-3.6-flash', 'gemini-3.7-flash',
         'deepseek-v4-flash-0731-prio'),
        # <1M (excluded): glm-5 (200K), glm-5.1 (200K), gpt-5.5 (275K), gpt-5.6-luna/sol/terra (271K),
        # gemini-3.1-pro (300K!), claude-sonnet-4-6 (200K), claude-opus-4-6 (200K), grok-4.20 (300K), qwen3.7-plus (250K)
    ],
    # commandcode GOAT plan: https://commandcode.ai/docs/plans/goat — kolom "Context"
    'code': [
        # 1M verified
        ('glm-5.3', 'deepseek-v4-flash', 'deepseek-v4-flash-fast', 'deepseek-v4-flash-vision-exp',
         'deepseek-v4-pro', 'qwen3.8-max', 'kimi-k3', 'muse-spark-1.2', 'tencent-hy4-preview',
         'incling-small', 'qwen3.7-flash',  # tencent-hy3 exluded (262K)
         'gpt-5.6-luna', 'gpt-5.6-sol'),  # Luna/Sol = 1.1M context
        # <1M excluded: grok-4.5/4.6 (500K), kimi-k2.7-code (256K), laguna-s-2.1 (256K), inkling (256K)
    ],
}

def should_add_model(prefix: str, model_id: str) -> bool:
    """Return True if model passes the ≥1M context filter."""
    raw = CONTEXT_WHITELIST_BY_PREFIX.get(prefix, [])
    if not raw:
        return True  # unknown prefix → allow all (fallback)
    # whitelist entries may be nested tuples — flatten to a set of names
    flat: set[str] = set()
    for entry in raw:
        if isinstance(entry, (list, tuple)):
            flat.update(str(x) for x in entry)
        else:
            flat.add(str(entry))

    if model_id in flat:
        return True

    # Normalize lowercase + strip suffixes like -prio, -fast
    mid = model_id.lower()
    base = re.match(r'^([a-zA-Z0-9_-]+)', mid).group(1)
    if base in {x.lower() for x in flat}:
        return True

    # Not in the ≥1M whitelist for this provider → filtered out
    return False


# Update sync_provider to apply the ≥1M context filter at line 173
def sync_provider(
    db: sqlite3.Connection, provider: dict, upstream_ids: list[str]
) -> dict:
    """Diff upstream vs cached; add/remove kv rows. Returns stats + removed ids.
    Applies ≥1M context filter on ADD (should_add_model) — models <1M are skipped.
    """
    node_id = provider["node_id"]
    name = provider["name"]
    prefix = provider.get("prefix", "")
    cached = get_cached_models(db, node_id)
    upstream_set = set(upstream_ids)
    to_add = upstream_set - cached
    to_remove = cached - upstream_set

    added = 0
    added_ids = []
    filtered = 0
    filtered_ids = []
    for mid in to_add:
        if not should_add_model(prefix, mid):
            filtered += 1
            filtered_ids.append(mid)
            continue
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

    if added or removed or filtered:
        log(f"  {name}: +{added} -{removed} ~{filtered}filtered<1M (cached: {len(cached)} → {len(upstream_set)})")
    else:
        log(f"  {name}: no change ({len(upstream_set)} models)")
    if filtered:
        log(f"    filtered (context <1M, skipped): {filtered_ids[:15]}{'...' if len(filtered_ids) > 15 else ''}")

    return {
        "name": name,
        "added": added,
        "removed": removed,
        "total": len(upstream_set),
        "added_ids": added_ids,
        "removed_ids": removed_ids,
        "filtered_ids": filtered_ids,
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
        if not isinstance(models, list):
            continue
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
        if not isinstance(models, list):
            continue
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
    """Symmetric cleanup for providers that no longer exist in 9Router.

    A provider is "gone" when its node prefix / connection slug / kv prefix
    appears in NO active providerNodes/providerConnections and is NOT a known
    alias (oc/bzl etc. still reachable via kv of a live connection).

    For every gone provider we mirror what sync_provider does on model REMOVE:
      - DELETE its rows from kv (scope customModels)
      - emit model_remove events (same shape as sync_provider)
      - prune its refs from every combo.models
    This makes deleted-provider cleanup behave IDENTICALLY to a provider-side
    model removal — just ordered after live-provider sync.

    Returns count of (kv row + combo ref) removals.
    """
    # Active provider identities: node prefix + name, connection slug + name.
    # Deliberately NOT derived from kv — kv rows of a deleted provider would
    # otherwise make that provider look "known" and never get cleaned up.
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
    # Legacy aliases that are NOT openai-compatible nodes but are still wired
    # into combos (e.g. oc -> openrouter, bzl -> bazaarlink). They have no kv
    # row of their own naming the provider, so we preserve them explicitly.
    known.update(("oc", "bzl", "openrouter", "open-00", "open-01", "open-02",
                  "kimchi", "kiro", "ollama", "kilo", "kilo-gateway", "bazaarlink"))

    if not known:
        return 0

    db.row_factory = sqlite3.Row
    total = 0

    # 1) kv customModels rows whose provider identity is entirely gone
    gone_providers = {}
    for r in db.execute("SELECT key FROM kv WHERE scope='customModels'"):
        key = r["key"]
        # Key format: "<nodeId>|<modelId>|llm" — split into 3 parts
        parts = key.split("|", 2)
        if len(parts) < 3:
            continue
        pref, model, label = parts
        if not pref:
            continue
        # identity: prefer node id (uuid) — but aliases (oc/bzl) map by live prefix
        if pref in known:
            continue
        gone_providers.setdefault(pref, []).append(model)
    for pref, models in gone_providers.items():
        for mid in models:
            db.execute(
                "DELETE FROM kv WHERE scope='customModels' AND key LIKE ?",
                (f"{pref}|{mid}|%",),
            )
            emit_event({
                "type": "model_remove",
                "provider": pref,
                "prefix": pref,
                "model": mid,
            })
            total += 1
        if models:
            log(f"  {pref}: deleted-provider kv REMOVE {len(models)} models {models[:5]}")

    # 2) combo refs whose provider prefix is gone
    for r in db.execute("SELECT id, name, models FROM combos WHERE models IS NOT NULL"):
        try:
            models = json.loads(r["models"]) if r["models"] else []
        except (ValueError, TypeError):
            continue
        if not isinstance(models, list):
            continue
        new_models = []
        removed_in_combo = []
        for m in models:
            if isinstance(m, str) and "/" in m:
                pref, _mid = m.split("/", 1)
                if pref and pref not in known:
                    removed_in_combo.append(m)
                    total += 1
                    continue
            new_models.append(m)
        if removed_in_combo:
            db.execute(
                "UPDATE combos SET models=?, updatedAt=datetime('now') WHERE id=?",
                (json.dumps(new_models), r["id"]),
            )
            log(f"  combo '{r['name']}': pruned deleted-provider refs {removed_in_combo}")
    return total


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
