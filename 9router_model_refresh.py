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


# ── context ≥1M model filter ────────────────────────────────────────────────
# GLOBAL_CONTEXT_BY_NAME: bare model-name (lowercase) → context window (tokens),
# aggregated from openrouter.ai/api/v1/models + provider pricing pages.
# Sourced at build time (roughly 848 entries, 294 with context >= 1M).

GLOBAL_CONTEXT_BY_NAME = {
    'aion-2.0': 131072,
    'aion-3.0': 131072,
    'aion-3.0-mini': 131072,
    'aion-labs/aion-2.0': 131072,
    'aion-labs/aion-3.0': 131072,
    'aion-labs/aion-3.0-mini': 131072,
    'aion-labs/aion-rp-llama-3.1-8b': 32768,
    'aion-rp-llama-3.1-8b': 32768,
    'amazon/nova-2-lite-v1': 1000000,
    'amazon/nova-lite-v1': 300000,
    'amazon/nova-micro-v1': 128000,
    'amazon/nova-premier-v1': 1000000,
    'amazon/nova-pro-v1': 300000,
    'anthracite-org/magnum-v4-72b': 32768,
    'anthropic/claude-3-haiku': 200000,
    'anthropic/claude-fable-5': 1000000,
    'anthropic/claude-fable-5.1': 1000000,
    'anthropic/claude-fable-5:batch': 1000000,
    'anthropic/claude-haiku-4.5': 200000,
    'anthropic/claude-haiku-4.5:batch': 200000,
    'anthropic/claude-opus-4': 200000,
    'anthropic/claude-opus-4.1': 200000,
    'anthropic/claude-opus-4.1:batch': 200000,
    'anthropic/claude-opus-4.5': 200000,
    'anthropic/claude-opus-4.5:batch': 200000,
    'anthropic/claude-opus-4.6': 1000000,
    'anthropic/claude-opus-4.6:batch': 1000000,
    'anthropic/claude-opus-4.7': 1000000,
    'anthropic/claude-opus-4.7:batch': 1000000,
    'anthropic/claude-opus-4.8': 1000000,
    'anthropic/claude-opus-4.8:batch': 1000000,
    'anthropic/claude-opus-5': 1000000,
    'anthropic/claude-opus-5:batch': 1000000,
    'anthropic/claude-sonnet-4': 1000000,
    'anthropic/claude-sonnet-4.5': 1000000,
    'anthropic/claude-sonnet-4.5:batch': 1000000,
    'anthropic/claude-sonnet-4.6': 1000000,
    'anthropic/claude-sonnet-4.6:batch': 1000000,
    'anthropic/claude-sonnet-5': 1000000,
    'anthropic/claude-sonnet-5:batch': 1000000,
    'arcee-ai/trinity-large-thinking': 262144,
    'auto': 2000000,
    'auto-beta': 2000000,
    'baidu/ernie-4.5-vl-424b-a47b': 123000,
    'bodybuilder': 128000,
    'bytedance-seed/seed-1.6': 262144,
    'bytedance-seed/seed-1.6-flash': 262144,
    'bytedance-seed/seed-2-1-turbo': 262144,
    'bytedance-seed/seed-2.0-code': 262144,
    'bytedance-seed/seed-2.0-lite': 262144,
    'bytedance-seed/seed-2.0-mini': 262144,
    'bytedance/ui-tars-1.5-7b': 128000,
    'claude-3-haiku': 200000,
    'claude-fable-5': 1000000,
    'claude-fable-5.1': 1000000,
    'claude-fable-5:batch': 1000000,
    'claude-fable-latest': 1000000,
    'claude-haiku-4.5': 200000,
    'claude-haiku-4.5:batch': 200000,
    'claude-haiku-latest': 200000,
    'claude-opus-4': 200000,
    'claude-opus-4.1': 200000,
    'claude-opus-4.1:batch': 200000,
    'claude-opus-4.5': 200000,
    'claude-opus-4.5:batch': 200000,
    'claude-opus-4.6': 1000000,
    'claude-opus-4.6:batch': 1000000,
    'claude-opus-4.7': 1000000,
    'claude-opus-4.7:batch': 1000000,
    'claude-opus-4.8': 1000000,
    'claude-opus-4.8:batch': 1000000,
    'claude-opus-5': 1000000,
    'claude-opus-5:batch': 1000000,
    'claude-opus-latest': 1000000,
    'claude-sonnet-4': 1000000,
    'claude-sonnet-4.5': 1000000,
    'claude-sonnet-4.5:batch': 1000000,
    'claude-sonnet-4.6': 1000000,
    'claude-sonnet-4.6:batch': 1000000,
    'claude-sonnet-5': 1000000,
    'claude-sonnet-5:batch': 1000000,
    'claude-sonnet-latest': 1000000,
    'codestral-2508': 256000,
    'cognitivecomputations/dolphin-mistral-24b-venice-edition': 128000,
    'cohere/command-a': 256000,
    'cohere/command-r-08-2024': 128000,
    'cohere/command-r-plus-08-2024': 128000,
    'cohere/command-r7b-12-2024': 128000,
    'cohere/north-mini-code:free': 256000,
    'command-a': 256000,
    'command-r-08-2024': 128000,
    'command-r-plus-08-2024': 128000,
    'command-r7b-12-2024': 128000,
    'cydonia-24b-v4.1': 131072,
    'deepseek-chat': 163840,
    'deepseek-chat-v3-0324': 163840,
    'deepseek-chat-v3.1': 163840,
    'deepseek-r1': 64000,
    'deepseek-r1-0528': 163840,
    'deepseek-r1-distill-llama-70b': 8192,
    'deepseek-v3.1-terminus': 163840,
    'deepseek-v3.2': 163840,
    'deepseek-v3.2-exp': 163840,
    'deepseek-v4-flash': 1048576,
    'deepseek-v4-flash-0731': 1310720,
    'deepseek-v4-flash-0731-prio': 1000000,
    'deepseek-v4-flash-0731:batch': 1048576,
    'deepseek-v4-flash-fast': 1000000,
    'deepseek-v4-flash-latest': 1310720,
    'deepseek-v4-flash-vision-exp': 1048576,
    'deepseek-v4-pro': 1048576,
    'deepseek-v4-pro-0813': 1048576,
    'deepseek-v4-pro-0813:batch': 1048576,
    'deepseek-v4-pro-prio': 1000000,
    'deepseek/deepseek-chat': 163840,
    'deepseek/deepseek-chat-v3-0324': 163840,
    'deepseek/deepseek-chat-v3.1': 163840,
    'deepseek/deepseek-r1': 64000,
    'deepseek/deepseek-r1-0528': 163840,
    'deepseek/deepseek-r1-distill-llama-70b': 8192,
    'deepseek/deepseek-v3.1-terminus': 163840,
    'deepseek/deepseek-v3.2': 163840,
    'deepseek/deepseek-v3.2-exp': 163840,
    'deepseek/deepseek-v4-flash': 1048576,
    'deepseek/deepseek-v4-flash-0731': 1310720,
    'deepseek/deepseek-v4-flash-0731:batch': 1048576,
    'deepseek/deepseek-v4-flash-vision-exp': 1048576,
    'deepseek/deepseek-v4-pro': 1048576,
    'deepseek/deepseek-v4-pro-0813': 1048576,
    'deepseek/deepseek-v4-pro-0813:batch': 1048576,
    'devstral-2512': 262144,
    'dolphin-mistral-24b-venice-edition': 128000,
    'dots-3-note-preview:free': 512000,
    'dots-studio/dots-3-note-preview:free': 512000,
    'ernie-4.5-vl-424b-a47b': 123000,
    'free': 200000,
    'fugu-ultra': 1000000,
    'fusion': 1000000,
    'gemini-2.5-flash': 1048576,
    'gemini-2.5-flash-image': 32768,
    'gemini-2.5-flash-lite': 1048576,
    'gemini-2.5-flash-lite:batch': 1048576,
    'gemini-2.5-flash:batch': 1048576,
    'gemini-2.5-pro': 1048576,
    'gemini-2.5-pro-preview': 1048576,
    'gemini-2.5-pro-preview-05-06': 1048576,
    'gemini-2.5-pro:batch': 1048576,
    'gemini-3-flash-preview': 1048576,
    'gemini-3-flash-preview:batch': 1048576,
    'gemini-3-pro-image': 131072,
    'gemini-3-pro-image-preview': 65536,
    'gemini-3.1-flash-image': 131072,
    'gemini-3.1-flash-image-preview': 65536,
    'gemini-3.1-flash-lite': 1048576,
    'gemini-3.1-flash-lite-image': 65536,
    'gemini-3.1-flash-lite-preview': 1048576,
    'gemini-3.1-flash-lite:batch': 1048576,
    'gemini-3.1-pro-preview': 1048576,
    'gemini-3.1-pro-preview-customtools': 1048576,
    'gemini-3.1-pro-preview:batch': 1048576,
    'gemini-3.5-flash': 1048576,
    'gemini-3.5-flash-lite': 1048576,
    'gemini-3.5-flash-lite:batch': 1048576,
    'gemini-3.5-flash:batch': 1048576,
    'gemini-3.6-flash': 1048576,
    'gemini-3.6-flash:batch': 1048576,
    'gemini-3.7-flash': 1048576,
    'gemini-3.7-flash:batch': 1048576,
    'gemini-flash-latest': 1048576,
    'gemini-pro-latest': 1048576,
    'gemma-2-27b-it': 8192,
    'gemma-3-12b-it': 131072,
    'gemma-3-27b-it': 131072,
    'gemma-3-4b-it': 131072,
    'gemma-4-26b-a4b-it': 262144,
    'gemma-4-26b-a4b-it:free': 262144,
    'gemma-4-31b-it': 262144,
    'gemma-4-31b-it:batch': 262144,
    'gemma-4-31b-it:free': 262144,
    'glm-4.5': 131072,
    'glm-4.5-air': 131072,
    'glm-4.5v': 65536,
    'glm-4.6': 204800,
    'glm-4.6v': 131072,
    'glm-4.7': 204800,
    'glm-4.7-flash': 202752,
    'glm-5': 204800,
    'glm-5-turbo': 202752,
    'glm-5.1': 204800,
    'glm-5.2': 1048576,
    'glm-5.2-fast': 1000000,
    'glm-5.2-prio': 1000000,
    'glm-5.2:free': 256000,
    'glm-5.3': 1310720,
    'glm-5.3-flash': 1310720,
    'glm-5.3-flash:batch': 1048575,
    'glm-5v-turbo': 202752,
    'glm-flash-latest': 1310720,
    'glm-latest': 1310720,
    'google/gemini-2.5-flash': 1048576,
    'google/gemini-2.5-flash-image': 32768,
    'google/gemini-2.5-flash-lite': 1048576,
    'google/gemini-2.5-flash-lite:batch': 1048576,
    'google/gemini-2.5-flash:batch': 1048576,
    'google/gemini-2.5-pro': 1048576,
    'google/gemini-2.5-pro-preview': 1048576,
    'google/gemini-2.5-pro-preview-05-06': 1048576,
    'google/gemini-2.5-pro:batch': 1048576,
    'google/gemini-3-flash-preview': 1048576,
    'google/gemini-3-flash-preview:batch': 1048576,
    'google/gemini-3-pro-image': 131072,
    'google/gemini-3-pro-image-preview': 65536,
    'google/gemini-3.1-flash-image': 131072,
    'google/gemini-3.1-flash-image-preview': 65536,
    'google/gemini-3.1-flash-lite': 1048576,
    'google/gemini-3.1-flash-lite-image': 65536,
    'google/gemini-3.1-flash-lite-preview': 1048576,
    'google/gemini-3.1-flash-lite:batch': 1048576,
    'google/gemini-3.1-pro-preview': 1048576,
    'google/gemini-3.1-pro-preview-customtools': 1048576,
    'google/gemini-3.1-pro-preview:batch': 1048576,
    'google/gemini-3.5-flash': 1048576,
    'google/gemini-3.5-flash-lite': 1048576,
    'google/gemini-3.5-flash-lite:batch': 1048576,
    'google/gemini-3.5-flash:batch': 1048576,
    'google/gemini-3.6-flash': 1048576,
    'google/gemini-3.6-flash:batch': 1048576,
    'google/gemini-3.7-flash': 1048576,
    'google/gemini-3.7-flash:batch': 1048576,
    'google/gemma-2-27b-it': 8192,
    'google/gemma-3-12b-it': 131072,
    'google/gemma-3-27b-it': 131072,
    'google/gemma-3-4b-it': 131072,
    'google/gemma-4-26b-a4b-it': 262144,
    'google/gemma-4-26b-a4b-it:free': 262144,
    'google/gemma-4-31b-it': 262144,
    'google/gemma-4-31b-it:batch': 262144,
    'google/gemma-4-31b-it:free': 262144,
    'google/lyria-3-clip-preview': 1048576,
    'google/lyria-3-pro-preview': 1048576,
    'gpt-3.5-turbo': 16385,
    'gpt-3.5-turbo-0613': 4095,
    'gpt-3.5-turbo-16k': 16385,
    'gpt-3.5-turbo-instruct': 4095,
    'gpt-3.5-turbo:batch': 16385,
    'gpt-4': 8191,
    'gpt-4-turbo': 128000,
    'gpt-4-turbo-preview': 128000,
    'gpt-4-turbo:batch': 128000,
    'gpt-4.1': 1047576,
    'gpt-4.1-mini': 1047576,
    'gpt-4.1-mini:batch': 1047576,
    'gpt-4.1-nano': 1047576,
    'gpt-4.1-nano:batch': 1047576,
    'gpt-4.1:batch': 1047576,
    'gpt-4o': 128000,
    'gpt-4o-2024-05-13': 128000,
    'gpt-4o-2024-08-06': 128000,
    'gpt-4o-2024-11-20': 128000,
    'gpt-4o-mini': 128000,
    'gpt-4o-mini-2024-07-18': 128000,
    'gpt-4o-mini:batch': 128000,
    'gpt-4o:batch': 128000,
    'gpt-5': 400000,
    'gpt-5-image': 400000,
    'gpt-5-image-mini': 400000,
    'gpt-5-mini': 400000,
    'gpt-5-mini:batch': 400000,
    'gpt-5-nano': 400000,
    'gpt-5-nano:batch': 400000,
    'gpt-5-pro': 400000,
    'gpt-5-pro:batch': 400000,
    'gpt-5.1': 400000,
    'gpt-5.1-codex': 400000,
    'gpt-5.1-codex-max': 400000,
    'gpt-5.1-codex-mini': 400000,
    'gpt-5.1:batch': 400000,
    'gpt-5.2': 400000,
    'gpt-5.2-chat': 128000,
    'gpt-5.2-codex': 400000,
    'gpt-5.2-pro': 400000,
    'gpt-5.2-pro:batch': 400000,
    'gpt-5.2:batch': 400000,
    'gpt-5.3-codex': 400000,
    'gpt-5.4': 1050000,
    'gpt-5.4-image-2': 272000,
    'gpt-5.4-mini': 400000,
    'gpt-5.4-mini:batch': 400000,
    'gpt-5.4-nano': 400000,
    'gpt-5.4-nano:batch': 400000,
    'gpt-5.4-pro': 1050000,
    'gpt-5.4-pro:batch': 1050000,
    'gpt-5.4:batch': 1050000,
    'gpt-5.5': 1050000,
    'gpt-5.5-pro': 1050000,
    'gpt-5.5-pro:batch': 1050000,
    'gpt-5.5:batch': 1050000,
    'gpt-5.6-luna': 1050000,
    'gpt-5.6-luna-pro': 1050000,
    'gpt-5.6-luna-pro:batch': 1050000,
    'gpt-5.6-luna:batch': 1050000,
    'gpt-5.6-sol': 1050000,
    'gpt-5.6-sol-pro': 1050000,
    'gpt-5.6-sol-pro:batch': 1050000,
    'gpt-5.6-sol:batch': 1050000,
    'gpt-5.6-terra': 1050000,
    'gpt-5.6-terra-pro': 1050000,
    'gpt-5.6-terra-pro:batch': 1050000,
    'gpt-5.6-terra:batch': 1050000,
    'gpt-5:batch': 400000,
    'gpt-audio': 128000,
    'gpt-audio-mini': 128000,
    'gpt-chat-latest': 400000,
    'gpt-latest': 1050000,
    'gpt-mini-latest': 400000,
    'gpt-oss-120b': 131072,
    'gpt-oss-120b:batch': 131072,
    'gpt-oss-20b': 131072,
    'gpt-oss-20b:batch': 131072,
    'gpt-oss-safeguard-20b': 131072,
    'granite-4.0-h-micro': 131000,
    'granite-4.1-8b': 131072,
    'granite-4.2-8b': 131072,
    'grok-4.20': 2000000,
    'grok-4.20-multi-agent': 2000000,
    'grok-4.3': 1000000,
    'grok-4.5': 500000,
    'grok-4.6': 500000,
    'grok-build-0.1': 256000,
    'grok-latest': 500000,
    'gryphe/mythomax-l2-13b': 8192,
    'hermes-3-llama-3.1-405b': 131072,
    'hermes-3-llama-3.1-70b': 131072,
    'hermes-4-405b': 131072,
    'hermes-4-70b': 131072,
    'hunyuan-a13b-instruct': 131072,
    'hy-mt2-1.8b': 8192,
    'hy-mt2-30b-a3b': 8192,
    'hy-mt2-7b': 8192,
    'hy3': 262144,
    'hy3-preview': 262144,
    'hy4-preview': 1048576,
    'ibm-granite/granite-4.0-h-micro': 131000,
    'ibm-granite/granite-4.1-8b': 131072,
    'ibm-granite/granite-4.2-8b': 131072,
    'inception/mercury-2': 128000,
    'inception/mercury-2.5-preview': 260000,
    'inclusionai/ling-3.0-flash': 262144,
    'inclusionai/ling-3.0-flash-fin:free': 262144,
    'inkling': 1048576,
    'inkling-small': 1048576,
    'inkling-small:batch': 524288,
    'inkling-small:free': 1048576,
    'inkling:batch': 524288,
    'inkling:free': 1048576,
    'kat-coder-pro-v2': 262144,
    'kat-coder-pro-v2.5': 262144,
    'kimi-k2': 131072,
    'kimi-k2-0905': 262144,
    'kimi-k2-thinking': 262144,
    'kimi-k2.5': 262144,
    'kimi-k2.6': 262144,
    'kimi-k2.7-code': 262144,
    'kimi-k3': 1048576,
    'kimi-k3-fast': 1000000,
    'kimi-k3-prio': 1000000,
    'kimi-k3:batch': 1048576,
    'kimi-latest': 1048576,
    'kwaipilot/kat-coder-pro-v2': 262144,
    'kwaipilot/kat-coder-pro-v2.5': 262144,
    'l3-lunaris-8b': 8192,
    'l3.1-euryale-70b': 131072,
    'l3.3-euryale-70b': 131072,
    'laguna-s-2.1': 1048576,
    'laguna-s-2.1:free': 262144,
    'laguna-xs-2.1': 262144,
    'laguna-xs-2.1:free': 262144,
    'lfm-2.5-2.6b:free': 65536,
    'ling-3.0-flash': 262144,
    'ling-3.0-flash-fin:free': 262144,
    'liquid/lfm-2.5-2.6b:free': 65536,
    'llama-3.1-70b-instruct': 131072,
    'llama-3.1-8b-instruct': 131072,
    'llama-3.2-1b-instruct': 60000,
    'llama-3.2-3b-instruct': 131072,
    'llama-3.3-70b-instruct': 131072,
    'llama-4-maverick': 1048576,
    'llama-4-scout': 1310720,
    'llama-guard-4-12b': 163840,
    'longcat-2.0': 1048756,
    'lyria-3-clip-preview': 1048576,
    'lyria-3-pro-preview': 1048576,
    'magnum-v4-72b': 32768,
    'mancer/weaver': 8000,
    'meituan/longcat-2.0': 1048756,
    'mercury-2': 128000,
    'mercury-2.5-preview': 260000,
    'meta-llama/llama-3.1-70b-instruct': 131072,
    'meta-llama/llama-3.1-8b-instruct': 131072,
    'meta-llama/llama-3.2-1b-instruct': 60000,
    'meta-llama/llama-3.2-3b-instruct': 131072,
    'meta-llama/llama-3.3-70b-instruct': 131072,
    'meta-llama/llama-4-maverick': 1048576,
    'meta-llama/llama-4-scout': 1310720,
    'meta-llama/llama-guard-4-12b': 163840,
    'meta/muse-glimmer-30b': 131072,
    'meta/muse-glimmer-30b:batch': 131072,
    'meta/muse-spark-1.1': 1048576,
    'meta/muse-spark-1.2': 1048576,
    'meta/muse-spark-1.2-contributor': 1048576,
    'microsoft/phi-4': 16384,
    'microsoft/wizardlm-2-8x22b': 65535,
    'mimo-v2.5': 1050000,
    'mimo-v2.5-pro': 1050000,
    'minimax-01': 1000192,
    'minimax-m1': 1000000,
    'minimax-m2': 204800,
    'minimax-m2-her': 65536,
    'minimax-m2.1': 204800,
    'minimax-m2.5': 204800,
    'minimax-m2.7': 204800,
    'minimax-m2.7:free': 196608,
    'minimax-m3': 1048576,
    'minimax-m3:batch': 524288,
    'minimax-m3:free': 1048576,
    'minimax/minimax-01': 1000192,
    'minimax/minimax-m1': 1000000,
    'minimax/minimax-m2': 204800,
    'minimax/minimax-m2-her': 65536,
    'minimax/minimax-m2.1': 204800,
    'minimax/minimax-m2.5': 204800,
    'minimax/minimax-m2.7': 204800,
    'minimax/minimax-m2.7:free': 196608,
    'minimax/minimax-m3': 1048576,
    'minimax/minimax-m3:batch': 524288,
    'minimax/minimax-m3:free': 1048576,
    'ministral-14b-2512': 262144,
    'ministral-3b-2512': 131072,
    'ministral-8b-2512': 262144,
    'mistral-large': 128000,
    'mistral-large-2407': 131072,
    'mistral-large-2512': 262144,
    'mistral-medium-3': 131072,
    'mistral-medium-3-5': 262144,
    'mistral-medium-3-5:batch': 262144,
    'mistral-medium-3.1': 131072,
    'mistral-nemo': 131072,
    'mistral-saba': 32768,
    'mistral-small-24b-instruct-2501': 32768,
    'mistral-small-2603': 262144,
    'mistral-small-3.1-24b-instruct': 128000,
    'mistral-small-3.2-24b-instruct': 131072,
    'mistralai/codestral-2508': 256000,
    'mistralai/devstral-2512': 262144,
    'mistralai/ministral-14b-2512': 262144,
    'mistralai/ministral-3b-2512': 131072,
    'mistralai/ministral-8b-2512': 262144,
    'mistralai/mistral-large': 128000,
    'mistralai/mistral-large-2407': 131072,
    'mistralai/mistral-large-2512': 262144,
    'mistralai/mistral-medium-3': 131072,
    'mistralai/mistral-medium-3-5': 262144,
    'mistralai/mistral-medium-3-5:batch': 262144,
    'mistralai/mistral-medium-3.1': 131072,
    'mistralai/mistral-nemo': 131072,
    'mistralai/mistral-saba': 32768,
    'mistralai/mistral-small-24b-instruct-2501': 32768,
    'mistralai/mistral-small-2603': 262144,
    'mistralai/mistral-small-3.1-24b-instruct': 128000,
    'mistralai/mistral-small-3.2-24b-instruct': 131072,
    'mistralai/mixtral-8x22b-instruct': 65536,
    'mistralai/voxtral-small-24b-2507': 32768,
    'mixtral-8x22b-instruct': 65536,
    'moonshotai/kimi-k2': 131072,
    'moonshotai/kimi-k2-0905': 262144,
    'moonshotai/kimi-k2-thinking': 262144,
    'moonshotai/kimi-k2.5': 262144,
    'moonshotai/kimi-k2.6': 262144,
    'moonshotai/kimi-k2.7-code': 262144,
    'moonshotai/kimi-k3': 1048576,
    'moonshotai/kimi-k3:batch': 1048576,
    'morph-v3-fast': 81920,
    'morph-v3-large': 262144,
    'morph/morph-v3-fast': 81920,
    'morph/morph-v3-large': 262144,
    'muse-glimmer-30b': 131072,
    'muse-glimmer-30b:batch': 131072,
    'muse-spark-1.1': 1048576,
    'muse-spark-1.2': 1048576,
    'muse-spark-1.2-contributor': 1048576,
    'mythomax-l2-13b': 8192,
    'nemotron-3-nano-30b-a3b': 262144,
    'nemotron-3-nano-omni-30b-a3b-reasoning:free': 256000,
    'nemotron-3-super-120b-a12b': 1000000,
    'nemotron-3-super-120b-a12b:free': 262144,
    'nemotron-3-ultra-550b-a55b': 262144,
    'nemotron-3-ultra-550b-a55b:batch': 512288,
    'nemotron-3-ultra-550b-a55b:free': 1000000,
    'nemotron-3.5-content-safety:free': 128000,
    'nemotron-3.5-lightning': 262144,
    'nemotron-3.5-lightning:free': 1000000,
    'nex-agi/nex-n2-mini': 262144,
    'nex-agi/nex-n2-pro': 262144,
    'nex-n2-mini': 262144,
    'nex-n2-pro': 262144,
    'north-mini-code:free': 256000,
    'nousresearch/hermes-3-llama-3.1-405b': 131072,
    'nousresearch/hermes-3-llama-3.1-70b': 131072,
    'nousresearch/hermes-4-405b': 131072,
    'nousresearch/hermes-4-70b': 131072,
    'nova-2-lite-v1': 1000000,
    'nova-lite-v1': 300000,
    'nova-micro-v1': 128000,
    'nova-premier-v1': 1000000,
    'nova-pro-v1': 300000,
    'nvidia/nemotron-3-nano-30b-a3b': 262144,
    'nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free': 256000,
    'nvidia/nemotron-3-super-120b-a12b': 1000000,
    'nvidia/nemotron-3-super-120b-a12b:free': 262144,
    'nvidia/nemotron-3-ultra-550b-a55b': 262144,
    'nvidia/nemotron-3-ultra-550b-a55b:batch': 512288,
    'nvidia/nemotron-3-ultra-550b-a55b:free': 1000000,
    'nvidia/nemotron-3.5-content-safety:free': 128000,
    'nvidia/nemotron-3.5-lightning': 262144,
    'nvidia/nemotron-3.5-lightning:free': 1000000,
    'o1': 200000,
    'o1-pro': 200000,
    'o3': 200000,
    'o3-mini': 200000,
    'o3-mini-high': 200000,
    'o3-mini:batch': 200000,
    'o3-pro': 200000,
    'o3:batch': 200000,
    'o4-mini': 200000,
    'o4-mini-high': 200000,
    'o4-mini:batch': 200000,
    'openai/gpt-3.5-turbo': 16385,
    'openai/gpt-3.5-turbo-0613': 4095,
    'openai/gpt-3.5-turbo-16k': 16385,
    'openai/gpt-3.5-turbo-instruct': 4095,
    'openai/gpt-3.5-turbo:batch': 16385,
    'openai/gpt-4': 8191,
    'openai/gpt-4-turbo': 128000,
    'openai/gpt-4-turbo-preview': 128000,
    'openai/gpt-4-turbo:batch': 128000,
    'openai/gpt-4.1': 1047576,
    'openai/gpt-4.1-mini': 1047576,
    'openai/gpt-4.1-mini:batch': 1047576,
    'openai/gpt-4.1-nano': 1047576,
    'openai/gpt-4.1-nano:batch': 1047576,
    'openai/gpt-4.1:batch': 1047576,
    'openai/gpt-4o': 128000,
    'openai/gpt-4o-2024-05-13': 128000,
    'openai/gpt-4o-2024-08-06': 128000,
    'openai/gpt-4o-2024-11-20': 128000,
    'openai/gpt-4o-mini': 128000,
    'openai/gpt-4o-mini-2024-07-18': 128000,
    'openai/gpt-4o-mini:batch': 128000,
    'openai/gpt-4o:batch': 128000,
    'openai/gpt-5': 400000,
    'openai/gpt-5-image': 400000,
    'openai/gpt-5-image-mini': 400000,
    'openai/gpt-5-mini': 400000,
    'openai/gpt-5-mini:batch': 400000,
    'openai/gpt-5-nano': 400000,
    'openai/gpt-5-nano:batch': 400000,
    'openai/gpt-5-pro': 400000,
    'openai/gpt-5-pro:batch': 400000,
    'openai/gpt-5.1': 400000,
    'openai/gpt-5.1-codex': 400000,
    'openai/gpt-5.1-codex-max': 400000,
    'openai/gpt-5.1-codex-mini': 400000,
    'openai/gpt-5.1:batch': 400000,
    'openai/gpt-5.2': 400000,
    'openai/gpt-5.2-chat': 128000,
    'openai/gpt-5.2-codex': 400000,
    'openai/gpt-5.2-pro': 400000,
    'openai/gpt-5.2-pro:batch': 400000,
    'openai/gpt-5.2:batch': 400000,
    'openai/gpt-5.3-codex': 400000,
    'openai/gpt-5.4': 1050000,
    'openai/gpt-5.4-image-2': 272000,
    'openai/gpt-5.4-mini': 400000,
    'openai/gpt-5.4-mini:batch': 400000,
    'openai/gpt-5.4-nano': 400000,
    'openai/gpt-5.4-nano:batch': 400000,
    'openai/gpt-5.4-pro': 1050000,
    'openai/gpt-5.4-pro:batch': 1050000,
    'openai/gpt-5.4:batch': 1050000,
    'openai/gpt-5.5': 1050000,
    'openai/gpt-5.5-pro': 1050000,
    'openai/gpt-5.5-pro:batch': 1050000,
    'openai/gpt-5.5:batch': 1050000,
    'openai/gpt-5.6-luna': 1050000,
    'openai/gpt-5.6-luna-pro': 1050000,
    'openai/gpt-5.6-luna-pro:batch': 1050000,
    'openai/gpt-5.6-luna:batch': 1050000,
    'openai/gpt-5.6-sol': 1050000,
    'openai/gpt-5.6-sol-pro': 1050000,
    'openai/gpt-5.6-sol-pro:batch': 1050000,
    'openai/gpt-5.6-sol:batch': 1050000,
    'openai/gpt-5.6-terra': 1050000,
    'openai/gpt-5.6-terra-pro': 1050000,
    'openai/gpt-5.6-terra-pro:batch': 1050000,
    'openai/gpt-5.6-terra:batch': 1050000,
    'openai/gpt-5:batch': 400000,
    'openai/gpt-audio': 128000,
    'openai/gpt-audio-mini': 128000,
    'openai/gpt-chat-latest': 400000,
    'openai/gpt-oss-120b': 131072,
    'openai/gpt-oss-120b:batch': 131072,
    'openai/gpt-oss-20b': 131072,
    'openai/gpt-oss-20b:batch': 131072,
    'openai/gpt-oss-safeguard-20b': 131072,
    'openai/o1': 200000,
    'openai/o1-pro': 200000,
    'openai/o3': 200000,
    'openai/o3-mini': 200000,
    'openai/o3-mini-high': 200000,
    'openai/o3-mini:batch': 200000,
    'openai/o3-pro': 200000,
    'openai/o3:batch': 200000,
    'openai/o4-mini': 200000,
    'openai/o4-mini-high': 200000,
    'openai/o4-mini:batch': 200000,
    'openrouter/auto': 2000000,
    'openrouter/auto-beta': 2000000,
    'openrouter/bodybuilder': 128000,
    'openrouter/free': 200000,
    'openrouter/fusion': 1000000,
    'openrouter/pareto-code': 2000000,
    'palmyra-x5': 1040000,
    'pareto-code': 2000000,
    'perceptron-mk1': 32768,
    'perceptron/perceptron-mk1': 32768,
    'perplexity/sonar': 127072,
    'perplexity/sonar-deep-research': 128000,
    'perplexity/sonar-pro': 200000,
    'perplexity/sonar-pro-search': 200000,
    'perplexity/sonar-reasoning-pro': 128000,
    'phi-4': 16384,
    'poolside/laguna-s-2.1': 1048576,
    'poolside/laguna-s-2.1:free': 262144,
    'poolside/laguna-xs-2.1': 262144,
    'poolside/laguna-xs-2.1:free': 262144,
    'qwen-2.5-72b-instruct': 32768,
    'qwen-2.5-7b-instruct': 32768,
    'qwen-2.5-coder-32b-instruct': 32768,
    'qwen-plus': 1000000,
    'qwen-plus-2025-07-28': 1000000,
    'qwen/qwen-2.5-72b-instruct': 32768,
    'qwen/qwen-2.5-7b-instruct': 32768,
    'qwen/qwen-2.5-coder-32b-instruct': 32768,
    'qwen/qwen-plus': 1000000,
    'qwen/qwen-plus-2025-07-28': 1000000,
    'qwen/qwen2.5-vl-72b-instruct': 128000,
    'qwen/qwen3-14b': 131072,
    'qwen/qwen3-235b-a22b': 131072,
    'qwen/qwen3-235b-a22b-2507': 262144,
    'qwen/qwen3-235b-a22b-thinking-2507': 131072,
    'qwen/qwen3-30b-a3b': 131072,
    'qwen/qwen3-30b-a3b-instruct-2507': 262144,
    'qwen/qwen3-30b-a3b-thinking-2507': 81920,
    'qwen/qwen3-32b': 131072,
    'qwen/qwen3-8b': 131072,
    'qwen/qwen3-coder': 262144,
    'qwen/qwen3-coder-30b-a3b-instruct': 262144,
    'qwen/qwen3-coder-flash': 1000000,
    'qwen/qwen3-coder-next': 262144,
    'qwen/qwen3-coder-plus': 1000000,
    'qwen/qwen3-max': 262144,
    'qwen/qwen3-max-thinking': 262144,
    'qwen/qwen3-next-80b-a3b-instruct': 262144,
    'qwen/qwen3-next-80b-a3b-thinking': 262144,
    'qwen/qwen3-vl-235b-a22b-instruct': 262144,
    'qwen/qwen3-vl-235b-a22b-thinking': 131072,
    'qwen/qwen3-vl-30b-a3b-instruct': 262144,
    'qwen/qwen3-vl-30b-a3b-thinking': 262144,
    'qwen/qwen3-vl-32b-instruct': 131072,
    'qwen/qwen3-vl-8b-instruct': 262144,
    'qwen/qwen3-vl-8b-thinking': 131072,
    'qwen/qwen3.5-122b-a10b': 262144,
    'qwen/qwen3.5-27b': 262144,
    'qwen/qwen3.5-35b-a3b': 262144,
    'qwen/qwen3.5-397b-a17b': 262144,
    'qwen/qwen3.5-9b': 262144,
    'qwen/qwen3.5-9b:batch': 262144,
    'qwen/qwen3.5-flash-02-23': 1000000,
    'qwen/qwen3.5-plus-02-15': 1000000,
    'qwen/qwen3.5-plus-20260420': 1000000,
    'qwen/qwen3.6-27b': 262144,
    'qwen/qwen3.6-35b-a3b': 262144,
    'qwen/qwen3.6-flash': 1000000,
    'qwen/qwen3.6-max-preview': 262144,
    'qwen/qwen3.6-plus': 1000000,
    'qwen/qwen3.7-flash': 1000000,
    'qwen/qwen3.7-max': 1000000,
    'qwen/qwen3.7-plus': 1000000,
    'qwen/qwen3.8-2.4t-a95b': 1048576,
    'qwen/qwen3.8-2.4t-a95b:batch': 1010000,
    'qwen/qwen3.8-27b': 1000000,
    'qwen/qwen3.8-flash': 1000000,
    'qwen/qwen3.8-max': 1000000,
    'qwen2.5-vl-72b-instruct': 128000,
    'qwen3-14b': 131072,
    'qwen3-235b-a22b': 131072,
    'qwen3-235b-a22b-2507': 262144,
    'qwen3-235b-a22b-thinking-2507': 131072,
    'qwen3-30b-a3b': 131072,
    'qwen3-30b-a3b-instruct-2507': 262144,
    'qwen3-30b-a3b-thinking-2507': 81920,
    'qwen3-32b': 131072,
    'qwen3-8b': 131072,
    'qwen3-coder': 262144,
    'qwen3-coder-30b-a3b-instruct': 262144,
    'qwen3-coder-flash': 1000000,
    'qwen3-coder-next': 262144,
    'qwen3-coder-plus': 1000000,
    'qwen3-max': 262144,
    'qwen3-max-thinking': 262144,
    'qwen3-next-80b-a3b-instruct': 262144,
    'qwen3-next-80b-a3b-thinking': 262144,
    'qwen3-vl-235b-a22b-instruct': 262144,
    'qwen3-vl-235b-a22b-thinking': 131072,
    'qwen3-vl-30b-a3b-instruct': 262144,
    'qwen3-vl-30b-a3b-thinking': 262144,
    'qwen3-vl-32b-instruct': 131072,
    'qwen3-vl-8b-instruct': 262144,
    'qwen3-vl-8b-thinking': 131072,
    'qwen3.5-122b-a10b': 262144,
    'qwen3.5-27b': 262144,
    'qwen3.5-35b-a3b': 262144,
    'qwen3.5-397b-a17b': 262144,
    'qwen3.5-9b': 262144,
    'qwen3.5-9b:batch': 262144,
    'qwen3.5-flash-02-23': 1000000,
    'qwen3.5-plus-02-15': 1000000,
    'qwen3.5-plus-20260420': 1000000,
    'qwen3.6-27b': 262144,
    'qwen3.6-35b-a3b': 262144,
    'qwen3.6-flash': 1000000,
    'qwen3.6-max-preview': 262144,
    'qwen3.6-plus': 1000000,
    'qwen3.7-flash': 1000000,
    'qwen3.7-max': 1000000,
    'qwen3.7-plus': 1000000,
    'qwen3.8-2.4t-a95b': 1048576,
    'qwen3.8-2.4t-a95b:batch': 1010000,
    'qwen3.8-27b': 1000000,
    'qwen3.8-flash': 1000000,
    'qwen3.8-max': 1000000,
    'reka-edge': 16384,
    'reka-flash-3': 65536,
    'rekaai/reka-edge': 16384,
    'rekaai/reka-flash-3': 65536,
    'relace-apply-3': 256000,
    'relace-search': 256000,
    'relace/relace-apply-3': 256000,
    'relace/relace-search': 256000,
    'remm-slerp-l2-13b': 6144,
    'sakana-namazu': 262144,
    'sakana/fugu-ultra': 1000000,
    'sakana/sakana-namazu': 262144,
    'sao10k/l3-lunaris-8b': 8192,
    'sao10k/l3.1-euryale-70b': 131072,
    'sao10k/l3.3-euryale-70b': 131072,
    'seed-1.6': 262144,
    'seed-1.6-flash': 262144,
    'seed-2-1-turbo': 262144,
    'seed-2.0-code': 262144,
    'seed-2.0-lite': 262144,
    'seed-2.0-mini': 262144,
    'skyfall-36b-v2': 32768,
    'solar-pro-3': 131072,
    'solar-pro4': 524288,
    'sonar': 127072,
    'sonar-deep-research': 128000,
    'sonar-pro': 200000,
    'sonar-pro-search': 200000,
    'sonar-reasoning-pro': 128000,
    'step-3.5-flash': 262144,
    'step-3.7-flash': 262144,
    'stepfun/step-3.5-flash': 262144,
    'stepfun/step-3.7-flash': 262144,
    'tencent-hy4-preview': 1000000,
    'tencent/hunyuan-a13b-instruct': 131072,
    'tencent/hy-mt2-1.8b': 8192,
    'tencent/hy-mt2-30b-a3b': 8192,
    'tencent/hy-mt2-7b': 8192,
    'tencent/hy3': 262144,
    'tencent/hy3-preview': 262144,
    'tencent/hy4-preview': 1048576,
    'thedrummer/cydonia-24b-v4.1': 131072,
    'thedrummer/skyfall-36b-v2': 32768,
    'thedrummer/unslopnemo-12b': 1024000,
    'thinkingmachines/inkling': 1048576,
    'thinkingmachines/inkling-small': 1048576,
    'thinkingmachines/inkling-small:batch': 524288,
    'thinkingmachines/inkling-small:free': 1048576,
    'thinkingmachines/inkling:batch': 524288,
    'thinkingmachines/inkling:free': 1048576,
    'trinity-large-thinking': 262144,
    'ui-tars-1.5-7b': 128000,
    'undi95/remm-slerp-l2-13b': 6144,
    'unslopnemo-12b': 1024000,
    'upstage/solar-pro-3': 131072,
    'upstage/solar-pro4': 524288,
    'voxtral-small-24b-2507': 32768,
    'weaver': 8000,
    'wizardlm-2-8x22b': 65535,
    'writer/palmyra-x5': 1040000,
    'x-ai/grok-4.20': 2000000,
    'x-ai/grok-4.20-multi-agent': 2000000,
    'x-ai/grok-4.3': 1000000,
    'x-ai/grok-4.5': 500000,
    'x-ai/grok-4.6': 500000,
    'x-ai/grok-build-0.1': 256000,
    'xiaomi/mimo-v2.5': 1050000,
    'xiaomi/mimo-v2.5-pro': 1050000,
    'z-ai/glm-4.5': 131072,
    'z-ai/glm-4.5-air': 131072,
    'z-ai/glm-4.5v': 65536,
    'z-ai/glm-4.6': 204800,
    'z-ai/glm-4.6v': 131072,
    'z-ai/glm-4.7': 204800,
    'z-ai/glm-4.7-flash': 202752,
    'z-ai/glm-5': 204800,
    'z-ai/glm-5-turbo': 202752,
    'z-ai/glm-5.1': 204800,
    'z-ai/glm-5.2': 1048576,
    'z-ai/glm-5.2:free': 256000,
    'z-ai/glm-5.3': 1310720,
    'z-ai/glm-5.3-flash': 1310720,
    'z-ai/glm-5.3-flash:batch': 1048575,
    'z-ai/glm-5v-turbo': 202752,
    '~anthropic/claude-fable-latest': 1000000,
    '~anthropic/claude-haiku-latest': 200000,
    '~anthropic/claude-opus-latest': 1000000,
    '~anthropic/claude-sonnet-latest': 1000000,
    '~deepseek/deepseek-v4-flash-latest': 1310720,
    '~google/gemini-flash-latest': 1048576,
    '~google/gemini-pro-latest': 1048576,
    '~moonshotai/kimi-latest': 1048576,
    '~openai/gpt-latest': 1050000,
    '~openai/gpt-mini-latest': 400000,
    '~x-ai/grok-latest': 500000,
    '~z-ai/glm-flash-latest': 1310720,
    '~z-ai/glm-latest': 1310720,
}

# Per-prefix overrides where the same model id has different context per provider
# (e.g. grok-4.5 = 1M at limitrouter, but 500K via OpenRouter/BT/commandcode).
CONTEXT_OVERRIDE_BY_PREFIX = {
    'bt': {'grok-4.5': 500000, 'grok-4.6': 500000, 'auto': None},
    'code': {'grok-4.5': 500000, 'grok-4.6': 500000},
    'limit': {'grok-4.5': 1000000, 'grok-4.6': 1000000},
    # kelontong — user-verified from kelontongai.my.id/user/models (Aug 2026):
    # kolom "Output" = max output, context window SEMUA = 1M. Setiap model ini di-set 1M.
    'lontong': {
        'kimi-k3': 1000000, 'gpt-5.6-sol': 1000000, 'gemini-3.6-flash': 1000000,
        'glm-5.3': 1000000, 'gpt-5.6-terra': 1000000, 'gemini-3.7-flash': 1000000,
        'deepseek-v4-flash-0731': 1000000, 'glm-5.3-flash': 1000000,
        'claude-opus-5': 1000000, 'gemini-3.1-pro': 1000000, 'muse-spark-1.2': 1000000,
        'claude-sonnet-5': 1000000, 'claude-opus-4.8': 1000000, 'mimo-v2.5': 1000000,
        'deepseek-v4-flash-vision-exp': 1000000, 'hy4': 1000000,  # thy4 di API = hy4 di kv
    },
}

# Vendor prefixes to strip when looking up a model by its bare name.
_VENDOR_PREFIXES = (
    'anthropic-', 'openai-', 'google-', 'deepseek-', 'meta-', 'mistral-',
    'qwen-', 'bt-', 'nvidia-', 'amazon-', 'cohere-', 'bytedance-',
    'baidu-', 'aion-labs-', 'cognitivecomputations-', 'arcee-ai-',
    'x-ai-', 'allenai-', 'square-', 'perplexity-', 'microsoft-',
    'deepseek-deepseek-', 'google-gemini-', 'openai-gpt-',
)


def _bare_name(model_id: str) -> str:
    """Strip any vendor prefix and return a lowercase bare model name."""
    m = model_id.split('/')[-1].lower()
    for p in _VENDOR_PREFIXES:
        if m.startswith(p):
            m = m[len(p):]
            break
    return m


def _context_of(prefix: str, model_id: str):
    """Return context-window size (int) for prefix/model, or None if unknown."""
    overrides = CONTEXT_OVERRIDE_BY_PREFIX.get(prefix, {})
    bare = _bare_name(model_id)
    if bare in overrides:
        return overrides[bare]
    if bare in GLOBAL_CONTEXT_BY_NAME:
        return GLOBAL_CONTEXT_BY_NAME[bare]
    # dash/dot/underscore variants refer to the same version — match by
    # compressed key (strip all non-alphanumerics): claude-opus-4-7 == claude-opus-4.7
    flat = re.sub(r"[^a-z0-9]", "", bare.lower())
    key = _COMPRESSED_INDEX.get(flat)
    if key is not None:
        return GLOBAL_CONTEXT_BY_NAME[key]
    # fall back to the full id as-is (e.g. 'bt/bt/claude-opus-5' already bare)
    if model_id.lower() in GLOBAL_CONTEXT_BY_NAME:
        return GLOBAL_CONTEXT_BY_NAME[model_id.lower()]
    return None


# Build-once compressed index: 'claudeopus47' -> canonical key 'claude-opus-4.7'
_COMPRESSED_INDEX = {}
for _k in GLOBAL_CONTEXT_BY_NAME:
    _flat = re.sub(r"[^a-z0-9]", "", _k.lower())
    if _flat and _flat not in _COMPRESSED_INDEX:
        _COMPRESSED_INDEX[_flat] = _k


def should_add_model(prefix: str, model_id: str) -> bool:
    """Return True if model passes the ≥1M-context filter.

    Unknown models (no context info) are ALLOWED (fallback) so the sync never
    silently drops a brand-new model we haven't yet catalogued. Actually-known
    sub-1M models are filtered out.
    """
    ctx = _context_of(prefix, model_id)
    if ctx is None:
        return True
    return ctx >= 1000000


# Update sync_provider to apply the ≥1M context filter at line 173

# ── observed context tracker ─────────────────────────────────────────────────
# Persist the max total tokens observed per model in real usage, so a model whose
# context we couldn't look up today can still be FILTERED later as soon as real
# usage proves it can't reach 1M (i.e. its observed max plateaus well below 1M),
# or credited as ≥1M the day a request actually uses that much.
# File: <script_dir>/observed_context.json — create it manually if you want to
# seed known models.
OBSERVED_CONTEXT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "observed_context.json"
)


def load_observed_context() -> dict:
    if not os.path.exists(OBSERVED_CONTEXT_FILE):
        return {}
    try:
        with open(OBSERVED_CONTEXT_FILE) as f:
            return json.load(f)
    except (ValueError, OSError):
        return {}


def save_observed_context(data: dict) -> None:
    tmp = OBSERVED_CONTEXT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, OBSERVED_CONTEXT_FILE)


def update_observed_context(db: sqlite3.Connection) -> dict:
    """Scan usageHistory, update max total tokens per model. Returns delta count."""
    data = load_observed_context()
    changed = 0
    for r in db.execute(
        "SELECT model, promptTokens, completionTokens FROM usageHistory"
    ):
        m = str(r["model"])
        t = (r["promptTokens"] or 0) + (r["completionTokens"] or 0)
        if t <= 0:
            continue
        if t > data.get(m, {}).get("max_tokens", 0):
            data[m] = {"max_tokens": t}
            changed += 1
    if changed:
        save_observed_context(data)
    return changed


def observed_context_of(prefix: str, model_id: str) -> int | None:
    """Max total tokens actually observed for this model (or None)."""
    try:
        data = load_observed_context()
    except Exception:
        return None
    bare = _bare_name(model_id)
    for cand in (model_id, f"{prefix}/{model_id}", bare):
        v = data.get(cand)
        if v:
            return v.get("max_tokens")
    return None


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

    # Track max observed context per model from real usage (sidecar JSON).
    try:
        changed = update_observed_context(db)
        if changed:
            log(f"observed-context tracker: {changed} new/updated entries")
    except Exception as e:
        log(f"observed-context tracker error: {e}")

    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
