# 9Router Model Refresh

Auto-sync custom provider model lists for [9Router](https://github.com/decolua/9router) — the AI router / token saver.

Every hour (cron), fetches `/v1/models` from each active **openai-compatible custom provider**, diffs against what 9Router has cached, then:

- ➕ **Adds** newly available model IDs
- ➖ **Removes** model IDs that no longer exist upstream
- 🧹 **Prunes** stale model references from `combos.models`

Runs entirely **outside** the 9Router app — no files under `/usr/local/lib/node_modules/9router` are touched, so it survives 9Router updates. It only reads/writes the SQLite DB directly.

Pattern modeled after OmniRoute's auto-sync ([#5444](https://github.com/diegosouzapw/OmniRoute/issues/5444)): fetch upstream `/v1/models` → diff → apply adds/removes → prune combos → log changes. A failed provider sync is logged and skipped; it never blocks other providers.

## Requirements

- Python 3.10+ (stdlib only — `sqlite3`, `urllib`, `json`)
- 9Router running with its SQLite DB at `~/.9router/db/data.sqlite`
- Active custom providers (type `openai-compatible`, `isActive=1`)

## Install

```bash
# 1. Copy script anywhere (e.g. ~/bin or ~/.hermes/scripts)
mkdir -p ~/.hermes/scripts
cp 9router_model_refresh.py ~/.hermes/scripts/
chmod +x ~/.hermes/scripts/9router_model_refresh.py

# 2. Manual test run
~/.hermes/scripts/9router_model_refresh.py

# 3. Cron every hour
(crontab -l 2>/dev/null | grep -v 9router_model_refresh; \
 echo "0 * * * * ~/.hermes/scripts/9router_model_refresh.py >> ~/.hermes/logs/9router-sync.log 2>&1") \
 | crontab -
```

## How it works

| Step | What |
|---|---|
| 1 | Read `providerNodes` + `providerConnections` from SQLite → active custom providers (baseUrl, apiKey, prefix) |
| 2 | `GET {baseUrl}/models` with `Authorization: Bearer <apiKey>` (browser User-Agent to pass Cloudflare) |
| 3 | Diff upstream model IDs vs `kv` table rows (scope `customModels`, key `<nodeId>\|<modelId>\|llm`) |
| 4 | INSERT new IDs, DELETE stale IDs |
| 5 | Prune `combos.models` entries that reference removed models (`prefix/model-id`) |
| 6 | WAL-safe backup to `~/.9router/db/backups/data.sqlite.pre-sync` before committing any change |

9Router computes model `capabilities` (contextWindow, vision, reasoning…) at serve time from the upstream response — the script only manages the model **IDs**, so metadata stays correct automatically.

## Safety

- **Single snapshot backup** before any mutation (SQLite online backup API — safe with active WAL). Rollback:
  ```bash
  cp ~/.9router/db/backups/data.sqlite.pre-sync ~/.9router/db/data.sqlite
  ```
- Changes only commit if at least one provider had an add/remove.
- Failed provider fetches are logged and skipped — no partial writes from that provider.
- Config via env vars (all optional):

| Env | Default | Purpose |
|---|---|---|
| `NINEROUTER_DB` | `~/.9router/db/data.sqlite` | Path to 9Router SQLite DB |
| `NINEROUTER_BACKUP_DIR` | `~/.9router/db/backups` | Where the pre-sync snapshot lives |
| `NINEROUTER_SYNC_UA` | browser UA | User-Agent sent to upstream `/v1/models` |
| `NINEROUTER_SYNC_TIMEOUT` | `30` | Per-provider fetch timeout (seconds) |

## Example output

```
[2026-08-01T12:53:16Z] syncing 3 custom providers
[2026-08-01T12:53:16Z]   limit (https://limitrouter.com/v1)
[2026-08-01T12:53:16Z]   limit: +0 -2 (cached: 49 → 47)
[2026-08-01T12:53:16Z]   kenari (https://kenari.id/v1)
[2026-08-01T12:53:16Z]   kenari: +1 -0 (cached: 43 → 44)
[2026-08-01T12:53:16Z]   kelontong (https://api.kelontongai.my.id/v1)
[2026-08-01T12:53:16Z]   kelontong: +0 -1 (cached: 30 → 29)
[2026-08-01T12:53:16Z] backup → /root/.9router/db/backups/data.sqlite.pre-sync
[2026-08-01T12:53:16Z] committed
```

## License

MIT
