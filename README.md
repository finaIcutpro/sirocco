# sirocco [![CodeFactor](https://www.codefactor.io/repository/github/noahcxrest/sirocco/badge)](https://www.codefactor.io/repository/github/noahcxrest/sirocco)

a single go binary that keeps discord rest buckets warm, retries the flaky edge for you, and surfaces the wait math right in the response so your bot can just send traffic. cuts ratelimits down by over 90%.

## zero-config wins (always on)

- **warm restarts without 429s** – bucket discoveries are persisted to disk and reloaded on boot, so new shards inherit the same buckets immediately.
- **adaptive upstream resilience** – idempotent requests are retried with jittered backoff, tuned connection pools, and `x-ratelimit-precision` out of the box.
- **actionable telemetry** – every response carries `x-sirocco-*` headers for planned wait, actual sleep, retries, and upstream latency; structured logs echo the same fields.
- **one port ops** – built-in `/_sirocco/health` and `/_sirocco/meta` endpoints expose readiness plus live limiter stats—no extra admin container or sidecar.

## quick start

1. grab the binary or run `go build ./cmd/sirocco`.
2. set the only required dial target:

   ```bash
   export PORT=8080
   export DISCORD_BASE_URL=https://discord.com
   ```

3. start the proxy: `./sirocco` (or run the docker image found in this repo).
4. point your bot http client at `http://host:8080/api`—the proxy handles retries, waits, and logging from here.

## out-of-the-box automation

- persistent route cache backed by your os cache folder (`SIROCCO_STATE_PATH` overrides it when you want).
- large, pre-tuned http pool (tls12+, 512 idle slots, per-host caps) with optional outbound ip pinning.
- invalid-request guard that throttles before cloudflare does, now visible through the meta endpoint.
- smart rate-limit heuristics with fifo bucket queues so bursts stay smooth even at large scale.

## Performance

- **low resource usage** – runs on less than 30 MB of RAM and uses only 5% CPU while handling 300 requests per second. sirocco can easily run on hetzners cheapest VPS.
- **effective rate limit reduction** – cuts down rate limits by over 90%, and most of the time above 95%. 

## why not roll your own gateway?

- you'd have to implement route normalization, warm-start heuristics, guard rails for 401/403 storms, multiple retries, jitter, and connection management yourself.
- sirocco already streams exact wait times back to the caller and ships json diagnostics, so your application code stays focused on discord logic.

## why sirocco over [nirn proxy](https://github.com/germanoeich/nirn-proxy)?

- no gossip or cluster bootstrap—drop one binary and get persistent buckets + health endpoints instantly.
- auto-heated buckets via heuristics *and* disk snapshots, so fresh deployments don't take the 429 tax.
- built-in invalid request dampener and retry policy instead of wiring prometheus + custom backoff rules.
- fewer moving parts: no extra listeners, fewer knobs, same structured output nirn expects you to assemble.

## operations cheat sheet

- **health:** `get /_sirocco/health` → `200 ok` if the listener is up.
- **meta:** `get /_sirocco/meta` → json with uptime, bucket/global counts, retry settings, and state path.
- **response headers:** every proxied request includes `x-sirocco-waited`, `x-sirocco-planned-wait`, `x-sirocco-upstream-status`, and retry counts.
- **env overrides:**

  | variable | default | notes |
  | --- | --- | --- |
  | `PORT` | `8080` | listener port |
  | `DISCORD_BASE_URL` | `https://discord.com` | upstream base |
  | `SIROCCO_STATE_PATH` | os cache dir + `route-state.json` | change where bucket state is persisted |
  | `UPSTREAM_RETRY_LIMIT` | `3` | idempotent retry attempts |
  | `UPSTREAM_RETRY_BASE_DELAY` | `200` ms | jittered exponential backoff floor |
  | `UPSTREAM_RETRY_MAX_DELAY` | `2000` ms | backoff ceiling |
  | `BOT_RATELIMIT_OVERRIDES` | unset | `token:rps` pairs for global overrides |
  | `LOG_LEVEL` | `info` | zerolog level |

that's it—ship the binary, aim your shards at it, and let sirocco keep your discord rest traffic fast, safe, and hands-off.
