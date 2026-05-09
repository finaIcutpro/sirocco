# sirocco

sirocco is a small discord rest proxy that does the annoying rate limit work before discord has to yell at you. run one binary, point your bot at it, and it keeps route buckets warm, retries the flaky edge cases that are safe to retry, validates requests against discord's openapi spec, and tells you what happened with plain `X-Sirocco-*` headers.

that's basically the whole pitch. no dashboard, no database, no sidecar, no cluster dance, no generated client maze. it is just the proxy and the stuff the proxy actually needs.

## what it does

- plans discord rest buckets before traffic goes upstream.
- learns real discord rate-limit headers and saves useful route state for warm restarts.
- keeps a per-token global limiter, with optional per-token overrides when you need them.
- retries idempotent requests on transient network errors, 5xx responses, and discord edge timeouts.
- blocks bad requests early when openapi validation is enabled.
- adds `X-Sirocco-*` response headers for route, wait time, retry count, status, and upstream latency.
- exposes `/_sirocco/health` and `/_sirocco/meta` so ops checks do not need a whole extra thing.

## quick start

```sh
go build -o bin/sirocco ./cmd/sirocco
SIROCCO_LISTEN=:8080 ./bin/sirocco
```

point your bot http client at `http://host:8080/api/v10`. sirocco rewrites proxied requests to `SIROCCO_DISCORD_BASE_URL`, which defaults to `https://discord.com`.

## config

| variable | default | description |
| --- | --- | --- |
| `SIROCCO_LISTEN` | `0.0.0.0:8080` via `BIND_IP`/`PORT` compatibility | http listen address |
| `SIROCCO_DISCORD_BASE_URL` | `https://discord.com` | discord upstream base url |
| `SIROCCO_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error` |
| `SIROCCO_VALIDATION_ENABLED` | `true` | validate requests against the embedded discord openapi spec |
| `SIROCCO_STATE_PATH` | os cache dir + `sirocco/routes.json` | learned route-state file |
| `SIROCCO_STATE_MAX_AGE` | `24h` | ignore persisted state older than this |
| `SIROCCO_STATE_FLUSH_EVERY` | `30s` | periodic state save interval |
| `SIROCCO_MAX_BODY_BYTES` | `33554432` | max buffered request body size |
| `SIROCCO_GLOBAL_RPS` | `45` | default per-token global request rate |
| `SIROCCO_TOKEN_RATES` | unset | `token:rps,token:rps` overrides |
| `SIROCCO_REQUEST_TIMEOUT` | `5s` | upstream request timeout; millisecond numbers are also accepted |
| `SIROCCO_DIAL_TIMEOUT` | `2.5s` | tcp/tls dial timeout |
| `SIROCCO_IDLE_CONN_TIMEOUT` | `90s` | upstream idle connection timeout |
| `SIROCCO_DISABLE_HTTP2` | `false` | disable http/2 to discord |
| `SIROCCO_OUTBOUND_IP` | unset | optional local egress ip |
| `SIROCCO_UPSTREAM_RETRY_LIMIT` | `3` | retry attempts for idempotent requests |
| `SIROCCO_UPSTREAM_RETRY_BASE_DELAY` | `200ms` | retry backoff floor |
| `SIROCCO_UPSTREAM_RETRY_MAX_DELAY` | `2s` | retry backoff ceiling |

the old `PORT`, `BIND_IP`, `DISCORD_BASE_URL`, `VALIDATION_ENABLED`, `REQUEST_TIMEOUT`, `DIAL_TIMEOUT`, `IDLE_CONN_TIMEOUT`, `DISABLE_HTTP_2`, `OUTBOUND_IP`, `UPSTREAM_RETRY_LIMIT`, `UPSTREAM_RETRY_BASE_DELAY`, `UPSTREAM_RETRY_MAX_DELAY`, `RATELIMIT_OVERRIDES`, and `LOG_LEVEL` names still work too.

in docker, use `SIROCCO_STATE_PATH=/var/lib/sirocco/routes.json`. the image creates that directory for the non-root user, so route state can actually persist instead of failing on permissions.

## endpoints

- `get /_sirocco/health` returns `200 ok` when the listener is up.
- `get /_sirocco/meta` returns uptime, limiter counters, validation counters, retry config, and state persistence status.
- every other path is proxied to discord.

## why not just use nirn-proxy

[nirn-proxy](https://github.com/germanoeich/nirn-proxy) is a serious project, and if you want the gossip/cluster style setup then yeah, go look at it. sirocco is for the more boring case where you want one thing to run, one port to point at, and fewer knobs to babysit at 2am.

the main difference is that sirocco keeps the common path stupidly simple: it has warm bucket state on disk, conservative route planning, idempotent retry logic, request validation, and useful response headers without needing a dashboard or a separate metrics story to understand what happened. that makes it easier to drop in front of a bot fleet, especially when you care more about not hitting 429s than about building a whole proxy control plane.

so the short version is: nirn-proxy is bigger and more distributed. sirocco is smaller, calmer, and honestly easier to trust when you just need discord rest traffic to stop wasting your time.

## openapi spec

the embedded spec comes from discord's official public preview repository:

```sh
make update-spec
```

that command downloads `https://raw.githubusercontent.com/discord/discord-api-spec/main/specs/openapi.json`, validates json, and runs validation tests. discord marks the spec as public preview, so runtime uses the last committed known-good spec until updates pass tests.

## production build

```sh
make test
make build
docker build -t sirocco:local .
```

the docker image is a static binary on a non-root distroless runtime with ca certificates.