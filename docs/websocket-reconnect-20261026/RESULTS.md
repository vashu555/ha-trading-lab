# WebSocket reconnect investigation — 26 October 2026

The reported 10-second delay was not reproduced in ordinary controlled API kills. Chrome tests before the change recovered in approximately 1.09 seconds through both HAProxys. A separate stalled-TLS test demonstrated an unbounded connection attempt in the old page: it was still reconnecting after six seconds.

Changes deployed:

- Browser retries begin after 150–250 ms, with exponential backoff capped around two seconds.
- Pending connection attempts are cancelled after two seconds.
- Connected / waiting for prices / prices flowing are separate states; a restored-stream duration is shown.
- An eight-second no-message watchdog retries silent connections; normal server heartbeats are approximately five seconds.
- APIs send their cached snapshot immediately when a stream opens.
- HAProxy retries failed WebSocket upgrades on another API, with a 500 ms connect timeout and a three-second upgrade response timeout. Established streams retain the one-hour tunnel timeout.
- HAProxy closes streams when their tracked API is marked down.
- API source is mounted read-only into the four API containers so this API-only update does not require restarting the shared-image exchange or control service.

Measured from the test action to the first replacement price message:

| Test | Before | After |
|---|---:|---:|
| API kill through HAProxy 1 | 1085.5 ms | 769.2 ms |
| API kill through HAProxy 2 | 1098.9 ms | 909.7 ms |
| One deliberately stalled TLS handshake | Still waiting after 6 seconds | 2623.5 ms |
| Paused API | Not measured | 4123.8 ms |

Verification: Python and browser JavaScript syntax passed; HAProxy configuration validation passed; two regression tests confirm that both an initial cached-price snapshot and an empty-cache heartbeat are sent before waiting for another tick. Actual Chrome tests covered kills through both proxies, a stalled TLS handshake, and a paused API.

Final state: Locust running with 100 users; internal traders 0; auto-heal False. All APIs used in tests were restored. The exchange boot ID was verified unchanged across deployment.

Latest dashboard audit (accounting checks may be cached):
```json
{
  "ok": true,
  "sent_twice": 0,
  "fills_missing": 0,
  "books_wrong": 0,
  "notify_missing": 0,
  "outbox_backlog": 2
}
```

Hard-refresh the live-price page with Ctrl+Shift+R to load the new JavaScript. Measurements are short lab tests, not a bound on recovery during every possible overload or network failure.