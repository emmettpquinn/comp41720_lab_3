#!/usr/bin/env python3
"""
COMP41720 Distributed Systems (2026-27) - Lab 3 Test Harness
Resilience Patterns with LLM API Circuit Breaker

This is the harness the demonstrator runs at your machine at the end of the
session. Run it yourself, early and often - if it is green here, it will be
green in the room.

WHAT IT CHECKS (the three pass criteria on the demonstrator's sheet):

  [1] Your service answers on POST /generate at port 8080: 2xx with a
      response body, for repeated requests. Metrics / Prometheus / Grafana
      reachability is probed here too (warnings, feeding criterion 3).
  [2] The breaker transitions live: you break your LLM dependency, the
      harness drives requests and watches CLOSED -> OPEN (slow failures
      giving way to instant 503 "circuit_open" fail-fasts), then you
      restore the dependency and it watches OPEN -> HALF-OPEN -> CLOSED
      (a trial request succeeds and stays succeeding). If your /metrics
      endpoint exposes a circuit-state gauge, the observed states are
      reported as extra evidence.
  [3] Grafana showed request rate, error rate and circuit state changing
      during that chaos run - Prometheus is queried for your metrics and
      the demonstrator confirms the dashboard.

USAGE
  python3 lab3_harness.py                 full check (criteria 1-3, interactive)
  python3 lab3_harness.py --quick         criterion 1 + reachability only
  python3 lab3_harness.py --help          all options

Requires only Python 3.8+ (standard library).

No part of this file needs modifying. If the harness misreads your setup,
use the flags (--base, --metrics, --prom, --grafana, --timeout) rather
than editing checks.
"""

import argparse
import json
import re
import sys
import threading
import time
import urllib.error
import urllib.request

VERSION = "1.0 (2026-08-26)"

# --------------------------------------------------------------------------
# output helpers
# --------------------------------------------------------------------------

USE_COLOUR = sys.stdout.isatty()

def _c(code, s):
    return f"\033[{code}m{s}\033[0m" if USE_COLOUR else s

def green(s):  return _c("32", s)
def red(s):    return _c("31", s)
def yellow(s): return _c("33", s)
def bold(s):   return _c("1", s)

PASSES, FAILS, WARNS = [], [], []

def ok(msg):
    PASSES.append(msg)
    print(f"  {green('PASS')}  {msg}")

def fail(msg, hint=None):
    FAILS.append(msg)
    print(f"  {red('FAIL')}  {msg}")
    if hint:
        print(f"        {yellow('hint:')} {hint}")

def warn(msg):
    WARNS.append(msg)
    print(f"  {yellow('WARN')}  {msg}")

def section(title):
    print()
    print(bold(f"--- {title} " + "-" * max(1, 66 - len(title))))

def pause(prompt):
    print()
    input(bold(f"  >> {prompt}  [press Enter] "))

def ask_yn(prompt):
    print()
    answer = input(bold(f"  >> {prompt}  [y/N] ")).strip().lower()
    return answer in ("y", "yes")

# --------------------------------------------------------------------------
# HTTP helpers (stdlib only)
# --------------------------------------------------------------------------

def http_json(method, url, body=None, timeout=10):
    """Return (status, parsed-json-or-text). Raises URLError on transport error."""
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode(errors="replace")
            status = resp.status
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        status = e.code
    try:
        return status, (json.loads(raw) if raw.strip() else {})
    except json.JSONDecodeError:
        return status, raw

def generate(base, prompt, timeout):
    """POST /generate. Returns (kind, status, body, seconds) where kind is
    'ok' | 'http-error' | 'timeout' | 'refused'."""
    t0 = time.monotonic()
    try:
        code, body = http_json("POST", f"{base}/generate", {"prompt": prompt}, timeout=timeout)
        kind = "ok" if 200 <= code < 300 else "http-error"
        return kind, code, body, time.monotonic() - t0
    except urllib.error.URLError as e:
        elapsed = time.monotonic() - t0
        reason = str(getattr(e, "reason", e)).lower()
        if "timed out" in reason or isinstance(getattr(e, "reason", None), TimeoutError):
            return "timeout", None, None, elapsed
        return "refused", None, None, elapsed
    except TimeoutError:
        return "timeout", None, None, time.monotonic() - t0

def looks_generated(body):
    """Did /generate return something with content in it?"""
    if isinstance(body, str):
        return bool(body.strip())
    if isinstance(body, dict):
        for v in body.values():
            if isinstance(v, str) and v.strip():
                return True
            if isinstance(v, dict) and looks_generated(v):
                return True
        return False
    return False

def is_open_signature(code, body):
    """The brief's mandated OPEN fallback: 503 + {"status": "circuit_open", ...}."""
    if code != 503:
        return False
    if isinstance(body, dict):
        blob = " ".join(str(v) for v in body.values()).lower()
        return "circuit" in blob or "open" in str(body.get("status", "")).lower()
    if isinstance(body, str):
        return "circuit" in body.lower()
    return False

# --------------------------------------------------------------------------
# metrics (Prometheus text format) - optional, used as evidence
# --------------------------------------------------------------------------

METRIC_LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eE]+)\s*$')

def fetch_metrics(urls):
    """Return (url, text) for the first metrics endpoint that answers."""
    for url in urls:
        try:
            code, body = http_json("GET", url, timeout=3)
            if code == 200 and isinstance(body, str) and body.strip():
                return url, body
        except urllib.error.URLError:
            continue
    return None, None

def circuit_state_from(text):
    """Best-effort read of a circuit-state gauge.
    Handles both shapes: labelled per-state gauges (state="open" 1.0) and a
    single numeric gauge (0/1/2). Returns (state-string, raw-evidence) or (None, None)."""
    labelled, single = {}, None
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        m = METRIC_LINE.match(line.strip())
        if not m:
            continue
        name, labels, value = m.group(1), m.group(2) or "", m.group(3)
        low = name.lower()
        if "circuit" not in low and not ("resilience4j" in low and "state" in low):
            continue
        if "state" not in low and "circuit_breaker" not in low:
            continue
        try:
            v = float(value)
        except ValueError:
            continue
        sm = re.search(r'state="([^"]+)"', labels)
        if sm:
            labelled[sm.group(1).lower()] = v
        else:
            single = (name, v)
    if labelled:
        active = [s for s, v in labelled.items() if v >= 1]
        if active:
            return active[0].replace("_", "-"), f'labelled gauge: {", ".join(f"{s}={int(v)}" for s, v in sorted(labelled.items()))}'
    if single:
        name, v = single
        guess = {0: "closed", 1: "open", 2: "half-open"}.get(int(v), f"value {v}")
        return guess, f"{name} = {v:g}"
    return None, None

class StateWatcher(threading.Thread):
    """Polls the metrics endpoint during the chaos run; records distinct states."""
    def __init__(self, url):
        super().__init__(daemon=True)
        self.url = url
        self.states = []          # [(seconds-since-start, state)]
        self.stop_flag = False
        self.t0 = time.monotonic()

    def run(self):
        while not self.stop_flag:
            try:
                code, body = http_json("GET", self.url, timeout=2)
                if code == 200 and isinstance(body, str):
                    state, _ = circuit_state_from(body)
                    if state and (not self.states or self.states[-1][1] != state):
                        self.states.append((time.monotonic() - self.t0, state))
            except urllib.error.URLError:
                pass
            time.sleep(1.0)

    def stop(self):
        self.stop_flag = True

# --------------------------------------------------------------------------
# criterion 1 - the contract
# --------------------------------------------------------------------------

def check_criterion_1(base, prom, grafana, metrics_urls, timeout):
    section("Criterion 1 - the service contract (POST /generate on :8080)")
    kind, code, body, elapsed = generate(base, "Harness check: reply with the single word OK.", timeout)
    if kind == "refused":
        fail(f"cannot reach {base}/generate (connection refused)",
             "is your service running, on port 8080, on this machine?")
        return None
    if kind == "timeout":
        fail(f"POST /generate did not answer within {timeout}s",
             "cold model? try --timeout 180; the configured LLM timeout should return 504, not hang")
        return None
    if kind == "http-error":
        if is_open_signature(code, body):
            fail(f"POST /generate answered 503 circuit_open - the breaker is already OPEN",
                 "restore your LLM dependency, wait for the breaker to close, then rerun")
        else:
            fail(f"POST /generate returned HTTP {code}: {str(body)[:100]}",
                 "with the LLM healthy this must be 2xx with a generated response")
        return None
    ok(f"POST /generate answered 2xx in {elapsed:.1f}s")
    if looks_generated(body):
        ok("response carries generated content")
    else:
        warn(f"response body looks empty: {str(body)[:80]} - return the model's text")
    if elapsed > 30:
        warn(f"{elapsed:.0f}s is above the brief's default 30s LLM timeout - tune or explain")

    lats = [elapsed]
    good = True
    for i in (2, 3):
        kind, code, body, el = generate(base, f"Harness check {i}: reply with the number {i}.", timeout)
        if kind == "ok":
            lats.append(el)
        else:
            good = False
            fail(f"request {i} failed ({kind}{'' if code is None else f' HTTP {code}'})")
    if good:
        ok(f"3 requests served (latencies: {', '.join(f'{l:.1f}s' for l in lats)})")

    # observability probes (warnings here; criterion 3 uses them after the chaos run)
    murl, mtext = fetch_metrics(metrics_urls)
    if murl:
        state, evidence = circuit_state_from(mtext)
        if state:
            ok(f"metrics endpoint found at {murl} with a circuit-state gauge ({evidence})")
        else:
            ok(f"metrics endpoint found at {murl}")
            warn("no circuit-state gauge recognised in the metrics - export one "
                 "(request count, error count, latency, circuit state, per Exercise 3.3)")
    else:
        warn("no metrics endpoint answered (tried: " + ", ".join(metrics_urls) + ") - "
             "criterion 3 needs your service scraped by Prometheus")
    for name, url, path in (("Prometheus", prom, "/-/healthy"), ("Grafana", grafana, "/api/health")):
        try:
            c, _ = http_json("GET", f"{url}{path}", timeout=3)
            if 200 <= c < 300:
                ok(f"{name} is up at {url}")
            else:
                warn(f"{name} at {url} answered HTTP {c}")
        except urllib.error.URLError:
            warn(f"{name} not reachable at {url} - start it before the session "
                 f"(docker run, as in the brief)")
    return murl

# --------------------------------------------------------------------------
# criterion 2 - the live transition drive
# --------------------------------------------------------------------------

def drive_to_open(base, timeout):
    """Send requests against a broken dependency until fail-fast OPEN behaviour
    is seen. Returns (opened, slow_failure_latencies)."""
    slow_lats, open_hits, successes = [], 0, 0
    budget = time.monotonic() + max(150, timeout * 2)
    i = 0
    while time.monotonic() < budget and i < 30:
        i += 1
        kind, code, body, el = generate(base, f"Harness chaos probe {i}.", min(50, max(20, timeout)))
        label = f"HTTP {code}" if code is not None else kind
        print(f"        probe {i:>2}: {label:<12} in {el:6.2f}s")
        if kind == "ok":
            successes += 1
            if successes >= 3:
                fail("requests keep succeeding although the dependency should be broken",
                     "stop Ollama / point LLM_API_URL at a dead port and restart your service")
                return False, slow_lats
            continue
        if is_open_signature(code, body):
            open_hits += 1
        elif kind in ("http-error", "timeout", "refused") :
            fast_503 = (code == 503 and slow_lats and el < 0.3 * max(slow_lats)
                        and max(slow_lats) > 1.0)
            if fast_503:
                open_hits += 1
                warn('a fast 503 without the specified body - return '
                     '{"status": "circuit_open", "retry_after": 10, ...} when OPEN')
            else:
                slow_lats.append(el)
        if open_hits >= 3:
            return True, slow_lats
        time.sleep(0.4)
    return open_hits >= 3, slow_lats

def drive_to_closed(base, timeout):
    """After the dependency is restored: wait out the open window, watch the
    HALF-OPEN trial succeed and the breaker stay closed."""
    budget = time.monotonic() + max(90, timeout)
    open_503s = 0
    while time.monotonic() < budget:
        kind, code, body, el = generate(base, "Harness recovery probe.", min(50, max(20, timeout)))
        if kind == "ok":
            ok(f"a trial request succeeded ({el:.1f}s) - the breaker let it through (HALF-OPEN)")
            good = 0
            for j in (1, 2):
                kind2, code2, _, el2 = generate(base, f"Harness post-recovery {j}.", min(50, max(20, timeout)))
                if kind2 == "ok":
                    good += 1
                    print(f"        post-recovery {j}: 2xx in {el2:.2f}s")
                else:
                    fail(f"request after the successful trial failed "
                         f"({kind2}{'' if code2 is None else f' HTTP {code2}'}) - "
                         "the breaker did not settle back to CLOSED")
                    return False
            if good == 2:
                ok("subsequent requests keep succeeding - CLOSED again")
                return True
        elif is_open_signature(code, body) or code == 503:
            open_503s += 1
            print(f"        still OPEN (fast 503) - waiting for the open timeout "
                  f"({int(budget - time.monotonic())}s left)   ", end="\r")
        else:
            print(f"        {kind}{'' if code is None else f' HTTP {code}'} in {el:.2f}s "
                  f"({int(budget - time.monotonic())}s left)   ", end="\r")
        time.sleep(2.0)
    print()
    if open_503s:
        fail("the breaker never left OPEN after the dependency was restored",
             "is the open timeout configured (default 10s)? did the HALF-OPEN trial "
             "actually reach the restored LLM?")
    else:
        fail("no successful request after restoring the dependency",
             "is the LLM really back? try it directly, then rerun")
    return False

def check_criterion_2(base, metrics_url, timeout):
    section("Criterion 2 - breaker transitions, live: CLOSED -> OPEN -> HALF-OPEN -> CLOSED")
    print("  The harness first confirms CLOSED (a request succeeds). Then you break")
    print("  the LLM dependency; it drives requests and watches slow failures give")
    print("  way to instant circuit_open 503s. Then you restore the dependency and")
    print("  it watches the trial request succeed and the breaker settle CLOSED.")

    kind, code, body, el = generate(base, "Harness pre-chaos check: reply OK.", timeout)
    if kind != "ok":
        fail(f"pre-chaos request did not succeed ({kind}{'' if code is None else f' HTTP {code}'}) - "
             "start from a healthy CLOSED state")
        return False
    ok(f"CLOSED confirmed: request served normally ({el:.1f}s)")

    watcher = None
    if metrics_url:
        watcher = StateWatcher(metrics_url)
        watcher.start()

    pause("BREAK the LLM dependency now (stop Ollama, or point LLM_API_URL at a dead "
          "port and restart your service), then press Enter")

    opened, slow_lats = drive_to_open(base, timeout)
    if not opened:
        if watcher:
            watcher.stop()
        fail("never saw fail-fast OPEN behaviour (instant 503 circuit_open)",
             "threshold is 5 failures in 10 by default - is the breaker wired around "
             "the LLM call at all?")
        return False
    ok("OPEN reached: instant 503 circuit_open responses, without calling the API")
    if slow_lats:
        if max(slow_lats) >= 1.0:
            ok(f"failures before OPEN took up to {max(slow_lats):.1f}s "
               f"(retries with backoff), then fail-fast took over")
        else:
            warn(f"pre-OPEN failures were instant (max {max(slow_lats):.2f}s) - "
                 "are retry attempts with exponential backoff configured?")

    pause("RESTORE the LLM dependency now (restart Ollama / fix the URL; restart your "
          "service only if your config needs it), then press Enter")

    closed = drive_to_closed(base, timeout)

    if watcher:
        watcher.stop()
        seq = [s for _, s in watcher.states]
        if seq:
            print(f"        circuit-state gauge observed: {' -> '.join(seq)}")
            if any("open" in s for s in seq) and seq[-1].startswith("closed"):
                ok(f"metrics gauge corroborates the transitions ({' -> '.join(seq)})")
    return closed

# --------------------------------------------------------------------------
# criterion 3 - Grafana during the chaos run
# --------------------------------------------------------------------------

def check_criterion_3(prom, grafana, quickless):
    section("Criterion 3 - Grafana showed rate, errors and circuit state during chaos")

    prom_ok = False
    try:
        code, names = http_json("GET", f"{prom}/api/v1/label/__name__/values", timeout=5)
        if code == 200 and isinstance(names, dict) and names.get("status") == "success":
            all_names = names.get("data", [])
            circuit = [n for n in all_names if "circuit" in n.lower() or "resilience4j" in n.lower()]
            reqly = [n for n in all_names
                     if any(t in n.lower() for t in ("request", "http_server", "llm", "generate"))]
            if circuit:
                ok(f"Prometheus is scraping a circuit metric ({circuit[0]}"
                   + (f" and {len(circuit)-1} more" if len(circuit) > 1 else "") + ")")
                prom_ok = True
            if reqly and not circuit:
                ok(f"Prometheus is scraping your service ({reqly[0]} ...) - no circuit "
                   "metric among the names though")
                prom_ok = True
            if not circuit and not reqly:
                warn("Prometheus answers but none of its metric names look like your "
                     "service - check the scrape target in prometheus.yml")
        else:
            warn(f"Prometheus query API at {prom} did not answer as expected")
    except urllib.error.URLError:
        warn(f"Prometheus not reachable at {prom}")

    try:
        code, _ = http_json("GET", f"{grafana}/api/health", timeout=5)
        if 200 <= code < 300:
            ok(f"Grafana is up at {grafana}")
        else:
            warn(f"Grafana at {grafana} answered HTTP {code}")
    except urllib.error.URLError:
        warn(f"Grafana not reachable at {grafana}")

    if quickless:
        if ask_yn("Demonstrator: during the chaos run, did the Grafana dashboard show "
                  "request rate, error rate and circuit state changing?"):
            ok("Grafana dashboard confirmed by the demonstrator during chaos")
            return True
        fail("Grafana dashboard not confirmed",
             "build the panels of Exercise 3.3 (request rate, error rate, p99 latency, "
             "circuit state) and rerun the chaos test with the dashboard on screen")
        return False
    return prom_ok

# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="COMP41720 Lab 3 test harness (resilience and circuit breaker).",
        epilog="Green here means green in the room. Run early, run often.")
    ap.add_argument("--base", default="http://localhost:8080",
                    help="base URL of your service (default: %(default)s)")
    ap.add_argument("--metrics",
                    help="metrics endpoint URL (default: tries BASE/metrics and "
                         "BASE/actuator/prometheus)")
    ap.add_argument("--prom", default="http://localhost:9090",
                    help="Prometheus URL (default: %(default)s)")
    ap.add_argument("--grafana", default="http://localhost:3000",
                    help="Grafana URL (default: %(default)s)")
    ap.add_argument("--timeout", type=int, default=60,
                    help="seconds to wait for a /generate answer (default: %(default)s; "
                         "raise for a cold model)")
    ap.add_argument("--quick", action="store_true",
                    help="run only criterion 1 + reachability (no interactive parts)")
    args = ap.parse_args()

    metrics_urls = [args.metrics] if args.metrics else [
        f"{args.base}/metrics", f"{args.base}/actuator/prometheus"]

    print(bold(f"COMP41720 Lab 3 harness {VERSION}"))
    print(f"service: {args.base}   prometheus: {args.prom}   grafana: {args.grafana}")

    metrics_url = check_criterion_1(args.base, args.prom, args.grafana,
                                    metrics_urls, args.timeout)
    c1 = not FAILS
    c2 = c3 = None
    if not args.quick:
        if c1:
            c2 = check_criterion_2(args.base, metrics_url, args.timeout)
            c3 = check_criterion_3(args.prom, args.grafana, quickless=True)
        else:
            warn("skipping criteria 2-3 - fix criterion 1 first")

    # ---------------------------------------------------------------- summary
    def box(flag):
        if flag is None:
            return "[- ]"
        return green("[ x ]") if flag else red("[   ]")

    section("Summary - demonstrator tick-sheet")
    print(f"   {box(c1)}  1. Service answers on POST /generate at port 8080")
    print(f"   {box(c2)}  2. Breaker transitions shown live: CLOSED -> OPEN -> "
          f"HALF-OPEN -> CLOSED")
    print(f"   {box(c3)}  3. Grafana showed request rate, errors and circuit state "
          f"during the chaos run")
    if args.quick:
        print(f"\n   (criteria 2-3 not run: --quick. Run the full harness in the session.)")
    print()
    total_bad = len(FAILS)
    if total_bad == 0:
        print(green(bold("   ALL CHECKS PASSED")) + f"   ({len(PASSES)} passes, {len(WARNS)} warnings)")
    else:
        print(red(bold(f"   {total_bad} CHECK(S) FAILED")) + f"   ({len(PASSES)} passes, {len(WARNS)} warnings)")
        print("   failed:")
        for f_ in FAILS:
            print(f"     - {f_}")
    sys.exit(0 if total_bad == 0 else 1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\ninterrupted.")
        sys.exit(130)
