#!/usr/bin/env python
"""Hand-rolled mutation check: apply ONE textual mutation at a time, run the unit tests, record whether at least
one test failed, then restore the original file.

Usage:  python scripts/mutation_check.py            (all mutations)
        python scripts/mutation_check.py M05 M10    (selected ids)
        python scripts/mutation_check.py --check    (only verify that every match text occurs exactly once)
Set MVN to the Maven executable if `mvn` is not on PATH.

Rules:
- Each mutation replaces its `old` text, which must occur EXACTLY ONCE in the file. Anything else is reported as
  BROKEN (the script is out of date), never as a survivor.
- A mutant that does not compile is reported as COMPILE ERROR (invalid), not as killed.
- Hangs count as kills: the test suite sets a 10 s per-test timeout (src/test/resources/junit-platform.properties,
  separate-thread mode so a busy loop is abandoned), and as a backstop a whole `mvn test` run that exceeds
  TIMEOUT_S seconds is killed (process tree) and reported as KILLED (timeout).
- Files are restored from the original bytes in a finally block, even on Ctrl-C.
"""
import os
import re
import shutil
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src", "main", "java", "com", "github", "sup3rmar10", "flowstreams")
MVN = os.environ.get("MVN") or shutil.which("mvn") or shutil.which("mvn.cmd") or "mvn"
TIMEOUT_S = 300

MUTATIONS = [
    ("M01", "demand addition no longer saturates", "Demand.java",
     "return r < 0 ? Long.MAX_VALUE : r;", "return r;"),
    ("M02", "demand multiplication no longer saturates", "Demand.java",
     "return (hi != 0 || lo < 0) ? Long.MAX_VALUE : lo;", "return lo;"),
    ("M03", "addCap adds without saturation (CAS value)", "Demand.java",
     "if (requested.compareAndSet(cur, add(cur, n))) {", "if (requested.compareAndSet(cur, cur + n)) {"),
    ("M04", "iterable source ignores request(n<=0)", "IterableSource.java",
     "invalid = true;", "invalid = false;"),
    ("M05", "iterable source never decrements served demand", "IterableSource.java",
     "r = requested.addAndGet(-e);", "r = requested.get();"),
    ("M06", "second onSubscribe is accepted instead of cancelled", "Link.java",
     "if (up != null) {", "if (false) {"),
    ("M07", "operators forward signals after done/cancel", "Link.java",
     "        if (done || cancelled) {\n            return;\n        }\n        next(t);",
     "        next(t);"),
    ("M08", "filter does not ask for a replacement of a rejected item", "FilterOp.java",
     "up.request(1);", "// replacement request removed"),
    ("M09", "take forwards demand beyond its limit", "TakeOp.java",
     "long grant = Math.min(n, b);", "long grant = n;"),
    ("M10", "take does not cancel upstream at the limit", "TakeOp.java",
     "up.cancel();", "// upstream not cancelled"),
    ("M11", "buffer(n) does not multiply downstream demand by n", "BufferOp.java",
     "up.request(n <= 0 ? n : Demand.multiply(n, size));", "up.request(n);"),
    ("M12", "buffer drops the short tail on completion", "BufferOp.java",
     "if (!rest.isEmpty()) {", "if (false) {"),
    ("M13", "flatMap asks the outer source for unbounded items", "FlatMapOp.java",
     "s.request(maxConcurrency);", "s.request(Long.MAX_VALUE);"),
    ("M14", "flatMap does not free a concurrency slot when an inner finishes", "FlatMapOp.java",
     "up.request(1); // a concurrency slot is free: pull the next outer item", "// slot not replenished"),
    ("M15", "flatMap never tops up an inner after its prefetch", "FlatMapOp.java",
     "s.request(limit);", "// inner not topped up"),
    ("M16", "flatMap drain ignores downstream demand", "FlatMapOp.java",
     "if (e == r || cancelled || error.get() != null) {", "if (cancelled || error.get() != null) {"),
    ("M17", "flatMap inner does not re-check cancellation after subscribing (race guard)", "FlatMapOp.java",
     "if (parent.cancelled) { // cancelAll() may have run before s was set", "if (false) {"),
    ("M18", "onErrorResume hands over the full demand instead of the outstanding demand", "OnErrorResumeOp.java",
     "requested = Math.max(0, requested - produced);", "requested = Math.max(0, requested);"),
    ("M19", "onErrorResume does not reset the delivered counter after a switch", "OnErrorResumeOp.java",
     "produced = 0;", "produced += 0;"),
    ("M20", "onErrorResume swallows the error caused by an invalid request", "OnErrorResumeOp.java",
     "if (switched || badRequest) {", "if (switched) {"),
    ("M21", "timeout ignores the generation of a stale timer", "TimeoutOp.java",
     "if (done || cancelled || g != generation) {", "if (done || cancelled) {"),
    ("M22", "timeout is not armed when demand arrives", "TimeoutOp.java",
     "if (timer == null) {", "if (false) {"),
    ("M23", "publishOn never tops up upstream demand", "PublishOnOp.java",
     "up.request(limit);", "// not topped up"),
    ("M24", "publishOn queue is no longer bounded by its capacity", "PublishOnOp.java",
     "this.queue = new ArrayBlockingQueue<>(capacity);", "this.queue = new ArrayBlockingQueue<>(capacity + 1000);"),
    ("M25", "merge subscribes one source at a time (concurrency 1)", "Source.java",
     ".flatMap(s -> s, list.size());", ".flatMap(s -> s, 1);"),
]


def kill_tree(p):
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(p.pid)], capture_output=True)
    else:
        p.kill()


def run_tests():
    p = subprocess.Popen([MVN, "-B", "-q", "test"], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         text=True, errors="replace")
    try:
        out, _ = p.communicate(timeout=TIMEOUT_S)
    except subprocess.TimeoutExpired:
        kill_tree(p)
        p.communicate()
        return "timeout", [], 0
    if "COMPILATION ERROR" in out:
        return "compile-error", [], 0
    failing = sorted(set(re.findall(r"\[ERROR\]\s{2,}(\w+Test\.\w+)", out)))
    m = re.findall(r"Tests run: (\d+), Failures: (\d+), Errors: (\d+)", out)
    n_bad = int(m[-1][1]) + int(m[-1][2]) if m else len(failing)
    if p.returncode == 0:
        return "survived", failing, n_bad
    return "killed", failing, n_bad


def main():
    args = sys.argv[1:]
    check_only = "--check" in args
    wanted = set(a for a in args if not a.startswith("--"))
    originals = {}
    results = []
    try:
        for mid, desc, fname, old, new in MUTATIONS:
            if wanted and mid not in wanted:
                continue
            path = os.path.join(SRC, fname)
            if fname not in originals:
                with open(path, "rb") as f:
                    originals[fname] = f.read()
            text = originals[fname].decode("utf-8").replace("\r\n", "\n")
            count = text.count(old)
            if count != 1:
                results.append((mid, desc, f"BROKEN (match text occurs {count} times, need exactly 1)", []))
                print(f"{mid} | {desc} | BROKEN ({count} matches)", flush=True)
                continue
            if check_only:
                print(f"{mid} | {desc} | match ok", flush=True)
                continue
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write(text.replace(old, new, 1))
            try:
                status, failing, n = run_tests()
            finally:
                with open(path, "wb") as f:
                    f.write(originals[fname])
            label = {"killed": "KILLED", "survived": "SURVIVED", "compile-error": "COMPILE ERROR (invalid)",
                     "timeout": "KILLED (timeout)"}[status]
            results.append((mid, desc, label, failing))
            print(f"{mid} | {desc} | {label} | {n} failing test(s): {', '.join(failing[:3])}", flush=True)
    finally:
        for fname, data in originals.items():
            with open(os.path.join(SRC, fname), "wb") as f:
                f.write(data)
    if check_only:
        return
    killed = sum(1 for r in results if r[2].startswith("KILLED"))
    survived = [r[0] for r in results if r[2] == "SURVIVED"]
    other = [f"{r[0]}={r[2]}" for r in results if not r[2].startswith("KILLED") and r[2] != "SURVIVED"]
    print(f"\n{killed}/{len(results)} mutations killed; survivors: {survived or 'none'}; other: {other or 'none'}")


if __name__ == "__main__":
    main()
