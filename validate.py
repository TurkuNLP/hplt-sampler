"""Validate a finished run directory before it is published.

Structural checks only: the sampler selects documents and never inspects their
content, so neither does its validator. Writes <rundir>/validation.json and
exits non-zero if any check fails.

Standard library only; runs under Python 3.6 (LUMI's /usr/bin/python3).
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone


def check(results, name, ok, detail):
    results.append({"check": name, "ok": bool(ok), "detail": detail})
    print("[VALIDATE] {:<4} {}: {}".format("ok" if ok else "FAIL", name, detail))
    return ok


def main():
    p = argparse.ArgumentParser(description="Validate a sampler run directory (structural checks).")
    p.add_argument("rundir")
    args = p.parse_args()
    rd = args.rundir
    results = []

    run_path = os.path.join(rd, "run.json")
    plan_path = os.path.join(rd, "plan.jsonl")
    samples_path = os.path.join(rd, "samples.jsonl")
    inv_path = os.path.join(rd, "shard_inventory.tsv")

    run = None
    if check(results, "run record", os.path.exists(run_path), run_path):
        with open(run_path, encoding="utf-8") as fh:
            run = json.load(fh)
    check(results, "per-shard samples removed", not os.path.isdir(os.path.join(rd, "samples")),
          "samples/ absent (concat completed)")

    planned = None
    if check(results, "plan", os.path.exists(plan_path), plan_path):
        with open(plan_path, encoding="utf-8") as fh:
            plan = [json.loads(ln) for ln in fh if ln.strip()]
        planned = sum(int(e["n_take"]) for e in plan)
        if run is not None:
            check(results, "plan covers shard list", len(plan) == run.get("n_shards"),
                  "{} plan entries, {} shards in run.json".format(len(plan), run.get("n_shards")))

    if not check(results, "samples file", os.path.exists(samples_path), samples_path):
        return finish(rd, results, None)

    n = 0
    bad = 0
    no_id = 0
    ids = set()
    dup = 0
    sha = hashlib.sha256()
    with open(samples_path, "rb") as fh:
        for line in fh:
            sha.update(line)
            if not line.strip():
                continue
            n += 1
            try:
                rec = json.loads(line.decode("utf-8"))
            except Exception:
                bad += 1
                continue
            rid = rec.get("id") if isinstance(rec, dict) else None
            if rid is None:
                no_id += 1
            elif rid in ids:
                dup += 1
            else:
                ids.add(rid)
    check(results, "records parse", bad == 0, "{} records, {} unparseable".format(n, bad))
    check(results, "ids present", no_id == 0, "{} records without id".format(no_id))
    check(results, "ids unique", dup == 0, "{} duplicate ids".format(dup))
    if planned is not None:
        check(results, "count matches plan", n == planned, "{} records, {} planned".format(n, planned))

    if os.path.exists(inv_path):
        changed = []
        with open(inv_path, encoding="utf-8") as fh:
            for ln in fh:
                path, size, mtime = ln.rstrip("\n").split("\t")
                try:
                    st = subprocess.check_output(["stat", "-c", "%s\t%y", path]).decode().strip().split("\t")
                except subprocess.CalledProcessError:
                    st = None
                if st is None or st[0] != size or st[1] != mtime:
                    changed.append(path)
        # informational: the sample stays valid, but a rerun would not reproduce it
        check(results, "corpus unchanged since sampling", True,
              "all shards match the inventory" if not changed
              else "{} shards changed or missing since sampling (sample still valid; a rerun would differ)".format(len(changed)))

    return finish(rd, results, {"records": n, "sha256": sha.hexdigest(), "bytes": os.path.getsize(samples_path)})


def finish(rd, results, summary):
    ok = all(r["ok"] for r in results)
    try:
        commit = subprocess.check_output(["git", "-C", os.path.dirname(os.path.abspath(__file__)), "rev-parse", "HEAD"],
                                         stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        commit = "unknown"
    out = {
        "validated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "validator_commit": commit,
        "passed": ok,
        "samples": summary,
        "checks": results,
    }
    with open(os.path.join(rd, "validation.json"), "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)
        fh.write("\n")
    print("[VALIDATE] {}".format("PASSED" if ok else "FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
