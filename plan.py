"""Sampling plan: how many documents to draw from each shard.

Used in-process by the submitter when every shard's count is in the index, and as
the plan job (`plan.py RUNDIR`) when some shards had to be counted first: it then
merges the run's fresh counts into the index and plans from the index.

Standard library only; runs under Python 3.6 (LUMI's /usr/bin/python3).
"""
import argparse
import bisect
import glob
import hashlib
import json
import math
import os
import random
import sys

import countsindex


METHODS = ("urn", "round")


def allocate(shards, n_samples, method="urn", seed=None):
    """shards: [{"filename", "url", "count"}] in shard-list order -> plan entries.

    urn    (default) draw n_samples of all documents without replacement and count
           how many fall in each shard (a multivariate hypergeometric draw): every
           document is equally likely, at any sample size. Needs the run seed.
    round  each shard's exact proportional share, rounded down, the remainder to
           the largest fractional parts. Deterministic; the method of runs before
           2026-11-17. Biased when shares are small: a shard whose share rounds to
           0 and misses the remainder can never be sampled."""
    if method not in METHODS:
        raise ValueError("allocation must be one of {}, not {!r}".format(METHODS, method))
    total_docs = sum(s["count"] for s in shards)
    if total_docs == 0:
        raise ValueError("no documents across shards")
    if n_samples > total_docs:
        print("[PLAN] Requested {} samples but only {} docs available; reducing".format(n_samples, total_docs))
        n_samples = total_docs
    if method == "urn":
        take = _urn(shards, n_samples, total_docs, seed)
    else:
        take = _round(shards, n_samples, total_docs)
    return [{"filename": s["filename"], "url": s["url"], "count": s["count"], "n_take": take[s["filename"]]}
            for s in shards]


def _urn(shards, n_samples, total_docs, seed):
    """Number every document 0..total-1, shards in filename order (so the draw does
    not depend on shard-list order), draw n_samples numbers, count them per shard."""
    if seed is None:
        raise ValueError("urn allocation needs a seed")
    order = sorted(shards, key=lambda s: s["filename"])
    if n_samples == total_docs:
        return {s["filename"]: s["count"] for s in order}
    ends, acc = [], 0
    for s in order:
        acc += s["count"]
        ends.append(acc)
    digest = hashlib.sha256("{}:allocation".format(seed).encode("utf-8")).hexdigest()
    rng = random.Random(int(digest[:16], 16))
    take = {s["filename"]: 0 for s in order}
    for i in rng.sample(range(total_docs), n_samples):
        take[order[bisect.bisect_right(ends, i)]["filename"]] += 1
    return take


def _round(shards, n_samples, total_docs):
    alloc = []
    for s in shards:
        exact = (n_samples * s["count"]) / total_docs
        base = math.floor(exact)
        alloc.append({"filename": s["filename"], "count": s["count"],
                      "n_take": int(min(base, s["count"])), "frac": exact - base})
    remaining = n_samples - sum(a["n_take"] for a in alloc)
    candidates = sorted(alloc, key=lambda x: x["frac"], reverse=True)
    i = 0
    while remaining > 0 and any(a["n_take"] < a["count"] for a in candidates):
        a = candidates[i % len(candidates)]
        if a["n_take"] < a["count"]:
            a["n_take"] += 1
            remaining -= 1
        i += 1
    return {a["filename"]: a["n_take"] for a in alloc}


def write_plan(entries, out_file):
    tmp = out_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")
    os.replace(tmp, out_file)


def from_index(urls, rows):
    """Plan inputs for a shard list from index rows; also the shards with no trusted count."""
    shards, missing = [], []
    for u in urls:
        c = countsindex.lookup(rows, u)
        if c is None:
            missing.append(u)
        else:
            shards.append({"filename": u.split("/")[-1], "url": u, "count": c})
    return shards, missing


def merge_run_counts(rundir, ipath):
    """Merge the run's fresh per-shard counts (<rundir>/counts/*.json) into the index."""
    new = []
    for f in sorted(glob.glob(os.path.join(rundir, "counts", "*.json"))):
        with open(f, encoding="utf-8") as fh:
            e = json.load(fh)
        new.append(countsindex.row(e["url"], e["count"], e.get("method", "scan"), e.get("bytes"), e.get("mtime")))
    return countsindex.merge(ipath, new)


def main():
    p = argparse.ArgumentParser(description="Plan job: merge the run's fresh counts into the index and write plan.jsonl.")
    p.add_argument("rundir")
    p.add_argument("--index-only", action="store_true", help="count-only run: merge the counts and check them, write no plan")
    args = p.parse_args()
    with open(os.path.join(args.rundir, "run.json"), encoding="utf-8") as fh:
        run = json.load(fh)
    ipath = run["counts_index_file"]
    n = merge_run_counts(args.rundir, ipath)
    print("[PLAN] Merged {} fresh counts into {}".format(n, ipath))
    with open(os.path.join(args.rundir, "shard_list.txt"), encoding="utf-8") as fh:
        urls = [ln.strip() for ln in fh if ln.strip()]
    shards, missing = from_index(urls, countsindex.read(ipath))
    if missing:
        print("[PLAN] ERROR: {} shards still have no trusted count, e.g. {}".format(len(missing), missing[0]), file=sys.stderr)
        return 3
    if args.index_only:
        print("[PLAN] Every one of the run's {} shards now has a trusted count in the index".format(len(urls)))
        return 0
    entries = allocate(shards, int(run["n_samples"]), run.get("allocation", "round"), run["seed"])
    write_plan(entries, os.path.join(args.rundir, "plan.jsonl"))
    print("[PLAN] Wrote plan ({} samples planned)".format(sum(e["n_take"] for e in entries)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
