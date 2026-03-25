import os
import sys
import time
import json
import math
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("lang")
parser.add_argument("--n-samples", type=int, default=100000)
parser.add_argument("--shard-list", default=None)
parser.add_argument("--counts-dir", default=None)
parser.add_argument("--out", default=None)
parser.add_argument("--poll-interval", type=int, default=10)
args = parser.parse_args()

root = os.path.dirname(__file__)
shard_list = args.shard_list or os.path.join(os.getcwd(), f"shard_list_{args.lang}.txt")
counts_dir = args.counts_dir or os.path.join(root, "counts")
out_file = args.out or os.path.join(root, f"{args.lang}_plan.jsonl")
n_samples = args.n_samples
poll = args.poll_interval

if not os.path.exists(shard_list):
    print(f"Shard list not found: {shard_list}", file=sys.stderr)
    sys.exit(2)

with open(shard_list, "r") as f:
    urls = [ln.strip() for ln in f if ln.strip()]
expected_fnames = [u.split("/")[-1] for u in urls]
expected_set = set(expected_fnames)
expected_n = len(expected_fnames)

print(f"[PLAN] Expecting {expected_n} shard count files in {counts_dir}")

# wait for all per-shard count files to appear
while True:
    if not os.path.isdir(counts_dir):
        os.makedirs(counts_dir, exist_ok=True)
    present = {fn[:-5] for fn in os.listdir(counts_dir) if fn.endswith(".json")}
    have = len(present & expected_set)
    print(f"[PLAN] Found {have}/{expected_n} shard count files", end="\r")
    if have >= expected_n:
        print()
        break
    time.sleep(poll)

# read counts
shards = []
total_docs = 0
for url in urls:
    fname = url.split("/")[-1]
    cf = os.path.join(counts_dir, f"{fname}.json")
    if not os.path.exists(cf):
        print(f"[PLAN] Missing count file for {fname}, aborting", file=sys.stderr)
        sys.exit(3)
    with open(cf, "r") as fh:
        entry = json.load(fh)
    cnt = int(entry.get("count", 0))
    shards.append({"filename": fname, "url": url, "count": cnt})
    total_docs += cnt

if total_docs == 0:
    print("[PLAN] No documents found across shards, exiting", file=sys.stderr)
    sys.exit(4)

if n_samples > total_docs:
    print(f"[PLAN] Requested {n_samples} samples but only {total_docs} docs available; reducing")
    n_samples = total_docs

# proportional allocation with largest fractional remainder tie-break
alloc = []
for s in shards:
    exact = (n_samples * s["count"]) / total_docs
    base = math.floor(exact)
    alloc.append({"filename": s["filename"], "url": s["url"], "count": s["count"], "n_take": int(min(base, s["count"])), "frac": exact - base})

remaining = n_samples - sum(a["n_take"] for a in alloc)

# distribute remaining to shards with largest fractional parts, respecting shard capacities
candidates = sorted(alloc, key=lambda x: x["frac"], reverse=True)
i = 0
while remaining > 0 and any(a["n_take"] < a["count"] for a in candidates):
    a = candidates[i % len(candidates)]
    if a["n_take"] < a["count"]:
        a["n_take"] += 1
        remaining -= 1
    i += 1
    # if we've looped many times and cannot assign (all full) break
    if i > len(candidates) * 10 and not any(x["n_take"] < x["count"] for x in candidates):
        break

final_total = sum(a["n_take"] for a in alloc)
if final_total != n_samples:
    print(f"[PLAN] Warning: final total {final_total} != requested {args.n_samples}. Proceeding with {final_total} samples.")

# write plan atomically
tmp = out_file + ".tmp"
with open(tmp, "w", encoding="utf-8") as fh:
    for a in alloc:
        fh.write(json.dumps({"filename": a["filename"], "url": a["url"], "count": a["count"], "n_take": a["n_take"]}) + "\n")
os.replace(tmp, out_file)
print(f"[PLAN] Wrote plan to {out_file} ({final_total} samples planned)")
