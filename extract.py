"""Extract mode, one shard: write every document whose id is in an id list, line as-is.

The id is found without parsing the record: a top-level key is written `"id":"<id>"`
in the raw line, and inside a JSON string the same characters would be escaped, so
only a candidate whose id is in the list is parsed, to confirm it is the record's
own id. Every line is read, so the shard's true document count comes for free; it
goes to a `.seen.json` sidecar like a sampling pass (finalize feeds the counts index).

Local shards only (HPLT v4, and HPLT v3's local copy on LUMI).
Standard library only; runs under Python 3.6 (LUMI's /usr/bin/python3).
"""
import argparse
import json
import os
import sys

import shardio

KEY = b'"id":"'


def load_ids(path):
    with open(path, encoding="utf-8") as fh:
        return {ln.strip().encode("utf-8") for ln in fh if ln.strip()}


def record_id(line, ids):
    """The record's id if it is in ids, else None."""
    start = 0
    while True:
        i = line.find(KEY, start)
        if i < 0:
            return None
        j = line.find(b'"', i + len(KEY))
        cand = line[i + len(KEY):j]
        if cand in ids:
            try:
                rid = json.loads(line.decode("utf-8", errors="replace")).get("id")
            except ValueError:
                rid = None
            return cand if rid == cand.decode("utf-8") else None
        start = i + 1


def main():
    p = argparse.ArgumentParser(description="Write the documents of one shard whose id is in an id list.")
    p.add_argument("shard")
    p.add_argument("--ids", required=True, help="file with one document id per line")
    p.add_argument("--outdir", required=True)
    args = p.parse_args()

    fname = args.shard.split("/")[-1]
    outfile = os.path.join(args.outdir, fname + ".jsonl")
    if os.path.exists(outfile):
        print("[EXTRACT] {} exists, skipping".format(outfile))
        return 0
    if not shardio.is_local(args.shard):
        print("[EXTRACT] ERROR: extract mode reads local shards only, not {}".format(args.shard), file=sys.stderr)
        return 2
    os.makedirs(args.outdir, exist_ok=True)
    ids = load_ids(args.ids)

    st = os.stat(args.shard)  # before the read, like a count
    size, mtime = st.st_size, int(st.st_mtime)
    tmp = outfile + ".tmp"
    seen = found = 0
    with open(tmp, "wb") as out:
        for line in shardio.iter_local_lines(args.shard):
            seen += 1
            if record_id(line, ids) is not None:
                out.write(line if line.endswith(b"\n") else line + b"\n")
                found += 1
    side = os.path.join(args.outdir, fname + ".seen.json")
    with open(side + ".tmp", "w", encoding="utf-8") as fh:
        json.dump({"filename": fname, "url": args.shard, "docs": seen, "found": found,
                   "bytes": size, "mtime": mtime}, fh)
        fh.write("\n")
    os.replace(side + ".tmp", side)  # sidecar first: an existing output always has its sidecar
    os.replace(tmp, outfile)
    print("[EXTRACT] {}: read {} docs, kept {}".format(fname, seen, found))
    return 0


if __name__ == "__main__":
    sys.exit(main())
