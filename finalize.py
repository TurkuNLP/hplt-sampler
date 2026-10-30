"""Finalize check: did every shard hold as many documents as the plan assumed?

The plan allocates from counts (mostly the shared index); sampling reads each shard
in full, so it knows the true count for free. On any mismatch the allocation was
wrong and the sample is not a proportional one: the index is corrected from the
reads, count_check.json names the shards, and the run stops before concatenation
(rerun it from scratch; the corrected index makes the new plan right).

Standard library only; runs under Python 3.6 (LUMI's /usr/bin/python3).
"""
import json
import os
import sys

import countsindex


def main():
    rundir = sys.argv[1]
    with open(os.path.join(rundir, "run.json"), encoding="utf-8") as fh:
        run = json.load(fh)
    with open(os.path.join(rundir, "plan.jsonl"), encoding="utf-8") as fh:
        plan = [json.loads(ln) for ln in fh if ln.strip()]
    sdir = os.path.join(rundir, "samples")
    ipath = run.get("counts_index_file")
    rows = countsindex.read(ipath) if ipath else {}

    missing, mismatched, fresh = [], [], []
    checked = 0
    for e in plan:
        if int(e["n_take"]) <= 0:
            continue
        side = os.path.join(sdir, e["filename"] + ".seen.json")
        if not (os.path.exists(os.path.join(sdir, e["filename"] + ".jsonl")) and os.path.exists(side)):
            missing.append(e["filename"])
            continue
        with open(side, encoding="utf-8") as fh:
            seen = json.load(fh)
        checked += 1
        if int(seen["docs"]) != int(e["count"]):
            mismatched.append({"shard": e["filename"], "plan_count": int(e["count"]), "read": int(seen["docs"])})
        r = rows.get(e["filename"])
        if r is None or r.get("docs") != str(seen["docs"]) or (
                seen.get("bytes") is not None and r.get("bytes") != str(seen["bytes"])):
            fresh.append(countsindex.row(e["url"], seen["docs"], "scan", seen.get("bytes"), seen.get("mtime")))

    updated = countsindex.merge(ipath, fresh) if ipath else 0
    out = {"checked": checked, "missing": missing, "mismatched": mismatched, "index_rows_updated": updated}
    with open(os.path.join(rundir, "count_check.json"), "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2)
        fh.write("\n")

    if missing:
        print("[FINALIZE] ERROR: {} shards have no sample yet (e.g. {}); resume the run".format(len(missing), missing[0]), file=sys.stderr)
        return 1
    if mismatched:
        print("[FINALIZE] ERROR: {} of {} shards held a different number of documents than planned (e.g. {}: "
              "plan {}, read {}). The allocation was wrong, so this is not a proportional sample. The index is "
              "corrected ({} rows); rerun from scratch. Details: count_check.json".format(
                  len(mismatched), checked, mismatched[0]["shard"], mismatched[0]["plan_count"],
                  mismatched[0]["read"], updated), file=sys.stderr)
        return 1
    print("[FINALIZE] Count check passed: {} shards read exactly as planned; {} index rows added or refreshed".format(checked, updated))
    return 0


if __name__ == "__main__":
    sys.exit(main())
