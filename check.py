import argparse
import json
import os
import sys


def main():
    p = argparse.ArgumentParser(
        description="Check per-shard sample files against the plan: report missing files and line-count mismatches."
    )
    p.add_argument("lang", nargs="?", default="eng_Latn", help="Language code (default: eng_Latn)")
    p.add_argument("--plan", default=None, help="Path to plan .jsonl (default: ./<lang>_plan.jsonl)")
    p.add_argument("--samples-dir", default="samples", help="Directory containing per-shard sample files (default: ./samples)")
    args = p.parse_args()

    plan_path = args.plan or f"{args.lang}_plan.jsonl"
    samples_dir = args.samples_dir

    if not os.path.exists(plan_path):
        print(f"Plan file not found: {plan_path}", file=sys.stderr)
        return 1

    tot_plan = 0
    tot_have = 0
    n_ok = 0
    n_missing = 0
    n_mismatch = 0

    with open(plan_path, "r", encoding="utf-8") as fh:
        for ln in fh:
            if not ln.strip():
                continue
            j = json.loads(ln)
            fn = j["filename"]
            n = j["n_take"]
            tot_plan += n
            sample_path = os.path.join(samples_dir, fn + ".jsonl")
            if os.path.exists(sample_path):
                with open(sample_path, "r", encoding="utf-8") as sf:
                    cnt = sum(1 for line in sf if line.strip())
                tot_have += cnt
                if cnt != n:
                    print(f"MISMATCH  {fn}: plan={n} have={cnt}")
                    n_mismatch += 1
                else:
                    n_ok += 1
            else:
                print(f"MISSING   {fn}: plan={n}")
                n_missing += 1

    total_shards = n_ok + n_mismatch + n_missing
    print(
        f"\ntotal plan={tot_plan} found={tot_have} | "
        f"shards: {total_shards} total, {n_ok} ok, {n_mismatch} mismatch, {n_missing} missing"
    )
    return 0 if (n_missing == 0 and n_mismatch == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
