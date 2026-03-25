import os
import sys
import json
import argparse
import time
import random

import requests
import zstandard as zstd

def stream_sample(url, n_take, timeout):
    reservoir = []
    total_seen = 0
    resp = None
    dctx = zstd.ZstdDecompressor()
    try:
        resp = requests.get(url, stream=True, timeout=timeout)
        resp.raise_for_status()
        with dctx.stream_reader(resp.raw) as reader:
            buf = b""
            while True:
                chunk = reader.read(65536)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not line.strip():
                        continue
                    text = line.decode("utf-8", errors="replace")
                    if total_seen < n_take:
                        reservoir.append(text)
                    else:
                        j = random.randint(0, total_seen)
                        if j < n_take:
                            reservoir[j] = text
                    total_seen += 1
            if buf.strip():
                text = buf.decode("utf-8", errors="replace")
                if total_seen < n_take:
                    reservoir.append(text)
                else:
                    j = random.randint(0, total_seen)
                    if j < n_take:
                        reservoir[j] = text
                total_seen += 1
    finally:
        if resp is not None:
            try:
                resp.close()
            except Exception:
                pass
    return reservoir, total_seen

def main():
    p = argparse.ArgumentParser()
    p.add_argument("lang")
    p.add_argument("shard_url")
    p.add_argument("--plan", default=None, help="Path to plan jsonl (default: <scriptdir>/<lang>_plan.jsonl)")
    p.add_argument("--outdir", default=None, help="Directory to write per-shard samples (default: ./samples)")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--backoff-base", type=float, default=5.0)
    args = p.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    script_root = os.path.dirname(__file__)
    plan_path = args.plan or os.path.join(script_root, f"{args.lang}_plan.jsonl")
    outdir = args.outdir or os.path.join(script_root, "samples")
    os.makedirs(outdir, exist_ok=True)

    fname = args.shard_url.split("/")[-1]
    print(f"[SAMPLE] lang={args.lang} shard={fname} plan={plan_path}")

    if not os.path.exists(plan_path):
        print(f"[SAMPLE] Plan file not found: {plan_path}", file=sys.stderr)
        return 2

    n_take = None
    with open(plan_path, "r", encoding="utf-8") as pf:
        for ln in pf:
            if not ln.strip():
                continue
            try:
                e = json.loads(ln)
            except Exception:
                continue
            if e.get("filename") == fname or e.get("url") == args.shard_url:
                n_take = int(e.get("n_take", 0))
                break

    if n_take is None:
        print(f"[SAMPLE] No plan entry found for shard {fname}", file=sys.stderr)
        return 3
    if n_take <= 0:
        print(f"[SAMPLE] n_take=0 for {fname}, nothing to do")
        return 0

    outfile = os.path.join(outdir, f"{fname}.jsonl")
    tmpfile = outfile + ".tmp"
    if os.path.exists(outfile):
        print(f"[SAMPLE] {outfile} exists, skipping")
        return 0

    attempt = 0
    while attempt <= args.retries:
        try:
            attempt += 1
            print(f"[SAMPLE] Attempt {attempt}/{args.retries} - streaming {args.shard_url}")
            reservoir, total_seen = stream_sample(args.shard_url, n_take, timeout=args.timeout)
            print(f"[SAMPLE] Streamed {total_seen} docs; selected {len(reservoir)}")

            # atomic write
            with open(tmpfile, "w", encoding="utf-8") as fh:
                for ln in reservoir:
                    fh.write(ln.rstrip("\n") + "\n")
            os.replace(tmpfile, outfile)
            print(f"[SAMPLE] Wrote {len(reservoir)} samples to {outfile}")
            return 0
        except requests.exceptions.RequestException as e:
            err = f"network error: {e}"
        except zstd.ZstdError as e:
            err = f"decompression error: {e}"
        except Exception as e:
            err = f"unexpected error: {e}"

        if os.path.exists(tmpfile):
            try:
                os.remove(tmpfile)
            except Exception:
                pass

        if attempt > args.retries:
            print(f"[SAMPLE] Failed after {args.retries} attempts: {err}", file=sys.stderr)
            return 4

        sleep_for = args.backoff_base * (2 ** (attempt - 1))
        sleep_for = sleep_for * (0.8 + 0.4 * random.random())
        print(f"[SAMPLE] {err}; retrying in {sleep_for:.1f}s (attempt {attempt}/{args.retries})", file=sys.stderr)
        time.sleep(sleep_for)

    return 4

if __name__ == "__main__":
    sys.exit(main())
