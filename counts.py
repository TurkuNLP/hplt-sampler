import argparse
import json
import os
import sys
import time
import random
from datetime import datetime, timezone

try:  # only needed for URL shards (HPLT v3); local shards (v4 on LUMI) use shardio
    import requests
    import zstandard as zstd
    NET_ERRORS = (requests.exceptions.RequestException, zstd.ZstdError)
except ImportError:
    requests = zstd = None
    NET_ERRORS = ()

import shardio

MAX_LINE_BYTES = 50 * 1024 * 1024  # 50 MB


def count_lines_from_url(url, timeout):
    count = 0
    buffer = b""
    response = None
    dctx = zstd.ZstdDecompressor()
    try:
        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()
        with dctx.stream_reader(response.raw) as reader:
            while True:
                chunk = reader.read(65536)
                if not chunk:
                    break
                buffer += chunk
                if len(buffer) > MAX_LINE_BYTES and b"\n" not in buffer:
                    print(
                        f"[COUNT] WARNING: oversized record (>{MAX_LINE_BYTES // 1024 // 1024} MB), counting and discarding",
                        file=sys.stderr,
                    )
                    count += 1
                    buffer = b""
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if line.strip():
                        count += 1
            if buffer.strip():
                count += 1
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
    return count


def main():
    p = argparse.ArgumentParser(description="Count non-empty lines in a zstd-compressed shard and write per-shard JSON count.")
    p.add_argument("lang", help="Language code (unused except for logging)")
    p.add_argument("shard_url", help="Full URL to shard (zstd .zst stream), or a local .jsonl.zst path")
    p.add_argument("--counts-dir", default=None, help="Directory to write per-shard count files (default: ./counts next to script)")
    p.add_argument("--timeout", type=int, default=60, help="HTTP request timeout (s)")
    p.add_argument("--retries", type=int, default=5, help="Number of retries on network/error")
    p.add_argument("--backoff-base", type=float, default=5.0, help="Base seconds for exponential backoff")
    args = p.parse_args()

    lang = args.lang
    url = args.shard_url
    fname = url.split("/")[-1]

    script_dir = os.path.dirname(__file__)
    outdir = args.counts_dir or os.path.join(script_dir, "counts", lang)
    os.makedirs(outdir, exist_ok=True)
    outfile = os.path.join(outdir, f"{fname}.json")
    tmpfile = outfile + ".tmp"

    print(f"[COUNT] lang={lang} shard={fname} -> {outfile}")

    if os.path.exists(outfile):
        print(f"[COUNT] {outfile} exists, skipping")
        return 0

    attempt = 0
    while attempt <= args.retries:
        try:
            attempt += 1
            print(f"[COUNT] Attempt {attempt}/{args.retries} - counting lines in: {url}")
            entry = {"filename": fname, "url": url}
            if shardio.is_local(url):
                st = os.stat(url)  # taken before the read: the count is valid for this size and mtime
                entry["bytes"], entry["mtime"] = st.st_size, int(st.st_mtime)
                count, method = shardio.count_local_lines(url)
                entry["method"] = method
            else:
                count = count_lines_from_url(url, timeout=args.timeout)
                entry["method"] = "scan"
            print(f"[COUNT] Found {count} non-empty lines in {fname}")

            # atomic write
            entry["count"] = count
            entry["fetched_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            with open(tmpfile, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(entry) + "\n")
            os.replace(tmpfile, outfile)
            print(f"[COUNT] Saved {fname} -> {outfile}")
            return 0
        except NET_ERRORS as e:
            err = f"network/decompression error: {e}"
        except Exception as e:
            err = f"unexpected error: {e}"

        # Clean up partial tmpfile if present
        if os.path.exists(tmpfile):
            try:
                os.remove(tmpfile)
            except Exception:
                pass

        if attempt > args.retries:
            print(f"[COUNT] Failed after {args.retries} attempts: {err}", file=sys.stderr)
            return 2

        # exponential backoff with jitter
        sleep_for = args.backoff_base * (2 ** (attempt - 1))
        sleep_for = sleep_for * (0.8 + 0.4 * random.random())
        print(f"[COUNT] {err}; retrying in {sleep_for:.1f}s (attempt {attempt}/{args.retries})", file=sys.stderr)
        time.sleep(sleep_for)

    return 2


if __name__ == "__main__":
    sys.exit(main())
