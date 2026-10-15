"""Shuffle the lines of a JSONL file reproducibly, without loading it into memory.

Only line offsets are held in memory (a few dozen bytes per line), so a 1M-line,
multi-GB sample shuffles in well under 1 GB. The output is written to a temporary
file and renamed into place, so the input is never left half-overwritten. The
shuffled file is checked to hold exactly the same lines as the input.

Standard library only; runs under Python 3.6 (LUMI's /usr/bin/python3).
"""
import argparse
import hashlib
import os
import random
import sys


def shuffle_seed(seed_base):
    """Shuffle seed derived from the run seed, distinct from the per-shard sampling seeds."""
    return int(hashlib.sha256("{}:shuffle".format(seed_base).encode("utf-8")).hexdigest()[:16], 16)


def line_hash(line):
    if not line.endswith(b"\n"):
        line += b"\n"
    return int.from_bytes(hashlib.blake2b(line, digest_size=8).digest(), "big")


def fingerprint(lines):
    """Order-independent fingerprint of a multiset of lines: (count, sum of hashes mod 2^64)."""
    n = 0
    total = 0
    for line in lines:
        n += 1
        total = (total + line_hash(line)) & 0xFFFFFFFFFFFFFFFF
    return n, total


def shuffle_file(src, dst, seed):
    offsets = []
    pos = 0
    n = 0
    total = 0
    with open(src, "rb") as fh:
        for line in fh:
            if line.strip():
                offsets.append((pos, len(line)))
                n += 1
                total = (total + line_hash(line)) & 0xFFFFFFFFFFFFFFFF
            pos += len(line)
    in_fp = (n, total)

    rng = random.Random(seed)
    rng.shuffle(offsets)

    tmp = dst + ".shuffle.tmp"
    with open(src, "rb") as fin, open(tmp, "wb") as fout:
        for off, length in offsets:
            fin.seek(off)
            data = fin.read(length)
            if not data.endswith(b"\n"):
                data += b"\n"
            fout.write(data)

    with open(tmp, "rb") as fh:
        out_fp = fingerprint(ln for ln in fh if ln.strip())
    if out_fp != in_fp:
        os.remove(tmp)
        raise RuntimeError("shuffle check failed: input {} vs output {}".format(in_fp, out_fp))
    os.replace(tmp, dst)
    return in_fp[0]


def main():
    p = argparse.ArgumentParser(description="Reproducibly shuffle the lines of a JSONL file (in place by default).")
    p.add_argument("file")
    p.add_argument("--seed-base", type=int, required=True, help="Run seed; the shuffle seed is derived from it")
    p.add_argument("--out", default=None, help="Output path (default: replace the input)")
    args = p.parse_args()

    dst = args.out or args.file
    print("[SHUFFLE] {} -> {} (seed base {})".format(args.file, dst, args.seed_base))
    n = shuffle_file(args.file, dst, shuffle_seed(args.seed_base))
    print("[SHUFFLE] Shuffled {} lines; content check passed".format(n))
    return 0


if __name__ == "__main__":
    sys.exit(main())
