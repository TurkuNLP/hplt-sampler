"""Shuffle the lines of a JSONL file reproducibly, without loading it into memory.

Two passes of sequential I/O through temporary bucket files; memory holds one
bucket (~256 MB) at a time. The output is written to a temporary file and renamed
into place only after it is checked to hold exactly the input's lines, so the
input is never left half-overwritten.

Standard library only; runs under Python 3.6 (LUMI's /usr/bin/python3).
"""
import argparse
import hashlib
import math
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


def shuffle_file(src, dst, seed, bucket_bytes=256 * 1024 * 1024):
    """Two-pass shuffle with sequential I/O only.

    Pass 1 streams the input and appends each line to one of B temporary bucket
    files chosen uniformly at random. Pass 2 loads one bucket at a time, shuffles
    it in memory and appends it to the output. Uniform bucket assignment followed
    by a uniform shuffle within each bucket yields a uniformly random permutation.
    (Seeking to each line in random order instead is far too slow on Lustre.)
    """
    n_buckets = max(1, int(math.ceil(os.path.getsize(src) / float(bucket_bytes))))
    rng = random.Random(seed)
    workdir = dst + ".shuffle.d"
    tmp = dst + ".shuffle.tmp"
    os.makedirs(workdir, exist_ok=True)
    try:
        paths = [os.path.join(workdir, "{:05d}".format(i)) for i in range(n_buckets)]
        buckets = [open(pth, "wb") for pth in paths]
        n_in = 0
        sum_in = 0
        try:
            with open(src, "rb") as fin:
                for line in fin:
                    if not line.strip():
                        continue
                    if not line.endswith(b"\n"):
                        line += b"\n"
                    n_in += 1
                    sum_in = (sum_in + line_hash(line)) & 0xFFFFFFFFFFFFFFFF
                    buckets[rng.randrange(n_buckets)].write(line)
        finally:
            for fh in buckets:
                fh.close()

        n_out = 0
        sum_out = 0
        with open(tmp, "wb") as fout:
            for pth in paths:
                with open(pth, "rb") as fh:
                    lines = fh.readlines()  # binary readlines splits on b"\n" only
                os.remove(pth)
                rng.shuffle(lines)
                for line in lines:
                    n_out += 1
                    sum_out = (sum_out + line_hash(line)) & 0xFFFFFFFFFFFFFFFF
                fout.writelines(lines)
                del lines

        if (n_out, sum_out) != (n_in, sum_in):
            raise RuntimeError("shuffle check failed: input {} vs output {}".format((n_in, sum_in), (n_out, sum_out)))
        os.replace(tmp, dst)
        return n_in, n_buckets
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
        for name in os.listdir(workdir) if os.path.isdir(workdir) else []:
            os.remove(os.path.join(workdir, name))
        if os.path.isdir(workdir):
            os.rmdir(workdir)


def main():
    p = argparse.ArgumentParser(description="Reproducibly shuffle the lines of a JSONL file (in place by default).")
    p.add_argument("file")
    p.add_argument("--seed-base", type=int, required=True, help="Run seed; the shuffle seed is derived from it")
    p.add_argument("--out", default=None, help="Output path (default: replace the input)")
    args = p.parse_args()

    dst = args.out or args.file
    print("[SHUFFLE] {} -> {} (seed base {})".format(args.file, dst, args.seed_base))
    n, b = shuffle_file(args.file, dst, shuffle_seed(args.seed_base))
    print("[SHUFFLE] Shuffled {} lines via {} buckets; content check passed".format(n, b))
    return 0


if __name__ == "__main__":
    sys.exit(main())
