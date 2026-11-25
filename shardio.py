"""Local-filesystem shard access (used for HPLT v4 on LUMI).

Kept free of third-party imports and compatible with Python 3.6, so it runs
under the system /usr/bin/python3. Decompression goes through the `zstdcat`
binary, which is also faster than the zstandard module.
"""
import os
import subprocess


def is_local(src):
    return not src.startswith(("http://", "https://"))


def iter_local_lines(path):
    """Yield non-empty raw lines (bytes, newline included) of a .jsonl.zst file."""
    proc = subprocess.Popen(["zstdcat", "--", path], stdout=subprocess.PIPE, bufsize=1 << 20)
    finished = False
    try:
        for line in proc.stdout:
            if line.strip():
                yield line
        finished = True
    finally:
        if not finished:
            proc.kill()
        proc.stdout.close()
        rc = proc.wait()
    if finished and rc != 0:
        raise RuntimeError("zstdcat exited with status {} on {}".format(rc, path))


def index_count(path):
    """Exact document count from the shard's hidden URL index, or None if absent.

    HPLT v4 shards on LUMI may carry `.<name>.urls.zst` next to `<name>.jsonl.zst`:
    one line per distinct URL, `url<TAB>n_docs<TAB>{positions}`. The n_docs column
    sums to the shard's document count, so the shard itself need not be read.
    """
    d, fname = os.path.split(path)
    if not fname.endswith(".jsonl.zst"):
        return None
    idx = os.path.join(d, "." + fname[: -len(".jsonl.zst")] + ".urls.zst")
    if not os.path.exists(idx):
        return None
    total = 0
    for line in iter_local_lines(idx):
        total += int(line.split(b"\t", 2)[1])
    return total


def count_local_lines(path):
    """Exact document count: URL index if present, else a full decompression pass."""
    n = index_count(path)
    if n is not None:
        return n, "index"
    return count_nonempty_lines(path), "scan"


def count_nonempty_lines(path, chunk=1 << 20):
    """Non-empty lines of a .jsonl.zst file, the same number iter_local_lines yields, in
    constant memory: read in fixed chunks, so a record of any size never has to fit
    (English HPLT 4 `poor` has lines too long for a 4 GB task)."""
    proc = subprocess.Popen(["zstdcat", "--", path], stdout=subprocess.PIPE)
    n, open_line = 0, False  # open_line: the current, unfinished line has non-whitespace
    try:
        while True:
            buf = proc.stdout.read(chunk)
            if not buf:
                break
            parts = buf.split(b"\n")
            for i, part in enumerate(parts):
                if part.strip():
                    open_line = True
                if i < len(parts) - 1:  # a newline ends this line
                    n += open_line
                    open_line = False
        n += open_line  # a last line without a newline
    finally:
        proc.stdout.close()
        rc = proc.wait()
    if rc != 0:
        raise RuntimeError("zstdcat exited with status {} on {}".format(rc, path))
    return n
