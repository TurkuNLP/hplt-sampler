"""The shared counts index: exact document counts per shard.

Layout: <index>/<corpus>/<subset>/<lang>.tsv with columns
    shard  bytes  mtime  docs  method  counted
where bytes and mtime (epoch seconds) are the shard's size and modification time
when it was counted. A local shard's row is trusted only while both still match;
a URL shard (HPLT v3 over HTTP) has no stat, so its row is trusted by name and the
sampling pass checks it. Writers lock the file and replace it atomically.

Standard library only; runs under Python 3.6 (LUMI's /usr/bin/python3).
"""
import fcntl
import os
from datetime import datetime, timezone

HEADER = ("shard", "bytes", "mtime", "docs", "method", "counted")


def index_file(index_dir, corpus, subset, lang):
    return os.path.join(index_dir, corpus, subset, lang + ".tsv")


def subset_of(shard, local_root):
    """Corpus subset a shard belongs to: the local root's last component (clean,
    noisy, poor, sorted), or the directory above the language in a URL."""
    if local_root:
        return os.path.basename(os.path.normpath(local_root))
    return shard.rstrip("/").split("/")[-3]


def shard_stat(shard):
    """(bytes, mtime) of a local shard; (None, None) for a URL."""
    if shard.startswith(("http://", "https://")):
        return None, None
    st = os.stat(shard)
    return st.st_size, int(st.st_mtime)


def read(path):
    """{shard name: row dict}; empty if the file does not exist."""
    rows = {}
    if not os.path.exists(path):
        return rows
    with open(path, encoding="utf-8") as fh:
        head = fh.readline().rstrip("\n").split("\t")
        for ln in fh:
            r = dict(zip(head, ln.rstrip("\n").split("\t")))
            if r.get("shard"):
                rows[r["shard"]] = r
    return rows


def lookup(rows, shard):
    """Trusted document count for a shard (path or URL), or None.

    A local shard needs a row whose bytes and mtime equal the file's now."""
    r = rows.get(shard.split("/")[-1])
    if r is None or not r.get("docs"):
        return None
    size, mtime = shard_stat(shard)
    if size is not None and (r.get("bytes") != str(size) or r.get("mtime") != str(mtime)):
        return None
    return int(r["docs"])


def row(shard, docs, method, size=None, mtime=None):
    """A new index row for a shard just read in full; size/mtime as taken before the read."""
    return {
        "shard": shard.split("/")[-1],
        "bytes": "" if size is None else str(size),
        "mtime": "" if mtime is None else str(mtime),
        "docs": str(docs),
        "method": method,
        "counted": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }


def _shard_key(name):
    lead = name.split("_", 1)[0]
    return (int(lead) if lead.isdigit() else 0, name)


def merge(path, new_rows):
    """Add or replace rows (by shard name) under an exclusive lock, atomically."""
    if not new_rows:
        return 0
    d = os.path.dirname(path)
    old = os.umask(0o007)  # the index is group-shared
    try:
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "." + os.path.basename(path) + ".lock"), "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            rows = read(path)
            for r in new_rows:
                rows[r["shard"]] = r
            tmp = os.path.join(d, ".{}.{}.tmp".format(os.path.basename(path), os.getpid()))
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write("\t".join(HEADER) + "\n")
                for name in sorted(rows, key=_shard_key):
                    fh.write("\t".join(rows[name].get(k, "") for k in HEADER) + "\n")
            os.replace(tmp, path)
    finally:
        os.umask(old)
    return len(new_rows)
