"""Count every shard of whole local corpus roots into the counts index.

    python3 count_all.py ROOT[=CORPUS] ... [--reserve N] [--dry-run]

e.g. count_all.py /appl/.../hplt/4.0/clean=v4 /appl/.../hplt/4.0/noisy=v4 /appl/.../hplt/3.0/sorted=v3

For each language under each root, largest first, it runs `hplt-sample LANG --count-only`
as soon as the queue has room: one count array plus one index job per language, every
shard the index does not know yet. Languages already complete are skipped by the
submitter itself; a language whose run directory exists (already submitted) is skipped
here. Run it from the directory that should hold the run directories, best as a
Slurm job of its own (it only sleeps and submits): sbatch sl-count-all.

Standard library only; runs under Python 3.6 (LUMI's /usr/bin/python3).
"""
import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from submit import default_max_tasks, user_jobs  # noqa: E402


def log(msg):
    print("[COUNT-ALL] {} {}".format(time.strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def targets(roots):
    out = []
    for spec in roots:
        root, _, corpus = spec.partition("=")
        corpus = corpus or "v4"
        for lang in sorted(os.listdir(root)):
            d = os.path.join(root, lang)
            if not os.path.isdir(d):
                continue
            size = sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d)
                       if f.endswith(".jsonl.zst") and not f.startswith("."))
            if size:
                out.append((size, root, corpus, lang))
    return sorted(out, reverse=True)


def main():
    p = argparse.ArgumentParser(description="Count every shard of local corpus roots into the counts index.")
    p.add_argument("roots", nargs="+", help="ROOT[=CORPUS], CORPUS v4 (default) or v3")
    p.add_argument("--reserve", type=int, default=30,
                   help="queue slots a language must find free before it is submitted (default: 30)")
    p.add_argument("--poll", type=int, default=120, help="seconds between queue checks (default: 120)")
    p.add_argument("--dry-run", action="store_true", help="list the languages in order and exit")
    a = p.parse_args()
    max_tasks = int(os.environ.get("MAX_TASKS") or default_max_tasks(os.environ.get("WORKER_PARTITION", "small")))

    todo = targets(a.roots)
    log("{} languages over {} roots, {:.1f} TB".format(len(todo), len(a.roots), sum(t[0] for t in todo) / 1e12))
    if a.dry_run:
        for size, root, corpus, lang in todo:
            print("{:>10.1f} GB  {}  {}  {}".format(size / 1e9, corpus, os.path.basename(root), lang))
        return 0
    failed = []
    for size, root, corpus, lang in todo:
        while max_tasks - user_jobs() < a.reserve:
            time.sleep(a.poll)
        cmd = [os.path.join(HERE, "hplt-sample"), lang, "--corpus", corpus, "--local-root", root, "--count-only"]
        log("{} {} ({:.1f} GB)".format(os.path.basename(root), lang, size / 1e9))
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        print(r.stdout, end="", flush=True)
        if r.returncode != 0 and "exists: resume it" not in r.stdout:
            failed.append("{}:{}".format(os.path.basename(root), lang))
        time.sleep(2)  # let squeue see the new jobs
    log("all submitted; {} failed to submit{}".format(len(failed), ": " + " ".join(failed) if failed else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
