"""hplt-sample: plan a sampler run on the login node and submit it to Slurm in one go.

1. List the shards (local files, or the HPLT v3 map over HTTP) and filter by WDS.
2. Look every shard up in the shared counts index (a local shard's row counts only
   while its size and mtime still match).
3. All counts known: write the plan here, then submit the sample array and the
   finalize job. Some unknown: submit a count array for those shards, a plan job,
   the sample array and finalize. Every stage is submitted at once, chained with
   afterok dependencies and --kill-on-invalid-dep, so nothing waits on a login
   node or an orchestrator job, and a finished job is never named as a dependency.
4. Shards are packed into at most MAX_TASKS array tasks, balanced by size. Each
   shard still gets its own process and its own seed (run seed + file name), so the
   packing does not change a sample.

Run through the `hplt-sample` wrapper, which reads hplt-sampler.conf.
Standard library only; runs under Python 3.6 (LUMI's /usr/bin/python3).
"""
import argparse
import json
import math
import os
import random
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

import countsindex
import plan as planner

HERE = os.path.dirname(os.path.abspath(__file__))
V4_DEFAULT_ROOT = "/appl/local/openeurollm/training/catalogue/hplt/4.0/clean"
V3_MAP = "https://data.hplt-project.org/three/sorted/{}.map"
READ_RATE = 80e6        # compressed bytes/s per task, one full read (LUMI, measured 2026-10: 80-100 MB/s)
RSS_PER_BYTE = 14.0     # reservoir memory per compressed byte kept (English L10: 14.2 GB shard -> 185 GB)
MAX_WALL = 72 * 3600    # LUMI small partition limit


def log(msg):
    print("[SUBMIT] " + msg, flush=True)


def die(msg):
    print("[SUBMIT] ERROR: " + msg, file=sys.stderr)
    sys.exit(1)


# ---------- shards ----------

def wds_set(spec):
    if not spec:
        return None
    if "-" in spec:
        lo, hi = spec.split("-")
        return {str(i) for i in range(int(lo), int(hi) + 1)}
    return set(spec.split(","))


def list_shards(lang, local_root, wds):
    if local_root:
        d = os.path.join(local_root, lang)
        if not os.path.isdir(d):
            die("no such directory: " + d)
        names = [os.path.join(d, f) for f in os.listdir(d) if f.endswith(".jsonl.zst") and not f.startswith(".")]
        # `sort -V` order, as the orchestrator used: the plan's tie-breaks follow shard-list order
        out = subprocess.run(["sort", "-V"], input="\n".join(names) + "\n", stdout=subprocess.PIPE,
                             universal_newlines=True, check=True).stdout
    else:
        out = subprocess.run(["curl", "-sf", V3_MAP.format(lang)], stdout=subprocess.PIPE,
                             universal_newlines=True).stdout
    shards = [ln.strip() for ln in out.splitlines() if ln.strip()]
    keep = wds_set(wds)
    if keep is not None:
        shards = [s for s in shards if s.split("/")[-1].split("_", 1)[0] in keep]
    return shards


def pack(shards, sizes, k):
    """Split shards into k lists balanced by size (largest first); keep shard-list order inside each."""
    k = max(1, min(k, len(shards)))
    order = {s: i for i, s in enumerate(shards)}
    bins = [[] for _ in range(k)]
    load = [0] * k
    if all(sizes.get(s) for s in shards):
        for s in sorted(shards, key=lambda s: -sizes[s]):
            i = load.index(min(load))
            bins[i].append(s)
            load[i] += sizes[s]
    else:  # URL shards: no sizes, deal them out in turn
        for n, s in enumerate(shards):
            bins[n % k].append(s)
            load[n % k] += 1
    return [sorted(b, key=order.get) for b in bins if b], load


def walltime(nbytes, local):
    if not local:
        return 24 * 3600
    return int(min(MAX_WALL, max(2 * 3600, nbytes / READ_RATE * 2 + 1800)))


def hms(sec):
    return "{:02d}:{:02d}:{:02d}".format(sec // 3600, sec % 3600 // 60, sec % 60)


# ---------- slurm ----------

def user_jobs():
    out = subprocess.run(["squeue", "-h", "-r", "-u", os.environ.get("USER", "")], stdout=subprocess.PIPE,
                         universal_newlines=True).stdout
    return len([ln for ln in out.splitlines() if ln.strip()])


def default_max_tasks(partition):
    # LUMI per-user submit caps: small/small-g/standard-g 220, standard 120; keep 20 spare
    return {"small": 200, "small-g": 200, "standard-g": 200, "standard": 100}.get(partition, 100)


def sbatch(args, env):
    for attempt in range(5):
        p = subprocess.run(["sbatch", "--parsable"] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           universal_newlines=True, env=env)
        if p.returncode == 0:
            return p.stdout.strip().split(";")[0]
        if "timed out" in p.stderr.lower() and attempt < 4:
            log("sbatch timed out; retrying in {}s".format(15 * 2 ** attempt))
            time.sleep(15 * 2 ** attempt)
            continue
        die("sbatch failed: " + p.stderr.strip())


# ---------- run ----------

def run_dir(corpus, lang, wds):
    tag = (wds or "all").replace(",", "+")
    prefix = "" if corpus == "v3" else corpus + "-"
    return os.path.join(os.getcwd(), "results-{}{}-{}".format(prefix, lang, tag)), "{}{}-{}".format(prefix, lang, tag)


def git(*a):
    p = subprocess.run(["git", "-C", HERE] + list(a), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                       universal_newlines=True)
    return p.stdout.strip() if p.returncode == 0 else None


def write_run(rundir, run):
    tmp = os.path.join(rundir, ".run.json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(run, fh, indent=2)
        fh.write("\n")
    os.replace(tmp, os.path.join(rundir, "run.json"))


def run_jobs_queued(tag):
    out = subprocess.run(["squeue", "-h", "-u", os.environ.get("USER", ""), "-o", "%j"], stdout=subprocess.PIPE,
                         universal_newlines=True).stdout
    return [j for j in out.split() if j.endswith("-" + tag) and j.startswith("hplt_")]


def main():
    p = argparse.ArgumentParser(prog="hplt-sample", description="Draw a simple random sample of an HPLT language on Slurm.")
    p.add_argument("lang", nargs="?", default="eng_Latn", help="HPLT language code (default: eng_Latn)")
    p.add_argument("--wds", default="", help="WDS scores to include: 9-10 | 8,9,10 | 10 (default: all shards)")
    p.add_argument("--corpus", choices=("v3", "v4"), default="v3", help="v3: HTTP, or local with --local-root; v4: local (default: v3)")
    p.add_argument("--local-root", default="", help="local corpus root containing LANG/ (v4 default: the OpenEuroLLM copy on LUMI)")
    p.add_argument("--n-samples", type=int, default=1000000, help="global sample size (default: 1000000)")
    p.add_argument("--seed", type=int, default=None, help="run seed; per-shard seeds derive from it (default: drawn and recorded)")
    p.add_argument("--no-shuffle", action="store_true", help="keep the final sample grouped by shard")
    p.add_argument("--mem", default=None, help="memory per sample task, e.g. 32G (default: estimated from the plan, at least WORKER_MEM)")
    p.add_argument("--time", default=None, help="wall time per sample task, HH:MM:SS (default: estimated from shard sizes)")
    p.add_argument("--max-tasks", type=int, default=None, help="array tasks a run may queue at once (default: MAX_TASKS or the partition's)")
    p.add_argument("--counts-index", default=None, help="counts index directory (default: COUNTS_INDEX)")
    p.add_argument("--resume", metavar="RUNDIR", default=None, help="resubmit only what an interrupted run has not finished")
    p.add_argument("--dry-run", action="store_true", help="show the scope and the jobs, then exit without writing or submitting")
    a = p.parse_args()

    account = os.environ.get("ACCOUNT", "")
    partition = os.environ.get("WORKER_PARTITION", "small")
    base_mem = os.environ.get("WORKER_MEM", "4G")
    index_dir = a.counts_index or os.environ.get("COUNTS_INDEX") or os.path.join(HERE, "counts-index")
    max_tasks = a.max_tasks or int(os.environ.get("MAX_TASKS") or default_max_tasks(partition))
    if not account and not a.dry_run:
        die("ACCOUNT is not set: cp presets/<system>.conf hplt-sampler.conf and set ACCOUNT")

    if a.resume:
        rundir = os.path.abspath(a.resume)
        with open(os.path.join(rundir, "run.json"), encoding="utf-8") as fh:
            run = json.load(fh)
        lang, corpus, local_root, wds = run["lang"], run["corpus"], run["local_root"], run["wds"]
        wds = "" if wds == "all" else wds
        seed, shuffle, n_samples = run["seed"], run["shuffle"], int(run["n_samples"])
        index_dir = run["counts_index"]
        tag = run["tag"]
        if run_jobs_queued(tag):
            die("jobs of this run are still queued ({}); wait for them or cancel them first".format(", ".join(sorted(set(run_jobs_queued(tag))))))
        if os.path.exists(os.path.join(rundir, "samples.jsonl")) and not os.path.isdir(os.path.join(rundir, "samples")):
            die("{} is finished (samples.jsonl exists); nothing to resume".format(rundir))
        with open(os.path.join(rundir, "shard_list.txt"), encoding="utf-8") as fh:
            shards = [ln.strip() for ln in fh if ln.strip()]
    else:
        lang, corpus, wds, n_samples, shuffle = a.lang, a.corpus, a.wds, a.n_samples, not a.no_shuffle
        local_root = a.local_root or (V4_DEFAULT_ROOT if corpus == "v4" else "")
        rundir, tag = run_dir(corpus, lang, wds)
        if os.path.exists(rundir) and not a.dry_run:
            die("{} exists: resume it with --resume, or move it away".format(rundir))
        seed = a.seed if a.seed is not None else random.SystemRandom().randrange(1, 2 ** 30)
        shards = list_shards(lang, local_root, wds)
    if not shards:
        die("no shards for {} (corpus {}, WDS {})".format(lang, corpus, wds or "all"))
    local = bool(local_root)
    log("{} shards for {} (corpus {}, WDS {}){}".format(len(shards), lang, corpus, wds or "all",
                                                       " under " + os.path.join(local_root, lang) if local else " from the v3 map"))

    # counts index
    subset = countsindex.subset_of(shards[0], local_root)
    ipath = countsindex.index_file(index_dir, corpus, subset, lang)
    rows = countsindex.read(ipath)
    known, unknown = planner.from_index(shards, rows)
    sizes = {s: countsindex.shard_stat(s)[0] for s in shards} if local else {}
    nbytes = sum(v for v in sizes.values() if v)
    log("counts index {}: {} of {} shards known".format(ipath, len(known), len(shards)))

    # what still has to run
    plan_path = os.path.join(rundir, "plan.jsonl")
    entries = None
    if a.resume and os.path.exists(plan_path):
        with open(plan_path, encoding="utf-8") as fh:
            entries = [json.loads(ln) for ln in fh if ln.strip()]
        unknown = []
    elif not unknown:
        entries = planner.allocate(known, n_samples)
    if a.resume:
        done_counts = {f[:-5] for f in os.listdir(os.path.join(rundir, "counts"))} if os.path.isdir(os.path.join(rundir, "counts")) else set()
        unknown = [s for s in unknown if s.split("/")[-1] not in done_counts]
        sdir = os.path.join(rundir, "samples")
        have = {f[:-6] for f in os.listdir(sdir) if f.endswith(".jsonl")} if os.path.isdir(sdir) else set()
        take = {e["filename"]: e["n_take"] for e in entries} if entries else None
        to_sample = [s for s in shards if s.split("/")[-1] not in have and (take is None or take.get(s.split("/")[-1], 1) > 0)]
    else:
        to_sample = shards
    need_plan = entries is None

    # task layout within the queue budget
    avail = max_tasks - (0 if a.dry_run else user_jobs())
    n_count = min(len(unknown), max(1, avail // 3)) if unknown else 0
    n_sample = min(len(to_sample), avail - n_count - (1 if need_plan else 0) - 1)
    if to_sample and n_sample < 1:
        die("no room in the queue: {} jobs allowed, {} already queued by you".format(max_tasks, max_tasks - avail))
    count_bins, count_load = pack(unknown, sizes, n_count) if unknown else ([], [])
    sample_bins, sample_load = pack(to_sample, sizes, n_sample) if to_sample else ([], [])

    # memory: the largest reservoir a task holds (a shard taken whole is streamed, not held)
    if a.mem:
        mem = a.mem
    elif entries is not None and local:
        need = 0.0
        for e in entries:
            if 0 < e["n_take"] < e["count"] and e["url"] in sizes:
                need = max(need, sizes[e["url"]] * e["n_take"] / e["count"] * RSS_PER_BYTE)
        mem = "{}G".format(max(int(base_mem.rstrip("Gg")), int(math.ceil(need / 1e9 * 1.2)) + 2))
    else:
        mem = base_mem
        if need_plan and n_samples * 50e3 > 4e9:  # rough: up to ~50 KB a document
            log("note: memory per sample task is {} (the plan is made after counting); "
                "pass --mem if one shard's share of the sample is large".format(mem))
    sample_time = a.time or hms(walltime(max(sample_load) if local and sample_load else 0, local))
    count_time = hms(walltime(max(count_load) if local and count_load else 0, local))

    # report
    read_hours = nbytes / READ_RATE / 3600 if local else None
    log("plan: {}".format("made now from the index ({} documents planned)".format(sum(e["n_take"] for e in entries))
                          if entries is not None and not a.resume else
                          "existing plan.jsonl" if entries is not None else
                          "after counting {} shards".format(len(unknown))))
    if count_bins:
        log("count array: {} tasks over {} shards, {} each".format(len(count_bins), len(unknown), count_time))
    if sample_bins:
        log("sample array: {} tasks over {} shards, {} each, mem {}".format(len(sample_bins), len(to_sample), sample_time, mem))
    log("finalize: count check, concatenation{}".format("" if not shuffle else ", shuffle"))
    if read_hours is not None:
        reads = 2 if unknown else 1
        log("reads: {:.1f} GB compressed, ~{:.1f} task-hours per full read, {} read{}".format(
            nbytes / 1e9, read_hours, reads, "s" if reads > 1 else ""))
    if a.dry_run:
        log("dry run: nothing written or submitted (run directory would be {})".format(rundir))
        return 0

    # write the run directory
    os.makedirs(rundir, exist_ok=True)
    shutil.rmtree(os.path.join(rundir, "tasks"), ignore_errors=True)  # a resume repacks what is left
    for d in ("tasks", "logs"):
        os.makedirs(os.path.join(rundir, d), exist_ok=True)
    if not a.resume:
        with open(os.path.join(rundir, "shard_list.txt"), "w", encoding="utf-8") as fh:
            fh.write("\n".join(shards) + "\n")
        if local:
            # shard inventory (size, mtime): the corpus copy is not ours and may change between runs
            with open(os.path.join(rundir, "shard_inventory.tsv"), "w", encoding="utf-8") as fh:
                subprocess.run(["xargs", "-d", "\n", "stat", "--printf", "%n\t%s\t%y\n"],
                               input="\n".join(shards) + "\n", stdout=fh, universal_newlines=True, check=True)
        if entries is not None:
            planner.write_plan(entries, plan_path)
    for mode, bins in (("count", count_bins), ("sample", sample_bins)):
        for i, b in enumerate(bins):
            with open(os.path.join(rundir, "tasks", "{}-{:03d}.txt".format(mode, i)), "w", encoding="utf-8") as fh:
                fh.write("\n".join(b) + "\n")

    if not a.resume:
        dirty = git("status", "--porcelain", "--untracked-files=no")
        run = {
            "started": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lang": lang, "corpus": corpus, "local_root": local_root,
            "map_url": "" if local else V3_MAP.format(lang),
            "wds": wds or "all", "n_samples": str(n_samples), "seed": seed, "shuffle": shuffle,
            "n_shards": len(shards), "tag": tag,
            "counts_index": index_dir, "counts_index_file": ipath,
            "counts_known_at_submit": len(known),
            "sampler_commit": git("rev-parse", "HEAD") or "unknown", "sampler_dirty": bool(dirty),
            "python": sys.version.split()[0], "cluster": os.environ.get("SLURM_CLUSTER_NAME", "lumi" if os.environ.get("LUMI_LMOD_FAMILY_COMPILER") else "unknown"),
            "submissions": [],
        }
    run.setdefault("submissions", [])
    write_run(rundir, run)  # the jobs read it

    # submit every stage at once
    env = dict(os.environ, RUNDIR=rundir, SAMPLER_HOME=HERE, SEED_BASE=str(seed),
               SHUFFLE="true" if shuffle else "false")
    worker = os.path.join(HERE, "sl-hplt-sampler")
    common = ["--account=" + account, "--partition=" + partition, "--export=ALL"]
    jobs = {}

    def submit(mode, extra, after=None):
        e = dict(env, MODE=mode)
        logs = os.path.join(rundir, "logs", "{}_%A_%a.out".format(mode))
        dep = ["--dependency=afterok:" + after, "--kill-on-invalid-dep=yes"] if after else []
        jid = sbatch(common + ["--job-name=hplt_{}-{}".format(mode, tag), "--output=" + logs,
                               "--error=" + logs] + dep + extra + [worker], e)
        jobs[mode] = jid
        log("submitted {} job {}{}".format(mode, jid, " (after {})".format(after) if after else ""))
        return jid

    last = None
    if count_bins:
        last = submit("count", ["--array=0-{}".format(len(count_bins) - 1), "--mem=" + base_mem, "--time=" + count_time])
    if need_plan:
        last = submit("plan", ["--mem=" + base_mem, "--time=00:30:00"], last)
    if sample_bins:
        last = submit("sample", ["--array=0-{}".format(len(sample_bins) - 1), "--mem=" + mem, "--time=" + sample_time], last)
    submit("finalize", ["--mem=" + base_mem, "--time=24:00:00"], last)

    run["submissions"].append({"at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                               "jobs": jobs, "count_tasks": len(count_bins), "sample_tasks": len(sample_bins),
                               "sample_mem": mem, "sample_time": sample_time})
    write_run(rundir, run)
    log("run directory {}; logs in logs/; done when samples.jsonl appears (squeue --me to watch)".format(rundir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
