# HPLT Sampler

## Overview

End-to-end Slurm pipeline that draws a **simple random sample** of documents from a sharded HPLT corpus, allocated across shards in proportion to their document counts.

- **HPLT v3**: shards are streamed over HTTP from `data.hplt-project.org`.
- **HPLT v4**: shards are read from a local copy (by default the OpenEuroLLM catalogue copy on LUMI). v4 has no usable public shard map (as of 2026-10-13 the `four/clean/*.map` files list `/three/` URLs).

Stages: (1) look every shard up in a shared **counts index**, (2) count only the shards it lacks (parallel), (3) plan the allocation, (4) reservoir-sample every shard (parallel), (5) finalize: check each shard's read count against the plan, concatenate, shuffle. `hplt-sample` runs on the login node, makes the plan itself when all counts are known, and submits every stage at once, chained by Slurm dependencies: no orchestrator job and nothing polling.

Verified: v3 and v4 on LUMI (v4: Finnish end-to-end tests, 2026-10-13/15; first English run 2026-10-15).

> **This tool is meant for production-scale samples.** Sampling reads every selected shard in full: a reservoir sample cannot stop early. HPLT English is ~18 TB compressed. Use `--dry-run` to see the scope before submitting, and `--wds` to narrow it where your data needs allow. If you only need a few hundred documents to inspect, read a shard directly: `zstdcat <shard>.jsonl.zst | head -n 1000`.

## Prerequisites

- Slurm access with a valid project account.
- **v3 (HTTP):** Python 3 with `requests` and `zstandard` (`python3 -m pip install --user zstandard`; `requests` is system-provided on LUMI). On HPC systems where pip packages aren't available, set `MODULE_LOAD_CMD` in `hplt-sampler.conf`.
- **v4 (local files):** only the Python standard library (Python ≥ 3.6) and the `zstdcat` binary. On LUMI use the system `/usr/bin/python3` (`PYTHON` in the conf; the LUMI preset sets it). The CSC AI `python3` is a container that **cannot see `/appl/local/openeurollm`**, so files there come back as "not found".

## Setup (one-time)

Copy the preset for your system and set `ACCOUNT`:

```
cp presets/lumi.conf hplt-sampler.conf    # LUMI
```

(`presets/puhti.conf` and `presets/mahti.conf` are deprecated: CSC has decommissioned both machines.)

With no preset, copy `hplt-sampler.conf.example` and fill in all values. `hplt-sampler.conf` is gitignored and never committed. 

Configuration keys:

| Key | Required | Meaning |
|---|---|---|
| `ACCOUNT` | yes | Slurm project account for the jobs |
| `WORKER_PARTITION` | yes | A CPU partition (e.g. LUMI `small`) |
| `COUNTS_INDEX` | no | Shared counts index directory (default: `counts-index/` next to the scripts). TurkuNLP on LUMI: `/projappl/project_462001491/hplt-counts` |
| `PYTHON` | no | Interpreter (default `python3`); LUMI preset: `/usr/bin/python3` |
| `MODULE_LOAD_CMD` | no | Shell command(s) run in each job before Python |
| `WORKER_MEM` | no | Memory for count, plan and finalize jobs, and the floor for sample tasks (LUMI preset `4G`; memory on `small` is billed as cores) |
| `MAX_TASKS` | no | Array tasks one run may queue at once (default per partition: LUMI `small` 200, under its 220-job submit cap) |

## Usage

```
./hplt-sample [LANG] [options]
```

| Option | Default | Meaning |
|---|---|---|
| `LANG` | `eng_Latn` | HPLT language code |
| `--wds SPEC` | all shards | WDS scores to include: `9-10`, `8,9,10`, `10` |
| `--corpus v3\|v4` | `v3` | v3: HTTP from the HPLT map, or a local copy with `--local-root`; v4: local files |
| `--local-root DIR` | v4: `/appl/local/openeurollm/training/catalogue/hplt/4.0/clean` | Local corpus root containing `LANG/` (e.g. `.../4.0/noisy`, or HPLT 3 at `.../hplt/3.0/sorted`) |
| `--n-samples N` | 1000000 | Global sample size |
| `--seed S` | drawn and recorded | Run seed (see Reproducibility) |
| `--no-shuffle` | shuffle | Keep the final sample grouped by shard |
| `--mem M` | estimated | Memory per sample task; by default the largest reservoir in the plan (shards taken whole are streamed and need none), at least `WORKER_MEM` |
| `--time T` | estimated | Wall time per sample task, from the bytes it reads |
| `--max-tasks N` | `MAX_TASKS` | Array tasks this run may queue |
| `--counts-index DIR` | `COUNTS_INDEX` | Counts index to use |
| `--resume RUNDIR` | | Resubmit only what an interrupted run has not finished (refused while its jobs are still queued) |
| `--dry-run` | | Show shards, known counts, the job layout, memory, wall time and read volume; writes and submits nothing |

Examples:

```
./hplt-sample eng_Latn --corpus v4 --dry-run                        # scope check
./hplt-sample eng_Latn --corpus v4 --n-samples 1000000 --seed 20261015
./hplt-sample fin_Latn --corpus v4 --wds 10 --n-samples 5000
./hplt-sample fin_Latn --corpus v3 --local-root /appl/local/openeurollm/training/catalogue/hplt/3.0/sorted
./hplt-sample eng_Latn --wds 9-10                                   # v3 over HTTP
```

A new run refuses an existing run directory; use `--resume` or move it away. Watch with `squeue --me`; the run is done when `samples.jsonl` appears (job logs in `<run>/logs/`).

## Outputs

Each run writes to its own directory, `results-<LANG>-<WDS>/` for v3 and `results-<corpus>-<LANG>-<WDS>/` for other corpora (e.g. `results-v4-eng_Latn-all/`, `results-v4-fin_Latn-9-10/`). `results-*/` is gitignored.

| File | Content |
|---|---|
| `samples.jsonl` | The sample: full original records, one per line, **shuffled** (so any prefix is itself a random sample), or grouped by shard with `--no-shuffle` |
| `plan.jsonl` | Per shard: document count and number of documents drawn |
| `shard_list.txt` | The shards (URLs or paths) the run used |
| `run.json` | Run record: arguments, seed, counts index used and how many shards it knew, sampler commit (and whether the tree was dirty), Python version, cluster, each submission's job ids and task layout |
| `shard_inventory.tsv` | Local corpora only: path, size and mtime of each shard, because a local copy may change between runs |
| `count_check.json` | Finalize's check: shards whose sampling read did not match the planned count (must be none) |
| `tasks/`, `logs/` | Which shards each array task processes; job logs |
| `counts/`, `samples/` | This run's fresh counts and per-shard samples (+ `.seen.json`: what each read found); `samples/` is deleted after a successful finalize |

## The counts index

Exact documents per shard, shared across runs and users: `<COUNTS_INDEX>/<corpus>/<subset>/<lang>.tsv` with `shard, bytes, mtime, docs, method, counted` (e.g. `v4/clean/eng_Latn.tsv`; HPLT 3: `v3/sorted/<lang>.tsv`).

- **Read:** a local shard's count is used only while its size and mtime equal the row's; otherwise it is counted again. URL shards (v3 over HTTP) have no stat and are trusted by name.
- **Checked:** sampling reads every shard in full, so it knows the true count at no cost. Finalize compares it with the plan; on any difference the allocation was wrong, the run stops before concatenation, the index row is corrected from the read, and the run should be redone from scratch.
- **Written:** by the plan job (fresh counts) and finalize (corrections), under a file lock, replaced atomically. Rows come only from full reads.

## WDS filtering

HPLT shards are organised by Web Docs Scorer (WDS) score, which the filename starts with: `9_1.jsonl.zst`, `10_3.jsonl.zst`; v4 English adds a source tag, `8_30.ia.jsonl.zst` (`cc` Common Crawl, `ia` Internet Archive, `ab` ArchiveBot). See [web-docs-scorer](https://github.com/pablop16n/web-docs-scorer). In v4 the per-document score is `doc_scores[0]` on a 0–1 scale, and the shard prefix is `floor(10 × score)`. HPLT v3 shards and the v4 "clean" subset cover scores 5–10. The match is anchored on `<score>_`, so `--wds 1` does not pick up WDS-10 shards.

**Sampling is proportional across all selected shards.** If WDS 9 holds 99× more documents than WDS 10, the sample reflects that ratio. For equal numbers per score, run once per score; each run gets its own directory:

```
./hplt-sample eng_Latn --corpus v4 --wds 9  --n-samples 100000 --seed 1
./hplt-sample eng_Latn --corpus v4 --wds 10 --n-samples 100000 --seed 1
```

## Reproducibility

Every run has a seed: `--seed S`, or one drawn and written to `run.json`. Each shard's seed is derived from the run seed and the shard filename (and reset before every retry), and the final shuffle's seed from the run seed, so a rerun with the same seed, corpus, shard list and counts reproduces the sample exactly, independent of shard order and task packing (verified 2026-10-30 against a run made by the earlier orchestrator). Commit your changes before a production run so `sampler_commit` in `run.json` identifies the code.

## Resources and limits

- A local v4 shard is read at roughly 80–100 MB/s compressed per task on LUMI (measured 2026-10). English v4 (18.2 TB) is about 63 task-hours of reading; packed into ~200 tasks that is well under an hour of wall time.
- Shards are packed into at most `MAX_TASKS` array tasks, balanced by size; each shard still runs as its own process. LUMI `small` allows 220 submitted jobs per user (pending + running, every array task counted), so the submitter counts your queued jobs and fits the run into what is left.
- Every stage is submitted at once with `afterok` dependencies. A failed stage cancels the ones after it (`--kill-on-invalid-dep=yes`) instead of leaving them pending; a task with a failed shard finishes the others, and `--resume` redoes only what is missing. (Naming an already finished job as a dependency is rejected once Slurm has forgotten it, `MinJobAge` = 300 s on LUMI; submitting everything up front never does that.)
- A shard taken whole (all of a WDS level) is streamed to disk, not held in memory.
- Large v3 runs over HTTP stream every shard and can reserve many CPU hours; on LUMI prefer the local copy (`--local-root .../hplt/3.0/sorted`).

## Validating and publishing

A finished run is validated and published to object storage with:

```
bash publish.sh results-<...> [DEST_PREFIX]      # default: lumi-462001491-private:hplt-samples
```

- `validate.py RUNDIR` (run first by `publish.sh`, or on its own) does structural checks only: run record present, plan covers the shard list, record count equals the plan, every record parses, ids present and unique, and whether the corpus files still match `shard_inventory.tsv`. It writes `validation.json`; any failure stops the publish.
- `publish.sh` compresses the sample (`samples.jsonl.zst`), uploads it with `run.json`, `plan.jsonl`, the shard list and inventory, `validation.json` and `SHA256SUMS` to `<DEST_PREFIX>/<corpus>/<lang>/<wds>-n<N>-s<seed>/`, and verifies the upload by downloading and comparing.
- **Contract:** publish only samples that someone other than the drawing session will read (a reference sample, or the input of a dataset others rebuild from); intermediate pools that feed a single build stay on scratch, and their `run.json`, shard inventory and `validation.json` are kept with what they built. What is published has passed validation, and an existing destination is never overwritten. Downstream tools read samples from there, not from raw shards. Needs `rclone` configured for the destination (on LUMI: `module load lumio; lumio-conf`).

## Manual steps and verification

Debug a single shard (a URL or a local path):

```
python3 counts.py <LANG> <SHARD> --counts-dir ./results-<...>/counts
python3 sample.py <LANG> <SHARD> --plan ./results-<...>/plan.jsonl --outdir ./results-<...>/samples --seed-base <S>
```

Shuffle an existing sample (e.g. one made with `--no-shuffle`); in place by default, with a content check before the file is replaced:

```
python3 shuffle.py results-<...>/samples.jsonl --seed-base <S>
```

`check.py` compares per-shard sample files against the plan; it needs `samples/`, which is kept only when finalize did not succeed.

## Robustness notes

- The shuffle is two passes of sequential I/O through temporary bucket files (~256 MB each; one in memory at a time). It checks that the output holds exactly the input's lines (order-independent fingerprint) and only then replaces the file.
- Per-shard outputs are written atomically (tmp → rename), and a rerun skips shards whose output exists.
- HTTP reads retry with exponential backoff; `sbatch` calls retry on socket timeouts.

## Known limitations / TODO

- **Roihu**: the preset is a stub; partition and module details are still to be filled in.
- **HPLT v2** is not supported.
- **One language per run.** Several languages in one submission (shared arrays over (language, shard) pairs) would need fewer queue slots than separate runs; not built.

## Licence

The code is licensed under the [Apache License 2.0](LICENSE).
