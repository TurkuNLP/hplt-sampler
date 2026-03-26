# HPLT Sampler

## Overview

End-to-end pipeline to proportionally sample documents from the sharded HPLT v3 corpus.
Stages: (1) per-shard counting (parallel), (2) global planning (single job), (3) per-shard sampling (parallel), (4) final concatenation.
Orchestration runs inside a Slurm job so polling/sleeps survive disconnects.

Currently verified with HPLT v3 on LUMI; designed to be portable across CSC systems via `hplt-sampler.conf`.

## Prerequisites

- Python 3 with `zstandard` (`requests` is system-provided on LUMI):
	- `python3 -m pip install --user zstandard`
	- On other systems, also install `requests` if not available: `python3 -m pip install --user requests zstandard`
	- On HPC systems where pip packages aren't available in the default environment, set `MODULE_LOAD_CMD` in your `hplt-sampler.conf` (see Setup).
- Slurm access with a valid project account.

## Setup (one-time)

System-specific presets live in `presets/`. Copy the one matching your system:

```
cp presets/lumi.conf hplt-sampler.conf    # LUMI
cp presets/puhti.conf hplt-sampler.conf   # Puhti
cp presets/mahti.conf hplt-sampler.conf   # Mahti
```

Then set `ACCOUNT` in `hplt-sampler.conf`. If no preset exists for your system, copy `hplt-sampler.conf.example` instead and fill in all values.

`hplt-sampler.conf` is gitignored and never committed.

**Alternatively**, run the orchestrator directly from the terminal without a conf file and it will prompt you interactively. On LUMI the system is auto-detected and preset defaults are pre-filled; on other systems it prompts with blanks.

Also edit the `#SBATCH --account` and `#SBATCH --partition` directives at the top of `sl-orchestrator` and `sl-hplt-sampler` to match your system — or override them on the `sbatch` command line (see below).

## Key scripts

- `sl-orchestrator` — submit this (`sbatch`) to run the whole pipeline (submits worker arrays and dependent jobs).
- `sl-hplt-sampler` — worker invoked by orchestrator; modes: counts, plan, sample, concat.
- `counts.py` — per-shard counting (writes `counts/<shard>.json`).
- `plan.py` — aggregator that waits for counts and writes `<LANG>_plan.jsonl`.
- `sample.py` — per-shard sampler that reads plan and writes `samples/<shard>.jsonl`.
- `check.py` — post-run verification: reports missing or mismatched sample files.

## Main outputs (relative to project dir)

- shard list: `shard_list_<LANG>.txt`
- per-shard counts: `counts/<shard_filename>.json`
- plan file: `<LANG>_plan.jsonl`
- per-shard samples: `samples/<shard_filename>.jsonl`
- final concatenated samples: `<LANG>_samples.jsonl`
- logs: `slurm-logs/`

## Run the full pipeline (recommended)

Submit the orchestrator. Override `--account` and `--partition` on the command line if you haven't edited the `#SBATCH` headers:

```
sbatch --account=YOUR_ACCOUNT --partition=small sl-orchestrator eng_Latn
```

With a WDS filter (see below):

```
sbatch --account=YOUR_ACCOUNT --partition=small sl-orchestrator eng_Latn --wds 9-10
```

If `#SBATCH --account` and `#SBATCH --partition` are already set correctly in `sl-orchestrator`, the flags can be omitted:

```
sbatch sl-orchestrator eng_Latn
```

## WDS filtering

HPLT v3 shards are organised by Web Docs Scorer (WDS) score — a per-document quality score from 0 to 10. Shard filenames are prefixed with their WDS score (e.g. `9_1.jsonl.zst`, `10_3.jsonl.zst`). See [web-docs-scorer](https://github.com/pablop16n/web-docs-scorer) for details.

By default the pipeline samples from all available shards. Use `--wds` to restrict to a score range or set:

```
--wds 9-10          # scores 9 and 10 (range)
--wds 8,9,10        # scores 8, 9, and 10 (comma-separated set)
--wds 10            # score 10 only
```

HPLT v3 shards cover WDS scores 5–10. Not all languages have shards at every score level; an empty shard list will abort with an error.

## Notes on resource limits

- Worker (`sl-hplt-sampler`) headers request: `mem=32G`, `time=3:00:00` (per array task).
- Orchestrator has a longer walltime (72h) for polling; it does little compute itself.
- Sampling large languages even with a WDS filter in place can reserve significant CPU quota. For example, HPLT v3 English at WDS 9–10 alone has over 300 shards — that's over 900 CPU hours reserved per run. It is preferrable to use `--wds` to narrow scope whenever warranted by your data needs.

## Changing sample size / manual steps

To change the global sample target, run `plan.py` directly (it waits for counts if needed):

```
python3 plan.py <LANG> --n-samples 200000 --shard-list ./shard_list_<LANG>.txt --counts-dir ./counts --out ./<LANG>_plan.jsonl
```

Debug a single shard:

```
python3 counts.py <LANG> <SHARD_URL> --counts-dir ./counts
python3 sample.py <LANG> <SHARD_URL> --plan ./<LANG>_plan.jsonl --outdir ./samples
```

Verify results after a run:

```
python3 check.py <LANG>
```

## Robustness notes

- Per-shard outputs are written atomically (tmp → rename); safe for parallel jobs.
- `plan.py` matches expected shards from `shard_list` and waits for all counts.
- The orchestrator batches array submissions to respect partition limits; monitor with `squeue` and `slurm-logs/`.
- Ensure Python deps are available in the job environment (pip or `MODULE_LOAD_CMD` in conf).
