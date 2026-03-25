# HPLT Sampler

## Overview

- End-to-end pipeline to sample documents from large per-shard .jsonl.zst archives.
- Currently verified to work only with HPLT v3 (and even that's iffy)
- Stages: (1) per-shard counting (parallel), (2) global planning (single job), (3) per-shard sampling (parallel), (4) final concatenation.
- Orchestration is performed by a Slurm job so polling/sleeps survive disconnects.

## Prerequisites

- Python 3 and pip packages: `requests`, `zstandard`
	- Install locally: `python3 -m pip install --user requests zstandard`
- Slurm on LUMI; ensure project/account access.

## Key scripts

- `sl-orchestrator` — submit this (sbatch) to run the whole pipeline (submits worker arrays and dependent jobs).
- `sl-hplt-sampler` — worker invoked by orchestrator; modes: counts, plan, sample, concat.
- `counts.py` — per-shard counting (writes `counts/<shard>.json`).
- `plan.py` — aggregator that waits for counts and writes `<LANG>_plan.jsonl`.
- `sample.py` — per-shard sampler that reads plan and writes `samples/<shard>.jsonl`.

## Main outputs (relative to project dir)

- shard list: `shard_list_<LANG>.txt`
- per-shard counts: `./counts/<shard_filename>.json`
- plan file: `<LANG>_plan.jsonl`
- per-shard samples: `./samples/<shard_filename>.jsonl`
- final concatenated samples: `<LANG>_samples.jsonl`
- logs: `./slurm-logs/`

## Run the full pipeline (recommended)

- Submit orchestrator (runs inside Slurm and submits all jobs):
	- `sbatch sl-orchestrator <LANG>`
	- Example: `sbatch sl-orchestrator eng_Latn`

## Notes on resource limits

- Worker (`sl-hplt-sampler`) headers currently request: `partition=small`, `mem=32G`, `time=3:00:00` (these apply per array task).
- Orchestrator has longer walltime (for polling); it does little work itself.
- Make sure per-task memory/time are sufficient for your planned per-shard sample sizes (`n_take`). If a single shard is assigned many samples, reservoir memory = `n_take * avg_doc_size`; increase mem if needed.
- If you want to burn project CPU quota very quickly, misusing this script does that for you very easily. E.g. for HPLT v3, English with WDS 9 or 10 already contains over 300 shards; that's over 900 CPU hours reserved per run. So use wisely.

## Changing sample size / manual steps

- To change the global sample target, run `plan.py` yourself (it waits for counts):
	- `python3 plan.py <LANG> --n-samples 200000 --shard-list ./shard_list_<LANG>.txt --counts-dir ./counts --out ./<LANG>_plan.jsonl`
- Debug single shard:
	- Count: `python3 counts.py <LANG> <SHARD_URL> --counts-dir ./counts`
	- Sample: `python3 sample.py <LANG> <SHARD_URL> --plan ./<LANG>_plan.jsonl --outdir ./samples`

## Robustness notes

- Per-shard outputs are written atomically (tmp -> rename); safe for parallel jobs.
- `plan.py` matches expected shards from `shard_list` and waits for all counts.
- The orchestrator batches array submissions to respect partition limits; monitor with `squeue` and `slurm-logs/`.
- Ensure Python deps are available in the job environment (module or user site).
