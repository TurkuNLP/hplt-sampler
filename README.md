# HPLT Sampler

## Overview

End-to-end pipeline to proportionally sample documents from the sharded HPLT v3 corpus.
Stages: (1) per-shard counting (parallel), (2) global planning (single job), (3) per-shard sampling (parallel), (4) final concatenation.
Orchestration runs inside a Slurm job so polling/sleeps survive disconnects.

Verified with HPLT v3 on LUMI, Puhti, and Mahti.

> **This tool is intended for production-scale corpus sampling.** HPLT v3 is one of the largest generally available multilingual datasets; running the pipeline without a WDS filter on a large language streams every shard and can consume hundreds of CPU hours. The intended use case is large, representative samples for academic use — where HPLT's quality stratification and proportionality guarantees matter. If you just need a few hundred or thousand documents to inspect, unpack an appropriate shard directly: `zstd -d <shard>.jsonl.zst | head -n 1000` etc.

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

**Alternatively**, run the orchestrator directly from the terminal without a conf file and it will prompt you interactively. On LUMI, Puhti, and Mahti the system is auto-detected and preset defaults are pre-filled; on other systems it prompts with blanks.

The `#SBATCH --account` and `#SBATCH --partition` directives at the top of `sl-orchestrator` (and `sl-hplt-sampler`) are parsed by Slurm before the script runs, so they **cannot** be set from `hplt-sampler.conf`. Set them by either editing the directives directly in the scripts, or by passing `--account=X --partition=Y` on the `sbatch` command line. Worker partition (for the array jobs) is read from `WORKER_PARTITION` in `hplt-sampler.conf` and works normally.

## Key scripts

- `sl-orchestrator` — submit this (`sbatch`) to run the whole pipeline (submits worker arrays and dependent jobs).
- `sl-hplt-sampler` — worker invoked by orchestrator; modes: counts, plan, sample, concat.
- `counts.py` — per-shard counting (writes `<rundir>/counts/<shard>.json`).
- `plan.py` — aggregator that waits for counts and writes `<rundir>/plan.jsonl`.
- `sample.py` — per-shard sampler that reads plan and writes `<rundir>/samples/<shard>.jsonl`.
- `check.py` — post-run verification: reports missing or mismatched sample files.

## Main outputs

Per-shard **counts** are written to `counts/<LANG>/` at the project root and persist across runs — a shard that has already been counted is skipped automatically. Each count file records the shard URL, document count, and fetch timestamp, so corpus provenance and date are always traceable. `counts/` is gitignored.

> **Future:** once a shared `projappl/` path is available on LUMI, set `COUNTS_DIR` in `hplt-sampler.conf` to point there so all LUMI users share a single counts cache and avoid redundant recomputation.

All other run outputs land in a per-run directory `results-<LANG>-<WDS>/` (e.g. `results-eng_Latn-9-10/`, `results-eng_Latn-all/`), created at launch. The directory is gitignored by `results-*/`.

- per-shard counts: `counts/<LANG>/<shard_filename>.json` (shared, persistent)
- shard list: `results-<LANG>-<WDS>/shard_list.txt`
- plan file: `results-<LANG>-<WDS>/plan.jsonl`
- per-shard samples: `results-<LANG>-<WDS>/samples/<shard_filename>.jsonl` (temporary; deleted after successful concat)
- final concatenated samples: `results-<LANG>-<WDS>/samples.jsonl`
- logs: `slurm-logs/` (at project root, shared across runs)

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

**Sampling is proportional to document count across all selected shards.** If WDS-9 contains 99× more documents than WDS-10, the sample will reflect that ratio — you get a representative random draw from the combined tier, not equal representation per score. To sample each WDS score equally, run the pipeline separately per score, for example like this:

```
sbatch sl-orchestrator eng_Latn --wds 9
# wait for completion, then:
sbatch sl-orchestrator eng_Latn --wds 10
# wait for completion, then:
cat results-eng_Latn-9/samples.jsonl results-eng_Latn-10/samples.jsonl > eng_Latn_equal_samples.jsonl
```

Each run writes to its own `results-eng_Latn-<WDS>/` directory, so no renaming is needed.

## Notes on resource limits

- Worker (`sl-hplt-sampler`) headers request: `mem=32G`, `time=3:00:00` (per array task).
- Orchestrator has a longer walltime (72h) for polling; it does little compute itself.
- Sampling large languages even with a WDS filter in place can reserve significant CPU quota. For example, HPLT v3 English at WDS 9–10 alone has over 300 shards — that's over 900 CPU hours reserved per run. It is preferrable to use `--wds` to narrow scope whenever warranted by your data needs.

## Changing sample size / manual steps

To change the global sample target, run `plan.py` directly (it waits for counts if needed):

```
python3 plan.py <LANG> --n-samples 200000 --shard-list ./results-<LANG>-<WDS>/shard_list.txt --counts-dir ./counts/<LANG> --out ./results-<LANG>-<WDS>/plan.jsonl
```

Alternatively, you can just change the hard-coded sample parameter (`--n-samples`) from the `argparser`'s defaults (around the top of `plan.py`).

Debug a single shard:

```
python3 counts.py <LANG> <SHARD_URL> --counts-dir ./counts/<LANG>
python3 sample.py <LANG> <SHARD_URL> --plan ./results-<LANG>-<WDS>/plan.jsonl --outdir ./results-<LANG>-<WDS>/samples
```

Verify a successful run (per-shard samples are deleted after concat, so just count lines):

```
wc -l results-<LANG>-<WDS>/samples.jsonl   # should match the planned total in plan.jsonl
```

`check.py` does granular per-shard verification against the plan, but requires the per-shard `samples/` directory — useful when the run failed before concat and the directory was preserved:

```
python3 check.py <LANG> --plan ./results-<LANG>-<WDS>/plan.jsonl --samples-dir ./results-<LANG>-<WDS>/samples
```

## Robustness notes

- Per-shard outputs are written atomically (tmp → rename); safe for parallel jobs.
- `plan.py` matches expected shards from `shard_list` and waits for all counts.
- The orchestrator batches array submissions to respect partition limits; monitor with `squeue` and `slurm-logs/`.
- Ensure Python deps are available in the job environment (pip or `MODULE_LOAD_CMD` in conf).

## TODO / known limitations

- **Verify on Puhti and Mahti with large languages.** Basic functionality confirmed; large-language runs (English-scale, 1000+ shards) have not been fully exercised on these systems.
- **Multi-version support.** Currently hardcoded to HPLT v3 (`/three/` in the map URL). Adding v2 and v4 (when available) would require parameterising the corpus version — likely a `--corpus` flag or a conf key that selects the base URL.
- **Shared counts cache.** Per-shard counts should be stable per corpus version and cheap to share. A hosted or group-shared `counts/` directory (e.g. in LUMI `projappl/`) would eliminate redundant recomputation across users. *Pending: check whether a shared path can be provisioned under the project allocation.*
- **Roihu support.** Preset stub exists; fill in once Roihu becomes generally available.
- **Local filesystem access on LUMI.** HPLT data lives in `projappl/`; on LUMI, sampling could read directly from the filesystem instead of streaming over HTTP, removing the main bottleneck (HTTP is brittle with these volumes). Needs a flag or auto-detection to switch between URL and local path modes.
- **`--n-samples` on the orchestrator CLI.** Currently only configurable by running `plan.py` manually; exposing it as `sl-orchestrator --n-samples N` would avoid the manual step.
- **Dry-run mode.** A `--dry-run` flag that fetches the shard list, prints shard count and estimated resource usage, and exits without submitting any jobs would help users sanity-check scope before committing quota.
