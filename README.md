# HPLT Sampler

## Overview

End-to-end Slurm pipeline that draws a **simple random sample** of documents from a sharded HPLT corpus, allocated across shards in proportion to their document counts.

- **HPLT v3**: shards are streamed over HTTP from `data.hplt-project.org`.
- **HPLT v4**: shards are read from a local copy (by default the OpenEuroLLM catalogue copy on LUMI). v4 has no usable public shard map (as of 2026-10-13 the `four/clean/*.map` files list `/three/` URLs).

Stages: (1) per-shard counting (parallel), (2) global planning (single job), (3) per-shard reservoir sampling (parallel), (4) concatenation and a seeded shuffle. Orchestration runs inside a Slurm job, so polling and waits survive disconnects.

Verified: v3 on LUMI, Puhti and Mahti (English, 1294 shards, 1M documents, on Mahti); v4 on LUMI (Finnish end-to-end tests, 2026-10-13/15; first English run 2026-10-15).

> **This tool is meant for production-scale samples.** Sampling reads every selected shard in full: a reservoir sample cannot stop early. HPLT English is ~18 TB compressed. Use `--dry-run` to see the scope before submitting, and `--wds` to narrow it where your data needs allow. If you only need a few hundred documents to inspect, read a shard directly: `zstdcat <shard>.jsonl.zst | head -n 1000`.

## Prerequisites

- Slurm access with a valid project account.
- **v3 (HTTP):** Python 3 with `requests` and `zstandard` (`python3 -m pip install --user zstandard`; `requests` is system-provided on LUMI). On HPC systems where pip packages aren't available, set `MODULE_LOAD_CMD` in `hplt-sampler.conf`.
- **v4 (local files):** only the Python standard library (Python ≥ 3.6) and the `zstdcat` binary. On LUMI use the system `/usr/bin/python3` (`PYTHON` in the conf; the LUMI preset sets it). The CSC AI `python3` is a container that **cannot see `/appl/local/openeurollm`**, so files there come back as "not found".

## Setup (one-time)

Copy the preset for your system and set `ACCOUNT`:

```
cp presets/lumi.conf hplt-sampler.conf    # LUMI
cp presets/puhti.conf hplt-sampler.conf   # Puhti
cp presets/mahti.conf hplt-sampler.conf   # Mahti
```

With no preset, copy `hplt-sampler.conf.example` and fill in all values. `hplt-sampler.conf` is gitignored and never committed. Run interactively without a conf file and the orchestrator prompts for the values (LUMI, Puhti and Mahti are auto-detected and their preset defaults pre-filled).

Configuration keys:

| Key | Required | Meaning |
|---|---|---|
| `ACCOUNT` | yes | Slurm project account for the worker jobs |
| `WORKER_PARTITION` | yes | Partition for the worker array jobs; a CPU partition (e.g. LUMI `small`), not a GPU one |
| `MODULE_LOAD_CMD` | no | Shell command(s) run in each worker before Python |
| `PYTHON` | no | Interpreter for the Python stages (default `python3`); LUMI preset: `/usr/bin/python3` |
| `WORKER_MEM` | no | Memory per worker task (default: the worker's `#SBATCH --mem=32G`). A per-shard reservoir needs far less, and on LUMI `small` requested memory is billed as cores; the LUMI preset uses `4G` |
| `BATCH` | no | Max array tasks per submission (auto-sized per system and partition) |

The orchestrator's own `#SBATCH --account` and `--partition` are read by Slurm before the script runs, so they cannot come from the conf. Edit those two lines in `sl-orchestrator`, or pass `--account=X --partition=Y` to `sbatch`.

## Usage

```
sbatch [--account=X --partition=Y] sl-orchestrator [LANG] [options]
bash sl-orchestrator [LANG] [options] --dry-run
```

| Option | Default | Meaning |
|---|---|---|
| `LANG` | `eng_Latn` | HPLT language code |
| `--wds SPEC` | all shards | WDS scores to include: `9-10`, `8,9,10`, `10` |
| `--corpus v3\|v4` | `v3` | v3 streams over HTTP; v4 reads local files |
| `--local-root DIR` | v4: `/appl/local/openeurollm/training/catalogue/hplt/4.0/clean` | Local corpus root containing `LANG/` (e.g. `.../4.0/noisy` for the noisy subset) |
| `--n-samples N` | 1000000 | Global sample size |
| `--seed S` | drawn and recorded | Run seed (see Reproducibility) |
| `--no-shuffle` | shuffle | Keep the final sample grouped by shard instead of shuffling it |
| `--dry-run` | | List the shards and estimate scope, then exit. Submits nothing, writes nothing, needs no `sbatch` |
| `-h`, `--help` | | Usage |

Unknown options are rejected, so a typo cannot be taken for the language code.

Examples:

```
bash   sl-orchestrator eng_Latn --corpus v4 --dry-run                      # scope check
sbatch sl-orchestrator eng_Latn --corpus v4 --n-samples 1000000 --seed 20261015
sbatch sl-orchestrator fin_Latn --corpus v4 --wds 10 --n-samples 5000
sbatch sl-orchestrator eng_Latn --wds 9-10                                 # v3, HTTP
```

The dry run reports the number of shards per WDS score, how many shard counts are already cached (and the document total when all are), the compressed size and a rough read time for local corpora.

## Outputs

Each run writes to its own directory, `results-<LANG>-<WDS>/` for v3 and `results-<corpus>-<LANG>-<WDS>/` for other corpora (e.g. `results-v4-eng_Latn-all/`, `results-v4-fin_Latn-9-10/`). `results-*/` is gitignored.

| File | Content |
|---|---|
| `samples.jsonl` | The sample: full original records, one per line, **shuffled** (so any prefix is itself a random sample), or grouped by shard with `--no-shuffle` |
| `plan.jsonl` | Per shard: document count and number of documents drawn |
| `shard_list.txt` | The shards (URLs or paths) the run used |
| `run.json` | Run record: arguments, seed, sampler commit (and whether the tree was dirty), Python version, cluster, orchestrator job id, start time |
| `shard_inventory.tsv` | Local corpora only: path, size and mtime of each shard, because a local copy may change between runs |
| `samples/` | Per-shard samples; temporary, deleted after a successful concat |

Per-shard **counts** are cached across runs: `counts/<LANG>/` (v3) and `counts/<corpus>/<LANG>/` (others), at the project root, gitignored. A shard already counted is skipped. Each count file records the shard, the count and when it was taken; v4 counts also record the method:
- `"index"`: summed from the shard's hidden URL index (`.<shard>.urls.zst`, second column), near-instant. Most English v4 shards on LUMI have one.
- `"scan"`: the shard was decompressed once and its lines counted.

Slurm logs go to `slurm-logs/` (shared across runs).

## WDS filtering

HPLT shards are organised by Web Docs Scorer (WDS) score, which the filename starts with: `9_1.jsonl.zst`, `10_3.jsonl.zst`; v4 English adds a source tag, `8_30.ia.jsonl.zst` (`cc` Common Crawl, `ia` Internet Archive, `ab` ArchiveBot). See [web-docs-scorer](https://github.com/pablop16n/web-docs-scorer). In v4 the per-document score is `doc_scores[0]` on a 0–1 scale, and the shard prefix is `floor(10 × score)`. HPLT v3 shards and the v4 "clean" subset cover scores 5–10. The match is anchored on `<score>_`, so `--wds 1` does not pick up WDS-10 shards.

**Sampling is proportional across all selected shards.** If WDS 9 holds 99× more documents than WDS 10, the sample reflects that ratio. For equal numbers per score, run once per score; each run gets its own directory:

```
sbatch sl-orchestrator eng_Latn --corpus v4 --wds 9  --n-samples 100000 --seed 1
sbatch sl-orchestrator eng_Latn --corpus v4 --wds 10 --n-samples 100000 --seed 1
```

## Reproducibility

Every run has a seed: `--seed S`, or one drawn and written to `run.json` (and the orchestrator log). Each shard's seed is derived from the run seed and the shard filename, and the final shuffle's seed from the run seed, so a rerun with the same seed, corpus, shard list and counts reproduces the sample exactly, independent of shard order and array batching. Commit your changes before a production run so `sampler_commit` in `run.json` identifies the code.

## Resources and limits

- Worker headers request `mem=32G` (override with `WORKER_MEM`) and `time=3:00:00` per array task. The orchestrator requests 72 h for polling and does little compute; `--mem=2G` on the `sbatch` line is enough.
- A local v4 shard is read at roughly 85 MB/s compressed per task on LUMI (measured 2026-10). English v4 (18.2 TB) is about 60 task-hours of reading.
- Array submissions are batched (`BATCH`) and serialized within a run: the next batch is submitted when none of the run's own jobs of that stage are left in the queue. Job names carry the run tag (e.g. `hplt_sample-v4-eng_Latn-all`), so separate runs proceed independently, but they share your queue limits.
- Large v3 runs stream every shard over HTTP and can reserve many CPU hours: English at WDS 9–10 alone is over 300 shards.

## Manual steps and verification

Change the plan without rerunning everything (it waits for counts if needed):

```
python3 plan.py <LANG> --n-samples 200000 --shard-list ./results-<...>/shard_list.txt --counts-dir ./counts/<...> --out ./results-<...>/plan.jsonl
```

Debug a single shard (a URL or a local path):

```
python3 counts.py <LANG> <SHARD> --counts-dir ./counts/<...>
python3 sample.py <LANG> <SHARD> --plan ./results-<...>/plan.jsonl --outdir ./results-<...>/samples --seed-base <S>
```

Verify a finished run:

```
wc -l results-<...>/samples.jsonl     # should equal the sum of n_take in plan.jsonl
```

Shuffle an existing sample (e.g. one made with `--no-shuffle`, or before shuffling existed); in place by default, with a content check before the file is replaced:

```
python3 shuffle.py results-<...>/samples.jsonl --seed-base <S>
```

`check.py` compares per-shard sample files against the plan. It needs the `samples/` directory, which is kept only when concat did not succeed:

```
python3 check.py <LANG> --plan ./results-<...>/plan.jsonl --samples-dir ./results-<...>/samples
```

## Robustness notes

- The shuffle holds only line offsets in memory, writes to a temporary file, checks that it holds exactly the input's lines (order-independent fingerprint) and only then replaces the file.
- Per-shard outputs are written atomically (tmp → rename), so parallel jobs are safe, and a rerun skips shards whose output exists.
- `plan.py` matches the expected shards from the shard list and waits for all counts.
- HTTP reads retry with exponential backoff; `sbatch` calls retry on socket timeouts; the array index `OFFSET` keeps batches within `MaxArraySize`.

## Known limitations / TODO

- **Roihu**: the preset is a stub; partition and module details are still to be filled in.
- **Puhti** has only been tested with smaller languages.
- **HPLT v2** is not supported. Local mode should work for a local v3 copy (`--local-root`) but is untested.
- **Shared counts cache**: counts are stable per corpus version, so a group-shared `COUNTS_DIR` (e.g. in LUMI `projappl/`) would spare other users the counting. Pending a shared path under the project allocation.
