#!/bin/bash
# Publish a finished, validated run to object storage (LUMI-O).
#
# Usage: bash publish.sh RUNDIR [DEST_PREFIX]
#   DEST_PREFIX default: lumi-462001491-private:hplt-samples
#
# The sample lands at <DEST_PREFIX>/<corpus>/<lang>/<wds>-n<n_samples>-s<seed>/ as
#   samples.jsonl.zst  run.json  plan.jsonl  shard_list.txt  [shard_inventory.tsv]
#   validation.json  SHA256SUMS
# Contract: publish only samples others will read (not intermediates of one build);
# only runs that pass validate.py are published, and a published sample
# is never overwritten (an existing destination aborts the publish).
# Login-node friendly (zstd + rclone); RCLONE overrides the rclone binary.

set -euo pipefail

RUNDIR="${1:?usage: publish.sh RUNDIR [DEST_PREFIX]}"
DEST_PREFIX="${2:-lumi-462001491-private:hplt-samples}"
RCLONE="${RCLONE:-$(command -v rclone || echo /appl/lumi/SW/system/EB/lumio-ext-tools/1.1.0/bin/rclone)}"
PYTHON="${PYTHON:-/usr/bin/python3}"
HERE="$(cd "$(dirname "$0")" && pwd)"

RUNDIR="$(cd "${RUNDIR}" && pwd)"
echo "[PUBLISH] Validating ${RUNDIR}"
"${PYTHON}" "${HERE}/validate.py" "${RUNDIR}"

DEST_REL=$("${PYTHON}" - "${RUNDIR}/run.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
n = r.get("n_samples", "default")
n = "1000000" if n == "default" else n
wds = str(r.get("wds", "all")).replace(",", "+")
print("{}/{}/{}-n{}-s{}".format(r["corpus"], r["lang"], wds, n, r["seed"]))
PY
)
DEST="${DEST_PREFIX}/${DEST_REL}"
echo "[PUBLISH] Destination: ${DEST}"

if [[ -n "$("${RCLONE}" lsf "${DEST}/" 2>/dev/null || true)" ]]; then
  echo "[PUBLISH] ERROR: ${DEST} already exists; published samples are never overwritten." >&2
  exit 1
fi

STAGE="${RUNDIR}/.publish"
rm -rf "${STAGE}"
mkdir -p "${STAGE}"
trap 'rm -rf "${STAGE}"' EXIT

echo "[PUBLISH] Compressing samples.jsonl"
zstd -q -T8 -10 "${RUNDIR}/samples.jsonl" -o "${STAGE}/samples.jsonl.zst"
zstd -q -t "${STAGE}/samples.jsonl.zst"
for f in run.json plan.jsonl shard_list.txt shard_inventory.tsv validation.json; do
  [[ -f "${RUNDIR}/${f}" ]] && cp "${RUNDIR}/${f}" "${STAGE}/"
done
(cd "${STAGE}" && sha256sum -- * > SHA256SUMS)

echo "[PUBLISH] Uploading"
"${RCLONE}" copy "${STAGE}/" "${DEST}/"
echo "[PUBLISH] Verifying upload (download and compare)"
"${RCLONE}" check --download "${STAGE}/" "${DEST}/"
echo "[PUBLISH] Published ${DEST}"
"${RCLONE}" ls "${DEST}/"
