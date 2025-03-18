#!/bin/bash
set -eux

if ! zeek --version; then
    echo "no zeek" >&2
    exit 1;
fi

DIR="$(cd "$(dirname "$0")" && pwd)"
RESULT_DIR=${DIR}/results/$(date +%Y%m%d-%H%M%S)

BACKENDS="broker zeromq"
TESTS="potential-scanner ping-pong broadcast"

ZEEKPATH=${DIR}/perf-tests:$(zeek-config --zeekpath)
export ZEEKPATH

ZEEK_CLUSTER_CONFIG=${ZEEK_CLUSTER_CONFIG:-${DIR}/cluster-config.yaml}
export ZEEK_CLUSTER_CONFIG

RUNS=3
CONFIGS="lowrate highrate"

for t in ${TESTS}; do
    for c in ${CONFIGS}; do
        for b in ${BACKENDS}; do
            for run in $(seq ${RUNS}); do
                test_dir="${RESULT_DIR}/${t}-${c}-${b}-${run}"
                mkdir -p "${test_dir}"
                (
                cd "${test_dir}";
                export TEST_BACKEND=${b};
                export TEST_CONFIG=${c}
                export ZEEK_EXTRA_SCRIPTS=${t}

                zeek "${DIR}/supervisor.js" 2>&1 | tee -a output
                )
            done
        done
    done
done
