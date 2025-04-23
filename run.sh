#!/bin/bash
set -eux

if ! zeek --version; then
    echo "no zeek" >&2
    exit 1;
fi

DIR="$(cd "$(dirname "$0")" && pwd)"

ZEEKPATH=${DIR}/perf-tests:$(zeek-config --zeekpath)
export ZEEKPATH

ZEEK_CLUSTER_CONFIG=${ZEEK_CLUSTER_CONFIG:-${DIR}/cluster-config.yaml}
export ZEEK_CLUSTER_CONFIG

SUFFIX=$(basename -s .yml $(basename -s .yaml "${ZEEK_CLUSTER_CONFIG}"))
RESULT_DIR=${DIR}/results/$(date +%Y%m%d-%H%M%S)-${SUFFIX}

if [ -n "${RESULT_DIR_SUFFIX:-}" ]; then
    RESULT_DIR="${RESULT_DIR}-${RESULT_DIR_SUFFIX}"
fi

mkdir -p $RESULT_DIR
cp $ZEEK_CLUSTER_CONFIG $RESULT_DIR

echo "=== zeek" >> "$RESULT_DIR/info.txt"
which zeek >> "$RESULT_DIR/info.txt" 2>&1
echo "=== zeek --version" >> "$RESULT_DIR/info.txt"
zeek --version >> "$RESULT_DIR/info.txt" 2>&1
echo "=== zeek cluster config ${ZEEK_CLUSTER_CONFIG}" >> "$RESULT_DIR/info.txt"
cat "${ZEEK_CLUSTER_CONFIG}" >> "$RESULT_DIR/info.txt"
echo "=== env" >> "$RESULT_DIR/info.txt"
env | grep -E 'ZEEK|PATH|USER[^=]*=.*' >> "$RESULT_DIR/info.txt" 2>&1


TESTS=${TESTS:-"logging logging-many potential-scanner broadcast ping-pong"}
BACKENDS=${BACKENDS:-"broker zeromq"}
CONFIGS=${CONFIGS:-"lowrate highrate"}
RUNS=${RUNS:-3}

for t in ${TESTS}; do
    for c in ${CONFIGS}; do
        for b in ${BACKENDS}; do
            for run in $(seq ${RUNS}); do
                test_dir="${RESULT_DIR}/${t}-${c}-${b}-${run}"
                mkdir -p "${test_dir}"
                (
                cd "${test_dir}";

                if [ "${b}" == "nats" ]; then
                    nats_pid=$(pgrep nats-server)
                    nats_ticks_start=$(awk '{ print $14 + $15 }' </proc/"${nats_pid}"/stat)
                fi

                export TEST_BACKEND=${b};
                export TEST_CONFIG=${c}
                export ZEEK_EXTRA_SCRIPTS=${t}

                zeek "${DIR}/supervisor.js" 2>&1 | tee -a output

                if [ "${b}" == "nats" ]; then
                    nats_ticks_end=$(awk '{ print $14 + $15 }' </proc/"${nats_pid}"/stat)

                    clk_ticks=$(getconf CLK_TCK)
                    nats_cpu_time=$(awk "BEGIN { print (${nats_ticks_end} - ${nats_ticks_start}) / ${clk_ticks} }")

                    echo -n "JSON_RESULT={\"node\": \"nats-server\", \"node_type\": \"Cluster::NATS\", " | tee -a output
                    echo "\"stats\": {\"proc_stats\": {\"user_system_time\": ${nats_cpu_time}}}}" | tee -a output

                fi
                )
            done
        done
    done
done
