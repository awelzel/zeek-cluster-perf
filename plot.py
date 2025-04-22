#!/usr/bin/env python3

# What do we want to plot? ZeroMQ vs Broker. CPU time per node type?
# Average CPU time for worker, proxy and manager? Sum? Min, max, stddev?
#
# For each test? Should this be a webpage?

# broker left
# zeromq right
import argparse
import json
import os
import pathlib

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir")
    parser.add_argument("--output", type=argparse.FileType("w"), default="-")
    parser.add_argument("--csv", type=str, default=None)

    args = parser.parse_args()

    output_file = "output"
    results = []

    for root, dirs, files in os.walk(args.results_dir):
        if output_file in files:
            dirs.clear()
            results.append(os.path.join(root, output_file))

    dicts = []
    for name in results:
        p = pathlib.Path(name)
        test_dir_name = p.parts[-2]
        test, config, backend, run = test_dir_name.rsplit("-", 3)

        base = {
            "backend": backend,
            "test": test,
            "config": config,
            "run": int(run),
        }


        with p.open() as fp:
            for l in fp:
                l = l.strip()
                # print(test_dir_name, l)

                idx = l.find("JSON_RESULT=")
                if idx >= 0:
                    idx += len("JSON_RESULT=")
                    row = base.copy()
                    row.update(json.loads(l[idx:]))

                    dicts.append(row)

                    json.dump(row, args.output)
                    args.output.write("\n")

    df = pd.json_normalize(dicts)

    renames = {
        "stats.proc_stats.real_time": "real_time",
        "stats.proc_stats.user_time": "user_time",
        "stats.proc_stats.system_time": "system_time",
        "stats.proc_stats.user_system_time": "user_system_time",
        "stats.proc_stats.max_rss": "max_rss",
    }

    df.rename(columns=renames, inplace=True)
    df["max_rss_mb"] = df.max_rss / 1024.0 / 1024.0


    if args.csv:
        df.to_csv(args.csv)

    grouped1 = df.groupby([df.test, df.config, df.backend, df.run, df.node_type])

    grouped2 = grouped1.agg({
        "real_time": ["max", "count"],
        "user_time": ["sum"],
        "system_time": ["sum"],
        "user_system_time": ["sum", "min", "max"],
        "max_rss_mb": ["sum"],
    })



    agged1 = grouped2.groupby(["test", "config", "backend", "node_type"]).agg({
        ("real_time", "max"): ["max", "mean", "std", "count"],
        ("user_system_time", "sum"): [
            "mean",
            "std",
            # "max"
        ],
        ("user_system_time", "min"): [
            "min",
        ],
        ("user_system_time", "max"): [
            "max",
        ],
        ("max_rss_mb", "sum"): [
            "mean",
            # "min",
            # "max"
        ],
    })


    agged2 = agged1.groupby(['test', 'config', 'backend']).agg({
        ("real_time", "max", "max"): ["max"],
        ("user_system_time", "sum", "mean"): ["sum"],
        ("max_rss_mb", "sum", "mean"): ["sum"],
    })

    agged1.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col
                      for col in agged1.columns]
    agged2.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col
                      for col in agged2.columns]

    print(agged1.to_string())
    print(agged2.to_string())

    # print(agged1)
    # print(agged2)


    # For plotting
    # g3 = df.groupby([df.test, df.config, df.node_type,
    #                       df.backend, df.run])["user_system_time"].sum().reset_index()
    agged1_plot = agged1.reset_index()
    agged1_plot["group"] = agged1_plot["test"] + "|" + agged1_plot["config"] + "|" + agged1_plot["node_type"]


    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    sns.barplot(data=agged1_plot, x='group', y='user_system_time_sum_mean', hue='backend', dodge=True, ax=axes[0])
    axes[0].set_title("User System Time")
    axes[0].set_ylabel("User and System Time [s]")
    axes[0].grid(True)
    plt.sca(axes[0])
    plt.xticks(rotation=45, ha="right")


    sns.barplot(data=agged1_plot, x="group", y="max_rss_mb_sum_mean", hue="backend", dodge=True, ax=axes[1])
    axes[1].set_title("Memory Usage (max_rss)")
    axes[1].set_ylabel("Memory [mb]")
    axes[1].grid(True)
    plt.sca(axes[1])
    plt.xticks(rotation=45, ha="right")

    plt.subplots_adjust(bottom=0.5)
    plt.tight_layout()

    __import__('IPython').embed()


if __name__ == "__main__":
    main()
