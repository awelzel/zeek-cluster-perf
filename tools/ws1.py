#!/usr/bin/env python3
#
# Just send messages as quickly as possible.
import argparse
import json
import os
import sys

import wstest

def run (args):
    with wstest.connect(args.name, args.url) as c:
        print('{"status": "start"}', file=sys.stderr)
        ack = c.hello_v1([])
        print(json.dumps(ack), file=sys.stderr)

        i = 0
        while i < args.total_publishes:
            # print("send", i)
            ev = wstest.build_event_v1(args.topic,
                                       "Cluster::Bench::WebSocket::ping", [i, args.name])
            c.send_json(ev)
            i = i + 1

        ev = wstest.build_event_v1(args.topic,
                                   "Cluster::Bench::WebSocket::bye", [args.name])
        c.send_json(ev)
        c.close()
        print("SENT", i)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--total-publishes", type=int, required=True)
    parser.add_argument("--topic", type=str, required=True)
    parser.add_argument("--name", type=str, required=True)

    args = parser.parse_args()

    run(args)

if __name__ == "__main__":
    main()
