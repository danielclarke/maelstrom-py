#!/usr/bin/env python

import fileinput
import json

from node import Node


def run_():
    node = Node()
    # files=["-"] means read from sys.stdin
    for line in fileinput.input(files=["-"]):
        req = json.loads(line)
        match req.get("body", {}).get("type"):
            case "init":
                node.init(
                    node_id=req.get("body", {}).get("node_id"),
                    node_ids=req.get("body", {}).get("node_ids"),
                )
                node.reply(req, body={"type": "init_ok"})
            case "echo":
                node.reply(
                    req,
                    body={"type": "echo_ok", "echo": req.get("body", {}).get("echo")},
                )
            case t:
                raise Exception(f"Unknown message type {t}")


def run():
    try:
        run_()
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    run()
