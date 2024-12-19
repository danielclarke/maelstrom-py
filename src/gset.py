#!/usr/bin/env python

import fileinput
import json
from threading import Thread
from typing import Any

from crdt import CRDT
from node import Node
from semaphore_context import lock


class GSet:
    def __init__(self, s: set | None = None):
        if s is None:
            self.data = set()
        else:
            self.data = s.copy()

    @staticmethod
    def from_serializable(value: list) -> "GSet":
        return GSet(set(value))

    def to_serializable(self) -> list:
        return list(self.data)

    def read(self) -> list:
        return list(self.data)

    def merge(self, value: list) -> "GSet":
        return GSet(self.data.union(value))

    def add(self, element: Any) -> "GSet":
        return GSet(self.data.union([element]))


class Counter:
    def __init__(self, value: dict | None = None):
        if value is None:
            self.data = {}
        else:
            self.data = value

    @staticmethod
    def from_serializable(value: dict) -> "Counter":
        return Counter(value)

    def to_serializable(self) -> dict:
        return self.data

    def read(self) -> dict:
        return self.data

    def merge(self, value: dict) -> "Counter":
        result = self.data.copy()

        for k, v in value.items():
            result[k] = max(result.get(k, 0), v)

        return Counter(result)

    def add(self, element: dict) -> "Counter":
        result = self.data.copy()

        result[element["node_id"]] = (
            result.get(element["node_id"], 0) + element["delta"]
        )

        return Counter(result)


class CRDTServer(Node):
    def __init__(self, crdt: CRDT):
        super().__init__()
        self.crdt = crdt

    def init(self, node_id, node_ids):
        super().init(node_id, node_ids)

        def sync():
            for id in filter(lambda x: x != self.node_id, self.node_ids):
                self.send(
                    dest=id, body={"type": "replicate", "value": self.crdt.read()}
                )

        self.repeat(dt_s=5, f=sync)
        self.run_tasks()

    def read(self, req: dict) -> None:
        self.reply(req=req, body={"type": "read_ok", "value": self.crdt.read()})

    def add(self, req: dict) -> None:
        with lock(self.lock):
            self.crdt = self.crdt.add(req["body"]["element"])
        self.reply(req=req, body={"type": "add_ok"})

    def replicate(self, req: dict) -> None:
        with lock(self.lock):
            self.crdt = self.crdt.merge(req["body"]["value"])

    def handle_message(self, req: dict) -> None:
        match req.get("body", {}).get("type"):
            case "init":
                self.init(
                    node_id=req.get("body", {}).get("node_id"),
                    node_ids=req.get("body", {}).get("node_ids"),
                )
                self.reply(req, body={"type": "init_ok"})
            case "add":
                self.add(req=req)
            case "replicate":
                self.replicate(req=req)
            case "read":
                self.read(req=req)

            case t:
                raise Exception(f"Unknown message type {t}")


def run():
    node = CRDTServer(crdt=GSet())

    for line in fileinput.input(files=["-"]):
        req = json.loads(line)

        handler = None
        if msg_id := req.get("body", {}).get("in_reply_to"):
            callback = node.callbacks.pop(msg_id)
            handler = lambda: callback(req)
        else:
            handler = lambda: node.handle_message(req=req)

        Thread(target=handler).start()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        exit()
