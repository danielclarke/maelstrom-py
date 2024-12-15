#!/usr/bin/env python

import fileinput
import json
from threading import Thread
from typing import Any

from node import Node


class GSet:
    def __init__(self, s: set | None = None):
        if s is None:
            self.data = set()
        else:
            self.data = s.copy()

    @staticmethod
    def from_array(values: list) -> "GSet":
        return GSet(set(values))

    def to_array(self) -> list:
        return list(self.data)

    def read(self) -> list:
        return list(self.data)

    def merge(self, values: list) -> "GSet":
        return GSet(self.data.union(values))

    def add(self, element: Any) -> "GSet":
        return GSet(self.data.union([element]))


class GSetServer(Node):
    def __init__(self):
        super().__init__()
        self.crdt = GSet()

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
        try:
            self.lock.acquire()
            self.crdt = self.crdt.add(req["body"]["element"])
        finally:
            self.lock.release()
        self.reply(req=req, body={"type": "add_ok"})

    def replicate(self, req: dict) -> None:
        try:
            self.lock.acquire()
            self.crdt = self.crdt.merge(req["body"]["value"])
        finally:
            self.lock.release()

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
    node = GSetServer()

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
