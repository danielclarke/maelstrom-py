#!/usr/bin/env python

import fileinput
import json
from threading import Thread
from typing import Self

from crdt import CRDT
from node import Node
from semaphore_context import lock


class GCounter:
    def __init__(self, value: dict | None = None):
        self.data = value or {}

    @staticmethod
    def from_serializable(value: dict) -> "GCounter":
        return GCounter(value)

    def to_serializable(self) -> dict:
        return self.data

    def read(self) -> int:
        return sum(self.data.values())

    def merge(self, other: Self) -> Self:
        result = self.data.copy()

        for k, v in other.data.items():
            result[k] = max(result.get(k, 0), v)

        return type(self)(result)

    def add(self, element: dict) -> Self:
        result = self.data.copy()

        result[element["node_id"]] = (
            result.get(element["node_id"], 0) + element["delta"]
        )

        return type(self)(result)


class PNCounter:
    def __init__(self, *, inc: GCounter | None = None, dec: GCounter | None = None):
        self.inc = inc or GCounter()
        self.dec = dec or GCounter()

    @staticmethod
    def from_serializable(value: dict) -> "PNCounter":
        return PNCounter(
            inc=GCounter.from_serializable(value["inc"]),
            dec=GCounter.from_serializable(value["dec"]),
        )

    def to_serializable(self) -> dict:
        return {"inc": self.inc.to_serializable(), "dec": self.dec.to_serializable()}

    def read(self) -> int:
        return self.inc.read() - self.dec.read()

    def merge(self, other: Self) -> Self:
        return type(self)(inc=self.inc.merge(other.inc), dec=self.dec.merge(other.dec))

    def add(self, element: dict) -> Self:
        if 0 <= element["delta"]:
            return type(self)(inc=self.inc.add(element=element), dec=self.dec)
        else:
            return type(self)(
                inc=self.inc,
                dec=self.dec.add(
                    element={"node_id": element["node_id"], "delta": -element["delta"]}
                ),
            )


class CRDTServer(Node):
    def __init__(self, crdt: CRDT):
        super().__init__()
        self.crdt = crdt

    def init(self, node_id, node_ids):
        super().init(node_id, node_ids)

        def sync():
            for id in filter(lambda x: x != self.node_id, self.node_ids):
                self.send(
                    dest=id,
                    body={"type": "replicate", "value": self.crdt.to_serializable()},
                )

        self.repeat(dt_s=5, f=sync)
        self.run_tasks()

    def read(self, req: dict) -> None:
        self.reply(req=req, body={"type": "read_ok", "value": self.crdt.read()})

    def add(self, req: dict) -> None:
        with lock(self.lock):
            self.crdt = self.crdt.add(
                {"node_id": req["src"], "delta": req["body"]["delta"]}
            )
        self.reply(req=req, body={"type": "add_ok"})

    def replicate(self, req: dict) -> None:
        with lock(self.lock):
            self.crdt = self.crdt.merge(
                self.crdt.from_serializable(req["body"]["value"])
            )

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
    node = CRDTServer(crdt=PNCounter())

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
