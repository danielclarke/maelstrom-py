#!/usr/bin/env python

import fileinput
import json
from threading import Thread

from node import Node


class GSetServer(Node):
    def __init__(self):
        super().__init__()
        self.data = set()

    def init(self, node_id, node_ids):
        super().init(node_id, node_ids)

        def sync():
            try:
                self.lock.acquire()
                data = list(self.data)
            finally:
                self.lock.release()

            for id in filter(lambda x: x != self.node_id, self.node_ids):
                self.send(dest=id, body={"type": "replicate", "value": data})

        self.repeat(dt_s=1, f=sync)
        self.run_tasks()

    def read(self, req: dict) -> None:
        self.reply(req=req, body={"type": "read_ok", "value": list(self.data)})

    def add(self, req: dict) -> None:
        try:
            self.lock.acquire()
            self.data.add(req["body"]["element"])
        finally:
            self.lock.release()
        self.reply(req=req, body={"type": "add_ok"})

    def replicate(self, req: dict) -> None:
        try:
            self.lock.acquire()
            self.data.update(req["body"]["value"])
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
