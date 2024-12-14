#!/usr/bin/env python

import fileinput
import json
from threading import Thread
from time import sleep
from typing import Any

from node import Node


class BroadcastNode(Node):
    def __init__(self):
        super().__init__()
        self.neighbours: list[str] = []
        self.messages: set[Any] = set()

    def set_neighbours(self, neighbours: list[str]) -> None:
        self.neighbours = neighbours

    def add_message(self, message: Any) -> bool:
        if self.messages.issuperset([message]):
            return False
        else:
            self.messages.add(message)
            return True


def handle_message(node: BroadcastNode, req: dict) -> None:
    match req.get("body", {}).get("type"):
        case "init":
            node.init(
                node_id=req.get("body", {}).get("node_id"),
                node_ids=req.get("body", {}).get("node_ids"),
            )
            node.reply(req, body={"type": "init_ok"})
        case "topology":
            node.set_neighbours(
                neighbours=req.get("body", {}).get("topology", {}).get(node.node_id)
            )
            node.reply(req, body={"type": "topology_ok"})
        case "read":
            node.reply(req, body={"type": "read_ok", "messages": list(node.messages)})
        case "broadcast":
            if req.get("body", {}).get("msg_id"):
                node.reply(req, body={"type": "broadcast_ok"})

            message = req.get("body", {}).get("message")
            if node.add_message(message=message):

                unacked = list(filter(lambda x: x != req.get("src"), node.neighbours))
                body = {"type": "broadcast", "message": message}

                def ack(response: dict) -> None:
                    if response.get("body", {}).get("type") == "broadcast_ok":
                        unacked.remove(response["src"])

                while unacked:
                    for n in unacked:
                        node.rpc(dest=n, body=body, handler=ack)
                    sleep(1)

        case t:
            raise Exception(f"Unknown message type {t}")


def run():
    node = BroadcastNode()
    # files=["-"] means read from sys.stdin

    for line in fileinput.input(files=["-"]):
        req = json.loads(line)

        handler = None
        if msg_id := req.get("body", {}).get("in_reply_to"):
            callback = node.callbacks.pop(msg_id)
            handler = lambda: callback(req)
        else:
            handler = lambda: handle_message(node=node, req=req)

        Thread(target=handler).start()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        exit()
