#!/usr/bin/env python

import fileinput
import json
from enum import StrEnum
from threading import Thread
from typing import Self

from crdt import CRDT
from node import Node
from semaphore_context import lock


class TransactionOp(StrEnum):
    READ = "r"
    APPEND = "append"


type Transaction = tuple[TransactionOp, int, int | None]


class State:
    def __init__(self):
        self.state = {}

    def transact(self, transaction: list[Transaction]) -> list[Transaction]:
        result = []
        for f, k, v in transaction:
            match f:
                case TransactionOp.READ:
                    result.append((f, k, self.state.get(k, [])))
                case TransactionOp.APPEND:
                    result.append((f, k, v))
                    self.state[k] = self.state.get(k, []) + [v]
        return result


class Transactor(Node):
    def __init__(self):
        super().__init__()
        self.state = State()

    def handle_message(self, req: dict) -> None:
        match req.get("body", {}).get("type"):
            case "init":
                self.init(
                    node_id=req.get("body", {}).get("node_id"),
                    node_ids=req.get("body", {}).get("node_ids"),
                )
                self.reply(req, body={"type": "init_ok"})
            case "txn":
                transaction = req["body"]["txn"]
                with lock(self.lock):
                    transaction_ = self.state.transact(transaction)
                self.reply(req, body={"type": "txn_ok", "txn": transaction_})
                self.log(f"DEBUG %%%% {self.state.state}")
            case t:
                raise Exception(f"Unknown message type {t}")


def run():
    node = Transactor()

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
