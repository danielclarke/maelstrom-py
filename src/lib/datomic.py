#!/usr/bin/env python

import asyncio
import fileinput
import json
from enum import StrEnum
from functools import reduce
from threading import Semaphore, Thread
from typing import Any

from lib.node import Node
from lib.semaphore_context import lock


class TransactionOp(StrEnum):
    READ = "r"
    APPEND = "append"


type Transaction = tuple[TransactionOp, int, int | None]


class Database:
    def __init__(self, data: dict | None = None):
        self.db = data or {}
    
    @staticmethod
    def deserialise(data: list[tuple[int, list[int]]] | None = None) -> "Database":
        return Database(data=dict(data) if data else {})
    
    def serialise(self) -> list[tuple[int, list[int]]]:
        return list(self.db.items())
    
    def __getitem__(self, k: Any) -> Any:
        return self.db[k]
    
    def get(self, k: Any) -> Any:
        return self.db.get(k)

    def assoc(self, k: Any, v: Any) -> "Database":
        return Database(data=self.db | {k: v})

    def transact(self, transaction: list[Transaction]) -> tuple["Database", list[Transaction]]:
        def _transact_aux(acc: tuple["Database", list[Transaction]], transaction: Transaction) -> tuple["Database", list[Transaction]]:
            f, k, v = transaction
            db, transactions = acc
            match f:
                case TransactionOp.READ:
                    transactions.append((f, k, db.get(k)))
                    return (db, transactions)
                case TransactionOp.APPEND:
                    transactions.append((f, k, v))
                    value1 = db.get(k)
                    value2 = (value1 or []) + [v]
                    return (db.assoc(k, value2), transactions)
        
        acc: tuple[Database, list[Transaction]] = (self, [])
        return reduce(_transact_aux, transaction, acc)

class State:
    key = "root"

    def __init__(self, node: Node):
        self.node = node

    async def transact(self, transaction: list[Transaction]) -> list[Transaction]:
        resp = await self.node.sync_rpc(dest="lin-kv", body={"type": "read", "key": self.key})

        self.node.log(resp["body"].get("value"))

        db = Database.deserialise(resp["body"].get("value"))

        db2, transaction2 = db.transact(transaction)

        resp = await self.node.sync_rpc(
            dest="lin-kv",
            body={
                "type": "cas",
                "key": self.key,
                "from": db.serialise(),
                "to": db2.serialise(),
                "create_if_not_exists": True,
            },
        )

        if resp["body"]["type"] != "cas_ok":
            raise Exception(f"CAS failed!")
        
        return transaction2


class Transactor:
    def __init__(self):
        self.node = Node()
        self.state = State(node=self.node)
        self.txn_lock = Semaphore()

    async def handle_message_async(self, req: dict) -> None:
        match req.get("body", {}).get("type"):
            case "init":
                self.node.init(
                    node_id=req.get("body", {}).get("node_id"),
                    node_ids=req.get("body", {}).get("node_ids"),
                )
                self.node.reply(req, body={"type": "init_ok"})
            case "txn":
                transaction = req["body"]["txn"]
                with lock(self.txn_lock):
                    transaction_ = await self.state.transact(transaction)
                self.node.reply(req, body={"type": "txn_ok", "txn": transaction_})
            case t:
                raise Exception(f"Unknown message type {t}")

    def handle(self, req: dict):
        handler = None
        if msg_id := req.get("body", {}).get("in_reply_to"):
            callback = self.node.callbacks.pop(msg_id)
            handler = lambda: callback(req)
            Thread(target=handler).start()
        else:
            Thread(
                target=asyncio.run, args=(self.handle_message_async(req=req),)
            ).start()

def run():
    transactor = Transactor()

    for line in fileinput.input(files=["-"]):
        req = json.loads(line)
        transactor.handle(req)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        exit()
