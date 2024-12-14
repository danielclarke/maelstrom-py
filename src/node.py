import json
import sys
from threading import Semaphore
from typing import Callable


class Node:
    def __init__(self):
        self.node_id = None
        self.node_ids = []
        self.next_msg_id = 0
        self.callbacks: dict[int, Callable[[dict], None]] = {}
        self.lock = Semaphore()

    def init(self, node_id: str, node_ids: list[str]) -> None:
        self.node_id = node_id
        self.node_ids = node_ids

    def log(self, msg: str):
        try:
            self.lock.acquire()
            print(msg, file=sys.stderr, flush=True)
        finally:
            self.lock.release()

    def reply(self, req: dict, body: dict) -> None:
        try:
            self.lock.acquire()
            self.next_msg_id += 1
        finally:
            self.lock.release()

        body_ = {
            **body,
            "msg_id": self.next_msg_id,
            "in_reply_to": req.get("body", {}).get("msg_id"),
        }
        self.send(dest=req.get("src"), body=body_)

    def send(self, dest: str, body: dict):
        msg = {"src": self.node_id, "dest": dest, "body": body}
        try:
            self.lock.acquire()
            print(json.dumps(msg), flush=True)
        finally:
            self.lock.release()

    def rpc(self, dest: str, body: dict, handler: Callable[[dict], None]) -> None:
        try:
            self.lock.acquire()
            self.next_msg_id += 1
            self.callbacks[self.next_msg_id] = handler
        finally:
            self.lock.release()

        body_ = {
            **body,
            "msg_id": self.next_msg_id,
        }
        self.send(dest=dest, body=body_)
