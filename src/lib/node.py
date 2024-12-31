import concurrent.futures
import json
import sys
from threading import Semaphore, Thread
from time import sleep
from typing import Callable

from lib.semaphore_context import lock


class Task:
    def __init__(self, dt_s: float, f: Callable[[], None]):
        self.dt_s = dt_s
        self.f = f


class Node:
    def __init__(self):
        self.node_id = None
        self.node_ids = []
        self.next_msg_id = 0
        self.callbacks: dict[int, Callable[[dict], None]] = {}
        self.tasks: list[Task] = []
        self.lock = Semaphore()

    def init(self, node_id: str, node_ids: list[str]) -> None:
        self.node_id = node_id
        self.node_ids = node_ids

    def handle_message(self, req: dict) -> None:
        raise NotImplementedError

    async def handle_message_async(self, req: dict) -> None:
        raise NotImplementedError

    def log(self, msg: str):
        with lock(self.lock):
            print(msg, file=sys.stderr, flush=True)

    def reply(self, req: dict, body: dict) -> None:
        with lock(self.lock):
            self.next_msg_id += 1

        body_ = {
            **body,
            "msg_id": self.next_msg_id,
            "in_reply_to": req.get("body", {}).get("msg_id"),
        }
        self.send(dest=req["src"], body=body_)

    def send(self, dest: str, body: dict):
        msg = {"src": self.node_id, "dest": dest, "body": body}
        with lock(self.lock):
            print(json.dumps(msg), flush=True)

    def rpc(self, dest: str, body: dict, handler: Callable[[dict], None]) -> None:
        with lock(self.lock):
            self.next_msg_id += 1
            self.callbacks[self.next_msg_id] = handler

        body_ = {
            **body,
            "msg_id": self.next_msg_id,
        }
        self.send(dest=dest, body=body_)

    async def sync_rpc(self, dest: str, body: dict) -> dict:
        f: concurrent.futures.Future = concurrent.futures.Future()
        self.rpc(dest=dest, body=body, handler=lambda resp: f.set_result(resp))
        concurrent.futures.wait([f], timeout=5)
        return f.result()

    def repeat(self, dt_s: float, f: Callable[[], None]) -> None:
        self.tasks.append(Task(dt_s=dt_s, f=f))

    def run_tasks(self) -> None:
        for t in self.tasks:

            def repeater() -> None:
                while True:
                    t.f()
                    sleep(t.dt_s)

            Thread(target=repeater).start()
