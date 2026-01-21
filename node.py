#!/usr/bin/env python3
import argparse
import json
import random
import threading
import time
from typing import List, Dict, Optional
from flask import Flask, request, jsonify
import requests

# -----------------------------
# Configurable timing constants
# -----------------------------
HEARTBEAT_INTERVAL = 0.5          # seconds
ELECTION_TIMEOUT_RANGE = (1.5, 3) # randomized per node
REQUEST_TIMEOUT = 1.0             # HTTP request timeout

# -----------------------------
# Raft node implementation
# -----------------------------
class RaftNode:
    def __init__(self, node_id: str, host: str, port: int, peers: List[str]):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers  # list of "http://IP:PORT"

        # Persistent (in real Raft). Here: in-memory only.
        self.currentTerm = 0
        self.votedFor: Optional[str] = None
        self.log: List[Dict] = []  # each entry: {"term": int, "cmd": str}

        # Volatile
        self.commitIndex = -1
        self.lastApplied = -1
        self.state = "Follower"  # Follower | Candidate | Leader

        # Leader-only
        self.nextIndex: Dict[str, int] = {}
        self.matchIndex: Dict[str, int] = {}

        # Timing
        self.lastHeartbeatTime = time.time()
        self.electionTimeout = self._random_election_timeout()

        # Concurrency
        self.lock = threading.RLock()
        self.stop_event = threading.Event()

        # Flask app
        self.app = Flask(__name__)
        self._register_routes()

        # Background threads
        self.election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.apply_thread = threading.Thread(target=self._apply_loop, daemon=True)

    # -----------------------------
    # Utility
    # -----------------------------
    def _random_election_timeout(self) -> float:
        return random.uniform(*ELECTION_TIMEOUT_RANGE)

    def _log(self, msg: str):
        # Standardized logging format for the lab
        print(f"[{self.node_id} | term {self.currentTerm} | {self.state}] {msg}", flush=True)

    def _majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1  # self + peers

    def _is_leader(self) -> bool:
        return self.state == "Leader"

    def _last_log_index(self) -> int:
        return len(self.log) - 1

    def _last_log_term(self) -> int:
        if len(self.log) == 0:
            return -1
        return self.log[-1]["term"]

    # -----------------------------
    # Flask routes (RPCs + client)
    # -----------------------------
    def _register_routes(self):
        @self.app.route("/request_vote", methods=["POST"])
        def request_vote():
            data = request.get_json(force=True)
            with self.lock:
                term = data.get("term")
                candidateId = data.get("candidateId")
                lastLogIndex = data.get("lastLogIndex")
                lastLogTerm = data.get("lastLogTerm")

                voteGranted = False
                if term > self.currentTerm:
                    self.currentTerm = term
                    self.state = "Follower"
                    self.votedFor = None
                    self._log("Term updated from RequestVote")

                if term < self.currentTerm:
                    return jsonify({"term": self.currentTerm, "voteGranted": False})

                # Candidate’s log must be at least as up-to-date
                up_to_date = (lastLogTerm > self._last_log_term()) or \
                             (lastLogTerm == self._last_log_term() and lastLogIndex >= self._last_log_index())

                if (self.votedFor is None or self.votedFor == candidateId) and up_to_date:
                    self.votedFor = candidateId
                    voteGranted = True
                    self.lastHeartbeatTime = time.time()  # reset to avoid immediate election
                    self._log(f"Voted for {candidateId}")

                return jsonify({"term": self.currentTerm, "voteGranted": voteGranted})

        @self.app.route("/append_entries", methods=["POST"])
        def append_entries():
            data = request.get_json(force=True)
            with self.lock:
                term = data.get("term")
                leaderId = data.get("leaderId")
                prevLogIndex = data.get("prevLogIndex")
                prevLogTerm = data.get("prevLogTerm")
                entries = data.get("entries", [])
                leaderCommit = data.get("leaderCommit")

                success = False

                if term > self.currentTerm:
                    self.currentTerm = term
                    self.state = "Follower"
                    self.votedFor = None
                    self._log("Term updated from AppendEntries")

                if term < self.currentTerm:
                    return jsonify({"term": self.currentTerm, "success": False})

                # Heartbeat received
                self.lastHeartbeatTime = time.time()

                # Consistency check
                if prevLogIndex != -1:
                    if prevLogIndex >= len(self.log) or self.log[prevLogIndex]["term"] != prevLogTerm:
                        # Conflict: reject
                        return jsonify({"term": self.currentTerm, "success": False})

                # Append new entries (overwrite conflicts)
                idx = prevLogIndex + 1
                for e in entries:
                    if idx < len(self.log):
                        if self.log[idx]["term"] != e["term"]:
                            # Delete conflicting entry and all that follow
                            self.log = self.log[:idx]
                            self._log(f"Deleted conflicting entries from index {idx}")
                            self.log.append(e)
                        else:
                            # Same term—replace command if needed
                            self.log[idx] = e
                    else:
                        self.log.append(e)
                    idx += 1

                # Update commitIndex
                if leaderCommit is not None and leaderCommit > self.commitIndex:
                    self.commitIndex = min(leaderCommit, self._last_log_index())
                    self._log(f"Commit index updated to {self.commitIndex}")

                success = True
                return jsonify({"term": self.currentTerm, "success": success})

        @self.app.route("/client_cmd", methods=["POST"])
        def client_cmd():
            data = request.get_json(force=True)
            cmd = data.get("cmd")
            with self.lock:
                if not self._is_leader():
                    return jsonify({"error": "Not leader", "leader": self._discover_leader_hint()}), 400

                # Append to leader log
                entry = {"term": self.currentTerm, "cmd": cmd}
                self.log.append(entry)
                index = self._last_log_index()
                self._log(f"Append log entry (term={self.currentTerm}, cmd={cmd}, index={index})")

            # Replicate to followers
            acks = 1  # leader itself
            for peer in self.peers:
                if self._send_append_entries(peer):
                    acks += 1

            # Majority commit
            with self.lock:
                if acks >= self._majority():
                    self.commitIndex = max(self.commitIndex, index)
                    self._log(f"Entry committed (index={index})")
                    return jsonify({"status": "committed", "index": index})
                else:
                    self._log(f"Entry not committed (acks={acks})")
                    return jsonify({"status": "pending", "acks": acks}), 202

        @self.app.route("/status", methods=["GET"])
        def status():
            with self.lock:
                return jsonify({
                    "node": self.node_id,
                    "term": self.currentTerm,
                    "state": self.state,
                    "votedFor": self.votedFor,
                    "commitIndex": self.commitIndex,
                    "lastApplied": self.lastApplied,
                    "log": self.log,
                    "peers": self.peers
                })

    # -----------------------------
    # Background loops
    # -----------------------------
    def _election_loop(self):
        while not self.stop_event.is_set():
            time.sleep(0.05)
            with self.lock:
                elapsed = time.time() - self.lastHeartbeatTime
                if self.state != "Leader" and elapsed > self.electionTimeout:
                    # Start election
                    self.state = "Candidate"
                    self.currentTerm += 1
                    self.votedFor = self.node_id
                    self.electionTimeout = self._random_election_timeout()
                    self._log(f"Timeout → Candidate (term {self.currentTerm})")

                    votes = 1  # vote for self
                    lastLogIndex = self._last_log_index()
                    lastLogTerm = self._last_log_term()

            # Request votes
            for peer in self.peers:
                try:
                    resp = requests.post(
                        f"{peer}/request_vote",
                        json={
                            "term": self.currentTerm,
                            "candidateId": self.node_id,
                            "lastLogIndex": lastLogIndex,
                            "lastLogTerm": lastLogTerm
                        },
                        timeout=REQUEST_TIMEOUT
                    )
                    data = resp.json()
                    with self.lock:
                        if data.get("term", 0) > self.currentTerm:
                            self.currentTerm = data["term"]
                            self.state = "Follower"
                            self.votedFor = None
                            self._log("Stepped down due to higher term in VoteResponse")
                        elif data.get("voteGranted"):
                            votes += 1
                except Exception:
                    pass

            with self.lock:
                if self.state == "Candidate" and votes >= self._majority():
                    self.state = "Leader"
                    self._log(f"Received votes → Leader (votes={votes})")
                    # Initialize leader state
                    for peer in self.peers:
                        self.nextIndex[peer] = self._last_log_index() + 1
                        self.matchIndex[peer] = -1
                    self.lastHeartbeatTime = time.time()

    def _heartbeat_loop(self):
        while not self.stop_event.is_set():
            time.sleep(HEARTBEAT_INTERVAL)
            with self.lock:
                if self.state != "Leader":
                    continue
            # Send heartbeats
            for peer in self.peers:
                self._send_append_entries(peer, heartbeat=True)

    def _apply_loop(self):
        # Apply committed entries in order
        while not self.stop_event.is_set():
            time.sleep(0.05)
            with self.lock:
                while self.lastApplied < self.commitIndex:
                    self.lastApplied += 1
                    cmd = self.log[self.lastApplied]["cmd"]
                    self._log(f"Applied entry index={self.lastApplied}, cmd={cmd}")

    # -----------------------------
    # Replication helpers
    # -----------------------------
    def _send_append_entries(self, peer: str, heartbeat: bool = False) -> bool:
        with self.lock:
            prevLogIndex = self._last_log_index()
            prevLogTerm = self._last_log_term()
            entries = [] if heartbeat else [self.log[-1]] if len(self.log) > 0 else []
            leaderCommit = self.commitIndex

        try:
            resp = requests.post(
                f"{peer}/append_entries",
                json={
                    "term": self.currentTerm,
                    "leaderId": self.node_id,
                    "prevLogIndex": prevLogIndex,
                    "prevLogTerm": prevLogTerm,
                    "entries": entries,
                    "leaderCommit": leaderCommit
                },
                timeout=REQUEST_TIMEOUT
            )
            data = resp.json()
            with self.lock:
                if data.get("term", 0) > self.currentTerm:
                    self.currentTerm = data["term"]
                    self.state = "Follower"
                    self.votedFor = None
                    self._log("Stepped down due to higher term in AppendResponse")
                    return False
                return data.get("success", False)
        except Exception:
            return False

    def _discover_leader_hint(self) -> Optional[str]:
        # naive hint: return first peer; in demos you’ll know the leader
        return self.peers[0] if self.peers else None

    # -----------------------------
    # Lifecycle
    # -----------------------------
    def start(self):
        self._log("Starting node")
        self.election_thread.start()
        self.heartbeat_thread.start()
        self.apply_thread.start()
        self.app.run(host=self.host, port=self.port, threaded=True)

    def stop(self):
        self.stop_event.set()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="Node ID (e.g., A)")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, required=True, help="Bind port")
    parser.add_argument("--peers", required=True,
                        help="Comma-separated peer addresses (e.g., http://10.0.0.2:8001,http://10.0.0.3:8002)")
    args = parser.parse_args()
    peers = [p.strip() for p in args.peers.split(",") if p.strip()]
    return args.id, args.host, args.port, peers


if __name__ == "__main__":
    node_id, host, port, peers = parse_args()
    node = RaftNode(node_id=node_id, host=host, port=port, peers=peers)
    node.start()