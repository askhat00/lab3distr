# Raft Lite (Leader election + log replication)

A minimal Raft-like implementation for a 3–5 node cluster on AWS EC2. Supports:
- Leader election with randomized timeouts
- Heartbeats
- Log replication
- Majority-based commit
- Leader/follower crash and recovery demos
- In-memory state (no persistence, no membership changes)

## 1. Prerequisites

- Python 3.9+
- `pip install flask requests`
- EC2 instances in the same VPC/subnet with security group allowing TCP ports (e.g., 8000–8004) inbound from the VPC.
- Use **private IPs** for peers.

## 2. Launch commands (3-node example)

On each node, replace IPs with the **private IPs** of your EC2 instances.

Node A:
```bash
python3 node.py --id A --host 0.0.0.0 --port 8000 --peers http://10.0.0.2:8001,http://10.0.0.3:8002
