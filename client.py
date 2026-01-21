#!/usr/bin/env python3
import argparse
import requests

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--leader", required=True, help="Leader base URL (e.g., http://10.0.0.1:8000)")
    parser.add_argument("--cmd", required=True, help='Command string (e.g., "SET x=5")')
    args = parser.parse_args()

    resp = requests.post(f"{args.leader}/client_cmd", json={"cmd": args.cmd}, timeout=2.0)
    print(resp.status_code, resp.text)

if __name__ == "__main__":
    main()