#!/usr/bin/env python3
"""Mint demo bearer tokens (HS256 JWTs) for the gateway.

Prints shell ``export`` lines on stdout, so you can load them with:

    eval "$(python create_demo_tokens.py)"
    curl -H "Authorization: Bearer $ADMIN_TOKEN" ...

The signing secret must match the gateway's, so export the same
$GATEWAY_JWT_SECRET in both terminals (there is no built-in default):

    export GATEWAY_JWT_SECRET=$(openssl rand -hex 32)
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import List, Optional

from gateway import SecretError, check_secret, issue_token

DEMO_USERS = (("ADMIN_TOKEN", "alice", "admin"), ("VIEWER_TOKEN", "bob", "viewer"))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Create demo admin and viewer tokens")
    parser.add_argument("--ttl", type=int, default=3600, help="token lifetime in seconds (default: %(default)s)")
    parser.add_argument("--secret", help="signing secret (default: $GATEWAY_JWT_SECRET)")
    args = parser.parse_args(argv)
    if args.ttl <= 0:
        parser.error("--ttl must be positive")

    try:
        secret = check_secret(args.secret or os.environ.get("GATEWAY_JWT_SECRET"))
    except SecretError as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 2
    for variable, sub, role in DEMO_USERS:
        print("export %s=%s" % (variable, issue_token(sub, role, secret, args.ttl)))
    print("note: tokens expire in %d seconds" % args.ttl, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
