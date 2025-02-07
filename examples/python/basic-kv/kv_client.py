#!/usr/bin/env python3

import sys
import json
from typing import Dict, Any
from util.msg import recv, node_id, send 

# Message types
CMD_REQ = "CMD_REQ"        # Request to server
CMD_RESP = "CMD_RESP"      # Response from server
REDIRECT = "REDIRECT"      # Redirect from secondary to primary

me = node_id()
reqid = 0  # global

def req(msg: Dict[str,Any]):
    global reqid
    reqid += 1
    msg["reqid"] = reqid
    send(msg)

def sendrecv(msg: Dict[str,Any]) -> Dict[str,Any]:
    req(msg)
    while True:
        resp = recv()
        if resp["type"] == REDIRECT:
            # Got redirected to primary, resend to primary
            msg["to"] = resp["primary"]
            req(msg)
        else:
            return resp

# Launch all three servers
req({"to": "distill", "type": "exec", "id" : "server.*"})

# Test write operations
print("Testing writes...")

# Write first value
msg = sendrecv({
    "to": "kv_server_1",
    "from": me,
    "type": CMD_REQ,
    "cmd": "W",
    "key": "aa",
    "value": "AA"
})
assert msg["type"] == CMD_RESP
assert msg["success"] == True
assert msg["version"] == 1, f"Expected version 1, got {msg['version']}"

# Write second value
msg = sendrecv({
    "to": "kv_server_1",
    "from": me,
    "type": CMD_REQ,
    "cmd": "W",
    "key": "bb",
    "value": "BB"
})
msg2 = sendrecv({
    "to": "kv_server_1",
    "from": me,
    "type": CMD_REQ,
    "cmd": "R",
    "key": "aa",
})
assert msg2["type"] == CMD_RESP
assert msg2["success"] == True
assert msg2["value"] == "AA"

assert msg["type"] == CMD_RESP
assert msg["success"] == True
assert msg["version"] == 1, f"Expected version 1, got {msg['version']}"


# Update first value
msg = sendrecv({
    "to": "kv_server_1",
    "from": me,
    "type": CMD_REQ,
    "cmd": "W",
    "key": "aa",
    "value": "AAA"
})
assert msg["type"] == CMD_RESP
assert msg["success"] == True
assert msg["version"] == 2, f"Expected version 2, got {msg['version']}"

print("Write tests passed")

# Test read operations
print("Testing reads...")

# Read from primary
msg = sendrecv({
    "to": "kv_server_1",
    "from": me,
    "type": CMD_REQ,
    "cmd": "R",
    "key": "aa"
})
assert msg["type"] == CMD_RESP
assert msg["success"] == True
assert msg["value"] == "AAA"
assert msg["version"] == 2

# Try reading from secondary (should redirect to primary)
msg = sendrecv({
    "to": "kv_server_2",
    "from": me,
    "type": CMD_REQ,
    "cmd": "R",
    "key": "bb"
})
assert msg["type"] == CMD_RESP
assert msg["success"] == True
assert msg["value"] == "BB"
assert msg["version"] == 1

print("Read tests passed")

# Test non-existent key
print("Testing error cases...")
msg = sendrecv({
    "to": "kv_server_1",
    "from": me,
    "type": CMD_REQ,
    "cmd": "R",
    "key": "nonexistent"
})
assert msg["type"] == CMD_RESP
assert msg["success"] == False

print("All tests passed!")