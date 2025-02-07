#!/usr/bin/env python3

from typing import Dict, Tuple
from util.msg import send, recv, node_id

# Message types
CMD_REQ = "CMD_REQ"        # From client to primary
REPLICATE_REQ = "REP_REQ"  # From primary to secondaries
REPLICATE_RESP = "REP_RESP" # From secondaries to primary
CMD_RESP = "CMD_RESP"      # From primary to client
REDIRECT = "REDIRECT"      # From secondary to client

# Store format: key -> (value, version)
kvstore: Dict[str, Tuple[str, int]] = {}

me = node_id()
is_primary = me == "kv_server_1"  # Check if this server is primary

# For primary to track replication responses
pending_writes = {}  # reqid -> (client_id, key, value, responses)

while True:
    msg = recv()
    msg_type = msg["type"]

    if msg_type == CMD_REQ:
        if not is_primary:
            # Secondary servers redirect to primary
            send({
                "to": msg["from"],
                "from": me,
                "type": REDIRECT,
                "primary": "kv_server_1"
            })
            continue

        if msg["cmd"] == "R":
            # Handle reads directly on primary
            key = msg["key"]
            val = kvstore.get(key, None)
            if val:
                value, version = val
                send({
                    "to": msg["from"],
                    "from": me,
                    "type": CMD_RESP,
                    "success": True,
                    "value": value,
                    "version": version
                })
            else:
                send({
                    "to": msg["from"],
                    "from": me,
                    "type": CMD_RESP,
                    "success": False
                })

        elif msg["cmd"] == "W":
            # Forward write to other servers
            reqid = msg["reqid"]
            key = msg["key"]
            value = msg["value"]
            
            # Store write info
            pending_writes[reqid] = (msg["from"], key, value, 0)
            
            # Forward to secondaries
            for server in ["kv_server_2", "kv_server_3"]:
                send({
                    "to": server,
                    "from": me,
                    "type": REPLICATE_REQ,
                    "reqid": reqid,
                    "key": key,
                    "value": value
                })

    elif msg_type == REPLICATE_REQ:
        # Secondary servers handle replication
        key = msg["key"]
        value = msg["value"]
        version = 1
        if key in kvstore:
            (_, old_version) = kvstore[key]
            version = old_version + 1
        kvstore[key] = (value, version)
        
        # Acknowledge to primary
        send({
            "to": msg["from"],
            "from": me,
            "type": REPLICATE_RESP,
            "reqid": msg["reqid"]
        })

    elif msg_type == REPLICATE_RESP:
        # Primary handles replication responses
        reqid = msg["reqid"]
        if reqid in pending_writes:
            client_id, key, value, responses = pending_writes[reqid]
            responses += 1
            
            if responses == 2:  # Both secondaries responded
                # Update primary's state
                version = 1
                if key in kvstore:
                    (_, old_version) = kvstore[key]
                    version = old_version + 1
                kvstore[key] = (value, version)
                
                # Respond to client
                send({
                    "to": client_id,
                    "from": me,
                    "type": CMD_RESP,
                    "success": True,
                    "version": version
                })
                
                # Clean up
                del pending_writes[reqid]
            else:
                # Still waiting for more responses
                pending_writes[reqid] = (client_id, key, value, responses)

    else:
        raise Exception(f"Unknown message type: {msg_type}")