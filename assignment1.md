
## Part 1 -- Setup


Download the distill framework from https://github.com/sriram-srinivasan/distill

Read the README, and compile and run the Java example

## Part 2 -- Basic KV Server

Write one client node and one server. 
The server takes commands of the following form (in JSON):

    {from: "client", "to": "server", 
     "type": "CMD_REQ", 
     "cmd": "W",
     "key": "...",
     "value": "..."}

The server responds with 

    {"from": "server",  "to", "client",
     "type": "CMD_RESP",
     "success": true
    }

That was a Write example. For reads, the request supplies a key, and the response comes back with a value.

Use the Utils convenience class to send and receive messages.

The client and server should be started on the distill command line, and the client should send a sequence of arbitrary write and read commands.

## Part 3 -- Replication

Configure 3 servers, "server1" .. "server3".

Let the client launch all three servers, then send Write and Read commands to server 1. 

Server 1 (the primary) should serve reads directly. 

For writes, however, Server 1 should forward the command to the other two servers, and as soon as both servers have responded, it should process the write command, change its local state and respond to the client.
