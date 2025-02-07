#!/usr/bin/env node
const msg = require("../util/msg");

class Server {
  constructor() {
    this.kvstore = new Map();
    this.nodeId = msg.getNodeId();
    this.isLeader = this.nodeId === "kv_server_1"; // server1 is leader
    this.pendingAcks = new Map(); // reqid -> {count, total, clientId, message}
  }

  apply(message) {
    const { key, cmd, value } = message;
    if (cmd === "W") {
      this.kvstore.set(key, value);
    }
    return this.kvstore.get(key);
  }

  quorumSize() {
    // For N servers, need (N/2 + 1) for majority
    const totalServers = msg.getNodes("kv_server.*").length;
    return Math.floor(totalServers / 2) + 1;
  }

  async onAppendReq(message) {
    // Follower handling append request from leader
    this.apply(message);
    msg.send({
      to: message.from,
      from: this.nodeId,
      type: "APPEND_RESP",
      reqid: message.reqid,
    });
  }

  async onClientCommand(message) {
    if (!this.isLeader) {
      // Redirect to leader
      msg.send({
        to: message.from,
        from: this.nodeId,
        type: "CMD_RESP",
        redirect: "kv_server_1",
      });
      return;
    }

    if (message.cmd === "R") {
      // Handle reads directly
      const value = this.kvstore.get(message.key);
      msg.send({
        to: message.from,
        from: this.nodeId,
        type: "CMD_RESP",
        value: value,
        reqid: message.reqid,
      });
    } else if (message.cmd === "W") {
      // Replicate writes
      this.replicate(message);
    }
  }

  replicate(message) {
    // Send append requests to all followers (excluding self)
    const others = msg.siblingNodes().filter((id) => id !== this.nodeId);
    for (const serverId of others) {
      msg.send({
        to: serverId,
        from: this.nodeId,
        type: "APPEND_REQ",
        cmd: message.cmd,
        key: message.key,
        value: message.value,
        reqid: message.reqid,
      });
    }

    // Track pending acks
    this.pendingAcks.set(message.reqid, {
      count: 1, // Include leader's own ack
      total: msg.getNodes("kv_server.*").length,
      clientId: message.from,
      message: message,
    });
  }

  onAppendResp(message) {
    const pending = this.pendingAcks.get(message.reqid);
    if (!pending) return;

    pending.count++;
    console.error(
      `Got ${pending.count} acks out of ${this.quorumSize()} needed`
    );

    // Only respond when we have majority (including leader's own ack)
    if (pending.count >= this.quorumSize()) {
      // Apply to leader's state
      this.apply(pending.message);

      // Respond to client
      msg.send({
        to: pending.clientId,
        from: this.nodeId,
        type: "CMD_RESP",
        success: true,
        reqid: message.reqid,
      });

      this.pendingAcks.delete(message.reqid);
    }
  }
  async listen() {
    while (true) {
      const message = await msg.recv();
      switch (message.type) {
        case "APPEND_REQ":
          await this.onAppendReq(message);
          break;
        case "APPEND_RESP":
          this.onAppendResp(message);
          break;
        case "CMD_REQ":
          await this.onClientCommand(message);
          break;
      }
    }
  }
}

// Start server
const server = new Server();
server.listen().catch(console.error);
