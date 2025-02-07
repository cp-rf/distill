#!/usr/bin/env node
const msg = require('../util/msg');

class Client {
    constructor() {
        this.nodeId = msg.getNodeId();
        this.reqId = 0;
    }

    async sendrecv(message) {
        this.reqId++;
        message.reqid = this.reqId;
        msg.send(message);
        
        while (true) {
            const response = await msg.recv();
            if (response.type === 'REDIRECT') {
                // If redirected, retry with primary
                message.to = response.primary;
                msg.send(message);
            } else {
                return response;
            }
        }
    }

    async write(key, value) {
        const message = {
            from: this.nodeId,
            to: 'kv_server_1', // Always send to primary
            type: 'CMD_REQ',
            cmd: 'W',
            key: key,
            value: value
        };

        const response = await this.sendrecv(message);
        if (!response.success) {
            throw new Error(response.errmsg || 'Write failed');
        }
        return response.version;
    }

    async read(key) {
        const message = {
            from: this.nodeId,
            to: 'kv_server_1', // Can read from primary
            type: 'CMD_REQ',
            cmd: 'R',
            key: key
        };

        const response = await this.sendrecv(message);
        if (!response.success) {
            throw new Error(response.errmsg || 'Read failed');
        }
        return response.value;
    }

    async testRun() {
        // Launch 3 server processes
        msg.send({
            to: 'distill',
            type: 'exec',
            id: 'kv_server.*'
        });

        console.log('Testing writes...');
        await this.write('a', 10);
        await this.write('a', 20);
        await this.write('b', 30);

        console.log('Testing reads...');
        const value = await this.read('a');
        console.assert(value === 20, `Expected value 20, got ${value}`);

        console.log('Testing error cases...');
        try {
            await this.read('nonexistent');
        } catch (err) {
            console.log('Expected error for nonexistent key:', err.message);
        }

        console.log('All tests passed!');
    }
}

// Start client tests
const client = new Client();
client.testRun().catch(console.error);