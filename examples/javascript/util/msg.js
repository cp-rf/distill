const readline = require('readline');
const fs = require('fs');

class MessageUtils {
    constructor() {
        this.reqid = 1;  // Global request ID counter
        this.nodeId = null;  // Global node ID
        this.config = null;  // Global configuration
        this.othersPat = null;  // Global pattern for sibling nodes
        this.dumpFile = null;  // Debug file writer
        this.messageQueue = [];  // Message queue
        this.readerThread = null;  // Thread for reading stdin
        this.setupStdin();
    }

    setupStdin() {
        const rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout,
            terminal: false
        });

        rl.on('line', (line) => {
            if (!line || line.startsWith('.q')) {
                process.exit(0);
            }
            try {
                const msg = JSON.parse(line.trim());
                this.messageQueue.push(msg);
            } catch (err) {
                if (line.startsWith('{'))
                    console.error('Malformed JSON:', line);
                else
                    console.error('***', line);
                console.error('***IGNORED**', line);
            }
        });
    }

    setTimeout(durationSeconds, name) {
        const timer = setTimeout(() => {
            const timeoutMsg = {
                type: 'TIMEOUT',
                name: name,
                timer: timer
            };
            this.messageQueue.push(timeoutMsg);
        }, durationSeconds * 1000);
        return timer;
    }

    send(msg) {
        if (!msg.to) {
            throw new Error("Message must have 'to' field");
        }
        console.log(JSON.stringify(msg));
    }

    req(msg) {
        if (typeof msg === 'object') {
            const msgReqid = `${this.reqid}.${this.getNodeId()}`;
            msg.reqid = msgReqid;
            this.send(msg);
            this.reqid++;
            return msgReqid;
        } else {
            // Handle varargs style
            const args = Array.from(arguments);
            if (args.length % 2 !== 0) {
                throw new Error('Expected even number of args');
            }
            const msg = {};
            for (let i = 0; i < args.length; i += 2) {
                msg[args[i]] = args[i + 1];
            }
            return this.req(msg);
        }
    }

    mkMsg(...args) {
        if (args.length % 2 !== 0) {
            throw new Error('Expected even number of args');
        }
        const msg = {};
        for (let i = 0; i < args.length; i += 2) {
            msg[args[i]] = args[i + 1];
        }
        return msg;
    }

    getNodeId() {
        if (this.nodeId) return this.nodeId;
        
        const args = process.argv;
        const idIndex = args.indexOf('--id');
        if (idIndex === -1 || !args[idIndex + 1]) {
            console.error('--id <id> argument not supplied');
            process.exit(1);
        }
        
        this.nodeId = args[idIndex + 1];
        if (!this.getNodes().includes(this.nodeId)) {
            console.error(`id ${this.nodeId} is not present in config file`);
            process.exit(1);
        }
        return this.nodeId;
    }

    siblingNodes() {
        const pat = this.siblingsPattern();
        return this.getNodes(pat);
    }


    getNodes(pattern = '.*') {
        if (!this.config) {
            const args = process.argv;
            const configIndex = args.indexOf('--config');
            if (configIndex === -1 || !args[configIndex + 1]) {
                throw new Error('--config <configfile> absent in command-line parameters');
            }
            
            const configFile = args[configIndex + 1];
            this.config = JSON.parse(fs.readFileSync(configFile, 'utf8'));
        }
        
        const regex = new RegExp(pattern);
        // Match Java's behavior using find() equivalent (test() in JavaScript)
        return Object.keys(this.config).filter(node => regex.test(node));
    }
    
    siblingsPattern() {
        if (this.othersPat) return this.othersPat;
        
        const me = this.getNodeId();
        // Match server ID pattern (e.g. "kv_server_1" -> ["kv_server_", "1"])
        const match = me.match(/^(.*)(\d+)$/);
        if (match) {
            const prefix = match[1];
            const digit = match[2];
            // Create pattern that matches same prefix but different number
            this.othersPat = `${prefix}[^${digit}]\\d*$`;
        }
        return this.othersPat;
    }


    dump(message) {
        if (!this.dumpFile) {
            this.dumpFile = fs.createWriteStream(`DEBUG_${this.getNodeId()}.txt`);
        }
        this.dumpFile.write(`${Date.now()} ${message}\n`);
    }

    recv() {
        return new Promise(resolve => {
            const checkQueue = () => {
                if (this.messageQueue.length > 0) {
                    resolve(this.messageQueue.shift());
                } else {
                    setTimeout(checkQueue, 100);
                }
            };
            checkQueue();
        });
    }

    info(obj) {
        console.error('INFO', obj);
    }
}

module.exports = new MessageUtils();