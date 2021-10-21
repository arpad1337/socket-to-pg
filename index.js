const { EventEmitter } = require('events');
const net = require('net');
const { Client } = require('pg');
const { config } = require('dotenv');

class DatabaseProvider {
    constructor(client) {
        this.client = client;
    }

    async connect() {
        await this.client.connect();
    }

    async createTable(tableName, columns) {
        try {
            await this.client.query(`CREATE TABLE ${tableName} (${columns.join(', ')})`);
        } catch(e) {
            console.error(e.message);
        }
    }

    async writeToTable(tableName, values) {
        try {
            console.log(`Inserting into ${tableName}:`, values);
            const query = 
            `INSERT INTO ${tableName} (${values.map((value) => value.columnName).join(', ')}) ` +
            `VALUES (${this._prepareInsertVariables(values.length)})`;
            await this.client.query(query, values.map((value) => value.value));
        } catch(e) {
            console.error(e.message);
        }
    }

    _prepareInsertVariables(numberOfFields) {
        return new Array(numberOfFields).fill('').map((_, index) => `$${index+1}`).join(', ');
    }
}

class SocketProcessor extends EventEmitter {
    constructor(socket) {
        super();
        this.socket = socket;
        this.recordBuffer = [];
        this.validationSchema = new RegExp('[0-9][,]{1}[0-9]');
    }

    start() {
        this.socket.on('data', (data) => {
            this._processChunk(data);
        });
    }

    _processChunk(dataBuffer) {
        const data = dataBuffer.toString();

        const fragmentBuffer = [];
        let inRecord = false;
        let i = 0;
        while (i < data.length) {
            if (inRecord) {
                this.recordBuffer.push(data[i]);
            }
            if (data[i] === '[') {
                inRecord = true;
            }
            if (data[i] === ']') {
                inRecord = false;
                fragmentBuffer.push(this.recordBuffer.join('').substring(0, this.recordBuffer.length - 1));
                this.recordBuffer.length = 0;
            }
            i++;
        }

        if (fragmentBuffer.length > 0) {
            fragmentBuffer.forEach((fragment) => {
                if (!this.validationSchema.test(fragment)) {
                    console.error('Fragment is broken, skipping...');
                    return;
                }
                this.emit('data', fragment);
            });
        }
    }
}

if (require.main === module) {
    (async function() {
        /*
            The service emits ( timestamp, value ) pairs via an ASCII protocol. Messages
            begin with "[", then have the timestamp (in nanoseconds since the UNIX epoch),
            the a ",", then the value (as an ASCII-encoded base-10 number), then "]".
            Messages are separated by a newline.

            An example session might look like:

                [1468009895549670789,397807]
                [1468009895567246398,758675]
                [1468009895577565428,538795]

            The values are in the range [0, 1000000).
         */

        config({path: '.env'});

        const TABLE_NAME = process.env.TABLE_NAME || 'socket_stream_values';

        const postgresClient = new Client({
            host: process.env.DB_HOST,
            port: process.env.DB_PORT,
            database: process.env.DB_DATABASE,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD
        });

        const dbProvider = new DatabaseProvider(postgresClient);

        await dbProvider.connect();
        await dbProvider.createTable(TABLE_NAME, ['timestamp bigint NOT NULL', 'value integer NOT NULL']);

        const socket = net.createConnection({
            host: process.env.SOCKET_HOST, 
            port: process.env.SOCKET_PORT
        });

        const socketProcessor = new SocketProcessor(socket);

        socketProcessor.on('data', (fragment) => {
            const fragments = fragment.split(',');
            const data = [{
                columnName: 'timestamp',
                value: fragments[0],
            }, {
                columnName: 'value',
                value: fragments[1],
            }];
            dbProvider.writeToTable(TABLE_NAME, data);
        });

        socketProcessor.start();
    })();
} else {
    module.exports = {
        DatabaseProvider,
        SocketProcessor
    };
}