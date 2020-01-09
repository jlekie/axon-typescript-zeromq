import * as ZeroMQ from 'zeromq';
import * as UuidV4 from 'uuid/v4';

import { ITransport, IClientTransport, IServerTransport, AClientTransport, AServerTransport, IReceivedData, ReceivedData } from '@jlekie/axon';
import { IZeroMQServerEndpoint, IZeroMQClientEndpoint } from '../endpoint';

export interface IZeroMQTransport extends ITransport {
}

export interface IRouterServerTransport extends IServerTransport {
    readonly endpoint: IZeroMQServerEndpoint;
}

export interface IDealerClientTransport extends IClientTransport {
    readonly endpoint: IZeroMQClientEndpoint;
    readonly identity: string;
}

export class RouterServerTransport extends AServerTransport implements IRouterServerTransport {
    public readonly endpoint: IZeroMQServerEndpoint;
    public readonly identity: string;

    private isRunning: boolean;
    private runningTask: Promise<void> | null;

    private isListening: boolean;
    private socket: ZeroMQ.Socket | null;

    private receiveBuffer: Array<Message>;
    private taggedReceiveBuffer: Map<string, Message>;
    private lastGetBufferedDataTimestamp: number;
    private lastGetBufferedTaggedDataTimestamp: number;

    public constructor(endpoint: IZeroMQServerEndpoint) {
        super();

        this.endpoint = endpoint;
        this.identity = UuidV4();

        this.isRunning = false;
        this.runningTask = null;

        this.isListening = false;
        this.socket = null;

        this.receiveBuffer = [];
        this.taggedReceiveBuffer = new Map();
        this.lastGetBufferedDataTimestamp = Date.now();
        this.lastGetBufferedTaggedDataTimestamp = Date.now();
    }

    public listen() {
        this.isRunning = true;
        const connectionString = this.endpoint.toConnectionString();

        this.runningTask = this.handler(connectionString);

        return Promise.resolve();
    }
    public async close() {
        this.isRunning = false;

        await this.runningTask;
    }

    public send(data: Buffer, metadata: Record<string, Buffer>) {
        if (!this.socket)
            throw new Error('Socket not ready');

        const frames: Record<string, Buffer> = {};
        let envelope: Buffer | undefined;

        for (const key in metadata) {
            switch (key) {
                case 'envelope':
                    envelope = metadata[key];
                    break;
                default:
                    frames[key] = metadata[key];
                    break;
            }
        }

        const message = new Message(0, frames, data, envelope);

        this.socket.send(message.toZeroMQMessage(true));

        return Promise.resolve();
    }
    public sendTagged(messageId: string, data: Buffer, metadata: Record<string, Buffer>) {
        if (!this.socket)
            throw new Error('Socket not ready');

        const encodedRid = Buffer.from(messageId, 'ascii');

        const frames: Record<string, Buffer> = {};

        let envelope: Buffer | undefined;
        for (const key in metadata) {
            switch (key) {
                case 'envelope':
                    envelope = metadata[key];
                    break;
                default:
                    frames[key] = metadata[key];
                    break;
            }
        }

        frames['rid'] = encodedRid;

        const message = new Message(0, frames, data, envelope);

        this.socket.send(message.toZeroMQMessage(true));

        return Promise.resolve();
    }

    public async receive() {
        const message = await this.getBufferedData();

        const data = message.payload;

        const metadata = { ...message.frames };
        if (message.envelope)
            metadata['envelope'] = message.envelope;

        return { data, metadata };
    }
    public async receiveTagged(messageId: string) {
        const message = await this.getTaggedBufferedData(messageId);

        const data = message.payload;

        const metadata = { ...message.frames };
        if (message.envelope)
            metadata['envelope'] = message.envelope;

        return { data, metadata };
    }

    public async receiveBufferedTagged() {
        const taggedMessage = await this.getNextTaggedBufferedData();
        const tag = taggedMessage.tag;
        const message = taggedMessage.message;

        const data = message.payload;

        const metadata = { ...message.frames };
        if (message.envelope)
            metadata['envelope'] = message.envelope;

        return { tag, data, metadata };
    }

    public sendAndReceive(): Promise<() => Promise<IReceivedData>> {
        throw new Error(`Not Implemented`);
    }

    private async handler(connectionString: string) {
        while (this.isRunning) {
            try {
                this.socket = ZeroMQ.socket('router');

                this.socket.on('message', (...frames) => {
                    const message = Message.fromZeroMQMessage(frames, true);

                    if (message.frames['rid'] !== undefined) {
                        const rid = message.frames['rid'].toString('ascii');
                        this.taggedReceiveBuffer.set(rid, message);
                    }
                    else {
                        this.receiveBuffer.push(message);
                    }
                });

                this.socket.on('listen', () => {
                    console.log(`Router socket listening at ${connectionString}`);
                    this.isListening = true;
                });
                this.socket.on('close', () => {
                    console.log(`Router socket closed at ${connectionString}`);
                    this.isListening = false;
                });

                this.socket
                    .monitor(500, 0)
                    .bind(connectionString);

                const start = Date.now();
                while (!this.isListening) {
                    if (Date.now() - start > 5000)
                        throw new Error('Socket bind timeout');

                    await delay(1000);
                }

                while (this.isRunning && this.isListening) {
                    await delay(1000);
                }

                this.socket.unbind(connectionString);
            }
            catch (err) {
                console.log(err.toString());
            }
        }
    }

    private async getBufferedData(timeout?: number): Promise<Message> {
        if (this.receiveBuffer.length > 0) {
            this.lastGetBufferedDataTimestamp = Date.now();
            return this.receiveBuffer.shift() as Message;
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.receiveBuffer.length > 0) {
                    this.lastGetBufferedDataTimestamp = Date.now();
                    return this.receiveBuffer.shift() as Message;
                }
                else if (timeout && (Date.now() - startTimestamp) > timeout) {
                    throw new Error('Message timeout');
                }

                // if ((Date.now() - this.lastGetBufferedDataTimestamp) > 1000)
                    await defer();
            }

            throw new Error('Transport closed');
        }
    }

    private async getTaggedBufferedData(rid: string, timeout?: number): Promise<Message> {
        if (this.taggedReceiveBuffer.size > 0 && this.taggedReceiveBuffer.has(rid)) {
            this.lastGetBufferedTaggedDataTimestamp = Date.now();

            const message = this.taggedReceiveBuffer.get(rid) as Message;
            this.taggedReceiveBuffer.delete(rid);

            return message;
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.taggedReceiveBuffer.size > 0 && this.taggedReceiveBuffer.has(rid)) {
                    this.lastGetBufferedTaggedDataTimestamp = Date.now();

                    const message = this.taggedReceiveBuffer.get(rid) as Message;
                    this.taggedReceiveBuffer.delete(rid);

                    return message;
                }
                else if (timeout && (Date.now() - startTimestamp) > timeout) {
                    throw new Error('Tagged message timeout');
                }

                // if ((Date.now() - this.lastGetBufferedTaggedDataTimestamp) > 1000)
                    await defer();
            }

            throw new Error('Transport closed');
        }
    }
    private async getNextTaggedBufferedData(timeout?: number): Promise<TaggedMessage> {
        if (this.taggedReceiveBuffer.size > 0) {
            this.lastGetBufferedTaggedDataTimestamp = Date.now();

            const tag = this.taggedReceiveBuffer.keys().next().value;
            const message = this.taggedReceiveBuffer.get(tag) as Message;
            this.taggedReceiveBuffer.delete(tag);

            return new TaggedMessage(tag, message);
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.taggedReceiveBuffer.size > 0) {
                    this.lastGetBufferedTaggedDataTimestamp = Date.now();

                    const tag = this.taggedReceiveBuffer.keys().next().value;
                    const message = this.taggedReceiveBuffer.get(tag) as Message;
                    this.taggedReceiveBuffer.delete(tag);

                    return new TaggedMessage(tag, message);
                }
                else if (timeout && (Date.now() - startTimestamp) > timeout) {
                    throw new Error('Tagged message timeout');
                }

                // if ((Date.now() - this.lastGetBufferedTaggedDataTimestamp) > 1000)
                    await defer();
            }

            throw new Error('Transport closed');
        }
    }
}

export class DealerClientTransport extends AClientTransport implements IDealerClientTransport {
    public readonly endpoint: IZeroMQClientEndpoint;
    public readonly identity: string;

    private isRunning: boolean;
    private runningTask: Promise<void> | null;

    private isConnected: boolean;
    private socket: ZeroMQ.Socket | null;

    private receiveBuffer: Array<Message>;
    private taggedReceiveBuffer: Map<string, Message>;
    private lastGetBufferedDataTimestamp: number;
    private lastGetBufferedTaggedDataTimestamp: number;

    private receiveCount: number = 0;
    private sendCount: number = 0;

    public constructor(endpoint: IZeroMQClientEndpoint) {
        super();

        this.endpoint = endpoint;

        this.identity = UuidV4();

        this.isRunning = false;
        this.runningTask = null;

        this.isConnected = false;
        this.socket = null;

        this.receiveBuffer = [];
        this.taggedReceiveBuffer = new Map();
        this.lastGetBufferedDataTimestamp = Date.now();
        this.lastGetBufferedTaggedDataTimestamp = Date.now();
    }

    public async connect(timeout?: number) {
        this.isRunning = true;
        const connectionString = this.endpoint.toConnectionString();

        this.runningTask = this.handler(connectionString);

        // const startTimestamp = Date.now();
        // while (!this.isConnected) {
        //     if (timeout && (Date.now() - startTimestamp) > timeout)
        //         throw new Error('Discovery timeout');

        //     await delay(500);
        // }
    }
    public async close() {
        this.isRunning = false;

        await this.runningTask;
    }

    public send(data: Buffer, metadata: Record<string, Buffer>) {
        if (!this.socket)
            throw new Error('Socket not ready');

        const encodedIdentity = Buffer.from(this.identity, 'ascii');

        const frames: Record<string, Buffer> = {};
        for (const key in metadata) {
            frames[key] = metadata[key];
        }

        const message = new Message(0, frames, data);
        this.socket.send(message.toZeroMQMessage(true));

        // this.sendCount++;
        // console.log(`SENT ${this.sendCount}`);

        return Promise.resolve();
    }
    public sendTagged(messageId: string, data: Buffer, metadata: Record<string, Buffer>) {
        if (!this.socket)
            throw new Error('Socket not ready');

        const encodedRid = Buffer.from(messageId, 'ascii');

        const frames: Record<string, Buffer> = {};
        for (const key in metadata) {
            frames[key] = metadata[key];
        }
        frames[`rid[${this.identity}]`] = encodedRid;

        const message = new Message(0, frames, data);

        this.socket.send(message.toZeroMQMessage(true));

        // this.sendCount++;
        // console.log(`SENT ${this.sendCount}`);

        return Promise.resolve();
    }

    public async receive() {
        const message = await this.getBufferedData(30000);

        if (message.signal !== 0) {
            const errorMessage = message.payload.toString('utf8');
            throw new Error(`Transport error (${message.signal}): ${errorMessage}`);
        }

        const data = message.payload;
        const metadata = { ...message.frames };

        return { data, metadata };
    }
    public async receiveTagged(messageId: string) {
        const message = await this.getTaggedBufferedData(messageId, 30000);

        if (message.signal !== 0) {
            const errorMessage = message.payload.toString('utf8');
            throw new Error(`Transport error (${message.signal}): ${errorMessage}`);
        }

        const data = message.payload;
        const metadata = { ...message.frames };

        return { data, metadata };
    }

    public async receiveBufferedTagged() {
        const taggedMessage = await this.getNextTaggedBufferedData();
        const tag = taggedMessage.tag;
        const message = taggedMessage.message;

        const data = message.payload;

        const metadata = { ...message.frames };

        return { tag, data, metadata };
    }

    public sendAndReceive(data: Buffer, metadata: Record<string, Buffer>): Promise<() => Promise<IReceivedData>> {
        if (!this.socket)
            throw new Error('Socket not ready');

        const rid = UuidV4();

        const encodedRid = Buffer.from(rid, 'ascii');
        // const encodedIdentity = Buffer.from(this.identity, 'ascii');

        const frames = { ...metadata };
        frames['rid'] = encodedRid;

        const message = new Message(0, frames, data);
        this.socket.send(message.toZeroMQMessage(true));

        return Promise.resolve(async () => {
            const responseMessage = await this.getTaggedBufferedData(rid, 30000);

            if (responseMessage.signal !== 0) {
                const errorMessage = responseMessage.payload.toString('utf8');
                throw new Error(`Transport error (${responseMessage.signal}): ${errorMessage}`);
            }

            const responsePayloadData = responseMessage.payload;
            const responseMetadata = { ...responseMessage.frames };

            return new ReceivedData(responsePayloadData, responseMetadata);
        });
    }

    private async handler(connectionString: string) {
        while (this.isRunning) {
            try {
                this.socket = ZeroMQ.socket('dealer');

                this.socket.on('message', (...frames) => {
                    const message = Message.fromZeroMQMessage(frames, false);

                    // this.receiveCount++;
                    // console.log(`RECEIVED ${this.receiveCount}`);
                    if (message.frames[`rid[${this.identity}]`] !== undefined) {
                        const rid = message.frames[`rid[${this.identity}]`].toString('ascii');

                        this.taggedReceiveBuffer.set(rid, message);
                    }
                    else {
                        this.receiveBuffer.push(message);
                    }
                });

                this.socket.on('connect', () => {
                    // console.log(`Dealer socket connected to ${connectionString}`);
                    this.isConnected = true;
                });
                this.socket.on('disconnect', () => {
                    // console.log(`Dealer socket disconnected from ${connectionString}`);
                    this.isConnected = false;
                });
                this.socket.on('close', () => {
                    // console.log(`Dealer socket closed`);
                    this.isConnected = false;
                });

                console.log(`Connecting to ${connectionString}...`);
                this.socket
                    .monitor(undefined, 0)
                    .connect(connectionString);

                const start = Date.now();
                while (!this.isConnected) {
                    if (!this.isRunning || Date.now() - start > 30000) {
                        this.socket.unmonitor().close();
                        throw new Error(`Connection to ${connectionString} timed out`);
                    }

                    await delay(1000);
                }

                console.log(`Connected to ${connectionString}`);

                while (this.isRunning && this.isConnected) {
                    await delay(1000);
                }

                // console.log('disconnecting socket');
                this.socket.unmonitor().close();
                // this.socket.disconnect(connectionString);
                // console.log('disconnected');

                // while (this.isConnected) {
                //     if (Date.now() - start > 10000)
                //         throw new Error('Socket disconnect timeout');

                //     await delay(1000);
                // }

                if (this.isRunning)
                    console.log(`Socket disconnected from ${connectionString}, reconnecting...`);
            }
            catch (err) {
                console.log(`Socket error [${err.toString()}], reconnecting...`);
            }
            // finally {
            //     if (this.socket) {
            //         // console.log('unmonitor socket');
            //         this.socket.unmonitor();
            //         // console.log('unmonitor');
            //     }
            // }
        }
    }

    private async getBufferedData(timeout?: number): Promise<Message> {
        if (this.receiveBuffer.length > 0) {
            this.lastGetBufferedDataTimestamp = Date.now();
            return this.receiveBuffer.shift() as Message;
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.receiveBuffer.length > 0) {
                    this.lastGetBufferedDataTimestamp = Date.now();
                    return this.receiveBuffer.shift() as Message;
                }
                else if (timeout && (Date.now() - startTimestamp) > timeout) {
                    throw new Error('Message timeout');
                }

                // if ((Date.now() - this.lastGetBufferedDataTimestamp) > 1000)
                    await defer();
            }

            throw new Error('Transport closed');
        }
    }

    private async getTaggedBufferedData(rid: string, timeout?: number): Promise<Message> {
        if (this.taggedReceiveBuffer.size > 0 && this.taggedReceiveBuffer.has(rid)) {
            this.lastGetBufferedTaggedDataTimestamp = Date.now();

            const message = this.taggedReceiveBuffer.get(rid) as Message;
            this.taggedReceiveBuffer.delete(rid);

            return message;
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.taggedReceiveBuffer.size > 0 && this.taggedReceiveBuffer.has(rid)) {
                    this.lastGetBufferedTaggedDataTimestamp = Date.now();

                    const message = this.taggedReceiveBuffer.get(rid) as Message;
                    this.taggedReceiveBuffer.delete(rid);

                    return message;
                }
                else if (timeout && (Date.now() - startTimestamp) > timeout) {
                    throw new Error('Tagged message timeout');
                }

                // if ((Date.now() - this.lastGetBufferedTaggedDataTimestamp) > 1000)
                    await defer();
            }

            throw new Error('Transport closed');
        }
    }
    private async getNextTaggedBufferedData(timeout?: number): Promise<TaggedMessage> {
        if (this.taggedReceiveBuffer.size > 0) {
            this.lastGetBufferedTaggedDataTimestamp = Date.now();

            const tag = this.taggedReceiveBuffer.keys().next().value;
            const message = this.taggedReceiveBuffer.get(tag) as Message;
            this.taggedReceiveBuffer.delete(tag);

            return new TaggedMessage(tag, message);
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.taggedReceiveBuffer.size > 0) {
                    this.lastGetBufferedTaggedDataTimestamp = Date.now();

                    const tag = this.taggedReceiveBuffer.keys().next().value;
                    const message = this.taggedReceiveBuffer.get(tag) as Message;
                    this.taggedReceiveBuffer.delete(tag);

                    return new TaggedMessage(tag, message);
                }
                else if (timeout && (Date.now() - startTimestamp) > timeout) {
                    throw new Error('Tagged message timeout');
                }

                // if ((Date.now() - this.lastGetBufferedTaggedDataTimestamp) > 1000)
                    await defer();
            }

            throw new Error('Transport closed');
        }
    }
}

async function delay(delay: number) {
    await new Promise((resolve) => setTimeout(() => resolve(), delay));
}
async function defer() {
    // await new Promise((resolve) => process.nextTick(() => {
    //     console.log('nextTick')
    //     resolve()
    // }));

    await new Promise((resolve) => setImmediate(() => resolve()));
}

class Message {
    public static fromZeroMQMessage(messageFrames: ReadonlyArray<Buffer>, dealerMessage: boolean): Message {
        const frames: Record<string, Buffer> =  {};
        let payload: Buffer | undefined;
        let envelope: Buffer | undefined;

        let startingFrame = 0;

        if (dealerMessage) {
            envelope = messageFrames[0];
            startingFrame++;
        }

        const signal = messageFrames[startingFrame].readInt32BE(0);
        startingFrame++;

        let partBuffer: Array<Buffer> = [];
        for (let a = startingFrame; a < messageFrames.length; a++) {
            const frame = messageFrames[a];

            if (frame.length === 0 || a >= messageFrames.length - 1) {
                if (partBuffer.length === 2) {
                    const name = partBuffer[0].toString('utf8');
                    const framePayload = partBuffer[1];

                    frames[name] = framePayload;
                }
                else if (partBuffer.length === 0) {
                    payload = frame;
                }
                else {
                    throw new Error(`Unexpected frame count ${partBuffer.length}`);
                }

                partBuffer = [];
            }
            else {
                partBuffer.push(frame);
            }
        }

        if (!payload)
            throw new Error('Missing payload');

        return new Message(signal, frames, payload, envelope);
    }

    public readonly frames: Record<string, Buffer>;
    public readonly payload: Buffer;
    public readonly envelope: Buffer | undefined;
    public readonly signal: number;

    public constructor(signal: number, frames: Record<string, Buffer>, payload: Buffer, envelope?: Buffer) {
        this.frames = { ...frames };
        this.payload = payload;
        this.envelope = envelope;
        this.signal = signal;
    }

    public pluckFrame(key: string): Buffer | undefined {
        if (this.frames[key] !== undefined) {
            const data = this.frames[key];
            delete this.frames[key];

            return data;
        }
    }

    public toZeroMQMessage(dealerMessage: boolean) {
        const zeroMQMessage: Buffer[] = [];

        if (this.envelope)
            zeroMQMessage.push(this.envelope);

        const signalBuffer = Buffer.alloc(4);
        signalBuffer.writeInt32BE(this.signal, 0);
        zeroMQMessage.push(signalBuffer);

        for (const frameKey in this.frames) {
            zeroMQMessage.push(Buffer.from(frameKey, 'ascii'));
            zeroMQMessage.push(this.frames[frameKey]);
            zeroMQMessage.push(Buffer.alloc(0));
        }

        zeroMQMessage.push(this.payload);

        return zeroMQMessage;
    }
}
class TaggedMessage {
    public readonly tag: string;
    public readonly message: Message;

    public constructor(tag: string, message: Message) {
        this.tag = tag;
        this.message = message;
    }
}