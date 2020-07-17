import * as _ from 'lodash';
import * as ZeroMQ from 'zeromq';
import * as UuidV4 from 'uuid/v4';

import { ITransport, IClientTransport, IServerTransport, AClientTransport, AServerTransport, IReceivedData, ReceivedData, TransportMessage, VolatileTransportMetadata, VolatileTransportMetadataFrame, TaggedTransportMessage } from '@jlekie/axon';
import { IZeroMQServerEndpoint, IZeroMQClientEndpoint } from '../endpoint';

export interface IZeroMQTransport extends ITransport {
}

export interface IRouterServerTransport extends IServerTransport {
    readonly endpoint: IZeroMQServerEndpoint;
}

export interface IDealerClientTransport extends IClientTransport {
    readonly endpoint: IZeroMQClientEndpoint;
}

export class RouterServerTransport extends AServerTransport implements IRouterServerTransport {
    public readonly endpoint: IZeroMQServerEndpoint;

    private isRunning: boolean;
    private runningTask: Promise<void> | null;

    private isListening: boolean;
    private socket: ZeroMQ.Socket | null;

    private receiveBuffer: Array<TransportMessage>;
    private taggedReceiveBuffer: Map<string, TransportMessage>;
    private lastGetBufferedDataTimestamp: number;
    private lastGetBufferedTaggedDataTimestamp: number;

    public constructor(endpoint: IZeroMQServerEndpoint) {
        super();

        this.endpoint = endpoint;

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

    public send(message: TransportMessage) {
        if (!this.socket)
            throw new Error('Socket not ready');

        const forwardedMessage = TransportMessage.fromMessage(message);
        const envelope = forwardedMessage.metadata.pluck(`envelope[${this.identity}]`);

        this.socket.send(toNetMQMessage(forwardedMessage, envelope));

        return Promise.resolve();
    }
    public sendTagged(messageId: string, message: TransportMessage) {
        if (!this.socket)
            throw new Error('Socket not ready');

        const forwardedMessage = TransportMessage.fromMessage(message);
        const envelope = forwardedMessage.metadata.pluck(`envelope[${this.identity}]`);

        const encodedMessageId = Buffer.from(messageId, 'ascii');
        forwardedMessage.metadata.add(`rid[${this.identity}]`, encodedMessageId);

        this.socket.send(toNetMQMessage(forwardedMessage, envelope));

        return Promise.resolve();
    }

    public async receive() {
        const message = await this.getBufferedData();

        return message;
    }
    public async receiveTagged(messageId: string) {
        const message = await this.getTaggedBufferedData(messageId);

        return message;
    }

    public async receiveBufferedTagged() {
        const taggedMessage = await this.getNextTaggedBufferedData();

        return taggedMessage;
    }

    public sendAndReceive(): Promise<() => Promise<TransportMessage>> {
        throw new Error(`Not Implemented`);
    }

    private async handler(connectionString: string) {
        while (this.isRunning) {
            try {
                this.socket = ZeroMQ.socket('router');

                this.socket.on('message', (...frames) => {
                    const [ message, envelope ] = toEnvelopedTransportMessage(frames);

                    message.metadata.add(`envelope[${this.identity}]`, envelope);

                    const encodedRid = message.metadata.find(`rid[${this.identity}]`);
                    if (encodedRid) {
                        const rid = encodedRid.toString('ascii');

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

    private async getBufferedData(timeout?: number): Promise<TransportMessage> {
        if (this.receiveBuffer.length > 0) {
            this.lastGetBufferedDataTimestamp = Date.now();
            return this.receiveBuffer.shift() as TransportMessage;
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.receiveBuffer.length > 0) {
                    this.lastGetBufferedDataTimestamp = Date.now();
                    return this.receiveBuffer.shift() as TransportMessage;
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

    private async getTaggedBufferedData(rid: string, timeout?: number): Promise<TransportMessage> {
        if (this.taggedReceiveBuffer.size > 0 && this.taggedReceiveBuffer.has(rid)) {
            this.lastGetBufferedTaggedDataTimestamp = Date.now();

            const message = this.taggedReceiveBuffer.get(rid) as TransportMessage;
            this.taggedReceiveBuffer.delete(rid);

            return message;
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.taggedReceiveBuffer.size > 0 && this.taggedReceiveBuffer.has(rid)) {
                    this.lastGetBufferedTaggedDataTimestamp = Date.now();

                    const message = this.taggedReceiveBuffer.get(rid) as TransportMessage;
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
    private async getNextTaggedBufferedData(timeout?: number): Promise<TaggedTransportMessage> {
        if (this.taggedReceiveBuffer.size > 0) {
            this.lastGetBufferedTaggedDataTimestamp = Date.now();

            const tag = this.taggedReceiveBuffer.keys().next().value;
            const message = this.taggedReceiveBuffer.get(tag) as TransportMessage;
            this.taggedReceiveBuffer.delete(tag);

            return new TaggedTransportMessage(tag, message);
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.taggedReceiveBuffer.size > 0) {
                    this.lastGetBufferedTaggedDataTimestamp = Date.now();

                    const tag = this.taggedReceiveBuffer.keys().next().value;
                    const message = this.taggedReceiveBuffer.get(tag) as TransportMessage;
                    this.taggedReceiveBuffer.delete(tag);

                    return new TaggedTransportMessage(tag, message);
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

    private isRunning: boolean;
    private runningTask: Promise<void> | null;

    private isConnected: boolean;
    private socket: ZeroMQ.Socket | null;

    private receiveBuffer: Array<TransportMessage>;
    private taggedReceiveBuffer: Map<string, TransportMessage>;
    private lastGetBufferedDataTimestamp: number;
    private lastGetBufferedTaggedDataTimestamp: number;

    private receiveCount: number = 0;
    private sendCount: number = 0;

    public constructor(endpoint: IZeroMQClientEndpoint) {
        super();

        this.endpoint = endpoint;

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

    public send(message: TransportMessage) {
        if (!this.socket)
            throw new Error('Socket not ready');

        const forwardedMessage = TransportMessage.fromMessage(message);

        this.socket.send(toNetMQMessage(forwardedMessage));

        this.messageSentEvent.emit(forwardedMessage);

        return Promise.resolve();
    }
    public sendTagged(messageId: string, message: TransportMessage) {
        if (!this.socket)
            throw new Error('Socket not ready');

        const forwardedMessage = TransportMessage.fromMessage(message);

        const encodedMessageId = Buffer.from(messageId, 'ascii');
        forwardedMessage.metadata.add(`rid[${this.identity}]`, encodedMessageId);

        this.socket.send(toNetMQMessage(forwardedMessage));

        this.messageSentEvent.emit(forwardedMessage);

        return Promise.resolve();
    }

    public async receive() {
        const message = await this.getBufferedData(30000);

        this.messageReceivedEvent.emit(message);

        return message;
    }
    public async receiveTagged(messageId: string) {
        const message = await this.getTaggedBufferedData(messageId, 30000);

        this.messageReceivedEvent.emit(message);

        return message;
    }

    public async receiveBufferedTagged() {
        const taggedMessage = await this.getNextTaggedBufferedData(30000);

        this.messageReceivedEvent.emit(taggedMessage.message);

        return taggedMessage;
    }

    public sendAndReceive(message: TransportMessage): Promise<() => Promise<TransportMessage>> {
        if (!this.socket)
            throw new Error('Socket not ready');

        const forwardedMessage = TransportMessage.fromMessage(message);

        const rid = UuidV4();

        const encodedRid = Buffer.from(rid, 'ascii');
        forwardedMessage.metadata.add(`rid[${this.identity}]`, encodedRid);

        this.socket.send(toNetMQMessage(forwardedMessage));

        this.messageSentEvent.emit(forwardedMessage);

        return Promise.resolve(async () => {
            const responseMessage = await this.getTaggedBufferedData(rid, 30000);

            this.messageReceivedEvent.emit(responseMessage);

            return responseMessage;
        });
    }

    private async handler(connectionString: string) {
        while (this.isRunning) {
            try {
                this.socket = ZeroMQ.socket('dealer');

                this.socket.on('message', (...frames) => {
                    try {
                        const message = toTransportMessage(frames);

                        const encodedRid = message.metadata.find(`rid[${this.identity}]`);
                        if (encodedRid) {
                            const rid = encodedRid.toString('ascii');
    
                            this.taggedReceiveBuffer.set(rid, message);
                        }
                        else {
                            this.receiveBuffer.push(message);
                        }
                    }
                    catch (err) {
                        console.log(`Message received error [${err.toString()}]`);
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

    private async getBufferedData(timeout?: number): Promise<TransportMessage> {
        if (this.receiveBuffer.length > 0) {
            this.lastGetBufferedDataTimestamp = Date.now();
            return this.receiveBuffer.shift() as TransportMessage;
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.receiveBuffer.length > 0) {
                    this.lastGetBufferedDataTimestamp = Date.now();
                    return this.receiveBuffer.shift() as TransportMessage;
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

    private async getTaggedBufferedData(rid: string, timeout?: number): Promise<TransportMessage> {
        if (this.taggedReceiveBuffer.size > 0 && this.taggedReceiveBuffer.has(rid)) {
            this.lastGetBufferedTaggedDataTimestamp = Date.now();

            const message = this.taggedReceiveBuffer.get(rid) as TransportMessage;
            this.taggedReceiveBuffer.delete(rid);

            return message;
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.taggedReceiveBuffer.size > 0 && this.taggedReceiveBuffer.has(rid)) {
                    this.lastGetBufferedTaggedDataTimestamp = Date.now();

                    const message = this.taggedReceiveBuffer.get(rid) as TransportMessage;
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
    private async getNextTaggedBufferedData(timeout?: number): Promise<TaggedTransportMessage> {
        if (this.taggedReceiveBuffer.size > 0) {
            this.lastGetBufferedTaggedDataTimestamp = Date.now();

            const tag = this.taggedReceiveBuffer.keys().next().value;
            const message = this.taggedReceiveBuffer.get(tag) as TransportMessage;
            this.taggedReceiveBuffer.delete(tag);

            return new TaggedTransportMessage(tag, message);
        }
        else {
            const startTimestamp = Date.now();

            while (this.isRunning) {
                if (this.taggedReceiveBuffer.size > 0) {
                    this.lastGetBufferedTaggedDataTimestamp = Date.now();

                    const tag = this.taggedReceiveBuffer.keys().next().value;
                    const message = this.taggedReceiveBuffer.get(tag) as TransportMessage;
                    this.taggedReceiveBuffer.delete(tag);

                    return new TaggedTransportMessage(tag, message);
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

function toTransportMessage(frames: Buffer[]) {
    const metadata = new VolatileTransportMetadata();

    let payload: Buffer | null = null;

    const signal = frames[0].readInt32BE(0);
    if (signal !== 0)
        throw new Error(`Message received with signal code ${signal}`);

    let partBuffer: Buffer[] = [];
    for (let a = 1; a < frames.length; a++) {
        const frame = frames[a];

        if (frame.length === 0 || a >= frames.length - 1) {
            if (partBuffer.length === 2) {
                const name = partBuffer[0].toString('ascii');
                const framePayload = partBuffer[1];

                metadata.add(name, framePayload);
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

    return new TransportMessage(payload, metadata);
}
function toEnvelopedTransportMessage(frames: Buffer[]) {
    const metadata = new VolatileTransportMetadata();

    let payload: Buffer | null = null;

    const envelope = frames[0];

    const signal = frames[1].readInt32BE(0);
    if (signal !== 0)
        throw new Error(`Message received with signal code ${signal}`);

    for (let a = 2; a < frames.length; a++) {
        const frame = frames[a];

        let partBuffer: Buffer[] = [];
        if (frame.length === 0 || a >= frames.length - 1) {
            if (partBuffer.length === 2) {
                const name = partBuffer[0].toString('ascii');
                const framePayload = partBuffer[1];

                metadata.add(name, framePayload);
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

    return [ new TransportMessage(payload, metadata), envelope ] as const;
}
function toNetMQMessage(message: TransportMessage, envelope?: Buffer) {
    const zeroMQMessage: Buffer[] = [];

    if (envelope)
        zeroMQMessage.push(envelope);

    const signalBuffer = Buffer.alloc(4);
    signalBuffer.writeInt32BE(0, 0);
    zeroMQMessage.push(signalBuffer);

    for (const frame of message.metadata.frames) {
        zeroMQMessage.push(Buffer.from(frame.id, 'ascii'));
        zeroMQMessage.push(frame.data);
        zeroMQMessage.push(Buffer.alloc(0));
    }

    zeroMQMessage.push(message.payload);

    return zeroMQMessage;
}