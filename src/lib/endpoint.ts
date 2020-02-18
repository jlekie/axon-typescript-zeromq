import * as Net from 'net';
import * as Url from 'url';

import { AServerEndpoint, AClientEndpoint, AEndpointDecoder, IEndpoint, IServerEndpoint, IClientEndpoint, IEncodableEndpoint } from '@jlekie/axon';

export interface IZeroMQEndpoint extends IEndpoint {
    toConnectionString(): string;
    serialize(): string;
}

export interface IZeroMQServerEndpoint extends IZeroMQEndpoint, IServerEndpoint, IEncodableEndpoint {
}

export interface IZeroMQClientEndpoint extends IZeroMQEndpoint, IClientEndpoint {
}

export abstract class AZeroMQServerEndpoint extends AServerEndpoint implements IZeroMQServerEndpoint {
    public abstract toConnectionString(): string;
    public abstract serialize(): string;

    public encode(): Buffer {
        return Buffer.from(this.serialize(), 'utf8');
    }
}

export class TcpServerEndpoint extends AZeroMQServerEndpoint {
    public static async allocate(startPort: number = 3000): Promise<TcpServerEndpoint> {
        const port = await findPort(startPort);

        return new TcpServerEndpoint(port, '127.0.0.1');
    }

    public readonly hostname: string | undefined;
    public readonly port: number;

    public constructor(port: number, hostname?: string) {
        super();

        this.hostname = hostname;
        this.port = port;
    }

    public toConnectionString() {
        return `tcp://${!!this.hostname ? this.hostname : '*'}:${this.port}`;
    }
    public serialize() {
        if (!this.hostname)
            throw new Error('Cannot encode TCP endpoint; hostname required');

        return `tcp|${this.hostname}|${this.port}`;
    }
}

export abstract class AZeroMQClientEndpoint extends AClientEndpoint implements IZeroMQClientEndpoint {
    public abstract toConnectionString(): string;
    public abstract serialize(): string;

    public encode(): Buffer {
        return Buffer.from(this.serialize(), 'utf8');
    }
}

export class TcpClientEndpoint extends AZeroMQServerEndpoint {
    public static fromUrl(url: string) {
        const parsedUrl = Url.parse(url);

        switch (parsedUrl.protocol) {
            case 'zmq:':
            case 'tcp:':
                if (!parsedUrl.hostname || !parsedUrl.port)
                    throw new Error('Invalid url');

                const port = parseInt(parsedUrl.port);

                return new this(parsedUrl.hostname, port);
            default:
                throw new Error(`Invalid protocol ${parsedUrl.protocol}`);
        }
    }

    public readonly hostname: string;
    public readonly port: number;

    public constructor(hostname: string, port: number) {
        super();

        this.hostname = hostname;
        this.port = port;
    }

    public toConnectionString() {
        return `tcp://${this.hostname}:${this.port}`;
    }
    public serialize() {
        return `tcp|${this.hostname}|${this.port}`;
    }
}

export class ClientEndpointDecoder extends AEndpointDecoder<IZeroMQClientEndpoint> {
    public decode(payload: Buffer): IZeroMQClientEndpoint {
        const decodedPayload = payload.toString('utf8');
        const facets = decodedPayload.split('|');

        switch (facets[0]) {
            case 'tcp':
                return new TcpClientEndpoint(facets[1], parseInt(facets[2]));
            default:
                throw new Error(`Unknown endpoint type ${facets[0]}`);
        }
    }
}

async function findPort(port: number = 3000) {
    return new Promise<number>((resolve, reject) => {
        const server = Net.createServer();
        server.on('error', (err) => {
            console.log(`Port in use ${port}`);
            server.listen(++port);
        }).on('listening', () => {
            server.close(() => resolve(port));
        });

        server.listen(port);
    });
}