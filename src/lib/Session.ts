import { Cell } from '@ton/core';
import { open, unlink, FileHandle, readFile, writeFile } from 'node:fs/promises';
type SessionConfig = {
    path: string,
    args: unknown,
    data?: string,
    start?: number,
    end?: number
    offset?: number,
    processed?: number
}

export class Session {

    args: unknown;

    protected start: number; 
    protected path: string;
    protected dataFile: FileHandle | undefined;
    protected end?: number;
    protected _offset: number;
    protected data?: string;

    processed: number;

    constructor(config: SessionConfig) {
        this.args  = config.args;
        this.path  = config.path;
        this.data  = config.data;
        this._offset= config.offset ?? 0;
        this.processed = config.processed ?? 0;
        this.start = Math.floor(Date.now() / 1000);
        if(this.data) {
            open(this.data, 'a').then(f => this.dataFile = f);
        }
    }

    async save(path?: string) {
        const savePath = path ?? this.path;
        await writeFile(savePath, JSON.stringify({
            start: this.start,
            end: Math.floor(Date.now() / 1000),
            offset: this.offset,
            processed: this.processed,
            data: this.data,
            args: this.args
        }));
    }
    nextChunk(n: number) {
        this._offset += n;
    }
    async finish() {
        console.log("Closing session...");
        await this.dataFile?.close();
        await unlink(this.path);
        if(this.data) {
            await unlink(this.data);
        }
    }

    async addData(data: unknown) {
        if(!this.dataFile) {
            throw new Error("Data file is not opened");
        }
        this.dataFile.appendFile(JSON.stringify(data, (k, v) => {
            if(v instanceof Cell) {
                return v.toBoc().toString('base64');
            }
            return v;
        }) + "\n");
    }

    async getDataFile() {
        if(!this.data) {
            throw new Error("This session has no data file");
        }
        return await open(this.data, 'r');
    }

    get offset() {
        return this._offset;
    }

    static async fromFile(path: string) {
        const conf = JSON.parse(
            await readFile(path, {encoding: 'utf8'})
        );
        conf.path = path;
        return new Session(conf);
    }
}
