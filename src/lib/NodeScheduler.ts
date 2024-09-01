import { NodeProcessor, PfxProcessed, PfxProcessedSerialized, ProcessorConfig} from './NodeProcessor';
import { Cell } from '@ton/core';
import { readAllPrefixes } from './Dictionary';
import { PfxCluster, StorageSqlite } from './BranchStorage';
import { joinSuffixes } from './util';
import { Worker } from 'node:worker_threads';
import { Session } from './Session';
import { readFile } from 'node:fs/promises';


type SchedulerConfig = ProcessorConfig & {
    storage: StorageSqlite,
    session: Session,
    workers: Worker[]
}

type MessagePfx = {
    type: 'pfx',
    pfx: PfxCluster[]
}
type MessageProcesed = {
    type: 'processed',
    pfx: {
        pfx: number | bigint,
        len: number,
        cell: string
    }[]
}
type SaveBranch = {
    type: 'branch',
    pfx: number | bigint,
    pfxLength: number,
    depth: number,
    hash: string
}
type SaveTop = {
    type: 'top',
    pfx: number | bigint,
    pfxLength: number,
    path: string,
    boc: string
}
type UpdatePath = {
    type: 'update_path',
    pfxs: number[],
    pfxLength: number,
    path: number[],
}

type WorkerError = {
    type: 'error',
    error: unknown
}

type NoMoreData = {
    type: 'no_more_data',
}

export type WorkerMessage = MessageProcesed | MessagePfx | SaveBranch | SaveTop | UpdatePath | NoMoreData | WorkerError;

export class NodeScheduler extends NodeProcessor{
    protected workers: Worker[]
    protected storage: StorageSqlite;
    protected session: Session;

    constructor(config: SchedulerConfig) {
        super(config);
        this.workers = config.workers
        this.storage = config.storage;
        this.session = config.session;
    }
    override async saveBranch(pfx: number | bigint, pfxLength: number, depth: number, hash: Buffer) {
        await this.storage.saveBranch(pfx, pfxLength, depth, hash);
    }

    override async saveTop(pfx: number | bigint, pfxLength: number, path: string, top: Cell) {
        const suffixLen = this.maxLen - pfxLength;
        const addresses = readAllPrefixes(top, pfx, pfxLength, suffixLen).map(
            p => '0:' + joinSuffixes(pfx, [suffixLen, p]).toString(16).padStart(64,'0')
        );
        await this.storage.saveTop(pfx, pfxLength, path, top.toBoc().toString('base64'), addresses);
    }
    override add(pfxs: PfxCluster[]) {
        this.queue.push(...pfxs);
        if(this.runningBatch.length == 0 || this.running == undefined) {
            this.running = this.run();
        }
        return this.running;
    }

    override async run() {
        let results: PfxProcessed[] = [];
        let batchCount = 0;
        const handleDb = async (msg: WorkerMessage) => {
            switch(msg.type) {
                case 'branch':
                    await this.saveBranch(msg.pfx, msg.pfxLength, msg.depth, Buffer.from(msg.hash, 'hex'));
                break;
                case 'top':
                    await this.saveTop(msg.pfx, msg.pfxLength, msg.path, Cell.fromBase64(msg.boc));
                break;
            }
        }
        const handleError = (msg: WorkerMessage) => {
            if(msg.type == 'error') {
                console.log("Error:", msg.error);
                throw(msg.error);
            }
        }

        this.workers.forEach(w => {
            w.on('message', handleDb);
            w.on('message', handleError);
        });

        while(this.queue.length > 0 || this.runningBatch.length > 0) {
            let toProcess = this.queue.length;

            const res = this.workers.map(w => new Promise((resolve:(v: PfxProcessed[]) => void, reject) => {
                w.once('error', (e) => reject(e));
                w.on('message', async function processedPfx (msg: WorkerMessage) {
                    if(msg.type == 'processed') {
                        // console.log("Got processed:", msg);
                        w.removeListener('message', processedPfx);
                        resolve(msg.pfx.map(p => {
                            return {
                                ...p,
                                cell: Cell.fromBase64(p.cell)
                            }
                        }));
                    }
                });
            }));
            const perWorker = Math.floor(toProcess / this.workers.length);
            for(let i = 0; i < this.workers.length && i < toProcess; i++) {
                const msgs = this.queue.splice(0, perWorker);
                toProcess -= msgs.length;
                // console.log("Posting...:");
                this.workers[i].postMessage({
                    type: 'pfx',
                    pfx: msgs
                });
            }

            this.workers.forEach(w => w.removeAllListeners('error'));
            results = (await Promise.all(res)).flatMap(r => r);
            batchCount++;
        }

        if(results.length > 2) {
            results = await this.joinResults(results);
        }

        await this.session.addData(results);

        // this.results.push(...results);

        this.workers.forEach(w => w.removeAllListeners('message'));

        return results;
    }
    override async updatePath(pfxs: number[], pfxLength: number, paths: number[]) {
        await this.clearCache();
        await this.storage.updatePath(pfxs, pfxLength, paths);
    }
    override async clearCache() {
        await this.storage.saveBranchCache();
        await this.storage.clearTopCache();
    }

    async finalize() {
        const dataFile = await this.session.getDataFile();
        if(!dataFile) {
            throw new Error("No data file to save");
        }
        const dataLeft = (await dataFile.readFile({encoding: 'utf8'})).trimEnd();
        const tail     = dataLeft.split("\n").flatMap(d => {
            return (JSON.parse(d) as PfxProcessedSerialized[]).map(k => {
                return {
                    ...k,
                    cell: Cell.fromBase64(k.cell)
                };
            });
        });
        const res  = await this.joinResults(tail);
        this.workers.forEach(w => w.postMessage({type: 'no_more_data'}));
        return res;
    }
}
