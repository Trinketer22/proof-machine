#!/usr/bin/env node
import arg from 'arg';
import { StorageSqlite } from '../lib/BranchStorage';
import { NodeProcessor } from '../lib/NodeProcessor';
import {open} from 'node:fs/promises';
import { Cell } from '@ton/core';
import cliProgress from 'cli-progress';
import { availableParallelism } from 'node:os';
import { Worker, isMainThread, parentPort} from 'node:worker_threads';

type TopProcessed = {
    type: 'top',
    worker: Worker
    value: Awaited<ReturnType<NodeProcessor['buildProofFromTop']>>;
}
type MoreData = {
    type: 'more_data',
    worker: Worker
}

type NoMoreData = 'no_more_data';
type ThreadRes = TopProcessed | MoreData;

function help() {
    console.log("--root-hash <tree root hash>");
    console.log("--output [path to csv file where branches will be saved]");
    console.log("--batch-size [tops(chunks of addresses) per worker");
    console.log(`--parallel [force count of threads] (normally picked automatically)`);
    console.log("--help, -h get this message\n\n");

    console.log(`${__filename} --root-hash <tree-hash> <database_path>`);
}
async function run () {
    const args = arg({
        '--root-hash': String,
        '--output': String,
        '--batch-size': Number,
        '--parallel': Number,
        '--help': Boolean,
        '-h': '--help'
    }, {stopAtPositional: true});

    if(args._.length == 0) {
        console.log("Database path is required\n\n");
        help();
        return;
    }
    if(!args['--root-hash']) {
        console.log("Root hash is required argument");
        help();
        return;
    }
    const storage   = new StorageSqlite(args._[0]);

    const processor = new NodeProcessor({
        type: 'main',
        airdrop_start: 1000,
        airdrop_end: 2000,
        max_parallel: 1,
        root_hash: Buffer.from(args['--root-hash'], 'hex'),
        storage,
        store_depth: 16,
    });
    
    if(isMainThread) {
        let topOffset = 0;
        let keepGoing = true;
        const outPath = args['--output'] ?? 'proofs.csv';
        const batchSize = args['--batch-size'] ?? 128;
        const parallel  = args['--parallel'] ?? availableParallelism();
        const total = await storage.getTotalRecords();
        if(total == 0) {
            console.log("No records present!");
            return;
        }

        let topBatch  = await storage.getTopBatch(topOffset, batchSize);
        if(topBatch.length == 0) {
            throw new Error("No top found. buildTree first?");
        }

        console.log(`Processing ${total} proofs...`);
        console.log(`Output to ${outPath}`);
        console.log(`Thread count: ${parallel}`);
        console.log(`Batch size ${batchSize}`);

        const fh = await open(outPath, 'w');

        let   processed = 0;
        const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)

        bar.start(total, 0);

        const workers: Worker[] = Array(parallel); 

        const perWorker = Math.floor(topBatch.length / parallel);

        const saveProofs = async (tops: TopProcessed) => {
            processed += tops.value.length;
            await fh.appendFile(tops.value.map(p => p[0].toString(16) + "," + p[1]).join("\n") + "\n",{encoding: 'utf8'});
            bar.update(processed);
        }

        let dataQueue: Worker[] = [];
        for(let i = 0; i < parallel; i++) {
            const w = new Worker(__filename, {argv: process.argv.slice(2)});
            const processMsg = async (msg: ThreadRes) => {
                // console.log(msg.type);
                msg.worker = w
                if(msg.type == 'top') {
                    await saveProofs(msg);
                }
            };
            const moreData = (msg: ThreadRes) => {
                if(msg.type == 'more_data') {
                    if(keepGoing) {
                        if(topBatch.length == 0) {
                            // console.log("Queueing...");
                            dataQueue.push(w);
                            // console.log(`Loading more: ${topOffset}:${batchSize}`);
                            // topBatch = await storage.getTopBatch(topOffset, batchSize);
                            // keepGoing = topBatch.length > 0;
                        }
                        else {
                            // console.log("Sending what's left");
                            w.postMessage(topBatch.splice(0, Math.min(perWorker, topBatch.length)));
                        }
                        /*
                        if(keepGoing) {
                            topOffset += batchSize;
                            w.postMessage(topBatch.splice(0, Math.min(perWorker, topBatch.length)));
                        }
                        */
                    }
                    if(!keepGoing) {
                        w.removeListener('message', moreData);
                        w.postMessage('no_more_data');
                    }
                }
            };
            w.on('message', processMsg);
            w.on('message', moreData);
            workers[i] = w;
        }


        const splitForWorkers = (batch: unknown[]) => {
            for(let i = 0; i < parallel; i++) {
                const workerChunk : unknown[] = batch.splice(0, perWorker);
                /*
                for(let k = 0; k  < perWorker && k + distAmount < batch.length; k++) {
                    workerChunk.push(batch[distAmount + k]);
                    distAmount++;
                }
                */
                if(workerChunk.length > 0) {
                    workers[i].postMessage(workerChunk);
                }
            }
        }

        splitForWorkers(topBatch);
        topOffset += batchSize;

        while(processed < total) {

            // We got to chew on something to let callbacks to trigger
            await Promise.race(workers.map(w => new Promise((resolve:(value: boolean) => void, reject) => {
                w.once('error',(e) => {
                    reject(e);
                });
                w.once('exit', e => {
                    resolve(true);
                    // workers.forEach(w => w.terminate());
                    // workers = [];
                });
                if(w.listenerCount('message') < 3) {
                    w.once('message', (msg) => {
                            resolve(true);
                    })
                }
            }))).then(v => workers.forEach(w => {
                w.removeAllListeners('error');
                w.removeAllListeners('exit');
            }));
            if(dataQueue.length > 0) {
                if(keepGoing) {
                    topBatch = await storage.getTopBatch(topOffset, batchSize);
                    keepGoing  = topBatch.length > 0;
                    if(keepGoing) {
                        topOffset += batchSize;
                        for(let i = 0; i < dataQueue.length; i++) {
                            if(topBatch.length > 0) {
                                dataQueue[i].postMessage(topBatch.splice(0, perWorker));
                            }
                        }
                    }
                    dataQueue = [];
                }
            }
        };

        bar.update(processed);
        if(workers.length > 0) {
            await Promise.all(workers.map(w => w.terminate()));
        }

        bar.stop();

        await fh.close();
    }
    else {
        if(parentPort) {
            let keepGoing = true;
            while(keepGoing) {
                await new Promise((resolve, reject) => {
                    parentPort!.once('message', async (topBatch: NoMoreData | Awaited<ReturnType<typeof storage['getTopBatch']>>) => {
                        if(topBatch == 'no_more_data') {
                            parentPort!.removeAllListeners('message');
                            keepGoing = false;
                            resolve(true);
                            return;
                        }
                        for(const top of topBatch) {
                            const proofs = await processor.buildProofFromTop(top.prefix, top.length, Cell.fromBase64(top.boc), top.path.split(',').map(p => Number(p)));
                            parentPort!.postMessage({
                                type: 'top',
                                value: proofs
                            });
                        }
                        resolve(true);
                        // parentPort!.postMessage({type: 'more_data'});
                    });
                }).then(v => parentPort!.postMessage({type: 'more_data'}));
            }
        } else {
            throw new Error ("No parent port");
        }
    }
}
run();
