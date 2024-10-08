#!/usr/bin/env node
import { Cell, DictionaryValue } from '@ton/core';
import { PfxCluster, StorageSqlite } from '../lib/BranchStorage';
import { NodeProcessor } from '../lib/NodeProcessor';
import { forceFork } from '../lib/Dictionary';
import { writeFile } from 'node:fs/promises';
import arg from 'arg';
import { availableParallelism } from 'node:os';
import { isMainThread, parentPort, workerData } from 'node:worker_threads';
import { getN } from '../lib/util';
import { NodeScheduler, WorkerMessage } from '../lib/NodeScheduler';
import { Worker } from 'node:worker_threads';
import { Session } from '../lib/Session';
import cliProgress from 'cli-progress';
import inquirer from 'inquirer';

let storage: StorageSqlite;
let scheduler: NodeScheduler;
let session : Session;
let total: number;

function sortPfxs(pfxs: PfxCluster[], maxLen: number) {
    const matching: PfxCluster[] = [];
    const postpone: PfxCluster[] = [];

    for(const pfx of pfxs) {
        if(pfx.keys.length <= maxLen) {
            matching.push(pfx);
        }
        else {
            postpone.push(pfx);
        }
    }

    return {
        matching,
        postpone
    }
}

type AirdropData = {
    amount: bigint,
    start_from: number,
    expire_at: number
};

const airDropValue: DictionaryValue<AirdropData> = {
    serialize: (src, builder) => {
        builder.storeCoins(src.amount);
        builder.storeUint(src.start_from, 48);
        builder.storeUint(src.expire_at, 48);
    },
    parse: (src) => {
        return {
            amount: src.loadCoins(),
            start_from: src.loadUint(48),
            expire_at: src.loadUint(48)
        }
    }
}

async function buildTree(pfxLen: number, limit: number, offset: number = 0) {

    const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)

    let root: Cell;
    console.log("Creating index...");
    await storage.createKeyIndex(pfxLen);
    console.log("Done!");
    
    let keepGoing = true;

    let pfxPromise = storage.groupPrefixes(pfxLen, offset, limit);
    bar.start(total, session.processed);

    do {
        const pfxs = await pfxPromise;
        // console.log("Pfxs:", pfxs);
        if(pfxs.length > 0) {

            offset += limit;
            pfxPromise = storage.groupPrefixes(pfxLen, offset, limit);

            let addrCount = 0;
            for(let i = 0; i < pfxs.length; i++) {
                const pfx = pfxs[i];
                const rootPos = await storage.findBranchRoot(pfx.len, pfx.keys[0]);
                if(rootPos != pfx.len) {
                    const delta  = pfx.len - rootPos;
                    const bitPos = 32 - rootPos - 1;
                    pfx.len      = rootPos;
                    if(typeof pfx.pfx == 'bigint') {
                        throw new Error("TODO");
                    }
                    let bitsToKey = getN(pfx.pfx, delta, false, pfx.len);
                    pfx.pfx >>= delta;
                    if(bitsToKey != 0) {
                        bitsToKey *= 2 ** bitPos
                        pfx.keys = pfx.keys.map(k => bitsToKey + k);
                    }
                    pfxs[i] = pfx;
                }
                addrCount += pfxs[i].keys.length;
            }

            await scheduler.add(pfxs);

            session.nextChunk(limit);
            session.processed += addrCount;
            bar.update(session.processed);
            await session.save();

            //const sorted = sortPfxs(pfxs, per_chain);
        }
        else {
            keepGoing = false;
        }
    } while(keepGoing);

    const nextLevel = await scheduler.finalize();
    bar.stop();

    if(nextLevel.length == 2) {
        let leftIdx: number;
        let rightIdx: number;
        if(BigInt(nextLevel[0].pfx) < BigInt(nextLevel[1].pfx)) {
            leftIdx  = 0;
            rightIdx = 1
        }
        else {
            leftIdx  = 1;
            rightIdx = 0;
        }
        root = forceFork(0b100 << 8, 11, 267, nextLevel[leftIdx].cell, nextLevel[rightIdx].cell);
    }
    else if(nextLevel.length == 1) {
        const tempRoot = nextLevel[0].cell.beginParse();
        root = forceFork(0b100 << 8, 11, 267, tempRoot.loadRef(), tempRoot.loadRef());
    }
    else {
        console.log(nextLevel.length);
        console.log(nextLevel);
        console.log(nextLevel.map(l => l.pfx.toString(2).padStart(l.len, '0')));
        // nextLevel.map(p => console.log(Number(p.pfx).toString(2).padStart(p.len, '0')));
        throw new Error("TODO?");
    }

    return root;
}

function help() {
    console.log("--per-worker [start bit prefix] default:(10)");
    console.log(`--parallel [force number of threads]`);
    console.log("--session [path where to save session files]");
    console.log("--temp-cache If specified, all temporary structures of DB go to memory. (For slow storage systems)");
    console.log("--resume [path to resume session from]");
    console.log("--batch-size' [size of the query results batch. Should be power of 2] default:(32 * parallel)");
    console.log("--cache-bits' [Up to that <= prefix length, each branch hash/depth is stored in db] default:(16)");
    console.log("--help, -h get this message\n\n");

    console.log(`${__filename} --start <airdrop start> --end <airdrop end> <database_path>`);
}
async function run() {
    let args = arg({
        '--start': String,
        '--end': String,
        '--resume': String,
        '--session': String,
        '--per-worker': Number,
        '--temp-cache': Boolean,
        '--parallel': Number,
        '--batch-size': Number,
        '--cache-bits': Number,
        '--help': Boolean,
        '-h': '--help'
    },{stopAtPositional: true});

    if(args._.length == 0) {
        console.log("Database argument required");
        help();
        return;
    }

    if(args['--help'] || args['-h']) {
        help();
        return;
    }

    if(isMainThread) {

        const tempCache = args['--temp-cache'] ?? false;

        if(args['--resume']) {
            session = await Session.fromFile(args['--resume']);
            args = session.args as any;
        } else {
            const defaultPath = args['--session'] ?? 'machine.session';
            try {
                session = await Session.fromFile(defaultPath);
                console.log(`Session with arguments:${JSON.stringify(session.args, null, 2)} found!`);
                const res = await inquirer.prompt([{
                    type: 'expand',
                    message: `Unfinished session found at ${defaultPath}`,
                    name: 'overwrite',
                    choices: [
                        {
                            key: 'o',
                            name: 'Overwrite',
                            value: 'overwrite',
                        },
                        {
                            key: 'r',
                            name: 'Resume from session state',
                            value: 'resume',
                        },
                        new inquirer.Separator(),
                        {
                            key: 'a',
                            name: 'Abort',
                            value: 'abort'
                        }
                    ]
                }]);
                if(res.overwrite == 'overwrite') {
                    await session.finish();
                    console.log("Starting new session");
                    session = new Session({
                        args: args,
                        offset: 0,
                        path: defaultPath,
                        data: `${defaultPath}.part`
                    });

                } else if(res.overwrite == 'abort') {
                    console.log("Aborting execution...");
                    return;
                }
                else {
                    console.log("Continuing");
                    args = session.args as any;
                }
            }
            catch(e) {
                console.log("Starting new session");
                session = new Session({
                    args: args,
                    offset: 0,
                    path: defaultPath,
                    data: `${defaultPath}.part`
                });
                await session.save();
            }
        }
        if(!args['--start']) {
            console.log("--start is an required argument");
            help();
            return;
        }
        if(!args['--end']) {
            console.log("--end is an required argument");
            help();
            return;
        }

        let airdropStart = Date.parse(args['--start']);
        let airdropEnd   = Date.parse(args['--end']);

        const perWorker = args['--per-worker'] ?? 10;
        let   storeBits = args['--cache-bits'] ?? 16;
        const parallel  = args['--parallel'] ?? availableParallelism();
        const batchSize = args['--batch-size'] ?? parallel * 32;


        console.log("Airdrop start:", new Date(airdropStart).toString());
        console.log("Unix:", Math.floor(airdropStart / 1000));
        console.log("Airdrop end:", new Date(airdropEnd).toString());
        console.log("Unix:", Math.floor(airdropEnd / 1000));

        const workers: Worker[] = Array(parallel);
        const batchLog = Math.log2(batchSize);
        if(!Number.isInteger(batchLog)) {
            throw new Error("Batch size should be power of two!")
        }

        storage   = new StorageSqlite(args._[0], { temp_in_memory: tempCache });
        total     = await storage.getTotalRecords();
        const effectiveBits = Math.ceil(Math.log2(total))
        const pfxLen        = Math.max(effectiveBits - perWorker, 1);
        console.log("Effective bits:", effectiveBits);
        console.log("Pfx len:", pfxLen);

        if(storeBits > effectiveBits) {
            storeBits = pfxLen + 1;
            console.log("Lowering cache bits:", storeBits);
        }


        for(let i = 0; i < parallel; i++) {
            workers[i] = new Worker(__filename, {argv: process.argv.slice(2), workerData: {
                storeBits,
                airdropStart,
                airdropEnd
            }});
        }
        scheduler = new NodeScheduler({
            airdrop_start: Math.floor(airdropStart / 1000),
            airdrop_end:   Math.floor(airdropEnd   / 1000),
            session: session,
            max_parallel: parallel,
            workers,
            storage,
            store_depth: storeBits,
        });

        /*
        const testRec = await storage.getRecPrefixed(20, 862452);
        scheduler.add([testRec]);
        */
        // Average effective bits used to identify record
        
        /*
        const testData = await storage.getRecPrefixed(18, 261923);
        await scheduler.processPfx(testData);
        */

        let rootHash: Buffer;
        const root = await buildTree(pfxLen, batchSize, session.offset);
        rootHash   = root.hash(0);
        console.log("Root hash:", rootHash.toString('hex'));
        console.log("Saving to root_hash");
        await writeFile('root_hash', rootHash.toString('hex') + "\n", {encoding: 'utf8'});
        await session.finish();
    }
    else {
        if(parentPort == null) {
            throw new Error("Parent port is null");
        }
        const processor = new NodeProcessor({
            airdrop_start: Math.floor(workerData.airdropStart / 1000),
            airdrop_end: Math.floor(workerData.airdropEnd / 1000),
            max_parallel: 1,
            parentPort,
            store_depth: workerData.storeBits
        });

        let keepGoing = true;
        if(parentPort) {
            while(keepGoing) {
                await new Promise((resolve, reject) => {
                    parentPort!.once('message', async (msg: WorkerMessage) => {
                        if(msg.type == 'pfx') {
                            msg.pfx.forEach(pfx => {
                                pfx.data.forEach(d => {
                                    d.suffix = Buffer.from(d.suffix);
                                });
                            });
                            try { 
                                const res = await processor.add(msg.pfx);
                                parentPort!.postMessage({
                                    type: 'processed',
                                    pfx: res.map(r => {
                                        return {
                                            ...r,
                                            cell: r.cell.toBoc().toString('base64')
                                        }
                                    })
                                });
                                resolve(res);
                            } catch(e) {
                                parentPort?.postMessage({
                                    type: 'error',
                                    error: e
                                });
                                console.log("Error:", e);
                                keepGoing = false;
                                reject(e);
                            }
                        }
                        else {
                            keepGoing = false;
                        }
                    });
                });
            }
        }
        else {
            throw new Error("No parent port");
        }
    }

    /*
     * USE batchProof utility instead
    let gotCount = 0;
    for await (let testRow of storage.getAll()) {
        const randomAddress = Address.parseRaw(testRow.address);
        let proof = await scheduler.buildProof(randomAddress);
        if(!proof.refs[0].hash(0).equals(rootHash)){ 
            console.log("Address:", randomAddress.toRawString());
            console.log("Got right:", gotCount);
            console.log("Dang, doesn't match");
            // const fullDict = Dictionary.loadDirect(Dictionary.Keys.Address(), airDropValue, root);
            // const testProof = fullDict.generateMerkleProof(randomAddress);
            // console.log("Root cell:", root);
            console.log("My proof:", proof);
            return;
        }
        gotCount++;
    }
    console.log("All good, bruh!");
    */

    // ts  = Date.now();
    // proof = await scheduler.buildProof(randomAddress);
    // console.log("Second proof took:", (Date.now() - ts) / 1000);

    /*
    console.log("Testing all proofs");
    let total = 0;
    let count = 0;
    for await (let tst of storage.getAll()) {
        const tstAddr = Address.parseRaw(tst.address);
        const ts = Date.now();
        const proof = await scheduler.buildProof(tstAddr);
        total += Date.now() - ts;
        if(!proof.refs[0].hash(0).equals(rootHash)) {
            throw new Error("Proof doesn't match:" + tstAddr.toRawString());
        }
        count++;
    }

    console.log("Total tested:", total);
    const avg = total / count;
    console.log("AVG proof takes:", avg);
    */

    /*
    const randomAddress = Address.parseRaw('0:2291740d9e8ef33944452651e9a7539829f047fc9692bebb026aebd7949f6f30'); //await getRandomRow();
    console.log("Test addr:", randomAddress.toRawString());
    const res = await findUniquePrefix(randomAddress, effectiveBits);

    console.log(`Fork: ${res.fork.toString(16)}`);
    console.log(`Data: ${res.data.toString(16)}`);
    */
}

run();
