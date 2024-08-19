#!/usr/bin/env node
import { Cell, DictionaryValue } from '@ton/core';
import { PfxCluster, StorageSqlite } from '../lib/BranchStorage';
import { NodeProcessor } from '../lib/NodeProcessor';
import { forceFork } from '../lib/Dictionary';
import {writeFile} from 'node:fs/promises';
import arg from 'arg';

let storage: StorageSqlite;
let processor: NodeProcessor;

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
async function buildTreeLevel(pfxLen: number) {
    let offset          = 0;
    const limit           = 128;
    let keepGoing       = true;

    do {
        const pfxs = await storage.groupPrefixes(pfxLen, offset, limit);
        // console.log("Pfxs:", pfxs);

        if(pfxs.length > 0) {
            offset += limit;
            //const sorted = sortPfxs(pfxs, per_chain);
            console.log("Processing batch...");
            await processor.add(pfxs);
        }
        else {
            console.log("Done!");
            keepGoing = false;
        }
    } while(keepGoing);

    console.log("Final merge...");
    return await processor.finalize();
}

async function buildTree(per_chain: number) {
    const effectiveBits = await storage.getEffectiveBits();
    const   pfxLen        = Math.max(effectiveBits - Math.floor(Math.log2(per_chain)), 0);
    let root: Cell;
    console.log("Effective bits:", effectiveBits);
    console.log("Pfx len:", pfxLen);
    
    const nextLevel = await buildTreeLevel(pfxLen);
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
        /*
        console.log("Trying to load root...");
        const fullDict = Dictionary.loadDirect(Dictionary.Keys.Address(), airDropValue, root);
        console.log(fullDict);
        for(let key of fullDict.keys()) {
            console.log(`${key.workChain}:${key.hash.toString('hex')}`);
        }
        */

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
    console.log("--per-worker [Apprximate amount of forks processed at a time] default:(1000)");
    // console.log(`--parallel [force count of threads] (TODO)`);
    console.log("--cache-bits' [Up to that <= prefix length, each branch hash/depth is stored in db] default:(16)");
    console.log("--help, -h get this message\n\n");

    console.log(`${__filename} <database_path>`);
}
async function run() {
    const args = arg({
        '--per-worker': Number,
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

    const perWorker = args['--per-worker'] ?? 1000;
    const storeBits = args['--cache-bits'] ?? 16;

    storage   = new StorageSqlite(args._[0]);
    processor = new NodeProcessor({
        type: 'main',
        airdrop_start: 1000,
        airdrop_end: 2000,
        max_parallel: 1, // Multi threaded version is not today
        storage,
        store_depth: storeBits,
    });

    /*
    const testRec = await storage.getRecPrefixed(20, 862452);
    processor.add([testRec]);
    */
    // Average effective bits used to identify record
    
    /*
    const testData = await storage.getRecPrefixed(18, 261923);
    await processor.processPfx(testData);
    */

    let rootHash: Buffer;
    const root = await buildTree(perWorker);
    rootHash   = root.hash(0);
    console.log("Root hash:", rootHash.toString('hex'));
    console.log("Saving to root_hash");
    await writeFile('root_hash', rootHash.toString('hex') + "\n", {encoding: 'utf8'});

    /*
     * USE batchProof utility instead
    let gotCount = 0;
    for await (let testRow of storage.getAll()) {
        const randomAddress = Address.parseRaw(testRow.address);
        let proof = await processor.buildProof(randomAddress);
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
    // proof = await processor.buildProof(randomAddress);
    // console.log("Second proof took:", (Date.now() - ts) / 1000);

    /*
    console.log("Testing all proofs");
    let total = 0;
    let count = 0;
    for await (let tst of storage.getAll()) {
        const tstAddr = Address.parseRaw(tst.address);
        const ts = Date.now();
        const proof = await processor.buildProof(tstAddr);
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