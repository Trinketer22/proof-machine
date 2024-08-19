import events from 'node:events';
import { MessagePort, parentPort } from 'node:worker_threads';
import { PfxCluster, PfxData, StorageSqlite } from "./BranchStorage";
import { bigIntFromBuffer, clearFirstBits, clearN, convertToMerkleProof, convertToPrunedBranch, extractBits, findNextFork, findNextForkLarge, getN, joinSuffixes } from './util';
import { Address, beginCell, Cell } from '@ton/core';
import { forceFork, generateMerkleProof, storeLabel } from './Dictionary';

export type EdgeNormal = {
    type: 'normal',
    edgeLen: number,
    bits: number, 
}
export type EdgeJumbo = {
    type: 'jumbo',
    edgeLen: number,
    bits: bigint
}

type Edge = EdgeNormal | EdgeJumbo; 

export type PfxProcessed = {
    pfx: number | bigint,
    len: number,
    cell: Cell
}

export type ProcessorConfigGeneral = {
    airdrop_start: number,
    airdrop_end: number,
    max_parallel: number,
    root_hash?: Buffer,
    store_depth: number
}

export type ProcessorConfigMain = ProcessorConfigGeneral & {
    type: 'main',
    storage: StorageSqlite
};
export type ProcessorConfigWorker = ProcessorConfigGeneral & {
    type: 'worker',
    parentPort: MessagePort;
}

export type ProcessorConfig = ProcessorConfigMain | ProcessorConfigWorker;

type ProcessPfxOptions = {
    check_root: boolean,
    pruned: boolean,
    save_path: boolean,
    check_cache: boolean
}

export class NodeProcessor extends events.EventEmitter {
    protected storage: StorageSqlite | undefined;
    protected parentPort: MessagePort | undefined;
    protected start_from: number;
    protected expire_at: number;
    protected store_depth: number;
    protected maxLen = 256;
    protected queue: PfxCluster[];
    protected max_parallel: number;
    protected runningBatch: Promise<PfxProcessed>[];
    protected running: Promise<PfxProcessed[]> | undefined;
    protected root_hash: Buffer | undefined;

    protected results: PfxProcessed[];
    protected branchMap: Map<string, {depth: number, hash: Buffer}> | undefined;

    constructor(config: ProcessorConfig) {
        super();
        this.start_from   = config.airdrop_start;
        this.expire_at    = config.airdrop_end;
        this.max_parallel = config.max_parallel;
        this.store_depth  = config.store_depth;
        if(config.type == 'main') { 
            this.storage = config.storage;
        }
        else {
            this.parentPort = config.parentPort;
        }
        if(config.root_hash) {
            this.root_hash = config.root_hash;
        }
        this.queue   = [];
        this.runningBatch = [];
        this.results      = [];
    }

    add(pfxs: PfxCluster[]) {
        this.queue.push(...pfxs);
        if(this.runningBatch.length == 0 || this.running == undefined) {
            this.running = this.run();
        }
        return this.running;
    }

    protected async run() {
        // MEH Deal with the propper runner when everying is smooth
        let results: PfxProcessed[] = [];
        let batchCount = 0;
        while(this.queue.length > 0 || this.runningBatch.length > 0) {
            const toProcess = this.queue.length;
            for(let i = 0; i < this.max_parallel && i < toProcess; i++) {
                this.runningBatch.push(this.processPfx(this.queue.shift()!))
            }
            const batchRes = await Promise.all(this.runningBatch);
            results.push(...batchRes);
            this.runningBatch = [];
            batchCount++;
        }
        /*
        Promise.race(this.runningBatch).then(res => {
            this.emit('one_done', res);
            console.log(res);
            if(this.queue.length > 0) {
                this.runningBatch[res.idx] = this.processPfx(this.queue.shift()!, res.idx);
                this.run();
            }
        });
        */
        if(results.length > 2) {
            results = await this.joinResults(results);
        }

        this.results.push(...results);

        return results;
    }

    async finalize() {
        if(this.results.length > 2) {
            this.results = await this.joinResults(this.results);
        }
        return this.results;
    }

    protected async joinResults(results: PfxProcessed[], depth: number = 0, paths: number[] = []): Promise<PfxProcessed[]> {
        const nextLevel: PfxProcessed[] = [];
        let resLevel: PfxProcessed[];
        const prevSet = [...results];
        let resultLeft = results.length;
        let matchCount = 0;
        for(let i = 0; i < resultLeft; i++) {
            const pfxA = prevSet[i].pfx;
            if(prevSet[i].len == 1) {
                continue;
            }
            if(typeof pfxA == 'bigint') {
                throw new Error("TODO");
            }
            for(let k = 0; k < resultLeft; k++) {
                if(i != k) {
                    const pfxB = prevSet[k].pfx;
                    if(typeof pfxB == 'bigint') {
                        throw new Error("TODO L2");
                    }
                    if((pfxA ^ pfxB) == 1) {
                        matchCount++;
                        const fork: Cell[] = new Array(2);
                        const isRight = pfxA % 2;

                        fork[isRight]     = prevSet[i].cell;
                        fork[isRight ^ 1] = prevSet[k].cell;
                        const forkCell    = forceFork(0, 0, prevSet[i].len - 1,fork[0], fork[1]);
                        const nextPfx     = pfxA >> 1;
                        nextLevel.push({
                            pfx: nextPfx,
                            len: prevSet[i].len - 1,
                            cell: convertToPrunedBranch(forkCell.hash(0), forkCell.depth(0))
                        });

                        if(prevSet[i].len - 1 < this.store_depth) {
                            if(this.storage) {
                                await this.storage.saveBranch(nextPfx, prevSet[i].len - 1, forkCell.depth(0), forkCell.hash(0));
                            }
                            else if(this.parentPort){
                                this.parentPort.postMessage({
                                    type: 'branch',
                                    pfx: nextPfx,
                                    length: prevSet[i].len - 1,
                                    depth: forkCell.depth(0),
                                    hash: forkCell.hash(0).toString('hex')
                                });
                            }
                        } 
                        prevSet.splice(k, 1);
                        prevSet.splice(i--, 1);
                        resultLeft -= 2;
                        break;
                    }
                }
            }
        }
        if(resultLeft > 0) {
            resLevel = results;
        }
        else {
            paths.unshift(nextLevel[0].len);
            if(matchCount > 2 && matchCount % 2 == 0) {
                resLevel = await this.joinResults(nextLevel, depth + 1, paths);
            }
            else {
                resLevel = nextLevel;
            }
            if(depth == 0) {
                console.log("Merging...");
                if(this.storage) {
                    await this.storage.saveBranchCache();
                    await this.storage.clearPathCache();
                    await this.storage.updatePath(resLevel.map(r => Number(r.pfx)), resLevel[0].len, paths);
                }
                else if(this.parentPort) {
                    this.parentPort.postMessage({
                        type: 'update_path',
                        pfxs: resLevel.map(r => r.pfx),
                        pfxLen: resLevel[0].len,
                        paths
                    });
                }
                else {
                    throw new Error("Invalid instance");
                }
            }
        }
        return resLevel;
    }

    protected packData(amount: bigint) {
        return beginCell().storeCoins(amount)
                          .storeUint(this.start_from, 48)
                          .storeUint(this.expire_at, 48)
    }
    protected async packFork(edge: Edge, pfx: PfxCluster, opts: Partial<ProcessPfxOptions>) {
        let nextPfx: number | bigint;
        let resCell: Cell;
        const nextLen = pfx.len + edge.edgeLen + 1;

        const above32 = nextLen % 32;
        const next32  = above32 == 0 ? 32 : above32;

        const keyRemain = this.maxLen - pfx.len;
        const isFork    = keyRemain - edge.edgeLen > 0;

        if(edge.type == 'jumbo' || typeof pfx.pfx == 'bigint') {
            nextPfx = (BigInt(pfx.pfx) << BigInt(edge.edgeLen)) | BigInt(edge.bits);
            if(isFork) {
                nextPfx *= 2n; // Make space for L/R bit
            }
        }
        else {
            nextPfx = pfx.pfx * (2 ** edge.edgeLen) + edge.bits;
            if(isFork) {
                nextPfx *= 2;
            }
        }

        const node = beginCell().store(storeLabel(edge.bits, edge.edgeLen, keyRemain));
        if(isFork) {
            let left: PfxCluster;
            let right: PfxCluster;

            const leftKeys: number[]   = [];
            const leftData: PfxData[]  = [];

            const rightKeys: number[]  = [];
            const rightData: PfxData[] = [];

            for(let i = 0; i < pfx.keys.length; i++) {
                const nextSfx = clearN(pfx.keys[i], next32, true);
                pfx.data[i].path.push(nextLen)
                // Right 
                if(getN(pfx.keys[i], next32, true) & 1) {
                    rightKeys.push(nextSfx);
                    rightData.push(pfx.data[i]);
                }
                else {
                    leftKeys.push(nextSfx);
                    leftData.push(pfx.data[i]);
                }
            }

            // console.log("Old pfx:", pfx);
            // console.log("Edge:", edge);
            left = {
                len: nextLen,
                keys: leftKeys,
                pfx: nextPfx,
                data: leftData
            }
            right = {
                len: nextLen,
                keys: rightKeys,
                data: rightData,
                pfx: ++nextPfx // 0b1 at the end
            }

            const branches = await Promise.all([this.processPfx(left, opts), this.processPfx(right, opts)]);
            node.storeRef(branches[0].cell).storeRef(branches[1].cell)
        }
        else {
            const fullPfx = '0:' + nextPfx.toString(16).padStart(64, '0');
            /*
            console.log("Checking:", fullPfx);
            if(!await this.storage.checkExist(fullPfx)) {
                throw new Error("Resulting prefix is wrong!");
            }
            */
            if(pfx.keys.length != 1 || pfx.data.length != 1) {
                throw new Error("Something went wrong!");
            }
            if(opts.save_path) {
                if(this.storage) {
                    await this.storage.savePath(fullPfx, pfx.data[0].path);
                }
                else if(this.parentPort) {
                    this.parentPort.postMessage({
                        type: 'save_path',
                        pfx: fullPfx,
                        path: pfx.data[0].path
                    })
                }
                else {
                    throw new Error("Invalid instance");
                }
            }
            node.storeSlice(this.packData(pfx.data[0].value).asSlice());
        }

        resCell = node.endCell();

        // console.log("Left:", left);
        // console.log("Right:", right);

        // Prune all data leafs even if opt.pruned = false
        return opts.pruned ? convertToPrunedBranch(resCell.hash(0), resCell.depth(0)) : resCell;
    }
    protected async processPfxCached(pfx: number | bigint, pfxLen: number, opts?:Partial<ProcessPfxOptions>) {
        let res: Cell;
        if(!opts) {
            opts = {
                pruned: true,
                check_cache: true,
                check_root: false,
                save_path: false
            }
        }
        const check_cache = opts.check_cache ?? true;
        if(this.branchMap === undefined) {
            if(!this.storage) {
                throw new Error("Storage is required for functioning");
            }

            this.branchMap = await this.storage.getBranchesMap();
        }
        const cached = check_cache ? this.branchMap.get(`${pfxLen.toString(16)}:${pfx.toString(16)}`) : false;
        if(cached) {
            // console.log("Cache hit!");
            res = convertToPrunedBranch(cached.hash, cached.depth);
        }
        else {
            if(!this.storage) {
                throw new Error("Storage is required for functioning");
            }

            console.log("Cache missed!");
            const pfxData = await this.storage.getRecPrefixed(pfxLen, pfx);
            if(!pfxData) {
                throw new Error("No data found!");
            }
            res = (await this.processPfx(pfxData, opts)).cell;
            // console.log("Hash:",res.hash(0));
        }
        return res;
    }
    protected async buildProofInternal(shortPfx: number, fullPfx: Buffer, prevFork: number, amount: bigint, path: string[]) {
        const fork: Cell[] = new Array(2);
        let   curPfx: number | bigint;
        let   siblingPfx: number | bigint;
        let   leaf: number | bigint;
        let   isRight: number;

        const   nextFork = Number(path.shift());
        const leafLen  = nextFork - prevFork - 1;
        const keyLeft  = this.maxLen - nextFork;
        if(!this.storage) {
            throw new Error("Storage is required for proof build");
        }

        if(nextFork <= 32) {
            curPfx  = getN(shortPfx, nextFork, true);
            leaf    = extractBits(shortPfx, prevFork, leafLen);
            isRight = curPfx % 2;
            siblingPfx = curPfx ^ 1;
        }
        else {
            leaf = extractBits(fullPfx, nextFork, leafLen);
            curPfx = bigIntFromBuffer(fullPfx, prevFork);
            siblingPfx = curPfx ^ 1n;
            isRight = Number(curPfx % 2n);
        }
        // console.log("Sibling pfx:", siblingPfx);
        // console.log("Leaf:", leaf);
        if(path.length > 0) {
            if(Number(path[0]) < this.store_depth) {
                fork[isRight]     = (await this.buildProofInternal(shortPfx, fullPfx, nextFork, amount, path)).cell;
                fork[isRight ^ 1] = await this.processPfxCached(siblingPfx, nextFork);
                // console.log("Next:", nextFork);
                // console.log("Cur pfx:", curPfx);
                // console.log("Is right:", isRight);
                // console.log(`Pfx len:${nextFork}:${fork[isRight ^ 1]}`);
            }
            else {
                // console.log("Next fork:", nextFork);
                // console.log("Cur pfx:", curPfx);
                // console.log("isRight:", isRight);
                fork[isRight]= Cell.fromBase64(await this.storage.getTop(curPfx, nextFork));
                const pruned = await this.processPfxCached(siblingPfx, nextFork);
                // console.log("Pruned picked:", pruned);
                const proofKey = BigInt('0x' + fullPfx.toString('hex'));
                // console.log("Proof key:", proofKey.toString(16));
                const myProof = generateMerkleProof(fork[isRight], curPfx, nextFork, [proofKey], keyLeft);

                // console.log("My proof:", myProof.hash(0));
                // console.log("My top:", fork[isRight].hash(0));
                fork[isRight]     = myProof;
                fork[isRight ^ 1] = convertToPrunedBranch(pruned.hash(0), pruned.depth(0));
            }
        }
        else {
            const finalLeaf = extractBits(fullPfx, nextFork, this.maxLen - nextFork);
            fork[isRight] = beginCell().store(
                storeLabel(finalLeaf, this.maxLen - nextFork, this.maxLen - nextFork)
            ).storeSlice(this.packData(amount).asSlice()).endCell();
            fork[isRight ^ 1] = await this.processPfxCached(siblingPfx, nextFork);
        }
        // console.log("Left:",  fork[0].hash(0));
        // console.log("Right:", fork[1].hash(0));

        return {
            cell: forceFork(leaf, leafLen, keyLeft, fork[0], fork[1]),
            l: fork[0],
            r: fork[1],
            pfx: curPfx
        }
    }
    async buildProof(address: Address) {
        if(!this.storage) {
            throw new Error("Storage is required for proof build");
        }

        const fullPfx = address.hash;
        const shortPfx  = fullPfx.readUintBE(0, 4);
        const proofPath = await this.storage.getPath(address.toRawString())
        if(!proofPath) {
            throw new Error("Path is not found for:" + fullPfx.toString());
        }
        const forkIdxs  = proofPath.path.split(',');
        if(forkIdxs.length == 0) {
            throw new Error("Path is not empty for:" + fullPfx.toString());
        }

        const proof = await this.buildProofInternal(shortPfx, fullPfx, 0, proofPath.amount, forkIdxs);

        return convertToMerkleProof(forceFork(0b100 << 8, 11, 267, proof.l, proof.r));
    }
    
    async buildProofFromTop(pfx: number, pfxLength: number, top: Cell) {

        if(!this.root_hash) {
            throw new Error("Root hash is required for buildProofFromTop operation");
        }
        if(!this.storage) {
            throw new Error("Storage is required for proof building");
        }

        const proofs: [bigint, string][] = [];
        let siblingPfx: number | bigint;
        let isRightInit: number;

        if(typeof pfx == 'number') {
            siblingPfx = pfx ^ 1;
            isRightInit    = pfx % 2;
        }
        else {
            siblingPfx  = pfx ^ 1n;
            isRightInit = Number(pfx % 2n);
        }
        // console.log("Is right:", isRightInit);
        // console.log("Sibling pfx:", siblingPfx);

        const pruned    = this.processPfxCached(siblingPfx, pfxLength);
        const addresses = await this.storage.getPathForPrefix(pfx, pfxLength);//readAllPrefixes(top, pfx, pfxLength, this.maxLen - pfxLength);

        const   fork: Cell[] = new Array(2);
        let   curPfx: number | bigint;
        let   leaf: number | bigint;

        for(const address of addresses) {
            // console.log("NEXT");
            const proofKey = BigInt('0x' + address.address);
            // console.log("Key:",proofKey.toString(16));
            const proof = generateMerkleProof(top, pfx, pfxLength, [proofKey], this.maxLen - pfxLength);
            // console.log("My proof:", proof.hash(0));
            // console.log("My top:", top.hash(0));
            const fullPfx  = Buffer.from(address.address, 'hex');
            const shortPfx = fullPfx.readUintBE(0, 4);
            fork[isRightInit]     = proof;
            fork[isRightInit ^ 1] = await pruned;
            // console.log("Pruned picked:", fork[isRight ^ 1]);
            const paths    = address.path.split(',').map(p => Number(p)).filter(p => p <= pfxLength);
            let   total    = paths.length - 1;
            let   prevFork = paths[total];
            let   totalLen = 0;

            while(total--) {
                let isRight: number;
                const nextFork = paths[total];
                const leafLen  = prevFork - nextFork - 1;
                totalLen += leafLen + 1;
                if(nextFork <= 32 && leafLen <= 32) {
                    curPfx = getN(shortPfx, nextFork, true);
                    leaf   = extractBits(shortPfx, nextFork, leafLen);
                    isRight = curPfx % 2;
                    siblingPfx = curPfx ^ 1;
                }
                else {
                    leaf = extractBits(fullPfx, nextFork, leafLen);
                    curPfx = bigIntFromBuffer(fullPfx, nextFork);
                    const longRight = curPfx % 2n;
                    siblingPfx = curPfx ^ 1n;
                    isRight    = Number(longRight);
                }
                // console.log("Cur pfx:", curPfx);
                // console.log("isRight:", isRight);
                // console.log("Next Fork:", nextFork);
                const forkLeaf = forceFork(leaf, leafLen, this.maxLen - totalLen, fork[0], fork[1]);
                // console.log("ForkLeaf:", forkLeaf);
                fork[isRight]  = forkLeaf;
                fork[isRight ^ 1] = await this.processPfxCached(siblingPfx, nextFork);
                // console.log("Pruned:", fork[isRight ^ 1]);

                // console.log("Left:",  fork[0].hash(0));
                // console.log("Right:", fork[1].hash(0));

                prevFork = nextFork;

            }
            const endProof = convertToMerkleProof(forceFork(0b100 << 8, 11, 267, fork[0], fork[1]));
            if(!endProof.refs[0].hash(0).equals(this.root_hash)){
                console.log(address);
                console.log("Cur proof:", proof);
                console.log("End proof:", endProof);
                throw new Error("Unable to build proof");
            }
            proofs.push([proofKey, endProof.toBoc().toString('base64')]);
        }
        return proofs;
    }
    /*
    async buildProofOld(address: Address) {

        const fullPfx   = address.hash;
        const shortPfx  = fullPfx.readUintBE(0, 4);

        const proofPath = await this.storage.getPath(address.toRawString())
        if(!proofPath) {
            throw new Error("Can't generate proof without path");
        }
        const forkIdxs  = proofPath.path.split(',');
        let   total     = forkIdxs.length - 1;
        if(total < 0) {
            throw new Error("No fork indexes present");
        }
        let   totalLen  = 0;
        let   isRight: number;
        let   leaf: number | bigint;
        let   curPfx: number | bigint;
        let   siblingPfx: number | bigint;
        let   fork: Cell[] = new Array(2);

        // unroll data leaf
        let   prevFork= Number(forkIdxs[total]);
        const dataLen = 256 - prevFork;
        leaf  = extractBits(fullPfx, prevFork, dataLen);
        if(prevFork <= 32) {
            curPfx  = getN(shortPfx, prevFork, true);
            isRight = curPfx % 2;
            siblingPfx = curPfx ^ 1;
        }
        else {
            curPfx  = bigIntFromBuffer(fullPfx, prevFork);
            // Local bigint isRight
            const longRight = curPfx % 2n;
            siblingPfx = curPfx ^ 1n;
            isRight = Number(longRight);
        }
        const dataLeaf = beginCell().store(
            storeLabel(leaf,dataLen, dataLen)
        ).storeSlice(this.packData(proofPath.amount).asSlice()).endCell();


        fork[isRight] = dataLeaf;
        fork[isRight ^ 1] = await this.processPfxCached(siblingPfx, prevFork);

        // console.log("Left:",  fork[0].hash(0));
        // console.log("Right:", fork[1].hash(0));


        while(total--) {
            const nextFork = Number(forkIdxs[total]);
            const leafLen  = prevFork - nextFork - 1;
            totalLen += leafLen + 1;

            if(nextFork <= 32 && leafLen <= 32) {
                curPfx = getN(shortPfx, nextFork, true);
                leaf   = extractBits(shortPfx, nextFork, leafLen);
                isRight = curPfx % 2;
                siblingPfx = curPfx ^ 1;
            }
            else {
                leaf = extractBits(fullPfx, nextFork, leafLen);
                const longRight = leaf % 2n;
                siblingPfx = leaf ^ 1n;
                isRight    = Number(longRight);
            }
            const forkLeaf = forceFork(leaf, leafLen, 256 - totalLen, fork[0], fork[1]);
            fork[isRight]  = forkLeaf;
            fork[isRight ^ 1] = await this.processPfxCached(siblingPfx, nextFork);

            // console.log("Left:",  fork[0].hash(0));
            // console.log("Right:", fork[1].hash(0));

            prevFork = nextFork;
        }

        return convertToMerkleProof(forceFork(0b100 << 8, 11, 267, fork[0], fork[1]));
    }
    */
    async processPfx(pfx: PfxCluster, opts?: Partial<ProcessPfxOptions>): Promise<PfxProcessed> {
        let resCell: Cell;
        let nextFork: number;
        let nextOptions: Partial<ProcessPfxOptions>;
        let label: Edge;
        const pfxWindow = pfx.len % 32;
        if(!opts) {
            opts = {
                pruned: true,
                check_root: true,
                check_cache: false,
                save_path: true
            }
        }
        const checkRoot = opts.check_root ?? false; 

        if(checkRoot) {
            if(!this.storage) {
                throw new Error("Storage is required for root check");
            }
            const rootPos = await this.storage.findBranchRoot(pfx.len, pfx.keys[0]);
            if(rootPos != pfx.len) {
                const delta  = pfx.len - rootPos;
                const bitPos = 32 - rootPos - 1;
                pfx.len      = rootPos;
                if(typeof pfx.pfx == 'bigint') {
                    throw new Error("TODO");
                }
                let bitsToKey = getN(pfx.pfx, delta, false);
                pfx.pfx >>= delta;
                if(bitsToKey != 0) {
                    bitsToKey *= 2 ** bitPos
                    pfx.keys = pfx.keys.map(k => bitsToKey + k);
                }
                return await this.processPfx(pfx, {...opts, check_root: false});
            }
        }
        if(pfx.len > 100) {
            console.log(pfx);
        }

        nextOptions = {...opts, check_root: false};
        
        nextFork = findNextFork(pfx.keys);
        if(nextFork < 32) {
            let labelBits = 0;
            const edgeLen = nextFork - pfxWindow;
            if(edgeLen > 0) {
                // edgeLen bits starting from next one
                labelBits = extractBits(pfx.keys[0], pfxWindow, edgeLen);
            }
            label = {
                type: 'normal',
                edgeLen,
                bits: labelBits
            }
        }
        else {
            const sfxs     = pfx.data.map(d => d.suffix);

            try {
            const sfxMatch = findNextForkLarge(sfxs);
            const keyLeft  = (32 - (pfx.len % 32)) % 32;
            const   forkLen  = keyLeft + sfxMatch.pos;
            const hasMatch = sfxMatch.pos > 0;

            const nextLabel = joinSuffixes(pfx.keys[0], [sfxMatch.pos, sfxMatch.pfx]);

            if(forkLen + pfx.len < this.maxLen) {
                for(let i = 0; i < pfx.keys.length; i++) {
                    /*
                    if(hasMatch) {
                        // Clear the bits read from match
                        sfxs[i] = clearFirstBits(sfxs[i], sfxMatch.pos);
                    }
                    */
                    // If anything left
                    if(sfxs[i].length > 0) {
                        const toRead = Math.min(4, sfxs[i].length);
                        // Populate next suffixes
                        pfx.keys[i]  = sfxs[i].subarray(0, toRead).readUintBE(0, toRead);
                        // And clear used bits
                        sfxs[i]      = clearFirstBits(sfxs[i], toRead * 8);
                    }                    // Update suffix ref in data
                    pfx.data[i].suffix = sfxs[i];
                }
            }

            label = {
                type: 'jumbo',
                edgeLen: forkLen,
                bits: nextLabel
            }
            } catch(e) {
                console.log(e);
                console.log(pfx);
                console.log("Data:", pfx.data);
                console.log("Failed while processing:", pfx);
                throw new Error("FIX DAT");
            }
        }

        const nextLen   = pfx.len + label.edgeLen + 1;
        const expectTop = pfx.len < this.store_depth && nextLen >= this.store_depth;
        if(expectTop) {
            nextOptions = {...nextOptions, pruned: false};
        }
        resCell = await this.packFork(label, pfx, nextOptions);

        if(opts.save_path && pfx.len < this.store_depth) {
            if(expectTop) {
                if(this.storage) {
                    await this.storage.saveTop(pfx.pfx, pfx.len, resCell.toBoc().toString('base64'));
                } else if(this.parentPort) {
                    this.parentPort.postMessage({
                        type: 'save_top',
                        pfx: pfx.pfx,
                        len: pfx.len,
                        boc: resCell.toBoc().toString('base64')
                    });
                }
                else {
                    throw new Error("Invalid instance");
                }
            }
            if(this.storage) {
                await this.storage.saveBranch(pfx.pfx, pfx.len, resCell.depth(0), resCell.hash(0));
            } else if(this.parentPort) {
                this.parentPort.postMessage({
                    type: 'save_branch',
                    pfx: pfx.pfx,
                    len: pfx.len,
                    depth: resCell.depth(0),
                    hash: resCell.hash(0).toString('base64')
                });
            }
            else {
                throw new Error("Invalid instance");
            }
        }

        return {pfx: pfx.pfx, len: pfx.len, cell: resCell};
    }
}
