import { beginCell, Builder, Cell, Slice } from "@ton/core";
import { convertToPrunedBranch, extractBits, filterSide, getN, getNBig, isOdd, upOrder } from "./util";

// Shameless borrow from @ton/core
function labelShortLength(labelLen: number) {
    return 1 + labelLen + 1 + labelLen;
}

function labelLongLength(labelLen: number, keyLength: number) {
    return 1 + 1 + Math.ceil(Math.log2(keyLength + 1)) + labelLen;
}
function labelSameLength(keyLength: number) {
    return 1 + 1 + 1 + Math.ceil(Math.log2(keyLength + 1));
}

function isSame(bits: number | bigint, len: number) {
    let res : boolean;
    if(typeof bits == 'number') {
        if(len > 52) {
            throw new RangeError("label length out of number range");
        }
        const allOnes = (2 ** len) - 1;
        res = bits == 0 || bits == allOnes;
    }
    else {
        const allOnes = (1n << BigInt(len)) - 1n;
        res = bits == 0n || bits == allOnes;
    }
    return res;
}
//

function storeSame(bit: number, labelLen: number | bigint, keyLen: number, storeLen?: number) {
    return (builder: Builder) => {
        builder.storeUint(0b11, 2)
               .storeBit(bit)
               .storeUint(labelLen, storeLen ?? Math.ceil(Math.log2(keyLen + 1)))
    }
}

function storeShort(label: number | bigint, labelLen: number) {
    return (builder: Builder) => {
        // 1...labelLen 0
        builder.storeBit(0)
        if(labelLen > 0) {
            const unaryLen = ((2 ** labelLen) - 1) * 2;
            builder.storeUint(unaryLen, labelLen + 1).storeUint(label, labelLen);
        }
        else {
            builder.storeBit(false);
        }
    }
}

function storeLong(label: number | bigint, labelLen: number, keyLength: number, storeLen?: number) {
    return (builder: Builder) => {
        const length = storeLen ?? Math.ceil(Math.log2(keyLength + 1));

        builder.storeUint(0b10, 2)
               .storeUint(labelLen, length)
               .storeUint(label, labelLen)
    }
}

export function storeLabel(label: number | bigint, labelLen: number, keyLen: number) {
    return (builder: Builder) => {

        const k = 32 - Math.clz32(keyLen);
        if(isSame(label, labelLen)) {
            if(labelLen > 1 && k < 2 * labelLen - 1) {
                builder.store(storeSame(Number(!isOdd(label)), labelLen, keyLen));
                return;
            }
        }
        if(k < labelLen) {
            builder.store(storeLong(label, labelLen, keyLen, k));
        }
        else {
            builder.store(storeShort(label, labelLen));
        }
    }
}

export function forceFork(prefix: number | bigint, labelLen: number, keyLen: number, left: Cell, right: Cell) {
    return beginCell().store(
        storeLabel(prefix, labelLen, keyLen)
    ).storeRef(left).storeRef(right).endCell();
}


export function readAllPrefixes(tree: Cell, pfx: number | bigint, pfxLen: number, keyLen: number) {
    const pfxRes = readPfx(tree, pfx, pfxLen, keyLen);

    if(pfxRes.keyLeft == 0) {
        return [pfxRes.pfx];
    }

    const res: (number | bigint)[] = [];
    const sl = tree.beginParse();
    const l = sl.loadRef();
    const r = sl.loadRef();
    const nextLen = keyLen - pfxRes.labelLen - 1;

    let pfxLeft: number | bigint;
    let pfxRight: number | bigint;
    let left: (number | bigint)[];
    let right: (number | bigint)[];

    if(typeof pfxRes.pfx == 'number') {
        pfxLeft  = pfxRes.pfx * 2;
        pfxRight = pfxLeft + 1;
    }
    else {
        pfxLeft  = pfxRes.pfx * 2n;
        pfxRight = pfxLeft + 1n;
    }

    if(!l.isExotic) {
        left = readAllPrefixes(l, pfxLeft, pfxRes.pfxLen, nextLen);
        res.push(...left);
    }
    if(!r.isExotic) {
        right = readAllPrefixes(r, pfxRight, pfxRes.pfxLen, nextLen);
        res.push(...right);
    }
    return res;
}

export function readPfx(tree: Cell, pfx: number | bigint, pfxLen: number, keyLen: number) {
    let labelLen = 0;

    const ds = tree.beginParse();

    const lb = ds.loadUint(2);
    if((lb >> 1) == 0) {
        if(lb & 1) {
            const chunkLen = Math.min(32, ds.remainingBits);
            const chunk    = ds.loadUint(chunkLen);
            const xorMask  = (2 ** chunkLen) - 1;
            labelLen = 32 -  Math.clz32(chunk ^ xorMask) - 1;
            if(labelLen < 0) {
                // Practically makes no sense for it to be short
                throw new Error("TODO extra length short label");
            }
            if(labelLen + pfxLen > 52) {
                pfx = BigInt(pfx);
            }
            if(typeof pfx == 'bigint') {
                pfx = upOrder(pfx , labelLen) + BigInt(extractBits(chunk, labelLen, labelLen, chunkLen));
            }
            else {
                pfx = pfx * (2 ** labelLen) + extractBits(chunk, labelLen, labelLen, chunkLen);
            }
        }
    }
    else {
        if(lb == 2) {
            labelLen = ds.loadUint(Math.ceil(Math.log2(keyLen + 1)));
            if(labelLen + pfxLen > 52) {
                pfx = BigInt(pfx);
            }
            if(typeof pfx == 'bigint') {
                pfx = upOrder(pfx, labelLen) + ds.loadUintBig(labelLen);
            }
            else {
                pfx = upOrder(pfx, labelLen) + ds.loadUint(labelLen);
            }
        }
        else {
            const bit = ds.loadUint(1);
            labelLen  = ds.loadUint(Math.ceil(Math.log2(keyLen + 1)));
            const mask = (2 ** labelLen) - 1;
            if(labelLen + pfxLen > 52) {
                pfx = BigInt(pfx);
            }
            if(typeof pfx == 'number') {
                pfx = upOrder(pfx, labelLen);
                if(bit) {
                    pfx += mask;
                }
            }
            else {
                pfx = upOrder(pfx, labelLen);
                if(bit) {
                    pfx += BigInt(mask)
                }
            }
        }
    }

    pfxLen += labelLen;
    let keyLeft = keyLen - labelLen;

    if(keyLeft > 0){
        pfxLen++;
        keyLeft--;
    }

    return {
        pfx,
        keyLeft,
        pfxLen,
        labelLen
    }
}
export function generateMerkleProof(tree: Cell, pfx: number | bigint, pfxLen: number, keys: (number | bigint)[], keyLen: number) {
    if(keys.length == 0) {
        return convertToPrunedBranch(tree.hash(0), tree.depth(0));
    }

    const labelLen = 0;
    let pfxNum: number;
    let pfxBig: bigint;

    if(typeof pfx == 'number') {
        pfxNum = pfx;
    }
    else {
        pfxBig = pfx;
    }
    /*
    const ds = tree.beginParse();

    const lb = ds.loadUint(2);
    if((lb >> 1) == 0) {
        if(lb & 1) {
            const chunkLen = Math.min(32, ds.remainingBits);
            const chunk    = ds.loadUint(chunkLen);
            const xorMask  = (2 ** chunkLen) - 1;
            labelLen = 32 -  Math.clz32(chunk ^ xorMask) - 1;
            if(typeof pfx == 'bigint') {
                pfx = upOrder(pfx , labelLen) | BigInt(getN(chunk, labelLen, true));
            }
            else {
                pfx = pfx * (2 ** labelLen) | getN(chunk, labelLen, true);
            }
        }
    }
    else {
        if(lb == 2) {
            labelLen = ds.loadUint(Math.ceil(Math.log2(keyLen + 1)));
            if(typeof pfx == 'bigint') {
                pfx = upOrder(pfx, labelLen) | ds.loadUintBig(labelLen);
            }
            else {
                pfx = upOrder(pfx, labelLen) | ds.loadUint(labelLen);
            }
        }
        else {
            let bit = ds.loadUint(1);
            labelLen  = ds.loadUint(Math.ceil(Math.log2(keyLen + 1)));
            const mask = (2 ** labelLen) - 1;
            if(typeof pfx == 'number') {
                pfx = upOrder(pfx, labelLen);
                if(bit) {
                    pfx |= mask;
                }
            }
            else {
                pfx = upOrder(pfx, labelLen);
                if(bit) {
                    pfx |= BigInt(mask)
                }
            }
        }
    }
    */
    const pfxRes = readPfx(tree, pfx, pfxLen, keyLen);
    if(pfxRes.keyLeft == 0) {
        if(!keys.includes(pfxRes.pfx)) {
            console.log(keys);
            throw new Error("Key doesn't match");
        }
        return tree;
    }
    else {
        pfxLen = pfxRes.pfxLen;
        pfx    = pfxRes.pfx;
        keyLen = pfxRes.keyLeft;
        const sl = tree.beginParse();
        let l = sl.loadRef();
        let r = sl.loadRef();
        // const nextLen = keyLen - labelLen - 1;
        let pfxLeft: number | bigint;
        let pfxRight: number | bigint;
        if(typeof pfx == 'number') {
            pfxLeft  = pfx * 2;
            pfxRight = pfxLeft + 1;
        }
        else {
            pfxLeft  = pfx * 2n;
            pfxRight = pfxLeft + 1n;
        }

        if(!l.isExotic) {
            const left = keys.filter(k => filterSide(k, pfxLen, keyLen, true));
            l = generateMerkleProof(l, pfxLeft, pfxLen, left, keyLen);
        }
        if(!r.isExotic) {
            const right = keys.filter(k => filterSide(k, pfxLen, keyLen, false));
            r = generateMerkleProof(r, pfxRight, pfxLen, right, keyLen);
        }
        return beginCell().storeSlice(sl).storeRef(l).storeRef(r).endCell();
    }
}

