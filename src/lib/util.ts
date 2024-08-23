import { beginCell, Cell } from '@ton/core';

function genMaskBig(count: number, msb: boolean, bit_size: number) {
    let mask = 2n ** BigInt(bit_size - count);
    if(!msb) {
        mask *= 2n ** BigInt(count);
    }
    return mask;
}
function genMask(count: number, msb: boolean, bit_size: number = 32) {
    // Geez JS is not meant for this (all the bitwise ops are SIGNED integer..)
    let mask = 2 ** (bit_size - count); // No - 1 because this is a modulo % mask not AND & 
    // Elevate 1 bits to MSB, so only low count bits masked
    if(!msb) {
        mask *= 2 ** count; 
    }

    return mask;
}
// @msb -> true means clear n most significant bits. LSB otherwise
export function clearN(n: number, count: number, msb: boolean, bit_size: number = 32) {
    return count == 0 ? n : n % genMask(count, msb, bit_size);
}
// Removes n bits in high part and move low part up
export function clearNext(n: number, count: number, bit_size: number = 32) {
    return count == 0 ? n : clearN(n, count, true, bit_size) * (2 ** count);
}

export function getNBig(n: bigint, count: number, bitLen: number) {
    if(n == 0n) {
        return n;
    }

    const clear = BigInt(bitLen - count);

    if(clear < 0n) {
        throw new Error("Count is > than bitLen");
    }
    return n >> clear;
}
export function getN(n: number, count: number, msb: boolean, bit_size:number = 32) {
    if(n == 0) {
        return n;
    }
    let loot: number;
    const clear = bit_size - count;

    if(msb) {
        // Drop low part
        loot = Math.floor(n / (2 ** clear));
    }
    else {
        // Mask hi part
        loot = n % genMask(clear, true, bit_size);
    }
    return loot;
}

export function extractBits(src: number, start: number, count: number, bit_size?: number) : number;
export function extractBits(src: Buffer, start: number, count: number, bit_size?: number) : bigint;
export function extractBits(src: number | Buffer, start: number, count: number, bit_size: number = 32): number | bigint {
    if(start < 0) {
        throw new Error("Start is negative");
    }
    if(count < 0) {
        throw new Error("Count is negative");
    }
    if(typeof src == 'number') {
        const totalLen = start + count;
        let loot = totalLen == bit_size ? src : getN(src, totalLen, true, bit_size);
        if(start > 32 ) {
            throw new Error("Start of range");
        }
        if(totalLen > 32) {
            throw new Error("Count out of range");
        }
        if(start > 0) {
            // Common case, saving some time skipping mask gen
            if(start == 1 && count == 1) {
                loot = loot % 2;
            }
            else {
                loot %= genMask(bit_size - count, true, bit_size);
            }
        }
        return loot;
    }

    // Byte indexes
    const startByte  = Math.floor(start / 8);
    const endByte    = (startByte + Math.ceil(count / 8)) - 1;

    if(endByte > src.length - 1) {
        throw new Error("Not enough data in the buffer");
    }
    let loot: bigint;
    const alignStart = start % 8;
    const bitsLeft   = (count + alignStart) % 8;

    // loot = 0n;
    if(alignStart == 0) {
        loot = BigInt(src[startByte]);
    }
    else {
        loot = BigInt(clearN(src[startByte], alignStart, true, 8));
    }

    for(let i = startByte + 1; i <= endByte; i++) {
        loot = (loot << 8n) + BigInt(src[i]);
    }
    if(bitsLeft) {
        loot >>= BigInt(8 - bitsLeft);
    }

    return loot;
}

export function isOdd(n: number | bigint) {
    if(typeof n == 'number') {
        return n % 2 == 0;
    }
    return n % 2n == 0n;
}

export function filterSide(n: number | bigint, pfxLen: number, size: number, left: boolean) {
    let chunk: number | bigint;
    if(typeof n == 'number') {
        chunk = getN(n, pfxLen, true, size + pfxLen)
    }
    else {
        chunk = getNBig(n, pfxLen, size + pfxLen)
    }
    const res = isOdd(chunk);
    return left ? res : !res;
}
export function upOrder(n: number, count: number): number;
export function upOrder(n: bigint, count: number): bigint;
export function upOrder(n: number | bigint, count: number) {
    if(typeof n == 'number') {
        return n * (2 ** count);
    }
    return n * (2n ** BigInt(count));
}


export function bigIntFromBuffer(src: Buffer, count: number) {
    if(count == 0) {
        return 0n;
    }
    const fullSize  = src.length * 8;
    const fullBytes = Math.ceil(count / 8)

    if(fullSize < count) {
        throw new Error(`${count}/${fullSize} is out of bound`);
    }

    const fullInt  = BigInt('0x' + src.subarray(0, fullBytes).toString('hex'));
    return fullInt >> BigInt(fullBytes * 8 - count);
}

// Removes first count bits from buffer
export function clearFirstBits(src: Buffer, count: number) {
    const bitClear = count % 8;
    const fullCount  = Math.ceil(count / 8);
    const startByte = Math.floor(count / 8);

    if(src.length < fullCount) {
        throw new Error(`${count}/${src.length * 8} is out of bound`);
    }

    const resBuff = src.subarray(startByte);
    
    if(bitClear > 0) {
        resBuff[0] = clearN(resBuff[0], bitClear, true, 8);
    }

    return resBuff;
}

export function findMatchingBits (a: number, b: number) {
    // a and b expected to be 8 bit
    if(a > 255 || b > 255) {
        throw new Error("Value out of range");
    }
    return 8 - (32 - Math.clz32(a ^ b));
    /*
    let keepGoing = true;
    do {
        const bitCount = bitPos + 1;
        const shift    = 8 - bitCount;
        if(((a >> shift) & 1) == ((b >> shift) & 1)) {
            bitPos++;
        }
        else {
            keepGoing = false;
        }
    } while(keepGoing && bitPos < 7);
    return bitPos;
    */
}
function padBuffer(src: Buffer, n: number, fill?: string | number | Uint8Array) {
    const padding = Buffer.alloc(n, fill);
    return Buffer.concat([src, padding]);
}

export function findCommonPrefix(a: Buffer, b: Buffer, stop?: number) {
    const keyLen   = stop ? Math.min(Math.ceil(stop / 8), a.length) : a.length;
    const lenLimit = stop ?? keyLen * 8;
    let byteMatch  = 0;
    let bitMatch   = 0;


    if(a.length != b.length) {
        throw new Error("Only equal length keys expected");
    }


    /*
    const padLength = a.length - b.length;

    if(padLength != 0) {
        if(padLength > 0) {
            b = padBuffer(b, Math.abs(padLength));
        }
        else  {
            a = padBuffer(a, Math.abs(padLength));
        }
    }
    */

    while(byteMatch < keyLen && a[byteMatch] == b[byteMatch]) {
        byteMatch++;
    }
    
    bitMatch = byteMatch * 8;

    if(bitMatch < lenLimit) {
        const lastIdx = byteMatch;
        bitMatch += findMatchingBits(a[lastIdx], b[lastIdx]);
    }

    return bitMatch;
}


export function findNextFork(pfxs:number []) {
    let nextFork = 32;
    if(pfxs.length == 1) {
        return nextFork;
    }
    // There is probably better way to doo it
    for(let i = 0; i < pfxs.length; i++) {
        for(let k = 0; k < pfxs.length; k++) {
            if(i != k) {
                nextFork = Math.min(Math.clz32(pfxs[i] ^ pfxs[k]), nextFork);
            }
        }
    }

    return nextFork;
}
export function findDirectFork(pfx: number, pfxs: number []) {
    for(let i = 0; i < pfxs.length; i++) {
        if(Math.abs(pfx - pfxs[i]) == 1) {
            return pfxs[i];
        }
    }
    return -1;
}

export function findNextForkLarge(pfxs: Buffer[]) {
    if(pfxs.length == 0) {
        throw new Error("Something went wrong");
    }
    const pfxCount = pfxs.length;
    const maxBits  = pfxCount > 0 ? pfxs[0].length * 8 : 0;
    let nextFork   = maxBits;
    if(pfxCount > 1) {
        for(let i = 0; i < pfxCount; i++) {
            for(let k = 0; k < pfxCount; k++) {
                if(i !== k) {
                    nextFork = Math.min(findCommonPrefix(pfxs[i], pfxs[k], nextFork), nextFork);
                    if(nextFork == maxBits) {
                        i = pfxCount;
                        k = pfxCount;
                    }
                }
            }
        }
    }

    return {
        pos: nextFork,
        pfx: bigIntFromBuffer(pfxs[0], nextFork)
    }
}

export function convertToPrunedBranch(hash: Buffer, depth: number): Cell {
    return beginCell()
        .storeUint(1, 8)
        .storeUint(1, 8)
        .storeBuffer(hash)
        .storeUint(depth, 16)
        .endCell({ exotic: true });
}

export function convertToMerkleProof(c: Cell): Cell {
    return beginCell()
        .storeUint(3, 8)
        .storeBuffer(c.hash(0))
        .storeUint(c.depth(0), 16)
        .storeRef(c)
        .endCell({ exotic: true });
}

export function joinSuffixes(pfx: number | bigint, ...suffixes:[number, number | bigint][]) {
    let res = BigInt(pfx);

    for(const suffix of suffixes) {
        res = (res << BigInt(suffix[0])) | BigInt(suffix[1]);
    }

    return res;
}
