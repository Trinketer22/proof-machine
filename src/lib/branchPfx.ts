import { Address } from "@ton/core";
export class BranchPrefix {
    private workchain: number;
    private pfx: bigint;
    
    constructor(address: Address, bits: number) {
        const bitPart = 0n;
        let resPfx  = 0n;
        const fullBytes = Math.floor(bits / 8);
        const bitsLeft  = BigInt(bits % 8);
        const nextByte  = BigInt(address.hash[fullBytes]);

        console.log(`Constructing ${bits} ${fullBytes}/${bitsLeft}`);

        this.workchain = address.workChain;
        if(fullBytes > 0) {
            resPfx = BigInt('0x' + address.hash.subarray(0, fullBytes).toString('hex'));
        }
        if(bitsLeft > 0) {
            resPfx = (resPfx << bitsLeft) | (nextByte >> (8n - bitsLeft));
        }

        if(resPfx === 0n) {
            throw TypeError("Can't create empty prefix");
        }
        this.pfx = resPfx;
    }

    [Symbol.toPrimitive](hint: string) {
        if(hint == 'string') {
            return this.toString();
        }
    }
    toString(radix: number = 16) {
        return `${this.workchain}:${this.pfx.toString(radix)}`;
    }
}
