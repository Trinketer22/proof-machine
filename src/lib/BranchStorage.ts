import sqlite3 from 'sqlite3';
import { bigIntFromBuffer, clearFirstBits, clearN } from './util';

export type PfxData = {
    value: bigint,
    path: number[],
    suffix: Buffer
}

export type PfxCluster = {
    pfx: number | bigint,
    len: number,
    keys: number[],
    data: PfxData[]
}

type CacheTimer = {
    timeout: NodeJS.Timeout,
    cleared: boolean
}

export class StorageSqlite {
    protected db: sqlite3.Database;
    protected rootCache:Map<string, boolean>;
    protected branchFile: FileHandle | undefined;
    protected pathQueue: {
        count: number,
        map: Map<string, string[]>;
    };
    protected pathCacheLocked: boolean;
    protected pathTimeout: CacheTimer | undefined;
    protected branchTimeout: CacheTimer | undefined;
    protected topTimeout: CacheTimer | undefined;
    protected branchCacheLocked: boolean;
    protected branchCache: {
        pfx: string,
        depth: number,
        hash: Buffer
    }[];
    protected topCacheLocked: boolean
    protected topCache: {
        pfx: string,
        boc: string
    }[]

    constructor(path: string) {
        this.db = new sqlite3.Database(path);
        this.rootCache = new Map<string, boolean> ();
        this.pathCacheLocked = false;
        this.branchCacheLocked = false;
        this.pathQueue = {count: 0, map: new Map()};
        this.branchCache = [];
        this.topCache    = [];
        this.topCacheLocked = false;
        this.db.run("pragma journal_mode = WAL");
        this.db.run("pragma synchronous  = normal");
        this.db.run("pragma temp_store = memory");
        // this.db.run("pragma mmap_size = 8000000000");
        // this.db.on('trace', (sql) => console.log("Run:", sql));
    }

    groupPrefixes(keyLen: number, offset: number = 0, limit: number = 100) {
        return new Promise((resolve:(value: PfxCluster[]) => void, reject) => {
            const shift = 32 - (keyLen % 32);
            const query = "SELECT `key` >> ? AS pfx, group_concat(`key`) AS keys, group_concat(`amount`) AS amounts, group_concat(substr(`address`, 11)) as `suffix` FROM `airdrop` GROUP BY (`key` >> ?) LIMIT ?,?"
            this.db.all<{
                total: number,
                pfx: number,
                keys: string,
                amounts: string,
                suffix: string
            }>(query,[shift, shift, offset, limit], (err, rows) => {
                if(err) {
                    reject(err);
                    return;
                }
                const pfxChunk: PfxCluster[] = new Array(rows.length);
                for(let i = 0; i < rows.length; i++) {
                    const amounts = rows[i].amounts.split(',');
                    const sfxs    = rows[i].suffix.split(',');
                    const data: PfxData[] = new Array(amounts.length);
                    for(let k = 0; k < amounts.length; k++) {
                        data[k] = {
                            value: BigInt(amounts[k]),
                            path: keyLen == 0 ? [] : [keyLen],
                            suffix: Buffer.from(sfxs[k], 'hex')
                        }
                    }

                    pfxChunk[i] = {
                        pfx: rows[i].pfx,
                        keys : rows[i].keys.split(',').map(k => clearN(Number(k), keyLen, true)),
                        len: keyLen,
                        data
                    }
                }
                resolve(pfxChunk);
            });
        })
    }

    isBranchRoot(keyLen: number, key: number) {
        return new Promise((resolve: (value: boolean) => void, reject) => {

            if(keyLen == 1) {
                resolve(true);
                return;
            }

            const query  = "SELECT COUNT(`address`) AS total FROM `airdrop` WHERE `key` >> ? = ? UNION SELECT COUNT(`address`) AS total FROM `airdrop` WHERE `key` >> ? = ?";
            const shift  = 32 - (keyLen % 32);
            const nextShift = shift + 1;
            const expKey  = Math.floor(key / (2 ** shift));
            const nextKey = Math.floor(expKey / 2);
            const keyStr  = shift.toString() + ':' + expKey.toString();
            const cached  = this.rootCache.get(keyStr);
            if(cached !== undefined) {
                // console.log("Got root from cache");
                resolve(cached);
                return;
            }

            this.db.all<{total: number}>(query,[Number(shift), expKey, Number(nextShift), nextKey], (err, rows) => {
                if(err) {
                    reject(err);
                    return;
                }
                if(rows.length != 2) {
                    this.rootCache.set(keyStr, false);
                    resolve(false);
                }
                else {
                    const result = rows[0].total < rows[1].total;
                    this.rootCache.set(keyStr, result);
                    resolve(result)
                }
            });
        });
    }

    async findBranchRoot(keyLen: number, key: number) {
        while(keyLen > 0 && !(await this.isBranchRoot(keyLen, key))) {
            keyLen--;
        }
        return keyLen;
    }
    saveBranchCache() {
        if(this.branchTimeout) {
            clearTimeout(this.branchTimeout.timeout);
             this.branchTimeout.cleared = true;
        }
        if(this.branchCache.length > 0) {
            this.branchCacheLocked = true;
            const params: unknown[] = [];
            let placeholders  = '(?,?,?)';
            let branchData = this.branchCache.shift()!;
            params.push(branchData.pfx, branchData.depth, branchData.hash.toString('base64'));
            do {
                let left = this.branchCache.length;
                while(left--) {
                    placeholders += ',(?,?,?)';
                    branchData = this.branchCache.shift()!;
                    params.push(branchData.pfx, branchData.depth, branchData.hash.toString('base64'));
                }
            } while(this.branchCache.length > 0);

            return new Promise((resolve, reject) => {
                const query = "INSERT INTO `branches` (`prefix`, `depth`, `hash`) VALUES " + placeholders;
                this.db.run(query, params, (err) => {
                    if(err) {
                        reject(err);
                    }
                    else {
                        resolve(true);
                    }
                });
            }).then(v => this.branchCacheLocked = false);
        }
    }
    async saveBranch(pfx: number | bigint, len: number, depth: number, hash: Buffer) {
        /*
        if(!this.branchFile) {
            throw new Error("Branch file is not open");
        }
        */
        const pfxKey = len.toString(16) + ':' + pfx.toString(16);
        // await this.branchFile.appendFile([pfxKey, depth.toString(), hash.toString('base64')].join(',') + "\n", {encoding: 'utf8'});
        if(this.branchTimeout) {
            clearTimeout(this.branchTimeout.timeout);
            this.branchTimeout.cleared = true;
        }

        this.branchCache.push({
            pfx: pfxKey,
            depth,
            hash
        });

        if(this.branchCache.length < 1024) {
            if(this.branchTimeout == undefined || this.branchTimeout.cleared) {
               this.branchTimeout = {
                timeout: setTimeout(() => {
                this.branchTimeout!.cleared = true;
                if(!this.branchCacheLocked) {
                    console.log("Clearing branches by timeout");
                    this.saveBranchCache();
                }
               }, 2000),
               cleared: false
               }
            }
        }
        else if(this.branchCacheLocked == false){
            return this.saveBranchCache();
        }
    }
    queryBranch(pfx: number | bigint, len: number) {
        return new Promise((resolve, reject) => {
            const query = "SELECT * FROM `branches` WHERE `prefix` = ?";
            this.db.get<{pfx: string, hash: string}>(query, [pfx.toString()], (err, row) => {
                if(err) {
                    reject(err);
                }
                else {
                    const [len, pfx] = row.pfx.split(':');
                    resolve({
                        len,
                        pfx,
                        hash: Buffer.from(row.hash, 'base64')
                    });
                }
            });
        });
    }

    getBranchesMap() {
        type BranchMapValue = {
            depth: number,
            hash: Buffer
        };
        return new Promise((resolve: (value: Map<string, BranchMapValue>) => void, reject) => {
            const query = "SELECT * FROM `branches`";
            this.db.all<{prefix: string, depth: number, hash: string}>(query, [], (err, rows) => {
                if(err) {
                    reject(err);
                    return;
                }
                else {
                    const branchMap = new Map<string, {depth: number, hash:Buffer}>();
                    for(const row of rows) {
                        branchMap.set(row.prefix, {depth: row.depth, hash:Buffer.from(row.hash, 'base64')});
                    }
                    resolve(branchMap);
                }
            });
        });
    }
    updatePath(pfxs: number[], pfxLen: number, path: number[]) {
        const shift  = 32 - pfxLen;
        console.log("PfxLen:", pfxLen);
        console.log("Shift:", shift);
        console.log("PFXS:", pfxs);
        const concat = "UPDATE `airdrop` SET `path` = concat_ws(',', ?, `path`) WHERE `key` >> ? IN(" + new Array(pfxs.length).fill('?').join(',') + ')';
        return new Promise((resolve, reject) => {
            this.db.run(concat, [path.join(','), shift, ...pfxs], (err) => {
                if(err) {
                    reject(err);
                }
                else {
                    resolve(true);
                }
            });
        });
    }
    clearPathCache() {
        this.pathCacheLocked = true;
        if(this.pathTimeout && !this.pathTimeout.cleared) {
            clearTimeout(this.pathTimeout.timeout);
            this.pathTimeout.cleared = true;
        }
        const queries: Promise<unknown>[] = [];
        for(const path of this.pathQueue.map.keys()) {
            const batch = this.pathQueue.map.get(path)!;
            queries.push(this.savePathInternal(path, batch));
        }
        return Promise.all(queries).then(v => this.pathCacheLocked = false);
    }
    protected savePathInternal(path: string, pfxs: string[]) {
        const placeholders = new Array(pfxs.length).fill('?').join(',');
        this.pathQueue.map.delete(path);
        this.pathQueue.count--;
        return new Promise((resolve, reject) => {
            const query  = 'UPDATE `airdrop` SET `path` = ? WHERE `address` IN (' + placeholders + ')';
            // console.log("Query:",query);
            this.db.run(query, [path, ...pfxs], (err) => {
                if(err) {
                    reject(err);
                }
                else {
                    resolve(true);
                }
            }); 
        });
    }
    savePath(pfx: string, path: number[]) {
        if(this.pathTimeout && !this.pathTimeout.cleared) {
            clearTimeout(this.pathTimeout.timeout);
            this.pathTimeout.cleared = true;
        }
        const pathStr = path.join(',');

        const addrBefore = this.pathQueue.map.get(pathStr);
        if(addrBefore) {
            addrBefore.push(pfx)
        }
        else {
            this.pathQueue.map.set(pathStr, [pfx]);
        }
        this.pathQueue.count++;

        if(this.pathQueue.count >= 1024 && !this.pathCacheLocked) {
            return this.clearPathCache();
        }
        else {
            this.pathTimeout = {
                timeout: setTimeout(() =>{
                if(!this.pathCacheLocked) {
                    // console.log("Clearing paths by timeout");
                    this.clearPathCache();
                }
                }, 2000),
                cleared: false
            }
        }
    }
    protected clearTopCache() {
        return new Promise((resolve, reject) => {
            this.topCacheLocked = true;
            let topCount = this.topCache.length;
            if(topCount > 0) {
                const firstTop = this.topCache.shift()!;
                const params: unknown[] = [firstTop.pfx, firstTop.boc];
                let placeholders = '(?, ?)';
                while(--topCount) {
                    const nextTop = this.topCache.shift()!;
                    placeholders += ',(?, ?)';
                    params.push(nextTop.pfx, nextTop.boc);
                }
                const query = "INSERT INTO `tops` (`prefix`, `boc`) VALUES " + placeholders;
                this.db.run(query, params, (err) => {
                    if(err) {
                        reject(err);
                    }
                    else {
                        this.topCacheLocked = false;
                        resolve(true);
                    }
                });
            }
            else {
                this.topCacheLocked = false;
                resolve(true);
            }
        });
    }
    saveTop(pfx: number | bigint, length: number, boc: string) {
        if(this.topTimeout && !this.topTimeout.cleared) {
            clearTimeout(this.topTimeout.timeout);
            this.topTimeout.cleared = true;
        }
        const pfxKey = length.toString(16) + ':' + pfx.toString(16);
        this.topCache.push({
            pfx: pfxKey,
            boc
        });
        if(this.topCache.length < 100) {
            this.topTimeout = {
                timeout: setTimeout(() => this.clearTopCache(), 2000),
                cleared: false
            };
        }
        else if(!this.topCacheLocked) {
            return this.clearTopCache();
        }
    }
    getTop(pfx: number | bigint, length: number) {
        return new Promise((resolve:(boc: string) => void, reject) => {
            const pfxKey = length.toString(16) + ':' + pfx.toString(16);
            const query = "SELECT `boc` FROM `tops` WHERE `prefix` = ?";
            this.db.get<{boc: string}>(query, [pfxKey], (err, row) => {
                if(err) {
                    reject(err);
                }
                else {
                    resolve(row.boc);
                }
            });
        });
    }
    getTopBatch(offset: number, limit: number) {
        type ResType = {prefix: number, length: number, boc: string};
        return new Promise((resolve:(top: ResType[]) => void, reject) => {
            const query = "SELECT `prefix`, `boc` FROM `tops` LIMIT ?,?";
            this.db.all<{prefix: string, boc: string}>(query, [offset, limit], (err, rows) => {
                if(err) {
                    reject(err);
                }
                else {
                    const results = rows.map(r => {
                        const [length, prefix] = r.prefix.split(':').map(p => Number('0x' + p))
                        return {
                            length,
                            prefix,
                            boc: r.boc
                        }
                    });
                    resolve(results);
                }
            });
        });
    }
    /*
    getTopAndSibl(topPfx: number | bigint, siblPfx: number | bigint, length: number) {
        return new Promise((resolve, reject) => {
            const topKey    = length.toString() + ':' + topPfx.toString(16);
            const prunedKey = length.toString() + ':' + siblPfx.toString(16);
        });
    }
    */
    getPathForPrefix(pfx: number, pfxLength: number) {
        if(pfxLength > 32) {
            throw new Error("Prefix length > 32 is not supported: " + pfxLength);
        }

        type ResType = {address: string, amount: string, path: string};
        return new Promise((resolve:(value: ResType[]) => void, reject) => {
            const query = "SELECT substr(`address`, 3) AS `address`, `amount`, `path` FROM `airdrop` WHERE `key` >> ? = ?";
            const shift = 32 - pfxLength;
            this.db.all<ResType>(query, [shift, pfx], (err, rows) => {
                if(err) {
                    reject(err);
                }
                else {
                    resolve(rows);
                }
            });
        });
    }
    getPath(address: string) {
        type ResType = {path: string, amount: string};
        return new Promise((resolve:(value: {path: string, amount: bigint}) => void, reject) => {
            const query = "SELECT `path`, `amount` FROM `airdrop` WHERE `address` = ?";
            this.db.get<ResType>(query, [address], (err, row) => {
                if(err) {
                    reject(err);
                }
                else {
                    resolve({
                        path: row.path,
                        amount: BigInt(row.amount)
                    });
                }
            });
        });
    }
    getTotalRecords() {
        return new Promise((resolve:(value: number) => void, reject) => {
            const query = "SELECT COUNT(`address`) AS `total` FROM `airdrop`";
            this.db.get<{total: number}>(query, (err, row) => {
                if(err) {
                    reject(err);
                }
                else {
                    resolve(row.total);
                }
            });
        });
    }
    getEffectiveBits() {
        return new Promise((resolve:(value: number) => void, reject) => {
            this.getTotalRecords().then(
                v => resolve(Math.ceil(Math.log2(v)))
            ).catch(e => reject(e));
            /*
            const query = "SELECT COUNT(`address`) AS `total` FROM `airdrop`";

            this.db.get<{total: number}>(query, (err, row) => {
                if(err) {
                    reject(err);
                }
                else {
                    resolve(Math.ceil(Math.log2(row.total)));
                }
            })
            */
        })
    }

    getRecPrefixedCount(keyLen: number, key: bigint) {
        return new Promise((resolve:(value: number) => void, reject) => {
            const shift  = BigInt(32 - (keyLen % 32));
            const expKey =  Number(key >> shift);
            const query  = "SELECT COUNT(`address`) AS total FROM `airdrop` WHERE `key` >> ? = ?";
            this.db.get<{total: number}>(query, [Number(shift), expKey], (err, row) => {
                if(err) {
                    reject(err);
                }
                else {
                    resolve(row.total);
                }
            });
        });
    }

    getRandomAddress(count: number) {
        return new Promise((resolve:(addresses: string[]) => void, reject) => {
            const query = "SELECT `address` FROM `airdrop` ORDER BY RANDOM() LIMIT ?";
            this.db.all<{address: string}>(query, [count], (err, rows) => {
                if(err) {
                    reject(err);
                }
                else {
                    resolve(rows.map(r => r.address));
                }
            });
        });
    }

    // getRecPrefixed(keyLen: number, key: bigint, opts?: {offset: number, limit: number}): Promise<PfxCluster>;
    // getRecPrefixed(keyLen: number, key: number, opts?: {offset: number, limit: number}): Promise<PfxCluster>;
    getRecPrefixed(keyLen: number, key: number | bigint, opts?: {offset: number, limit: number}) {
        return new Promise((resolve:(value: PfxCluster) => void, reject) => {
            let params: unknown[];
            let query: string;
            let expKey: number | string;
            let longPfx: boolean;
            if(typeof key == 'number') {
                const shift = 32 - keyLen;
                longPfx  = false;
                /*
                if(keyLen >= 4) {
                    const aligned4 = key >> (keyLen % 4);
                    const minNibbles = Math.floor(keyLen / 4);
                    query    = "SELECT substr(`address`, 3) as `suffix` ,`key`, `amount` FROM `airdrop` WHERE `address` LIKE ? AND `key` >> ? = ?";
                    params   = [`0:${aligned4.toString(16)}%`, shift, key];
                }
                */
                query    = "SELECT substr(`address`, 3) as `suffix` ,`key`, `amount` FROM `airdrop` WHERE `key` >> ? = ?";
                params   = [shift, key];
            }
            else {
                const alignedLen = keyLen % 4;
                const aligned4 = key >>= BigInt(alignedLen);
                longPfx  = true;
                query    = "SELECT substr(`address`, 3) as `suffix` ,`key`, `amount` FROM `airdrop` WHERE `address` LIKE ?";
                params   = [`0:${aligned4.toString(16).padStart(alignedLen, '0')}%`];
            }
            let res: PfxCluster;
            if(opts) {
                query += ' LIMIT ?,?';
                params.push(opts.offset, opts.limit);
            }

            this.db.all<{suffix: string, key: number, amount: string}>(query, params, (err, rows) => {
                if(err) {
                    reject(err);
                    return;
                }
                if(!longPfx) {
                    res = {
                        pfx: key,
                        len: keyLen,
                        keys: new Array(rows.length),
                        data: new Array(rows.length)
                    }
                } else {
                    res = {
                        pfx: key,
                        len: keyLen,
                        keys: [],
                        data: []
                    }
                }
                for(let i = 0; i < rows.length; i++) {
                    let bitsLeft = 256 - keyLen;
                    let suffix = Buffer.from(rows[i].suffix, 'hex');
                    if(longPfx) {
                        if(bigIntFromBuffer(suffix, keyLen) !== key) {
                            continue;
                        }
                        suffix = clearFirstBits(suffix, keyLen);
                        if(key === bigIntFromBuffer(suffix, keyLen)) {
                            if(keyLen > 32) {
                                const bytesLeft       = Math.ceil((bitsLeft) / 8);
                                const bytesConsumed = Math.min(bytesLeft, 4);
                                const bitsConsumed  = bytesConsumed * 8;
                                res.keys.push(
                                    suffix.readUintBE(0, bytesConsumed)
                                );
                                suffix    = clearFirstBits(suffix, bitsConsumed);
                                bitsLeft -= bitsConsumed;
                            }
                            else {
                                res.keys.push(clearN(rows[i].key, keyLen, true));
                            }
                            res.data.push({
                                path: [keyLen],
                                value: BigInt(rows[i].amount),
                                suffix
                            });
                        }
                    }
                    else {
                        res.keys[i] = clearN(rows[i].key, keyLen, true);
                        res.data[i] = {
                            path: [keyLen],
                            value: BigInt(rows[i].amount),
                            suffix: suffix.subarray(4)
                        }
                    }
                }
                resolve(res);
            });
        });
    }
    checkExist(pfx: string) {
        return new Promise((resolve, reject) => {
            this.db.get<{address: string}>("SELECT `address` FROM `airdrop` WHERE `address` = ?", [pfx], (err, row) => {
                if(err) {
                    reject(err);
                }
                if(row) {
                    resolve(row.address === pfx);
                }
                else {
                    resolve(false);
                }
            });
        });
    }
    async* getAll() {
        type ResType = {address: string, path: string, amount: string};
        let   next: ResType | undefined;
        let keepGoing = true;
        const query = this.db.prepare("SELECT `address`,`amount`, `path` FROM `airdrop`");
        const getNext = () => {
            return new Promise((resolve:(value: ResType | undefined) => void, reject) => {
                query.get<ResType>((err, row) => {
                    if(err) {
                        reject(err);
                    }
                    else {
                        resolve(row);
                    }
                });
            });
        }
        do {
            next = await getNext();
            if(next !== undefined) {
                yield next;
            }
            else {
                keepGoing = false;
            }
        } while(keepGoing);
        query.finalize();
    }
}
