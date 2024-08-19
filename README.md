# Proof Machine

## Purpose

This project inspired by [compressed jettons](https://github.com/cJetton/cJetton) and [this TEP](https://github.com/tonkeeper/TEPs2/blob/custom-payload/text/0000-jetton-offchain-payloads.md)  

intended to allow to generate merkle roots and proofs of arbitrary size trees.

## Disclaimer

This project iss **heavily** under development,
Use at your own risk, business as usual.

## Tools

`cli/buildTree` Main tool for root calculation 

``` bash

buildTree.js <database_path>
--per-worker [Approximate amount of forks processed at a time] default:(1000)
--cache-bits' [Up to that <= prefix length, each branch hash/depth is stored in db] default:(16)


npx buildTree <path to db>
```

Builds tree in chunks of approximately `per-woker` apexes, and saves the `root_hash` to the file.   


`cli/batchProof` Atempts to generate all proofs and save those to csv.

``` bash
--root-hash <tree root hash>
--output [path to csv file where branches will be saved]
--batch-size [tops(chunks of addresses) per worker
--parallel [force count of threads] (normally picked automatically)
--help, -h get this message

npx batchProof --root-hash <tree-hash> --batch-size 256 <database_path>

```
Idea is that afterwards you put this data into the key-value storage of your choice.

`cli/testProof` Builds proof for specific user address only.

``` bash
--database <db to read data from>
--root-hash <hash to check proofs agains>
--help, -h get this message

npx testProof --root-hash <tree-hash> --database <database_path> <user address>
```

Mainly for illustrative purposes.  
One could make web api or whatever based on this code.


`cli/import` imports csv to the sqlite db  
Current data structure:

``` SQL
CREATE TABLE `airdrop` (`address` TEXT, `amount` BIGINT, `key` INTEGER, `path` DEFAULT '', UNIQUE(`address`) ON CONFLICT ROLLBACK)
```

User csv should be in format `address` in raw form and it's claim `amount`.  
Rest will be populated during buildTree.  

`npx iimport <csv path> <db path>`

## Quickstart

### Build it

`npm run build`


### Import
Dump `address, amount` dat to csv and import it into db.

``` bash

npx import /path/to/test.csv test.db

```

### Build tree

``` bash

npx buildTree --per-worker 1000 --cache-bits 18 test.db

.....
Final merge...
Merging...
PfxLen: 1
Shift: 31
PFXS: [ 0, 1 ]
Root hash: fd01982f571cb919159d001e007911b824524ec25ef0b2f103a937eba37f6e2e

```

If you're dealing with very large amounts of data (100KK and above), i would recommend to use higher cache-bits parameter.

### Build proofs

``` bash

npx batchProof --batch-size 256 --output my-proofs.csv --root-hash fd01982f571cb919159d001e007911b824524ec25ef0b2f103a937eba37f6e2e /mnt/cache/airdrop.db
Processing 1000000 proofs...
Output to proofs.csv
Thread count: 4
Batch size 256
 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 0% | ETA: 1277s | 3194/1000000
 ....

```

my-proofs.csv will contain data in format `address,proof_boc_b64`

## How tree calculation works

Generation of a tree starts not from the very first prefix bit, but somewhat towards the vertexes.
How far from the root the generation would start is determined by the `--per-worker` parameter.

That parameter determines roughly how many addresses each prefix subtree would contain.
Higher the value, process starts closer to the actual root, and each round processes larger branch.
Lower the value, process starts closer to the vertexes.

Main idea is to have control over memory consumption of each worker round.
Worker takes 128 batches of branches and process them one by one, recursively traversing to the very top.

When the top(data leaf) is reached, algorithm returns recursively to the starting point,  
Then, it takes this buch of 128 prefixes, and finds recursively all the direct brenches.  
If found, it joins them together in a new pruned branch, with a shorted index.  
It does so untill only two branches left, or their count becomes odd.  
Saves such forest for the later processing, and takes next batch.

After all batches are processed, same process repeats with previously saved branches till it reducec to the root cell with both branches pruned.

## Stored data

When the algorithm reaches the top(leaf with the actual prefix data), it save the `path` for this address.
`path` is a list of indexes, indicating each fork offset on the way.

Here another important parameter kicks in - `--cache-bits`.
It determines the cacheable prefix length.

By cacheable, i mean that every branch, with pfx length `<= cache-bits` will be saved as pruned branch `hash, depth` in `branches` table.  
The `threshold length` - prefix length where `curent prefix length < cache-bits and next prefix length >= cache-bits` is saved as `branch`, but also as `top`.  
`top` is the whole subtree starting from the `cache-bits` up to the vertexes.  
All the forks above this threshold are not saved to DB and only participate in tree hash calculation.  
In the end we get sort of "merkle tree" ending with the actual `boc` at the very top.


## How proof generation works

In order to generate the proof one needs address, it's path and the top it belongs to.

`branch` table with hashes is supposed to be entirely cacheable in memory, so don't overdo with the `--cache-bits` value.

Pretty much normal merkle proof algorithm, with the exception that pruned branches values are taken from cache instead of actual calculations.
When the `threshold length` is reached, worker queues the db for the appropriate `top` address belongs to, and then proceeds with normal merkle proof algo.

## Gotchas

- Make sure starting prefix length picked by buildTree is lower than `--cache-bits` value.
- Tree calculation is yet single threaded.

## What to do if i have a different data layout?

You'll have to override the `packData` function in `NodeProcessor` for now.
Later on, we'll figure something out.

## Why tree calculation is single threaded?

You don't want to know.
But soon it will be properly threaded.
