#!/usr/bin/env node
import arg from 'arg';
import { StorageSqlite } from '../lib/BranchStorage';
import { NodeProcessor } from '../lib/NodeProcessor';
import { Address } from '@ton/core';

function help() {
    console.log('--database <db to read data from>');
    console.log('--root-hash <hash to check proofs agains>');
    console.log("--help, -h get this message\n\n");

    console.log(`${__filename} --root-hash <tree-hash> --database <database_path> <user address>`);
}
async function run() {
    const args = arg({
        '--database': String,
        '--root-hash': String
    });

    if(args._.length == 0) {
        console.log("Address to proof required");
        help();
        return;
    }
    if(!args['--database']) {
        console.log("database path is required");
        help();
        return;
    }

    if(!args['--root-hash']) {
        console.log("root hash is required");
        help();
        return;
    }

    const rootHash = Buffer.from(args['--root-hash'], 'hex');
    const testAddress = Address.parse(args._[0]);
    const storage = new StorageSqlite(args['--database']);
    const processor = new NodeProcessor({
        type: 'main',
        storage,
        store_depth: 16,
        airdrop_start: 1000,
        airdrop_end: 2000,
        max_parallel: 1
    });

    const proof = await processor.buildProof(testAddress);
    if(proof.refs[0].hash(0).equals(rootHash)){ 
        console.log("Proof:", proof.toBoc().toString('base64'));
        console.log("Done!");
        console.log("Proof hash:", proof.refs[0].hash(0));
    }
    else {
        // console.log("Correct count:",correctCount);
        console.log("Dang, doesn't match");
        console.log("Address:", testAddress);
        console.log("My proof:", proof);
    }
}
run();


