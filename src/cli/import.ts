#!/usr/bin/env node
import sqlite3 from 'sqlite3';
import arg from 'arg';
import {open, FileHandle} from 'node:fs/promises';
import { Address } from '@ton/core';

let importFile: FileHandle;
let db: sqlite3.Database;

function runQuery (query: string) {
    return new Promise((resolve, reject) => {
        db.run(query, [], function(err) {
            if(err) {
                reject(err);
            }
            resolve(true);
        })
    });
}
async function create_table() {
    await runQuery("DROP TABLE IF EXISTS `airdrop`");
    await runQuery("DROP TABLE IF EXISTS `branches`");
    await runQuery("DROP TABLE IF EXISTS `tops`");
    await runQuery("CREATE TABLE `airdrop` (`address` TEXT, `amount` BIGINT, `key` INTEGER, `path` DEFAULT '', UNIQUE(`address`) ON CONFLICT ROLLBACK)");
    await runQuery("CREATE TABLE `branches` (`prefix` TEXT, `depth` INTEGER, `hash` TEXT, UNIQUE(`prefix`) ON CONFLICT IGNORE)");
    await runQuery("CREATE TABLE `tops` (`prefix` TEXT, `boc` TEXT, UNIQUE(`prefix`) ON CONFLICT IGNORE)");
}

function help() {
    console.log("Import airdrop csv data into database");
    console.log(`${__filename} <csv_path> <database_path>`);
}

async function run() {
    const args = arg({});

    if(args._.length == 0) {
        console.log("Path to import csv is required");
        help();
        return;
    }
    if(args._.length == 1) {
        console.log("Path to database is required");
        help();
        return;
    }

    importFile = await open(args._[0], 'r');

    db = new sqlite3.Database(args._[1]);

    await create_table();

    let insertParams: string[] = [];
    let placeholders: string[] = [];
    let recCount = 0;
    for await (const line of importFile.readLines({encoding: 'utf8'})) {
        const [addr, amount] = line.split(',');
        const parsedAddr     = Address.parse(addr);
        insertParams.push(...[parsedAddr.toRawString(), amount.toString(), Number('0x' + parsedAddr.hash.subarray(0, 4).toString('hex')).toString() ]);
        placeholders.push('(?, ?, ?)');
        if(++recCount % 10000 == 0) {
            db.run("INSERT INTO `airdrop` (`address`, `amount`, `key`) VALUES " + placeholders.join(','), insertParams);
            insertParams = [];
            placeholders = [];
        }
    }
    if(insertParams.length > 0) {
        // console.log("Inserting last batch");
        db.run("INSERT INTO `airdrop` (`address`, `amount`, `key`) VALUES " + placeholders.join(','), insertParams);
    }
    db.close();
}

run();
