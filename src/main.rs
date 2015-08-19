// TODO
// - use mio non blocking IO (mioco)
// - rocksdb binding error handling could be better for get call?
// - extract (de)serialization methods to separate file
// - implement more asd methods

use std::option::Option;
use std::io::prelude::*;
use std::io::{Cursor};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::sync::{Arc};

extern crate rocksdb;
use rocksdb::{RocksDB, RocksDBResult, Writable, WriteBatch};

mod deser;
use deser::*;

fn prologue(mut stream: &mut TcpStream) -> Result<()> {
    println!("1");
    let magic = b"aLbA";
    let magic1 = try!(deser::read_bytes_raw(stream, 4));
    assert!(magic1 == magic);

    println!("2");
    let version = try!(deser::read_u32(stream));
    assert!(version == 1);

    println!("3");
    let lido = try!(deser::read_bytes_option(stream));

    let long_id = b"the_hardcoded_id";

    match lido {
        None => (),
        Some(lid) =>
            if lid == long_id {
                ()
            } else {
                // TODO reply long id
                assert!(false)
            }
    };

    try!(deser::write_u32(stream, &0));
    try!(deser::write_bytes(stream, long_id));
    try!(stream.flush());

    println!("5");
    Ok(())
}

enum AsdError {
    UnknownOperation = 4
}

fn reply_unknown(mut stream : &mut TcpStream) -> Result<()> {
    println!("replying unknown!");
    try!(deser::write_bytes(
        stream,
        &try!(deser::serialize(deser::write_u32,
                               &(AsdError::UnknownOperation as u32)))));
    try!(deser::write_u32(stream, &4));
    try!(deser::write_u32(stream,
                          &(AsdError::UnknownOperation as u32)));
    try!(stream.flush());
    Ok(())
}

#[macro_use] extern crate enum_primitive;
extern crate num;
use num::FromPrimitive;

enum_from_primitive! {
    enum Operation {
        Range = 1,
        MultiGet = 2
    }
}

fn handle_multiget(mut stream: &mut TcpStream,
                   mut cur: &mut Read,
                   db: &RocksDB) -> Result<()> {
    let keys = try!(deser::read_bytes_list(&mut cur));
    let mut res : Vec<Option<_>> = Vec::new();
    // let db = db.snapshot();
    // TODO snapshot meegeven in readoptions
    // voor de get call (needs changes in the
    // rocksdb binding...
    // yak shaving here I come?

    // also maybe provide a non atomic
    // multiget variant
    for key in keys {
        res.push(match db.get(&key) {
            RocksDBResult::Error(e) => panic!(e),
            RocksDBResult::None => Option::None,
            RocksDBResult::Some(s) => Option::Some(s)
        });
    };

    let mut buf = Vec::new();
    try!(deser::write_u32(&mut buf, &0));
    // try!(deser::write_list(&mut buf,
    //                        | writer, &rv | {
    //                            deser::write_option(
    //                                writer,
    //                                deser::write_bytes,
    //                                &rv)
    //                        },
    //                        &res2));

    try!(deser::write_bytes(stream, &buf));
    Ok(())
}

fn handle_client(mut stream: TcpStream, db: Arc<RocksDB>) -> Result<()> {
    // TODO wrap TcpStream with io::BufReader

    try!(prologue(&mut stream));

    println!("6");

    loop {
        let msg = try!(deser::read_bytes(&mut stream));
        let mut cur = Cursor::new(msg);
        println!("7");
        let op = Operation::from_u8(try!(deser::read_u8(&mut cur)));
        try!(match op {
            None => reply_unknown(&mut stream),
            Some(operation) =>
                match operation {
                    Operation::MultiGet =>
                        handle_multiget(&mut stream,
                                        &mut cur,
                                        &*db),
                    Operation::Range => reply_unknown(&mut stream)
                }
        });
        let _ = db.put(b"my key", b"my key");
        db.get(b"my key");
        let batch = WriteBatch::new();
        batch.put(b"key", b"value").unwrap();
        db.write(batch).unwrap();
    }
}

fn main() {

    let listener = TcpListener::bind("127.0.0.1:8090").unwrap();

    let db = RocksDB::open_default("/tmp/asd_rocks").unwrap();
    let dbx = Arc::new(db);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let dbx = dbx.clone();
                thread::spawn(move|| {
                    // connection succeeded
                    println!("Hello, world!");
                    handle_client(stream, dbx)
                });
            }
            Err(_) => { /* connection failed */ }
        }
    }

    drop(listener)
}
