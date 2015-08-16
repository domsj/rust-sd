// how to start?

// - link rocksdb binding
// - open server socket
// - figure out serialization
// - brute strategy: blocking write to rocksdb

// TODO remove unwrap usages ... proper error handling!
// (there was a good blog post available about that)

use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::vec::*;
use std::sync::{Arc, Mutex};

// TODO use mio non blocking IO

extern crate rocksdb;
use rocksdb::{RocksDB, Writable, WriteBatch};

extern crate byteorder;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

fn read_bytes_raw(reader : &mut Read, size : usize) -> Vec<u8> {
    // TODO return result

    // hmm, there's got to be a better way
    let mut res = Vec::<u8>::with_capacity(size);
    unsafe { res.set_len(size) }

    let mut read = 0;
    while read < size {
        println!("cuc1 {} {}", read, size);
        let read_extra =
            reader
            .read(&mut res[read..])
            .unwrap();
        println!("{}", read_extra);
        read += read_extra;
    };
    res
}

fn read_bytes(reader : &mut Read) -> Vec<u8> {
    let len = reader.read_u32::<LittleEndian>().unwrap();
    read_bytes_raw(reader, len as usize)
}

fn read_bool(reader : &mut Read) -> bool {
    // TODO return result
    match reader.read_u8().unwrap() {
        0 => false,
        1 => true,
        _ => true               // TODO Error
    }
}

fn read_bytes_option(reader : &mut Read) -> Option<Vec<u8>> {
    if read_bool(reader) {
        Some(read_bytes(reader))
    } else {
        None
    }
}

fn write_bytes(writer : &mut Write, bytes : &[u8]) {
    writer.write_u32::<LittleEndian>(bytes.len() as u32).unwrap();
    writer.write_all(bytes).unwrap();
}

fn prologue(mut stream: &mut TcpStream) {
    println!("1");
    let magic = b"aLbA";
    let magic1 = read_bytes_raw(&mut stream, 4);
    assert!(magic1 == magic);

    println!("2");
    let version = stream.read_u32::<LittleEndian>().unwrap();
    assert!(version == 1);

    println!("3");
    let lido = read_bytes_option(&mut stream);

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

    stream.write_u32::<LittleEndian>(0).unwrap();
    write_bytes(stream, long_id);
    stream.flush().unwrap();

    println!("5");
}

enum Error {
    UnknownOperation = 4
}

fn reply_unknown(mut stream : &mut TcpStream) {
    println!("replying unknown!");
    stream.write_u32::<LittleEndian>(4).unwrap();
    stream.write_u32::<LittleEndian>(Error::UnknownOperation as u32).unwrap();
    stream.flush().unwrap();
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


fn handle_client(mut stream: TcpStream, db: Arc<Mutex<RocksDB>>) {
    // TODO wrap TcpStream with io::BufReader

    prologue(&mut stream);

    println!("6");

    loop {
        let msg = read_bytes(&mut stream);
        println!("7");
        match Operation::from_u8(msg[0]) {
            None => reply_unknown(&mut stream),
            Some(Operation::MultiGet) => reply_unknown(&mut stream),
            Some(Operation::Range) => reply_unknown(&mut stream)
        };
        let _ = db.lock().unwrap().put(b"my key", b"my key");
        db.lock().unwrap().get(b"my key");
        let batch = WriteBatch::new();
        batch.put(b"key", b"value").unwrap();
        db.lock().unwrap().write(batch).unwrap();
    }
}

fn main() {

    let listener = TcpListener::bind("127.0.0.1:8090").unwrap();

    let db = RocksDB::open_default("/tmp/asd_rocks").unwrap();
    let dbx = Arc::new(Mutex::new(db));
    // hmm, via channel alles naar rocksdb agent pushen?

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
            Err(e) => { /* connection failed */ }
        }
    }

    drop(listener)
}
