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
use std::vec::*;
use std::sync::{Arc};

extern crate rocksdb;
use rocksdb::{RocksDB, RocksDBResult, Writable, WriteBatch};

extern crate byteorder;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug)]
enum Error {
    ParseError,
    ByteorderError(byteorder::Error),
    IoError(std::io::Error)
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::ParseError => write!(f, "Parse error"),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            Error::ByteorderError(ref err) => write!(f, "ByteorderError: {}", err),
        }
    }
}
impl From<std::io::Error> for Error {
    fn from(err : std::io::Error) -> Error {
        Error::IoError(err)
    }
}
impl From<byteorder::Error> for Error {
    fn from(err : byteorder::Error) -> Error {
        Error::ByteorderError(err)
    }
}
impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::ParseError => "parse error!",
            Error::IoError(ref err) => std::error::Error::description(err),
            Error::ByteorderError(ref err) => byteorder::Error::description(err),
        }
    }
}
type Result<T> = std::result::Result<T, Error>;

fn read_bytes_raw(reader : &mut Read, size : usize) ->
    Result<Vec<u8>> {

    // hmm, there's got to be a better way
    let mut res = Vec::<u8>::with_capacity(size);
    unsafe { res.set_len(size) }

    let mut read = 0;
    while read < size {
        println!("cuc1 {} {}", read, size);
        let read_extra = try!(reader.read(&mut res[read..]));
        println!("{}", read_extra);
        read += read_extra;
    };
    Ok(res)
}

fn read_bytes(reader : &mut Read) -> Result<Vec<u8>> {
    let len = try!(reader.read_u32::<LittleEndian>());
    read_bytes_raw(reader, len as usize)
}

fn read_bool(reader : &mut Read) -> Result<bool> {
    // TODO return result
    match try!(reader.read_u8()) {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Error::ParseError)
    }
}

fn read_option<F, T>(reader: &mut Read, read_t: F)
               -> Result<Option<T>>
    where F : Fn(&mut Read) -> Result<T> {
    if try!(read_bool(reader)) {
        Ok(Some(try!(read_t(reader))))
    } else {
        Ok(None)
    }

}
fn read_bytes_option(reader : &mut Read) -> Result<Option<Vec<u8>>> {
    read_option(reader, read_bytes)
}

fn read_list<F, T>(reader: &mut Read, read_t: F)
                   -> Result<Vec<T>>
    where F : Fn(&mut Read) -> Result<T> {
    let len = reader.read_u32::<LittleEndian>().unwrap();
    let mut res = Vec::new();
    for _ in 0..len {
        res.push(try!(read_t(reader)))
    }
    Ok(res)    
}

fn read_bytes_list(reader : &mut Read) -> Result<Vec<Vec<u8>>> {
    read_list(reader, read_bytes)
}

fn write_bytes(writer : &mut Write, bytes : &[u8]) {
    writer.write_u32::<LittleEndian>(bytes.len() as u32).unwrap();
    writer.write_all(bytes).unwrap();
}

fn prologue(mut stream: &mut TcpStream) -> Result<()> {
    println!("1");
    let magic = b"aLbA";
    let magic1 = read_bytes_raw(&mut stream, 4).ok().unwrap();
    assert!(magic1 == magic);

    println!("2");
    let version = stream.read_u32::<LittleEndian>().unwrap();
    assert!(version == 1);

    println!("3");
    let lido = try!(read_bytes_option(&mut stream));

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
    Ok(())
}

enum AsdError {
    UnknownOperation = 4
}

fn reply_unknown(mut stream : &mut TcpStream) {
    println!("replying unknown!");
    stream.write_u32::<LittleEndian>(4).unwrap();
    stream.write_u32::<LittleEndian>(AsdError::UnknownOperation as u32).unwrap();
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

fn handle_client(mut stream: TcpStream, db: Arc<RocksDB>) -> Result<()> {
    // TODO wrap TcpStream with io::BufReader

    try!(prologue(&mut stream));

    println!("6");

    loop {
        let msg = try!(read_bytes(&mut stream));
        let mut cur = Cursor::new(msg);
        println!("7");
        match Operation::from_u8(cur.read_u8().unwrap()) {
            None => reply_unknown(&mut stream),
            Some(operation) =>
                match operation {
                    Operation::MultiGet => {
                        let keys = try!(read_bytes_list(&mut cur));
                        let mut res = Vec::new();
                        for key in keys {
                            res.push(match db.get(&key) {
                                RocksDBResult::Error(e) => panic!(e),
                                RocksDBResult::None => Option::None,
                                RocksDBResult::Some(s) => Option::Some(s)
                            });
                        }
                        reply_unknown(&mut stream)
                    },
                    Operation::Range => reply_unknown(&mut stream)
                }
        };
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
            Err(_) => { /* connection failed */ }
        }
    }

    drop(listener)
}
