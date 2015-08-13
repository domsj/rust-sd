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
use std::ops::IndexMut;
use std::ops::Range;
// TODO use mio non blocking IO

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
            .read(
                &mut res
                    // TODO
                // &mut res.index_mut(
                //     Range{ start : read,
                //            end   : (size - read - 1) })
                    )
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

fn handle_client(mut stream: TcpStream) {
    // TODO wrap TcpStream with io::BufReader

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
    stream.write_u32::<LittleEndian>(long_id.len() as u32).unwrap();
    let written = stream.write(long_id).unwrap();
    assert!(written == long_id.len());
    println!("4");

    match lido {
        None => (),
        Some(lid) =>
            if lid == long_id {
                ()
            } else {
                assert!(false)
            }
    };

    println!("5");

    // TODO loop die messages leest en afhandelt
    loop {
        let msg = read_bytes(&mut stream);
    }
}

fn main() {

    let listener = TcpListener::bind("127.0.0.1:8090").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move|| {
                    // connection succeeded
                    println!("Hello, world!");
                    handle_client(stream)
                });
            }
            Err(e) => { /* connection failed */ }
        }
    }

    drop(listener)
}

