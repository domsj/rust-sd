use std::option::Option;
use std::io::prelude::*;
use std::io;
use std::error;
use std::result;
use std::vec::*;
use std::fmt;
use std::fmt::{Formatter, Display};

extern crate byteorder;
use self::byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug)]
pub enum Error {
    ParseError,
    ByteorderError(byteorder::Error),
    IoError(io::Error)
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Error::ParseError => write!(f, "Parse error"),
            Error::IoError(ref err) => write!(f, "IO error: {}", err),
            Error::ByteorderError(ref err) => write!(f, "ByteorderError: {}", err),
        }
    }
}
impl From<io::Error> for Error {
    fn from(err : io::Error) -> Error {
        Error::IoError(err)
    }
}
impl From<byteorder::Error> for Error {
    fn from(err : byteorder::Error) -> Error {
        Error::ByteorderError(err)
    }
}
impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::ParseError => "parse error!",
            Error::IoError(ref err) => error::Error::description(err),
            Error::ByteorderError(ref err) => byteorder::Error::description(err),
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

pub fn read_bytes_raw(reader: &mut Read, size : usize) ->
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

pub fn read_u32(reader : &mut Read) -> Result<u32> {
    Ok(try!(reader.read_u32::<LittleEndian>()))
}

pub fn read_u8(reader : &mut Read) -> Result<u8> {
    Ok(try!(reader.read_u8()))
}

pub fn read_bytes(reader: &mut Read) -> Result<Vec<u8>> {
    let len = { try!(reader.read_u32::<LittleEndian>()) };
    read_bytes_raw(reader, len as usize)
}

pub fn read_bool(reader : &mut Read) -> Result<bool> {
    match try!(reader.read_u8()) {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Error::ParseError)
    }
}

pub fn read_option<F, T>(reader: &mut Read, read_t: F)
               -> Result<Option<T>>
    where F : Fn(&mut Read) -> Result<T> {
    if try!(read_bool(reader)) {
        Ok(Some(try!(read_t(reader))))
    } else {
        Ok(None)
    }
}

pub fn read_bytes_option(reader : &mut Read) -> Result<Option<Vec<u8>>> {
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

pub fn read_bytes_list(reader : &mut Read) -> Result<Vec<Vec<u8>>> {
    read_list(reader, read_bytes)
}

pub fn write_u32(writer: &mut Write, i: &u32) -> Result<()> {
    Ok(try!(writer.write_u32::<LittleEndian>(*i)))
}

pub fn write_bool(writer: &mut Write, b: &bool) -> Result<()> {
    try!(writer.write_u8(if *b { 1 } else { 0 }));
    Ok(())
}

pub fn write_option<F, T>(writer: &mut Write,
                          write_t: F,
                          item_o: &Option<&T>) -> Result<()>
    where F: Fn(&mut Write, &T) -> Result<()>
{
    match *item_o {
        Option::None => write_bool(writer, &false),
        Option::Some(ref t) => {
            try!(write_bool(writer, &true));
            write_t(writer, t)
        }
    }
}

pub fn write_list<F, T>(writer: &mut Write,
                        write_t: F,
                        items: &Vec<&T>) -> Result<()>
    where F: Fn(&mut Write, &T) -> Result<()>
{
    try!(write_u32(writer, &(items.len() as u32)));
    for t in items {
        try!(write_t(writer, t))
    };
    Ok(())
}

pub fn write_bytes(writer : &mut Write, bytes : &[u8]) -> Result<()> {
    try!(writer.write_u32::<LittleEndian>(bytes.len() as u32));
    try!(writer.write_all(bytes));
    Ok(())
}

pub fn serialize<F, T>(serializer: F, item: &T) -> Result<Vec<u8>>
    where F: Fn(&mut Write, &T) -> Result<()> {
        let mut buf = Vec::new();
        try!(serializer(&mut buf, item));
        Ok(buf)
}
