// function to start contents of single thread
// queue to accept new fds
// from fd, make 'service'?
// accepts commands such as PartialRead(Multi), sends sth back


extern crate bytes;
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
// extern crate tokio_proto;
// extern crate tokio_service;

// step 1, implement some code
// hoe? eerst commands definieren?


use std::iter::Iterator;
use std::thread;
use std::os::unix::io::FromRawFd;

use futures::Sink;
use futures::Stream;


#[derive(Debug)]
enum ThreadCommand {
    HandleFd(std::os::unix::io::RawFd),
}

pub struct PartialReadService {
    thread_queues: Vec<futures::sync::mpsc::Sender<ThreadCommand>>,
    next: usize, // rocksdb handle? or pass directly to threads...
}

mod protocol {
    use std;
    use tokio_io::codec::Decoder;


    enum Request {
        Version,
        // PartialRead(String, Vec<(usize, usize)>),
    }

    enum Error {
        UnknownMessageType,
        // DeserialisationError,
        // KeyNotFound,
        IoError(std::io::Error),
    }

    impl std::convert::From<std::io::Error> for Error {
        fn from(e: std::io::Error) -> Error {
            Error::IoError(e)
        }
    }

    enum Response {
        Version(String),
        // PartialReadResult(Result<Vec<Option<Vec<u8>>>, Error>),
    }

    type RequestId = u64;

    enum Message {
        Notification,
        Request(RequestId, Request),
        Response(RequestId, Response),
    }

    struct MessageCodec;

    use bytes::BytesMut;
    use bytes::LittleEndian;
    use bytes::IntoBuf;
    use bytes::Buf;

    fn get_varint<T>(buf: &mut T) -> u64
        where T: Buf
    {
        let mut b = buf.get_u8();
        let mut res: u64 = 0;
        let mut shift = 0;
        while b < 0x80 {
            res += ((b & 0x7f) as u64) << shift;
            b = buf.get_u8();
            shift += 7;
        }
        res + (b << shift) as u64
    }

    impl Decoder for MessageCodec {
        type Item = Message;
        type Error = Error;


        fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<(Message)>, Error> {
            // message serialisation:
            // - 4 bytes length
            // - varint  type
            // - payload depending on type

            if buf.len() < 4 {
                return Ok(None);
            }

            let length = buf[0..4].into_buf().get_u32::<LittleEndian>();
            if buf.len() < (length as usize)
            // + 4
            {
                return Ok(None);
            }

            // TODO consumeert die into_buf automatisch al nest bytes?
            // ik ga er vanuit van niet...
            // (to be seen once we can test it...)
            // maybe start with implementing echo command first
            let message_type = get_varint(&mut buf[..].into_buf());

            let msg = match message_type {
                // merde, gebruik hier capnproto ofzo?
                // of iets van serde ? json berichten ;-)?
                1 => Message::Notification,
                2 => {
                    let request_id = (&mut buf[..]).into_buf().get_u64::<LittleEndian>();
                    // TODO deser payload
                    Message::Request(request_id, Request::Version)
                }
                3 => {
                    let request_id = (&mut buf[..]).into_buf().get_u64::<LittleEndian>();
                    // TODO deser payload
                    Message::Response(request_id, Response::Version("0.0.1".to_string()))
                }
                _ => return Result::Err(Error::UnknownMessageType),
            };

            Result::Ok(Option::Some(msg))
        }
    }
}

fn start_thread(queue: futures::sync::mpsc::Receiver<ThreadCommand>) -> () {
    thread::spawn(move || {
        // read from queue
        // run event loop
        // list for new handle_fd commands
        // listen for partial read commands from fds
        let mut reactor = tokio_core::reactor::Core::new().unwrap();
        let handle = &reactor.handle();

        let f = queue.for_each(|command| {
            match command {
                ThreadCommand::HandleFd(fd) => {
                    let tcp_stream =
                        tokio_core::net::TcpStream::from_stream(unsafe {
                                                                    FromRawFd::from_raw_fd(fd)
                                                                },
                                                                handle)
                            .unwrap();
                    // TODO, listen for commands on this tcp_stream ..
                    // tcp_stream.for_each ?
                    // handle.spawn()

                    // https://tokio.rs/docs/going-deeper-tokio/multiplex/
                    // 1) transport
                    // 2) protocol
                    // 3) service ?!? -> hook that up with handle?

                    futures::future::ok(())
                }
            }
        });

        reactor.run(f).unwrap();
    });
}

#[no_mangle]
pub extern "C" fn create_partial_read_service(count: usize) -> Box<PartialReadService> {
    let mut thread_queues = Vec::new();
    for _ in 1..count {
        let (sender, receiver) = futures::sync::mpsc::channel(1);
        start_thread(receiver);
        thread_queues.push(sender);
    }
    Box::new(PartialReadService {
        thread_queues: thread_queues,
        next: 0,
    })
}

#[no_mangle]
pub extern "C" fn handover_fd(service: PartialReadService, fd: std::os::unix::io::RawFd) -> () {
    let index = service.next % service.thread_queues.len();
    futures::sync::mpsc::Sender::wait(service.thread_queues.into_iter().nth(index).unwrap())
        .send(ThreadCommand::HandleFd(fd))
        .unwrap();
}

fn main() {}
