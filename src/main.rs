//! So where does this comment end up?

extern crate byteorder;
extern crate nine;
#[macro_use]
extern crate failure;

mod server;
mod utils;

use byteorder::{LittleEndian, WriteBytesExt};
use nine::common::MessageTypeId;
use nine::de::*;
use nine::p2000::*;
use nine::ser::*;
use server::*;
use std::cell::RefCell;
use std::collections::hash_map::HashMap;
use std::env::args;
use std::env::var;
use std::io::prelude::*;
use std::io::{self, Cursor};
use std::os::unix::net::UnixListener;
use std::rc::{Rc, Weak};

macro_rules! match_message {
    {
        ($self:ident, $serializer:ident, $received_discriminant:ident)
        $($message_type:ty[$pat:pat] => $func:ident),*
    } => {
        match $received_discriminant {
            $(
            $pat => {
                let msg: $message_type = $self.read_a().unwrap();
                let tag = msg.tag;
                println!("<{:?}", &msg);
                let type_id = match $self.$func(msg) {
                    Ok(response) => {
                        println!(">{:?}", &response);
                        response.serialize(&mut $serializer).unwrap();
                        response.msg_type_id()
                    },
                    Err(ServerFail::NonFatal(response)) => {
                        let response = Rerror { tag: tag, ename: format!("{}", response).into() };
                        println!(">{:?}", &response);
                        response.serialize(&mut $serializer).unwrap();
                        response.msg_type_id()
                    },
                    e => { e.unwrap(); 0}
                };

                let buf = $serializer.into_writer().into_inner();

                $self.write_msg(type_id, buf).unwrap();
            },
            )*
            _ => {
                eprintln!("unknown msg type {}", $received_discriminant);
            }
        }
    }
}

struct Server<Stream>
where
    for<'a> &'a Stream: Read + Write,
{
    stream: Stream,
    fids: HashMap<u32, Fid>,
    root: Rc<RefCell<Directory>>,
    user: Option<String>,
}

impl<Stream> Server<Stream>
where
    for<'a> &'a Stream: Read + Write,
{
    fn read_a<'de, T: Deserialize<'de>>(&self) -> std::result::Result<T, DeError> {
        let mut deserializer = ReadDeserializer(&self.stream);
        Deserialize::deserialize(&mut deserializer)
    }

    fn write_msg(&self, mtype: u8, buf: &[u8]) -> io::Result<()> {
        let writer = &mut &self.stream;
        let msize = buf.len() as u32 + 5;
        writer.write_u32::<LittleEndian>(msize)?;

        // println!(
        //     "writing msg of length {} and type {}: {:?}",
        //     msize, mtype, buf
        // );
        Write::write_all(writer, &[mtype])?;
        Ok(Write::write_all(writer, buf)?)
    }
    fn handle_client(&mut self) {
        let mut write_buffer = Vec::new();

        let quit_msg = loop {
            write_buffer.clear();
            let mut serializer = WriteSerializer::new(Cursor::new(&mut write_buffer));
            let _mlen: u32 = match self.read_a::<u32>() {
                Ok(x) => x,
                Err(derror) => if derror.0.get_ref().is_eof() {
                    break "Client disconnected";
                } else {
                    panic!(derror)
                },
            };

            let mtype: u8 = self.read_a().unwrap();
            match_message! {
                (self, serializer, mtype)
                Tversion[Tversion::MSG_TYPE_ID] => version,
                Tauth[Tauth::MSG_TYPE_ID] => auth,
                Tattach[Tattach::MSG_TYPE_ID] => attach,
                Tflush[Tflush::MSG_TYPE_ID] => flush,
                Twalk[Twalk::MSG_TYPE_ID] => walk,
                Topen[Topen::MSG_TYPE_ID] => open,
                Tcreate[Tcreate::MSG_TYPE_ID] => create,
                Tread[Tread::MSG_TYPE_ID] => read,
                Twrite[Twrite::MSG_TYPE_ID] => write,
                Tclunk[Tclunk::MSG_TYPE_ID] => clunk,
                Tremove[Tremove::MSG_TYPE_ID] => remove,
                Tstat[Tstat::MSG_TYPE_ID] => stat,
                Twstat[Twstat::MSG_TYPE_ID] => wstat
            }
        };

        println!("{}", quit_msg);
    }

    fn version(&mut self, msg: Tversion) -> NineResult<Rversion> {
        Ok(Rversion {
            tag: msg.tag,
            msize: msg.msize,
            version: "9P2000".into(),
        })
    }

    fn auth(&mut self, _msg: Tauth) -> NineResult<Rauth> {
        rerr("no auth needed")
    }

    fn attach(&mut self, msg: Tattach) -> NineResult<Rattach> {
        self.fids.insert(
            msg.fid,
            Fid {
                open: None,
                file: FileInTree::Dir(Rc::clone(&self.root)),
            },
        );
        self.user = Some(msg.uname.into_owned());
        Ok(Rattach {
            tag: msg.tag,
            qid: self.root.borrow().qid(),
        })
    }

    fn flush(&mut self, msg: Tflush) -> NineResult<Rflush> {
        Ok(Rflush { tag: msg.tag })
    }

    fn walk(&mut self, msg: Twalk) -> NineResult<Rwalk> {
        if self.fids.contains_key(&msg.newfid) {
            rerr("newfid already in use")
        } else if let Some(current_fid) = self.fids.get(&msg.fid).cloned() {
            if current_fid.open.is_some() {
                rerr("cannot walk from an open fid")
            } else {
                match msg.wname.len() {
                    0 => {
                        self.fids.insert(msg.newfid, current_fid);
                        Ok(Rwalk {
                            tag: msg.tag,
                            wqid: vec![],
                        })
                    }
                    n => {
                        let mut qids = Vec::with_capacity(n);
                        for (i, file) in current_fid
                            .file
                            .walk(self.root.clone(), msg.wname.iter())
                            .enumerate()
                        {
                            qids.push(file.qid());
                            if i == n - 1 {
                                self.fids.insert(msg.newfid, Fid::new(file));
                            }
                        }

                        if qids.len() == 0 && n != 1 {
                            rerr("couldn't walk first element")
                        } else {
                            Ok(Rwalk {
                                tag: msg.tag,
                                wqid: qids,
                            })
                        }
                    }
                }
            }
        } else {
            rerr("unknown fid for walk")
        }
    }

    fn open(&mut self, msg: Topen) -> NineResult<Ropen> {
        if let Some(ref mut fid) = self.fids.get_mut(&msg.fid) {
            let (qid, iounit) = fid.open(self.user.as_ref().unwrap().as_ref(), msg.mode)?;
            Ok(Ropen {
                tag: msg.tag,
                qid,
                iounit,
            })
        } else {
            rerr("unknown fid")
        }
    }

    fn create(&mut self, msg: Tcreate) -> NineResult<Rcreate> {
        if let Some(ref mut fid) = self.fids.get_mut(&msg.fid) {
            let file = fid.create(
                self.user.as_ref().unwrap().as_ref(),
                msg.name.as_ref(),
                msg.perm,
                msg.mode,
            )?;
            Ok(Rcreate {
                tag: msg.tag,
                qid: file.qid(),
                iounit: u64::max_value(),
            })
        } else {
            rerr("unknown fid")
        }
    }

    fn read(&mut self, msg: Tread) -> NineResult<Rread> {
        // TODO: move the errors to the file impl and perhaps convert
        match self.fids.get_mut(&msg.fid) {
            Some(ref mut fid) => if fid.is_readable(msg.offset) {
                let mut buf = vec![0; msg.count as usize];
                let amount = fid.read(msg.offset, buf.as_mut_slice())?;
                buf.truncate(amount as usize);
                Ok(Rread {
                    tag: msg.tag,
                    data: Data(buf),
                })
            } else {
                rerr("invalid open mode")
            },
            _ => rerr("unknown fid"),
        }
    }

    fn write(&mut self, msg: Twrite) -> NineResult<Rwrite> {
        match self.fids.get_mut(&msg.fid) {
            Some(ref mut fid) => Ok(Rwrite {
                tag: msg.tag,
                count: fid.write(
                    self.user.as_ref().unwrap().as_ref(),
                    msg.offset,
                    msg.data.as_ref(),
                )?,
            }),
            _ => rerr("unknown fid"),
        }
    }

    fn clunk(&mut self, msg: Tclunk) -> NineResult<Rclunk> {
        self.fids.remove(&msg.fid);
        Ok(Rclunk { tag: msg.tag })
    }

    fn remove(&mut self, _msg: Tremove) -> NineResult<Rremove> {
        unimplemented!()
    }

    fn stat(&mut self, msg: Tstat) -> NineResult<Rstat> {
        if let Some(ref fid) = self.fids.get(&msg.fid) {
            Ok(Rstat {
                tag: msg.tag,
                stat: fid.file.stat(),
            })
        } else {
            rerr("unknown fid for stat")
        }
    }

    fn wstat(&mut self, msg: Twstat) -> NineResult<Rwstat> {
        if let Some(ref mut fid) = self.fids.get_mut(&msg.fid) {
            fid.wstat(self.user.as_ref().unwrap().as_ref(), msg.stat)?;
            Ok(Rwstat { tag: msg.tag })
        } else {
            rerr("unknown fid for wstat")
        }
    }
}

fn main() {
    let bind_path = args().nth(1).unwrap();

    let listener = {
        let x = UnixListener::bind(&bind_path);
        if x.is_err() {
            std::fs::remove_file(&bind_path).unwrap();
            UnixListener::bind(&bind_path).unwrap()
        } else {
            x.unwrap()
        }
    };

    let whoami = var("USER").unwrap();

    let foo_file = Rc::new(RefCell::new(RegularFile {
        content: String::from("Hello (from rust), world!\n").into_bytes(),
        parent: Weak::new(),
        meta: CommonFileMetaData {
            version: 10,
            path: 11,
            mode: FileMode::OWNER_READ
                | FileMode::GROUP_READ
                | FileMode::OTHER_READ
                | FileMode::OTHER_WRITE,
            atime: 12,
            mtime: 13,
            name: "foo".into(),
            uid: whoami.clone().into(),
            gid: "group".into(),
            muid: "modifier".into(),
        },
    }));
    let root = Rc::new(RefCell::new(Directory {
        parent: None,
        stat_bytes: Vec::new(),
        meta: CommonFileMetaData {
            version: 8,
            path: 9,
            mode: FileMode::DIR
                | FileMode::OWNER_READ
                | FileMode::GROUP_READ
                | FileMode::OTHER_READ
                | FileMode::OTHER_WRITE,
            atime: 3,
            mtime: 4,
            name: "root".into(),
            uid: whoami.into(),
            gid: "gid".into(),
            muid: "muid".into(),
        },
        children: HashMap::new(),
    }));

    foo_file.borrow_mut().parent = Rc::downgrade(&root);
    root.borrow_mut()
        .children
        .insert("foo".into(), FileInTree::File(foo_file));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let mut server = Server {
                    stream,
                    fids: HashMap::new(),
                    root: root.clone(),
                    user: None,
                };
                server.handle_client();
            }
            Err(err) => {
                eprintln!("{}", err);
                break;
            }
        }
    }
}
