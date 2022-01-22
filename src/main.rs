use std::{
    ffi::OsStr,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use clap::{crate_version, App, Arg};
use fuser::{
    Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyWrite, Request,
    TimeOrNow,
};
use futures::executor::block_on;
use pyxis_fs_common::transact;
use quinn::{Endpoint, NewConnection, TransportConfig};

const TTL: Duration = Duration::from_secs(1); // 1 second

struct PyxisFS {
    conn: NewConnection,
}

impl PyxisFS {
    pub fn new(server: &str) -> Self {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(&rustls::Certificate(
                std::fs::read("../pyxis-key.pub").unwrap(),
            ))
            .unwrap();

        let mut client_crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(roots)
            .with_no_client_auth();

        client_crypto.alpn_protocols = <&[&[u8]]>::from(&[b"hq-29"])
            .iter()
            .map(|&x| x.into())
            .collect();

        let mut client_config = quinn::ClientConfig::new(std::sync::Arc::new(client_crypto));

        let mut txpt = TransportConfig::default();
        txpt.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
        client_config.transport = Arc::new(txpt);

        let mut endpoint = Endpoint::client("[::]:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(client_config);

        let mut conn = block_on(
            endpoint
                .connect(SocketAddr::new(server.parse().unwrap(), 4413), "pyxis")
                .unwrap(),
        )
        .unwrap();

        let rsp = block_on(transact(
            &mut conn.connection,
            &pyxis_fs_common::Request::Mount("Mounting PyxisFS\n".to_string()),
        ));
        if let pyxis_fs_common::Response::Mount(v) = rsp {
            println!("Mount id: {}", v);
            Self { conn }
        } else {
            panic!();
        }
    }
}

impl Filesystem for PyxisFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::Lookup(parent, name.to_str().unwrap().to_owned()),
        ));
        match rsp {
            pyxis_fs_common::Response::Lookup(attr) => reply.entry(&TTL, &attr.into(), 0),
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::Getattr(ino),
        ));
        match rsp {
            pyxis_fs_common::Response::Getattr(attr) => reply.attr(&TTL, &attr.into()),
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::Read(ino, offset, size),
        ));
        match rsp {
            pyxis_fs_common::Response::Read(v) => reply.data(&v),
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::ReadDir(ino, offset),
        ));
        match rsp {
            pyxis_fs_common::Response::ReadDir(entries) => {
                for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
                    if reply.add(entry.0, (i + 1) as i64, entry.1.into(), entry.2) {
                        break;
                    }
                }
                reply.ok();
            }
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }
    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::Readlink(ino),
        ));
        match rsp {
            pyxis_fs_common::Response::Readlink(v) => reply.data(&v),
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::Write(ino, offset, data.to_vec()),
        ));
        match rsp {
            pyxis_fs_common::Response::Write(count) => reply.written(count),
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::Mknod(
                parent,
                name.to_str().unwrap().to_owned(),
                mode,
                umask,
                rdev,
            ),
        ));
        match rsp {
            pyxis_fs_common::Response::Mknod(attr) => reply.entry(&TTL, &attr.into(), 0),
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::Mkdir(
                parent,
                name.to_str().unwrap().to_owned(),
                mode,
                umask,
            ),
        ));
        match rsp {
            pyxis_fs_common::Response::Mkdir(attr) => reply.entry(&TTL, &attr.into(), 0),
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::Unlink(parent, name.to_str().unwrap().to_owned()),
        ));
        match rsp {
            pyxis_fs_common::Response::Unlink => reply.ok(),
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }
    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::Unlink(parent, name.to_str().unwrap().to_owned()),
        ));
        match rsp {
            pyxis_fs_common::Response::Unlink => reply.ok(),
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }
    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::Rename(
                parent,
                name.to_str().unwrap().to_owned(),
                newparent,
                newname.to_str().unwrap().to_owned(),
                flags,
            ),
        ));
        match rsp {
            pyxis_fs_common::Response::Rename => reply.ok(),
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<std::time::SystemTime>,
        fh: Option<u64>,
        crtime: Option<std::time::SystemTime>,
        chgtime: Option<std::time::SystemTime>,
        bkuptime: Option<std::time::SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let atime = match atime {
            None => None,
            Some(TimeOrNow::SpecificTime(t)) => Some(t),
            Some(TimeOrNow::Now) => Some(SystemTime::now()),
        };
        let mtime = match mtime {
            None => None,
            Some(TimeOrNow::SpecificTime(t)) => Some(t),
            Some(TimeOrNow::Now) => Some(SystemTime::now()),
        };

        let rsp = block_on(transact(
            &mut self.conn.connection,
            &pyxis_fs_common::Request::Setattr(
                ino, mode, uid, gid, size, atime, mtime, ctime, fh, crtime, chgtime, bkuptime,
                flags,
            ),
        ));
        match rsp {
            pyxis_fs_common::Response::Setattr(attr) => reply.attr(&TTL, &attr.into()),
            pyxis_fs_common::Response::Error(e) => reply.error(e),
            _ => panic!(),
        }
    }
}

#[tokio::main]
async fn main() {
    let matches = App::new("pyxis")
        .version(crate_version!())
        .author("chordtoll")
        .arg(
            Arg::new("MOUNT_POINT")
                .required(true)
                .index(1)
                .help("Act as a client, and mount FUSE at given path"),
        )
        .arg(
            Arg::new("SERVER")
                .required(true)
                .index(2)
                .help("Act as a client, and mount FUSE at given path"),
        )
        .arg(
            Arg::new("auto_unmount")
                .long("auto_unmount")
                .help("Automatically unmount on process exit"),
        )
        .arg(
            Arg::new("allow-root")
                .long("allow-root")
                .help("Allow root user to access filesystem"),
        )
        .get_matches();
    env_logger::init();
    let mountpoint = matches.value_of("MOUNT_POINT").unwrap();
    let server = matches.value_of("SERVER").unwrap();
    let mut options = vec![MountOption::RW, MountOption::FSName("pyxis".to_string())];
    options.push(MountOption::AutoUnmount);
    options.push(MountOption::AllowRoot);
    fuser::mount2(PyxisFS::new(server), mountpoint, &options).unwrap();
}
