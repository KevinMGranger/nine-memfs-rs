mod errors;
pub use self::errors::*;

use nine::p2000::*;
use nine::ser::into_bytes;
use std::cell::RefCell;
use std::collections::hash_map::HashMap;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::iter::FusedIterator;
use std::rc::{Rc, Weak};
use utils::atomic_maybe_change;

pub trait File {
    fn kind(&self) -> FileKind;
    fn stat(&self) -> Stat;
    fn qid(&self) -> Qid {
        self.stat().qid
    }

    /// # Changable fields
    /// - Name: by anyone with write permission in the parent dir
    /// - Length: changes actual length of file
    /// - Mode, Mtime: byowner or group leader. Not the dir it
    /// - gid, owner if also a member of new group, or leader of current group
    ///
    /// "None of the other data can be altered by a wstat and attempts to change
    /// them will trigger an error. In particular, it is illegal to attempt to
    /// change the owner of a file.
    /// (These conditions may be relaxed when establishing the initial state of a
    /// file server; see Plan 9’s fsconfig(8).)"
    ///
    /// wat
    fn wstat(&mut self, user: &str, wstat: Stat) -> NineResult<()>;

    fn create(
        &mut self,
        user: &str,
        name: &str,
        perm: FileMode,
        mode: OpenMode,
    ) -> NineResult<FileInTree>;
    fn open(&mut self, user: &str, mode: OpenMode) -> NineResult<(Qid, u32)>;

    fn read(&mut self, offset: u64, buf: &mut [u8]) -> NineResult<u64>;

    // TODO: move these to dir, make them not defaultly implemented
    #[allow(unused_variables)]
    fn write(&mut self, user: &str, offset: u64, buf: &[u8]) -> NineResult<u32> {
        rerr("not writable")
    }
    #[allow(unused_variables)]
    fn truncate(&mut self, user: &str) -> NineResult<()> {
        rerr("not truncatable")
    }
}

#[derive(Clone)]
pub struct CommonFileMetaData {
    pub mode: FileMode,
    pub version: u32,
    pub path: u64,
    pub atime: u32,
    pub mtime: u32,
    pub name: CowStr, // should this just be in the hashmap? make the conversion from the entry?
    pub uid: CowStr,
    pub gid: CowStr,
    pub muid: CowStr,
}

pub struct RegularFile {
    pub content: Vec<u8>,
    pub parent: Weak<RefCell<Directory>>, // TODO: is this really proper? read paper on canonicalization
    pub meta: CommonFileMetaData,
}

impl<'a> From<&'a RegularFile> for Stat {
    fn from(file: &'a RegularFile) -> Self {
        Stat {
            type_: 1,
            dev: 2,
            qid: Qid {
                file_type: file.meta.mode.into(),
                version: file.meta.version,
                path: file.meta.path,
            },
            mode: file.meta.mode,
            atime: file.meta.atime,
            mtime: file.meta.mtime,
            length: file.content.len() as u64,
            name: file.meta.name.clone(),
            uid: file.meta.uid.clone(),
            gid: file.meta.gid.clone(),
            muid: file.meta.muid.clone(),
        }
    }
}

impl File for RegularFile {
    fn kind(&self) -> FileKind {
        FileKind::File
    }
    fn stat(&self) -> Stat {
        self.into()
    }

    // TODO: this should maybe be abstracted with a change builder object, and a policy trait.
    fn wstat(&mut self, user: &str, wstat: Stat) -> NineResult<()> {
        if let Some(new_meta) = {
            let is_owner = user == self.meta.uid;
            let my_name = self.meta.name.as_ref();
            let parent = &self.parent;
            let content = &mut self.content;
            atomic_maybe_change(&self.meta, |new_meta| {
                if wstat.mode.bits() != u32::max_value() {
                    if !is_owner {
                        return rerr("only the owner or group leader can chang a file's mode");
                    }
                    if wstat.mode.contains(FileMode::DIR) {
                        return rerr("can't change dir bit");
                    }

                    new_meta.to_mut().mode = wstat.mode;
                }

                if wstat.mtime != u32::max_value() {
                    new_meta.to_mut().mtime = wstat.mtime;
                }

                // TODO: gid

                if wstat.name.len() != 0 {
                    if let Some(parent) = parent.upgrade() {
                        let mut parent = parent.borrow_mut();
                        if parent.children.contains_key(wstat.name.as_ref()) {
                            return rerr("can't rename to existing file");
                        }
                        if let Some(file) = parent.children.remove(my_name) {
                            parent.children.insert(wstat.name.clone().into(), file);
                            new_meta.to_mut().name = wstat.name.into();
                        // TODO: update parent mtime
                        } else {
                            return Err(ServerFail::ImmediateFatal(format_err!(
                                "file's name ({}) isn't present in parent",
                                my_name
                            )));
                        }
                    } else {
                        return Err(ServerFail::ImmediateFatal(format_err!(
                            "file {} was orphaned in rc",
                            my_name
                        )));
                    }
                }

                if wstat.length != u64::max_value() {
                    if wstat.length as usize != content.len() {
                        content.resize(wstat.length as usize, 0);
                    }
                }

                Ok(())
            })?
        } {
            self.meta = new_meta;
        }

        Ok(())
    }

    fn create(
        &mut self,
        _user: &str,
        _name: &str,
        _perm: FileMode,
        _mode: OpenMode,
    ) -> NineResult<FileInTree> {
        rerr("cannot create a child of a file")
    }
    //TODO: exec
    fn open(&mut self, user: &str, mode: OpenMode) -> NineResult<(Qid, u32)> {
        let stat = self.stat();
        if mode.is_readable() && !stat.readable_for(user) {
            return rerr("file not readable");
        }
        if mode.is_writable() {
            if !stat.writable_for(user) {
                return rerr("file not writable");
            }
            if mode.contains(OpenMode::TRUNC) {
                self.truncate(user)?;
            }
        }

        Ok((stat.qid, u32::max_value()))
    }

    fn read(&mut self, offset: u64, buf: &mut [u8]) -> NineResult<u64> {
        let mut cursor = Cursor::new(&self.content);
        cursor.seek(SeekFrom::Start(offset)).unwrap();
        Ok(cursor.read(buf).unwrap() as u64)
    }

    fn write(&mut self, user: &str, offset: u64, buf: &[u8]) -> NineResult<u32> {
        self.meta.muid = user.to_string().into();
        let mut cursor = Cursor::new(&mut self.content);
        cursor.seek(SeekFrom::Start(offset)).unwrap();
        Ok(cursor.write(buf).unwrap() as u32)
    }
    fn truncate(&mut self, user: &str) -> NineResult<()> {
        self.meta.muid = user.to_string().into();
        Ok(self.content.truncate(0))
    }
}

pub struct Directory {
    pub children: HashMap<String, FileInTree>,
    pub parent: Option<Weak<RefCell<Directory>>>, // TODO: is this really proper? read paper on canonicalization
    pub meta: CommonFileMetaData,
    pub stat_bytes: Vec<u8>,
}

impl<'a> From<&'a Directory> for Stat {
    fn from(file: &'a Directory) -> Stat {
        Stat {
            type_: 1,
            dev: 2,
            qid: Qid {
                file_type: file.meta.mode.into(),
                version: file.meta.version,
                path: file.meta.path,
            },
            mode: file.meta.mode,
            atime: file.meta.atime,
            mtime: file.meta.mtime,
            length: 0,
            name: file.meta.name.clone(),
            uid: file.meta.uid.clone(),
            gid: file.meta.gid.clone(),
            muid: file.meta.muid.clone(),
        }
    }
}

impl File for Directory {
    fn kind(&self) -> FileKind {
        FileKind::Dir
    }
    fn stat(&self) -> Stat {
        self.into()
    }

    fn create(
        &mut self,
        user: &str,
        name: &str,
        perm: FileMode,
        _mode: OpenMode,
    ) -> NineResult<FileInTree> {
        if self.children.contains_key(name) {
            return rerr("a file with that name already exists");
        }

        if !self.stat().writable_for(user) {
            return rerr("no write permission for this dir");
        }

        // TODO: the fancy permissions stuff

        let meta = CommonFileMetaData {
            mode: perm,
            version: 0,
            path: 0,  // TODO
            atime: 0, // TODO
            mtime: 0, // TODO
            name: name.to_string().into(),
            uid: user.to_string().into(),
            gid: "some_group".into(), // TODO
            muid: user.to_string().into(),
        };
        let file = if perm.contains(FileMode::DIR) {
            FileInTree::Dir(Rc::new(RefCell::new(Directory {
                children: Default::default(),
                parent: None,
                stat_bytes: Default::default(),
                meta,
            })))
        } else {
            FileInTree::File(Rc::new(RefCell::new(RegularFile {
                content: Default::default(),
                parent: Weak::new(),
                meta,
            })))
        };

        self.children.insert(name.to_string(), file.clone());
        self.stat_bytes.truncate(0);

        Ok(file)
    }
    fn wstat(&mut self, user: &str, wstat: Stat) -> NineResult<()> {
        if let Some(new_meta) = {
            let is_owner = user == self.meta.uid;
            let my_name = self.meta.name.as_ref();
            let parent = &self.parent;
            atomic_maybe_change(&self.meta, |new_meta| {
                if wstat.mode.bits() != u32::max_value() {
                    if !is_owner {
                        return rerr("only the owner or group leader can chang a file's mode");
                    }
                    if !wstat.mode.contains(FileMode::DIR) {
                        return rerr("can't change dir bit");
                    }

                    new_meta.to_mut().mode = wstat.mode;
                }

                if wstat.mtime != u32::max_value() {
                    new_meta.to_mut().mtime = wstat.mtime;
                }

                // TODO: gid

                if wstat.length != u64::max_value() && wstat.length != 0 {
                    return rerr("can't resize a dir");
                }

                if wstat.name.len() != 0 {
                    if let Some(parent) = parent {
                        if let Some(parent) = parent.upgrade() {
                            let mut parent = parent.borrow_mut();
                            if parent.children.contains_key(wstat.name.as_ref()) {
                                return rerr("can't rename to existing file");
                            }
                            if let Some(file) = parent.children.remove(my_name) {
                                parent.children.insert(wstat.name.clone().into(), file);
                                new_meta.to_mut().name = wstat.name.into();
                                parent.stat_bytes.clear(); // TODO: what if there's a multi-message read in progress? How is that handled?
                                                           // TODO: update parent mtime
                            } else {
                                return Err(ServerFail::ImmediateFatal(format_err!(
                                    "dir's name ({}) isn't present in parent",
                                    my_name
                                )));
                            }
                        } else {
                            return Err(ServerFail::ImmediateFatal(format_err!(
                                "dir {} was orphaned in rc",
                                my_name
                            )));
                        }
                    } else {
                        return rerr("can't rename root file"); // or can you?
                    }
                }

                Ok(())
            })?
        } {
            self.meta = new_meta;
        }

        Ok(())
    }
    fn open(&mut self, user: &str, mode: OpenMode) -> NineResult<(Qid, u32)> {
        if mode.is_writable() {
            return rerr("can't write to a directory");
        }
        if mode.contains(OpenMode::TRUNC) {
            return rerr("can't truncate a directory");
        }
        if mode.contains(OpenMode::CLOSE) {
            return rerr("can't remove dir on close");
        }

        let stat = self.stat();
        if mode.is_readable() {
            if stat.readable_for(user) {
                Ok((stat.qid, u32::max_value()))
            } else {
                rerr("not readable")
            }
        } else {
            unreachable!()
        }
    }
    fn read(&mut self, offset: u64, buf: &mut [u8]) -> NineResult<u64> {
        if self.stat_bytes.len() == 0 {
            // make the bytes here
            for bytes in self.children
                .values()
                .map(File::stat)
                .map(|x| into_bytes(&x))
            {
                self.stat_bytes.extend(bytes);
            }
        }
        let mut cursor = Cursor::new(&self.stat_bytes);
        cursor.seek(SeekFrom::Start(offset)).unwrap();
        Ok(cursor.read(buf).unwrap() as u64)
    }
}

pub enum FileKind {
    File,
    Dir,
}

#[derive(Clone)]
pub enum FileInTree {
    File(Rc<RefCell<RegularFile>>),
    Dir(Rc<RefCell<Directory>>),
}

impl FileInTree {
    // TODO: permissions check
    pub fn walk<S, I>(
        self,
        root: Rc<RefCell<Directory>>,
        wname: I,
    ) -> impl Iterator<Item = FileInTree>
    where
        S: AsRef<str>,
        I: Iterator<Item = S>,
    {
        Walker {
            current_file: self,
            root: root,
            wname: wname.fuse(),
        }
    }

    pub fn ptr_eq(this: &FileInTree, other: &FileInTree) -> bool {
        use self::FileInTree::*;
        match (this, other) {
            (File(ref this), File(ref other)) => Rc::ptr_eq(this, other),
            (Dir(ref this), Dir(ref other)) => Rc::ptr_eq(this, other),
            _ => false,
        }
    }
    pub fn is_writable(&self) -> NineResult<()> {
        match self.kind() {
            FileKind::File => Ok(()),
            FileKind::Dir => rerr("directories are not writable"),
        }
    }

    fn set_parent(&mut self, parent: Weak<RefCell<Directory>>) {
        match self {
            FileInTree::Dir(ref mut x) => x.borrow_mut().parent = Some(parent),
            FileInTree::File(ref mut x) => x.borrow_mut().parent = parent,
        }
    }
}

impl File for FileInTree {
    fn kind(&self) -> FileKind {
        match self {
            FileInTree::File(_) => FileKind::File,
            FileInTree::Dir(_) => FileKind::Dir,
        }
    }

    fn stat(&self) -> Stat {
        match self {
            FileInTree::File(x) => x.borrow().stat(),
            FileInTree::Dir(x) => x.borrow().stat(),
        }
    }

    fn wstat(&mut self, user: &str, wstat: Stat) -> NineResult<()> {
        match self {
            FileInTree::File(ref mut x) => x.borrow_mut().wstat(user, wstat),
            FileInTree::Dir(ref mut x) => x.borrow_mut().wstat(user, wstat),
        }
    }

    fn open(&mut self, user: &str, mode: OpenMode) -> NineResult<(Qid, u32)> {
        match self {
            FileInTree::File(ref mut x) => x.borrow_mut().open(user, mode),
            FileInTree::Dir(ref mut x) => x.borrow_mut().open(user, mode),
        }
    }

    fn create(
        &mut self,
        user: &str,
        name: &str,
        perm: FileMode,
        mode: OpenMode,
    ) -> NineResult<FileInTree> {
        match self {
            FileInTree::File(ref mut x) => x.borrow_mut().create(user, name, perm, mode),
            FileInTree::Dir(ref mut x) => {
                let mut file = x.borrow_mut().create(user, name, perm, mode)?;
                file.set_parent(Rc::downgrade(x));
                Ok(file)
            }
        }
    }

    fn read(&mut self, offset: u64, buf: &mut [u8]) -> NineResult<u64> {
        match self {
            FileInTree::File(ref mut x) => x.borrow_mut().read(offset, buf),
            FileInTree::Dir(ref mut x) => x.borrow_mut().read(offset, buf),
        }
    }

    fn write(&mut self, user: &str, offset: u64, buf: &[u8]) -> NineResult<u32> {
        match self {
            FileInTree::File(ref mut x) => x.borrow_mut().write(user, offset, buf),
            FileInTree::Dir(ref mut x) => x.borrow_mut().write(user, offset, buf),
        }
    }
    fn truncate(&mut self, user: &str) -> NineResult<()> {
        match self {
            FileInTree::File(ref mut x) => x.borrow_mut().truncate(user),
            FileInTree::Dir(ref mut x) => x.borrow_mut().truncate(user),
        }
    }
}

#[derive(Clone)]
pub struct OpenView {
    pub mode: OpenMode,
    pub last_offset: u64,
}

#[derive(Clone)]
pub struct Fid {
    pub file: FileInTree,
    pub open: Option<OpenView>,
}

impl Fid {
    pub fn new(file: FileInTree) -> Self {
        Fid { file, open: None }
    }

    pub fn is_readable(&self, offset: u64) -> bool {
        match self.open {
            None => false,
            Some(ref view) => if view.mode.is_readable() {
                match self.file.kind() {
                    FileKind::Dir if view.last_offset == offset || offset == 0 => true,
                    FileKind::File => true,
                    _ => false,
                }
            } else {
                false
            },
        }
    }

    pub fn is_writable(&self) -> NineResult<()> {
        self.file.is_writable()?;
        if let Some(ref view) = self.open {
            if view.mode.is_writable() {
                Ok(())
            } else {
                rerr("fid is not open for writing")
            }
        } else {
            rerr("fid is not open")
        }
    }
}

struct Walker<S, I>
where
    S: AsRef<str>,
    I: FusedIterator<Item = S>,
{
    current_file: FileInTree,
    root: Rc<RefCell<Directory>>,
    wname: I,
}

impl<S, I> Iterator for Walker<S, I>
where
    S: AsRef<str>,
    I: FusedIterator<Item = S>,
{
    type Item = FileInTree;
    fn next(&mut self) -> Option<Self::Item> {
        use FileInTree::*;
        if let Some(name) = self.wname.next() {
            let name = name.as_ref();
            let (new_cur, ret) = match self.current_file {
                Dir(ref dir) => {
                    let bdir = dir.borrow();
                    if name == ".." {
                        if let Some(ref parent) = bdir.parent {
                            (
                                Some(FileInTree::Dir(parent.upgrade().unwrap().clone())),
                                Some(self.current_file.clone()),
                            )
                        } else if Rc::ptr_eq(&dir, &self.root) {
                            (None, Some(self.current_file.clone()))
                        } else {
                            (None, None)
                        }
                    } else if let Some(child) = bdir.children.get(name) {
                        (Some(child.clone()), Some(child.clone()))
                    } else {
                        (None, None)
                    }
                }
                File(_) => (None, None),
            };
            if let Some(new_cur) = new_cur {
                self.current_file = new_cur;
            }
            ret
        } else {
            None
        }
    }
}

impl File for Fid {
    fn kind(&self) -> FileKind {
        self.file.kind()
    }
    fn stat(&self) -> Stat {
        self.file.stat()
    }

    fn wstat(&mut self, user: &str, wstat: Stat) -> NineResult<()> {
        self.file.wstat(user, wstat)
    }

    fn create(
        &mut self,
        user: &str,
        name: &str,
        perm: FileMode,
        mode: OpenMode,
    ) -> NineResult<FileInTree> {
        if self.open.is_some() {
            return rerr("cannot create on an open fid")
        }

        self.file.create(user, name, perm, mode)
    }

    /// Changes open mode of file
    fn open(&mut self, user: &str, mode: OpenMode) -> NineResult<(Qid, u32)> {
        if self.open.is_some() {
            return rerr("file is already open");
        }

        let res = self.file.open(user, mode)?;
        self.open = Some(OpenView {
            mode: mode,
            last_offset: 0,
        });

        Ok(res)
    }

    fn read(&mut self, offset: u64, buf: &mut [u8]) -> NineResult<u64> {
        match self.file {
            FileInTree::File(ref mut x) => match self.open {
                Some(ref view) if view.mode.is_readable() => x.borrow_mut().read(offset, buf),
                _ => panic!(), // TODO: rerror
            },
            FileInTree::Dir(ref mut x) => match self.open {
                Some(ref mut view) if offset == 0 || offset == view.last_offset => {
                    let len = x.borrow_mut().read(offset, buf)?;
                    view.last_offset += len;
                    Ok(len)
                }
                _ => {
                    panic!() // TODO: rerror
                }
            },
        }
    }

    // TODO: make sure we're standards compliant for when permissions are checked
    fn write(&mut self, user: &str, offset: u64, buf: &[u8]) -> NineResult<u32> {
        self.is_writable()?;

        self.file.write(user, offset, buf)
    }

    fn truncate(&mut self, user: &str) -> NineResult<()> {
        self.file.truncate(user)
    }
}
