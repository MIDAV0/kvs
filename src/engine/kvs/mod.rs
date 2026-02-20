pub mod buffer;

use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::{Path, PathBuf}
};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use crate::{error::{KvsError,Result}, engine::engine::KvsEngine};

use buffer::{BufReaderWithPos, BufWriterWithPos};

#[derive(Deserialize, Serialize)]
pub enum Command {
    Set { key: String, value: String },
    Rm { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Self {
        Command::Set { key, value }
    }

    fn rm(key: String) -> Self {
        Command::Rm { key }
    }
}

struct CommandPos {
    epoch: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((epoch, range): (u64, Range<u64>)) -> CommandPos {
        CommandPos {
            epoch,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

pub struct KvStore {
    // log directory
    path: PathBuf,
    // map epoch number to the file reader
    readers: HashMap<u64, BufReaderWithPos<File>>,
    // current epoch file writer
    writer: BufWriterWithPos<File>,
    current_epoch: u64,
    // in-memory index
    index: BTreeMap<String, CommandPos>,
    // number of bytes to be deleted during compaction
    uncompacted: u64,
}

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        let epoch_list = sorted_epoch_list(&path)?;
        let mut uncompacted = 0;

        for &epoch in &epoch_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, epoch))?)?;
            uncompacted += load(epoch, &mut reader, &mut index)?;
            readers.insert(epoch, reader);
        }

        let current_epoch = epoch_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_epoch, &mut readers)?;
        
        Ok(KvStore {
            path,
            readers,
            writer,
            current_epoch,
            index,
            uncompacted
        })
    }

    /// Clears stale entries in the log.
    pub fn compact(&mut self) -> Result<()> {
        // increase current gen by 2. current_gen + 1 is for the compaction file.
        let compaction_epoch = self.current_epoch + 1;
        self.current_epoch += 2;
        self.writer = self.new_log_file(self.current_epoch)?;

        let mut compaction_writer = self.new_log_file(compaction_epoch)?;

        let mut new_pos = 0; // pos in the new log file.
        for cmd_pos in &mut self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&cmd_pos.epoch)
                .ok_or(KvsError::ReaderNotFound)?;
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }

            let mut entry_reader = reader.take(cmd_pos.len);
            let len = io::copy(&mut entry_reader, &mut compaction_writer)?;
            *cmd_pos = (compaction_epoch, new_pos..new_pos + len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;

        // remove stale log files.
        let stale_epochs: Vec<_> = self
            .readers
            .keys()
            .filter(|&&epoch| epoch < compaction_epoch)
            .cloned()
            .collect();
        for stale_epoch in stale_epochs {
            self.readers.remove(&stale_epoch);
            fs::remove_file(log_path(&self.path, stale_epoch))?;
        }
        self.uncompacted = 0;

        Ok(())
    }

    fn new_log_file(&mut self, epoch: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, epoch, &mut self.readers)
    }

    
}


impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key.clone(), value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        if let Some(old_cmd) = self.index.insert(key, (self.current_epoch, pos..self.writer.pos).into()) {
            self.uncompacted += old_cmd.len;
        }

        if self.uncompacted >= COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self.readers.get_mut(&cmd_pos.epoch).ok_or(KvsError::ReaderNotFound)?;
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                return Ok(Some(value));
            } else {
                return Err(KvsError::UnexpectedCommandType);
            }
        }
        Ok(None)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::rm(key.clone());
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            let old_cmd = self.index.remove(&key).unwrap();
            self.uncompacted += old_cmd.len;
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)            
        }
    }

}


fn log_path(dir: &Path, epoch: u64) -> PathBuf {
    dir.join(format!("{}.log", epoch))
}

fn new_log_file(
    path: &Path,
    epoch: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(path, epoch);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(epoch, BufReaderWithPos::new(File::open(path)?)?);
    Ok(writer)
}


fn load(
    epoch: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.insert(key, (epoch, pos..new_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            Command::Rm { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

fn sorted_epoch_list(path: &Path) -> Result<Vec<u64>> {
    let mut epoch_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    epoch_list.sort_unstable();
    Ok(epoch_list)
}