pub mod buffer;

use std::{
    cell::RefCell,
    collections::BTreeMap,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}
};
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use crate::error::{KvsError,Result};
use super::KvsEngine;


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

#[derive(Debug, Clone, Copy)]
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

/// Threadsafe KvStore struct
#[derive(Clone)]
pub struct KvStore {
    index: Arc<SkipMap<String, CommandPos>>, // shared in memory index
    reader: KvsReader,                       // reader struct
    writer: Arc<Mutex<KvsWriter>>,           // shared writer struct 
}


struct KvsWriter {
    path: Arc<PathBuf>,                         // path to the logs directory   
    index: Arc<SkipMap<String, CommandPos>>,    // in memory index
    writer: BufWriterWithPos<File>,             // file writer
    current_epoch: u64,                         // current epoch
    uncompacted: u64,                           // uncompacted size
    reader: KvsReader,                          // reader struct
}

struct KvsReader {
    path: Arc<PathBuf>,                                         // path to the logs directory
    safe_epoch: Arc<AtomicU64>,                                 // epoch before which all epochs are stale
    readers: RefCell<BTreeMap<u64, BufReaderWithPos<File>>>,    // collection of file readers
}

impl Clone for KvsReader {
    // cloning maintaince the file path, safe_epoch and creates new collection of readers
    fn clone(&self) -> Self {
        KvsReader {
            path: Arc::clone(&self.path),
            safe_epoch: Arc::clone(&self.safe_epoch),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;

        let mut readers = BTreeMap::new();
        let index = Arc::new(SkipMap::new());

        let epoch_list = sorted_epoch_list(&path)?;
        let mut uncompacted = 0;

        // init reader for each epoch file
        for &epoch in &epoch_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, epoch))?)?;
            uncompacted += load(epoch, &mut reader, &*index)?;
            readers.insert(epoch, reader);
        }

        // latest epoch
        let current_epoch = epoch_list.last().unwrap_or(&0) + 1;
        // init new writer for the latest epoch
        let writer = new_log_file(&path, current_epoch)?;
        // init safe epoch
        let safe_epoch = Arc::new(AtomicU64::new(0));

        let reader = KvsReader {
            path: path.clone(),
            safe_epoch: safe_epoch.clone(),
            readers: RefCell::new(readers),
        };

        let writer = KvsWriter {
            path: path,
            index: index.clone(),
            writer: writer,
            current_epoch: current_epoch,
            uncompacted: uncompacted,
            reader: reader.clone()
        };
        
        Ok(KvStore {
            index: index,
            writer: Arc::new(Mutex::new(writer)),
            reader: reader,
        })
    }
    
}


impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer
            .lock().unwrap().set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            if let Command::Set { value, .. } = self.reader.read_and(*cmd_pos.value(), |reader| {
                Ok(serde_json::from_reader(reader)?)
            })? {
                return Ok(Some(value));
            } else {
                return Err(KvsError::UnexpectedCommandType);
            }
        }
        Ok(None)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer
            .lock().unwrap().remove(key)
    }
}

impl KvsReader {

    /// Clears stale epoch readers from collection
    fn clear_stale_epochs(&self) {
        let mut readers = self.readers.borrow_mut();

        while !readers.is_empty() {
            if let Some(entry) = readers.first_entry() {
                if *entry.key() >= self.safe_epoch.load(Ordering::SeqCst) {
                    break;
                }
                entry.remove();
            }
        }
    }

    /// Reads from corresponding reader and passes the target reader to the provided function
    /// 1. Clears stale epochs
    /// 2. Creates a file and a reader if it doesn't exists for target epoch
    /// 3. Reads from file at specified position
    /// 4. Passes reader to the provided function
    fn read_and<F, R>(&self, cmd_pos: CommandPos, f: F) -> Result<R>
    where 
        F: FnOnce(io::Take<&mut BufReaderWithPos<File>>) -> Result<R>
    {

        self.clear_stale_epochs();

        let mut readers = self.readers
            .borrow_mut();

        if !readers.contains_key(&cmd_pos.epoch) {
            readers.insert(cmd_pos.epoch, BufReaderWithPos::new(File::open(log_path(&self.path, cmd_pos.epoch))?)?);
        }

        let reader = readers.get_mut(&cmd_pos.epoch).ok_or(KvsError::ReaderNotFound)?;
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let cmd_reader = reader.take(cmd_pos.len);
        f(cmd_reader)
    }
}

impl KvsWriter {
    /// Sets key-value
    /// 1. Writes set command to the file
    /// 2. Updates index with key and new command position in file
    /// 3. Updates uncompacted counter with the len of previous set command if exists
    /// 4. Performs compaction if threshold reached
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self.index.get(&key) {
                // removes the previous set command
                self.uncompacted += old_cmd.value().len; 
            }
            self.index
                .insert(key, (self.current_epoch, pos..self.writer.pos).into());         
        }
        if self.uncompacted >= COMPACTION_THRESHOLD {
            self.compact()?;
        }   

        Ok(())
    }

    /// Removes key
    /// 1. Writes remove command to the file
    /// 2. Updates uncompacted counter with the len of previous set command
    /// 3. Updates uncompacted counter with the len of currently written remove command
    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::rm(key);
            let pos = self.writer.pos;
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Rm { key } = cmd {
                let old_cmd = self.index.remove(&key).unwrap();
                // removes the previous set command
                self.uncompacted += old_cmd.value().len;
                // removes the currently written remove command
                self.uncompacted += self.writer.pos - pos;             
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)            
        }
    }

    /// Clears stale entries in the log.
    fn compact(&mut self) -> Result<()> {
        // increase current epoch by 2. current_epoch + 1 is for the compaction file
        let compaction_epoch = self.current_epoch + 1;
        self.current_epoch += 2;
        
        // init writer for new epoch
        self.writer = new_log_file(&self.path, self.current_epoch)?;
        // init writer for compacted data
        let mut compaction_writer = new_log_file(&self.path, compaction_epoch)?;

        let mut new_pos = 0; // pos in the new log file

        // iterate through the index (to fetch data from readers) and compact data from stale epochs into compaction writer
        for cmd_pos in self.index.iter() {
            let len = self.reader.read_and(*cmd_pos.value(), |mut entry_reader| {
                // copy to writer
                Ok(io::copy(&mut entry_reader, &mut compaction_writer)?)
            })?;

            // update command position in the index
            self.index.insert(
                cmd_pos.key().clone(),
                (compaction_epoch, new_pos..new_pos + len).into()
            );
            new_pos += len;
        }
        compaction_writer.flush()?;

        // update safe_epoch point
        self.reader
            .safe_epoch
            .store(compaction_epoch, Ordering::SeqCst);

        self.reader.clear_stale_epochs();

        // id stale log files
        let stale_epoch_list: Vec<u64> = sorted_epoch_list(&self.path)?
            .into_iter()
            .filter(|&e| e < compaction_epoch)
            .collect();

        // remove stale log files
        for stale_epoch in stale_epoch_list {
            let file_path = log_path(&self.path, stale_epoch);
            if let Err(e) = fs::remove_file(&file_path) {
                slog::error!(slog_scope::logger(), "File cannot be deleted"; "file" => file_path.to_str(), "error" => e);
            }
        }
        self.uncompacted = 0;

        Ok(())
    }

}

fn log_path(dir: &Path, epoch: u64) -> PathBuf {
    dir.join(format!("{}.log", epoch))
}

fn new_log_file(
    path: &Path,
    epoch: u64,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(path, epoch);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    Ok(writer)
}


fn load(
    epoch: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &SkipMap<String, CommandPos>
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.get(&key) {
                   uncompacted += old_cmd.value().len; 
                }
                index.insert(key, (epoch, pos..new_pos).into());
            }
            Command::Rm { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.value().len;
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