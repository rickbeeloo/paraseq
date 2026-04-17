use anyhow::Result;
use clap::Parser;
use paraseq::htslib::{self, Aux, RefRecord};
use paraseq::parallel::{ParallelProcessor, ParallelReader};
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct TagEntry {
    id: String,
    thread_id: usize,
    mm: Option<String>,
    ml: Option<Vec<u8>>,
}

#[derive(Clone)]
struct TagCollector {
    thread_id: usize,
    local: Vec<TagEntry>,
    global: Arc<Mutex<Vec<TagEntry>>>,
}
impl TagCollector {
    fn new() -> Self {
        Self {
            thread_id: 0,
            local: Vec::new(),
            global: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<'a> ParallelProcessor<RefRecord<'a>> for TagCollector {
    fn set_thread_id(&mut self, thread_id: usize) {
        self.thread_id = thread_id;
    }

    fn process_record(&mut self, record: RefRecord<'a>) -> paraseq::parallel::Result<()> {
        let bam = record.inner;
        let id = std::str::from_utf8(bam.qname()).unwrap().to_string();
        let mm = match bam.aux(b"MM") {
            Ok(Aux::String(s)) => Some(s.to_string()),
            _ => None,
        };
        let ml = match bam.aux(b"ML") {
            Ok(Aux::ArrayU8(arr)) => Some(arr.iter().collect::<Vec<u8>>()),
            _ => None,
        };
        self.local.push(TagEntry {
            id,
            thread_id: self.thread_id,
            mm,
            ml,
        });
        Ok(())
    }

    fn on_batch_complete(&mut self) -> paraseq::parallel::Result<()> {
        self.global.lock().append(&mut self.local);
        Ok(())
    }
}

#[derive(Parser)]
struct Cli {
    /// Input BAM/SAM/CRAM file (reads stdin if not provided)
    input_file: Option<String>,

    /// Number of threads to use for processing
    #[clap(short = 'T', default_value = "4")]
    num_threads: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let reader = htslib::Reader::from_optional_path(args.input_file.as_ref())?;

    let mut proc = TagCollector::new();
    reader.process_parallel(&mut proc, args.num_threads)?;

    let entries = proc.global.lock();
    let total = entries.len();
    let with_mm = entries.iter().filter(|e| e.mm.is_some()).count();
    let with_ml = entries.iter().filter(|e| e.ml.is_some()).count();

    println!("records processed : {total}");
    println!("records with MM   : {with_mm}");
    println!("records with ML   : {with_ml}");
    println!("first 5 entries:");
    for entry in entries.iter().take(5) {
        println!(
            "  [thread {}] {} MM={:?} ML={:?}",
            entry.thread_id, entry.id, entry.mm, entry.ml
        );
    }
    // To see if indeed different threads are processing different records
    println!("last 5 entries:");
    for entry in entries.iter().rev().take(5) {
        println!(
            "  [thread {}] {} MM={:?} ML={:?}",
            entry.thread_id, entry.id, entry.mm, entry.ml
        );
    }

    Ok(())
}
