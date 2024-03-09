/// writing the 1brc challenge in rust after reaching 300MBps in go, which was after 40MBps in 4D
/// I am using copilot for the first time. Writing this comment is so weird when copilot is suggesting the next word.
/// Definitely disrupts flow of thought while I am writing.
///
/// 1st - Basic struct, map and loop over each line from a file buffer.
//// 10-20MBps. WEAK. First go in go was 100MBps. But maybe copilot has given a suboptimal solution. I don't know!
//// --release! ~70MBps, 185s. Still a lot slower than go's 1st for equivalent implementation ;-)
/// The file is 1.5MB. So, it should be done in 0.1s. But it is taking 0.15s. So, it is not good.
/// 2nd - I will try to use the same code as go. I will use the same file and same logic. I will use the same file and same logic.
/// 3rd - I will try to use the same code as go. I will use the same file and same logic. I will use the same file and same logic.
/// 4th - I will try to use the same code as go. I will use the same file and same logic. I will use the same file and same logic.
/// LOL copilot is insane
/// Ok ok.
///
/// 2nd - Tweak to get to golang level
/// After some measuring, there was a Vec in there that seemed overkill for just getting 2 halves of a string.
/// So first did what I understood and just used the iterator and that was ugly. Read all the options for a string
/// And there was split_once() which I figure does nearly the same thing.
//// ~100MBps, 125s. Same class as golang now!
///
/// 3rd - Multi threaded crudely (I guess?) like my goroutines. Also stopped co pilot here so I absorbed some docs maybe.
/// So I wasn't getting results I expected. Turns out the loop, doing nothing but checking Ok(line) only pulls ~250MBps
/// Not good, go in similar circumstance did 700MBps :(. Anyway...
//// ~75MBps, TOO LONG. Maybe copilot will help... maybe. Maybe a pool is better? Better ways to read? Better IPC?
///
/// 4th - create readers of spans of the file rather than Vecs. Each thread reads from it's own file handle.
/// Copilot was a goose here so I decided to turn it off. Kept suggesting ways that I knew were slower, or making up APIs...
/// Also super annoying when you ask for a suggestion and it overwrites half your code!???
/// So, 10 threads of my Apple Macbook Pro M2 Pro go flat tack in this solution!
/// Note the default buffer size is 8KiB. I change to 4MiB, which brings us from ~450MBps (31.0s) to ~750MBps (18.5s). Beyond that seems CPU bound.
/// A quick search online reckons max read speeds from disk should be 5000MBps so I should be safe from that bottleneck.
//// ~750MBps 18.5s. Hell yeeeee.
///
use std::{
    cmp::min,
    collections::HashMap,
    fs,
    io::{BufRead, BufReader, Seek, SeekFrom},
    sync::mpsc::{self, Receiver, Sender},
    thread::{self, available_parallelism},
    time,
};

const FILE: &str = "../1brc/measurements.txt";
const WORKER_READER_BUFFER_SIZE: usize = 1024 * 1024 * 4; // 1MB is about 1s slower compared to 4MB+ empirically (19.5 vs 18.5). I think CPU bottleneck beyond there.

struct CityAnalysis {
    min: f32,
    max: f32,
    sum: f32,
    count: usize,
}

impl CityAnalysis {
    fn new() -> Self {
        CityAnalysis {
            min: f32::MAX,
            max: f32::MIN,
            sum: 0.0,
            count: 0,
        }
    }
}

fn main() {
    // start timer
    let start = time::Instant::now();
    let mut total_duration = time::Duration::new(0, 0);

    let (tx, rx): (
        Sender<HashMap<String, CityAnalysis>>,
        Receiver<HashMap<String, CityAnalysis>>,
    ) = mpsc::channel();

    let file = fs::File::open(FILE).unwrap();
    let file_length = fs::metadata(FILE).unwrap().len();
    let num_para = available_parallelism().unwrap().get() as u64;
    let chunk_size: i64 = (file_length / num_para).try_into().unwrap();

    println!("File length: {}", file_length);
    println!("Parallelism: {}", num_para);
    println!("chunk_size: {}", chunk_size);

    let mut children = Vec::new();
    let mut start_chunk = 0;
    let mut reader = BufReader::new(file);
    for i in 0..num_para {
        let mut buf = String::new(); // Have to use this because read_line requires it. skip_until would be better but is a nightly feature
        let position = reader.seek(SeekFrom::Current(chunk_size)).unwrap();
        let end_chunk = min(
            position + reader.read_line(&mut buf).unwrap() as u64,
            file_length,
        );

        println!(
            "position: {}, start {}, end {}",
            position, start_chunk, end_chunk
        );

        let thread_tx = tx.clone();
        let child = thread::spawn(move || {
            println!("start worker {}", i);

            let mut data: HashMap<String, CityAnalysis> = HashMap::new();
            let file = fs::File::open(FILE).unwrap();
            let mut reader = BufReader::with_capacity(WORKER_READER_BUFFER_SIZE, file);
            reader.seek(SeekFrom::Start(start_chunk)).unwrap();

            for line in reader.lines() {
                if let Ok(line) = line {
                    start_chunk += line.len() as u64;

                    if start_chunk >= end_chunk {
                        break;
                    }

                    let parts = line.split_once(';').unwrap();
                    let city = parts.0;
                    let temperature: f32 = parts.1.parse().unwrap();
                    let entry = data.entry(city.to_string()).or_insert(CityAnalysis::new());

                    entry.min = entry.min.min(temperature);
                    entry.max = entry.max.max(temperature);
                    entry.sum += temperature;
                    entry.count += 1;
                }
            }
            thread_tx.send(data).unwrap();
            println!("end worker {}", i);
        });

        children.push(child);
        start_chunk = end_chunk
    }

    let mut data: HashMap<String, CityAnalysis> = HashMap::new();
    for child in children {
        child.join().expect("OH NO");
        let analysis_map = rx.recv().expect("better be a map lol");

        analysis_map.iter().for_each(|(key, city)| {
            let entry = data.entry(key.to_string()).or_insert(CityAnalysis::new());

            entry.min = entry.min.min(city.min);
            entry.max = entry.max.max(city.max);
            entry.sum += city.sum;
            entry.count += city.count;
        });
    }

    // print the results
    let mut output = Vec::new();
    for (city, analysis) in data {
        // append to the output string
        output.push(format!(
            "{}={:.1}/{:.1}/{:.1}",
            city,
            analysis.min,
            analysis.sum / analysis.count as f32,
            analysis.max,
        ));
    }

    output.sort();
    let output_text = output.join(", ");
    println!("{{{}}}", output_text);

    // // end timer
    total_duration += start.elapsed();
    println!("Time: {:.2} ms", total_duration.as_millis());
}
