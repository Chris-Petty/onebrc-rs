// writing the 1brc challenge in rust after reaching 300MBps in go, which was after 40MBps in 4D
// I am using copilot for the first time. Writing this comment is so weird when copilot is suggesting the next word.
// Definitely disrupts flow of thought while I am writing.
//
// 1st - Basic struct, map and loop over each line from a file buffer.
//// 10-20MBps. WEAK. First go in go was 100MBps. But maybe copilot has given a suboptimal solution. I don't know!
//// --release! ~70MBps, 185s. Still a lot slower than go's 1st for equivalent implementation ;-)
// The file is 1.5MB. So, it should be done in 0.1s. But it is taking 0.15s. So, it is not good.
// 2nd - I will try to use the same code as go. I will use the same file and same logic. I will use the same file and same logic.
// 3rd - I will try to use the same code as go. I will use the same file and same logic. I will use the same file and same logic.
// 4th - I will try to use the same code as go. I will use the same file and same logic. I will use the same file and same logic.
// LOL copilot is insane
// Ok ok.
// 2nd - Tweak to get to golang level
// After some measuring, there was a Vec in there that seemed overkill for just getting 2 halves of a string.
// So first did what I understood and just used the iterator and that was ugly. Read all the options for a string
// And there was split_once() which I figure does nearly the same thing.
//// ~100MBps, 125s. Same class as golang now!
// 3rd - Multi threaded crudely (I guess?) like my goroutines. Also stopped co pilot here so I absorbed some docs maybe.
// So I wasn't getting results I expected. Turns out the loop, doing nothing but checking Ok(line) only pulls ~250MBps
// Not good, go in similar circumstance did 700MBps :(. Anyway...
//// ~75MBps, TOO LONG. Maybe copilot will help... maybe. Maybe a pool is better? Better ways to read? Better IPC?
//

use std::{
    collections::HashMap,
    fs,
    io::{BufRead, BufReader},
    sync::mpsc::{self, Receiver, Sender},
    thread, time,
};

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

    // open the file ../../1brc/measurements.txt
    let file = fs::File::open("../1brc/measurements.txt").unwrap();
    let reader = BufReader::new(file);

    let (tx, rx): (
        Sender<HashMap<String, CityAnalysis>>,
        Receiver<HashMap<String, CityAnalysis>>,
    ) = mpsc::channel();

    let mut chunk: Vec<String> = Vec::new();
    let mut children = Vec::new();

    for line in reader.lines() {
        if let Ok(line) = line {
            // Up to here is only 250MBps :'(
            chunk.push(line);
            if chunk.len() == 50000 {
                let thread_chunk = chunk.clone();
                let thread_tx = tx.clone();
                let child = thread::spawn(move || {
                    let mut data: HashMap<String, CityAnalysis> = HashMap::new();

                    for line in thread_chunk {
                        let parts = line.split_once(';').unwrap();
                        let city = parts.0;
                        let temperature: f32 = parts.1.parse().unwrap();
                        let entry = data.entry(city.to_string()).or_insert(CityAnalysis::new());

                        entry.min = entry.min.min(temperature);
                        entry.max = entry.max.max(temperature);
                        entry.sum += temperature;
                        entry.count += 1;
                    }
                    thread_tx.send(data).unwrap();
                });
                children.push(child);
                chunk.clear();
            }
        }
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
