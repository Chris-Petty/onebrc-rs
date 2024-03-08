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

use std::io::BufRead;
use std::io::BufReader;
use std::vec; // Pretty sure we don't have to import this in rust. Copilot is wrong here.

struct CityAnalysis {
    min: f32,
    max: f32,
    sum: f32,
    count: usize,
}

impl CityAnalysis {
    fn new(temperature: f32) -> Self {
        CityAnalysis {
            min: temperature,
            max: temperature,
            sum: temperature,
            count: 1,
        }
    }
}

fn main() {
    // start timer
    let start = std::time::Instant::now();
    // open the file ../../1brc/measurements.txt
    let file = std::fs::File::open("../1brc/measurements.txt").unwrap();
    let reader = BufReader::new(file);

    let mut data: std::collections::HashMap<String, CityAnalysis> =
        std::collections::HashMap::new();

    for line in reader.lines() {
        if let Ok(line) = line {
            let parts: Vec<&str> = line.split(';').collect();
            let city = parts[0];
            let temperature: f32 = parts[1].parse().unwrap();
            let entry = data
                .entry(city.to_string())
                .or_insert(CityAnalysis::new(temperature));

            entry.min = entry.min.min(temperature);
            entry.max = entry.max.max(temperature);
            entry.sum += temperature;
            entry.count += 1;
        }
    }

    let mut output: Vec<String> = vec![];
    // print the results
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

    // end timer
    let duration = start.elapsed();

    println!("{{{}}}", output_text);
    println!("Time: {:.2} seconds", duration.as_secs_f32());
}
