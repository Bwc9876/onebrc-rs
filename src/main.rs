use std::{arch::asm, collections::HashMap, fs::File, os::fd::AsRawFd};

use fx_hash::FxHasher;

type Row = (i32, i32, i32, usize); // (min, max, sum, count)
type RowMap<'a> = HashMap<&'a str, Row, FxHasher>; // (min, max, sum, count)

const SEPARATOR: char = ';';
const FILE_NAME: &str = "measurements.txt";
const MAP_CAPACITY: usize = 10_000; // Taken from the problem description, "There is a maximum of 10,000 unique station names."
const PAGE_SIZE: u64 = 4096;

#[inline]
const fn get_page_round_up(n: u64) -> u64 {
    (n + PAGE_SIZE - 1) & !(PAGE_SIZE - 1)
}

fn main() {
    let instant = std::time::Instant::now();

    let num_threads: usize = std::thread::available_parallelism()
        .expect("Error getting number of threads")
        .into();
    let file = File::open(FILE_NAME).expect("File not found");
    let file_len = file.metadata().expect("Error getting file metadata").len();

    let file_len_rounded = get_page_round_up(file_len);

    let fd = file.as_raw_fd();

    let mut start = 9_usize;

    unsafe {
        // Map the file to memory, this will allow us to read the file without using the file system
        asm!(
            "syscall",
            inout("rax") start, // syscall number (in), return value (out)
            in("rdi") 0, // Address of the memory mapping
            in("rsi") file_len_rounded, // Length of the file, aligned to page size
            in("rdx") 1, // Read permission
            in("r10") 2, // Flags
            in("r8") fd, // File descriptor
            in("r9") 0, // Offset, aligned to page size
        );
        if (start as i64) <= 0 {
            panic!("Error mapping file ({start})");
        }
    }

    // We don't need the file handle anymore
    drop(file);

    let chunk_len = (file_len as usize) / num_threads;

    let mut slices = Vec::with_capacity(num_threads);

    let global_end = start + (file_len as usize) - 1;

    eprintln!(
        "Total range: 0x{:x} - 0x{:x}",
        start,
        start + (file_len as usize) - 1
    );

    // Create slices for each thread, finding the next newline for the end
    for i in 0..num_threads {
        let end = if i == num_threads - 1 {
            global_end
        } else {
            let mut i = start + chunk_len;
            while unsafe { *((i) as *mut u8) } != 10 {
                // Newline
                i += 1;
                if i > global_end {
                    panic!("Error finding newline");
                }
            }
            i
        };

        eprintln!("Thread {}: 0x{:x} - 0x{:x}", i + 1, start, end);

        let slice = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                start as *const u8,
                end - start,
            ))
        };

        slices.push(slice);
        start = end + 1;
    }

    eprintln!(
        "====== Init took {} ms ======",
        instant.elapsed().as_millis()
    );

    let instant = std::time::Instant::now();

    let map_capacity = MAP_CAPACITY / num_threads;

    let row_map = slices
        .into_iter()
        .enumerate()
        .map(|(t, slice)| {
            let handle = std::thread::spawn(move || {
                eprintln!("Thread {} started", t + 1);
                let mut row_map =
                    RowMap::with_capacity_and_hasher(map_capacity, FxHasher::default());
                for line in slice.lines() {
                    let (name, temp) = line.split_once(SEPARATOR).expect("Error splitting line");
                    let temp = (temp
                        .trim()
                        .parse::<f32>()
                        .expect("Error parsing temperature")
                        * 10.0) as i32;

                    row_map
                        .entry(name)
                        .and_modify(|entry| {
                            if temp < entry.0 {
                                entry.0 = temp;
                            } else if temp > entry.1 {
                                entry.1 = temp;
                            }
                            entry.2 += temp;
                            entry.3 += 1;
                        })
                        .or_insert_with(|| (temp, temp, temp, 1));
                }
                row_map
            });
            (t, handle)
        })
        .collect::<Vec<_>>(); // Need to collect to wait for threads to finish

    let row_map = row_map
        .into_iter()
        .map(|(i, t)| {
            let r = t.join().expect("Error joining thread");
            eprintln!("Thread {} finished", i + 1);
            r
        })
        .reduce(|mut a, b| {
            for (k, v) in b {
                a.entry(k)
                    .and_modify(|entry| {
                        if v.0 < entry.0 {
                            entry.0 = v.0;
                        } else if v.1 > entry.1 {
                            entry.1 = v.1;
                        }
                        entry.2 += v.2;
                        entry.3 += v.3;
                    })
                    .or_insert(v);
            }
            a
        })
        .expect("Error reducing threads");

    eprintln!(
        "====== Processing took {} ms ======",
        instant.elapsed().as_millis()
    );

    let instant = std::time::Instant::now();

    let mut entries = row_map.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|a, b| a.0.cmp(b.0));

    print!("{{");
    for (i, (name, (min, max, sum, count))) in entries.iter().enumerate() {
        print!(
            "{}{name}={:.1}/{:.1}/{:.1}",
            if i == 0 { "" } else { ", " },
            *min as f32 / 10.0,
            *max as f32 / 10.0,
            (sum / *count as i32) as f32 / 10.0
        );
    }
    println!("}}");

    eprintln!(
        "====== Printing / Sorting took {} ms ======",
        instant.elapsed().as_millis()
    );
}

mod fx_hash {
    // An implementation of the Firefox Hasher
    // This is kinda a not good hasher but for our use case it's worth a shot!

    use std::hash::{BuildHasher, Hasher};

    pub struct FxHasher {
        hash: u64,
    }

    impl Default for FxHasher {
        #[inline]
        fn default() -> Self {
            Self { hash: 0 }
        }
    }

    const PI: u64 = 0x0100_0000_01b3;

    impl BuildHasher for FxHasher {
        type Hasher = Self;

        #[inline]
        fn build_hasher(&self) -> Self {
            Self::default()
        }
    }

    impl Hasher for FxHasher {
        #[inline]
        fn finish(&self) -> u64 {
            self.hash
        }

        #[inline]
        fn write(&mut self, bytes: &[u8]) {
            for byte in bytes {
                self.hash = self.hash.wrapping_mul(PI);
                self.hash ^= *byte as u64;
            }
        }
    }
}
