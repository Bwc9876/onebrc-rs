# one-billion-row-challenge-rust

This is a Rust implementation of the [one-billion-row challenge](https://1brc.dev/). The challenge is to read a 1 billion row text file and calculate the min, max, and average of a weather station's temperature.

This implementation is an attempt at a pure-Rust solution. It uses no external crates at all! It's multi-threaded and will use all available CPU cores to process the file.

## Running

> Note: This implementation has to be run on a Linux machine. It uses the `mmap` system call to map the file into memory.

First things first you'll need a dataset, I'm too lazy to make code to generate one so head on over to the [C implementation](https://github.com/dannyvankooten/1brc#running-the-challenge) where someone else made it.

Now with the dataset saved in a file called `measurements.txt`, simply do:

```sh
cargo run --release
```

This will output logs to stderr and the results to stdout.

## Performance

On my machine (Framework 13) this runs in ~8 seconds. It uses all available CPU cores to process the file so this will heavily depend on your machine's CPU.
