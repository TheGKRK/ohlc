# ohlc

A Rust library for computing rolling-window OHLC (Open/High/Low/Close) data from raw exchange tick data. For every input tick, it produces one OHLC record covering the preceding `window_length` milliseconds (a sliding window, not fixed time buckets).

## How it works

**Price** is computed as the mid-price: `(bid + ask) / 2`

**Input** - Binance `bookTicker` WebSocket events (NDJSON, one record per line):
```json
{"e":"bookTicker","u":1875301568520,"s":"TURBOUSDT","b":"0.3261","B":"226654.3","a":"0.3262","A":"75762.5","T":1662022800005,"E":1662022800010}
```

**Output** - one OHLC record per input tick (NDJSON):
```json
{"symbol":"TURBOUSDT","timestamp":1662022800005,"open":"0.326150","high":"0.326150","low":"0.326150","close":"0.326150"}
```

## Usage

```rust
use ohlc::ohlc::OHLCMaker;

let maker = OHLCMaker::new();

// Single-threaded
maker.make("data/dataset-a.txt", 300_000, "data/output.txt");

// Multi-threaded (uses all CPU cores)
maker.parallel_make("data/dataset-a.txt", 300_000, "data/output.txt");
```

`window_length` is in milliseconds. The example above uses a 5-minute (300,000 ms) window.

## Technical details

### Rolling window algorithm

The core of the library is `make_batch_ohlc`. It maintains a per-symbol `OHLCWindow` state as it scans ticks sequentially:

- **Open** - price of the oldest tick still within the window for that symbol
- **High / Low** - running max/min over all same-symbol ticks within the window
- **Close** - price of the current tick (mid-price at that moment)

`window_begin` tracks the index of the oldest tick inside the window per symbol. On each tick:
1. If the oldest tick has aged out (`current_T - window_begin_T > window_length`), scan forward to find the new window start for that symbol, then rescan the range to recompute high/low.
2. If the window is still valid, just update high/low incrementally.

### Parallel execution

`parallel_make` splits the tick array into `N` ranges (one per CPU core) and processes each range in a separate thread. A critical detail: ticks at the start of a range may need context from ticks in the previous range to compute the correct open/high/low. Each split therefore computes a `window_begin` (the earliest index whose timestamp falls within `window_length` of the split's first tick) and starts processing from there. Only ticks within the assigned `[range_begin, range_end]` are emitted; the lookback ticks are used solely to warm up the window state.

```
tick array:   [0 ........... N/4 ........... N/2 ........... 3N/4 ........... N]
                  thread 0          thread 1          thread 2          thread 3
              ^                 ^
              window_begin[1] --+-- range_begin[1]
              (lookback, not emitted)
```

Threads write results into a shared `HashMap<u32, Vec<OHLCData>>` (keyed by thread index) and are assembled in order after all threads join, preserving the original tick sequence in the output.

### Data types

| Type | Description |
|---|---|
| `TickData` | Deserialised `bookTicker` event. `price` is derived as `(b + a) / 2` on load. |
| `OHLCWindow` | In-memory rolling state per symbol: `open`, `high`, `low`, `begin_index` (absolute index into the tick vec) |
| `OHLCData` | Output record: `symbol`, `timestamp`, `open`, `high`, `low`, `close` (all prices as 6 d.p. strings) |

### Dependencies

| Crate | Use |
|---|---|
| `serde` / `serde_json` | NDJSON deserialisation of tick data and serialisation of OHLC output |
| `num_cpus` | Determine available CPU cores for parallel mode |
| `criterion` (dev) | Benchmarking harness |

## Build

```bash
cargo build --release
```

## Benchmark

```bash
cargo bench
```

Benchmarks run against 1,000,000 mock ticks using [Criterion](https://github.com/bheisler/criterion.rs). Both single-threaded and parallel modes are covered.

## Project structure

```
ohlc/
├── src/
│   ├── lib.rs
│   ├── ohlc.rs              # OHLCMaker, core OHLC computation
│   └── tools/
│       ├── datas.rs         # TickData, OHLCWindow, OHLCData types
│       └── tick_generator.rs # File reader and mock data generator
├── benches/
│   └── main.rs
└── data/
    ├── dataset-a.txt        # Sample tick input
    ├── dataset-b.txt        # Sample tick input
    └── ohlc-5m-a.txt        # Sample output (5-minute window)
```
