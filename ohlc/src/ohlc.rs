use std::collections::HashMap;
use std::io::Write;
use std::thread;
use std::sync::{Arc, Mutex, RwLock};
use num_cpus;
use crate::tools::datas::{TickData, OHLCWindow, OHLCData};
use crate::tools::tick_generator::TickGenerator;

#[derive(Clone)]
struct TickDataRangeIndex {
    pub range_begin: usize,
    pub range_end: usize,
    pub window_begin: usize
}

pub struct OHLCMaker {}


impl OHLCMaker {
    pub fn new() -> Self{
        OHLCMaker{}
    }

    // single thread solution
    pub fn make(&self, 
        tick_path: &str, // input dataset path
        window_length: u64, // input window length in ms
        ohlc_path: &str // output ohlc path
    ) {
        let tick_generator = TickGenerator::new();
        let tick_datas = tick_generator.from_file(&tick_path);
        if tick_datas.is_empty() {
            return;
        }
        let ohlc_datas = make_batch_ohlc(&tick_datas, window_length, 0, tick_datas.len() - 1, 0);

        let mut out = std::fs::File::create(ohlc_path).expect("file creation failed");
        for item in ohlc_datas {
            out.write((serde_json::to_string(&item).unwrap() + "\n").as_bytes()).expect("file writing failed.");
        }
    }

    // multi thread solution
    pub fn parallel_make(&self, 
        tick_path: &str, 
        window_length: u64, 
        ohlc_path: &str 
    ) {
        let tick_generator = TickGenerator::new();
        let tick_datas =  Arc::new(RwLock::new(tick_generator.from_file(&tick_path)));
        let ohlc_datas = self.make_ohlc_parallel(tick_datas, window_length);

        let mut out = std::fs::File::create(ohlc_path).expect("file creation failed");
        for item in ohlc_datas {
            out.write((serde_json::to_string(&item).unwrap() + "\n").as_bytes()).expect("file writing failed.");
        }
    }

    pub fn make_ohlc_parallel(&self, tick_datas_r: Arc<RwLock<Vec<TickData>>>, window_length: u64) -> Vec<OHLCData>{
        let tick_datas = tick_datas_r.read().unwrap();
        if tick_datas.is_empty() {
            return Vec::new();
        }
        // make tick datas to splits; cap split count so no split has range_begin >= len
        let split_count = num_cpus::get().min(tick_datas.len());
        let tick_data_splits = self.split_tick_data(&tick_datas, split_count, window_length);
        let ohlc_data_splits: Arc<Mutex<HashMap<u32, Vec<OHLCData>>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut ohlc_datas: Vec<OHLCData> = Vec::with_capacity(tick_datas.len());

        let mut handles = Vec::new();
        for i in 0..split_count {
            let c_tick_data_splits = Arc::clone(&tick_data_splits);
            let c_shared_tick_datas = Arc::clone(&tick_datas_r);
            let c_ohlc_data_splits = Arc::clone(&ohlc_data_splits);
            let thread_handle = thread::spawn(move || {
                let splits = c_tick_data_splits.read().unwrap();
                let range_begin = splits.get(&(i as usize)).unwrap().range_begin;
                let range_end = splits.get(&(i as usize)).unwrap().range_end;
                let window_begin = splits.get(&(i as usize)).unwrap().window_begin;

                // println!("range: {} {} {}", range_begin, range_end, window_begin);
                let tick_datas = c_shared_tick_datas.read().unwrap();
                let ohlc_split = make_batch_ohlc(&tick_datas, window_length, range_begin, range_end, window_begin);
                
                // lock it when this batch id done
                c_ohlc_data_splits.lock().unwrap().insert(i as u32, ohlc_split);
            });
            handles.push(thread_handle);
        }

        for h in handles {
            h.join().unwrap();
        }
        for i in 0..split_count {
            let mut ohlc_spilts = ohlc_data_splits.lock().unwrap();
            let ohlc_split = ohlc_spilts.get_mut(&(i as u32)).unwrap();

            ohlc_datas.append(ohlc_split);
        }
        ohlc_datas
    }

    fn split_tick_data(&self, tick_datas: &Vec<TickData>, split_count: usize, window_length: u64) -> Arc<RwLock<HashMap<usize, TickDataRangeIndex>>>{
        // Split tick datas into split_count of parts.
        // Return range index and window index of all parts.

        let splits: Arc<RwLock<HashMap<usize, TickDataRangeIndex>>> = Arc::new(RwLock::new(HashMap::new()));
        let mut split_size = tick_datas.len() / split_count as usize;
        if split_size == 0 {
            split_size = 1;
        }
        for split_index in 0..split_count {
            let split_index = split_index as usize;
            if split_index == 0 {
                splits.write().unwrap().insert(split_index, TickDataRangeIndex{
                    range_begin: 0,
                    range_end: split_size - 1,
                    window_begin: 0
                });
            }
            else {
                splits.write().unwrap().insert(split_index, TickDataRangeIndex{
                    range_begin: split_index * split_size,
                    range_end: (|index: usize| {
                        if index != split_count as usize - 1 {
                            (index + 1) * split_size as usize - 1
                        }
                        else {
                            //  last split
                            tick_datas.len() - 1
                        }
                    })(split_index),
                    window_begin: (|index|{
                        // compute the begin of the window
                        let _range_begin: usize = index * split_size;
                        let mut _window_begin: usize = _range_begin;
                        while _window_begin > 0 {
                            if tick_datas[_range_begin].T - tick_datas[_window_begin - 1].T <= window_length {
                                _window_begin -= 1;
                            }
                            else {
                                // here we reach the edge of the window
                                break;
                            }
                        }
                        _window_begin
                    })(split_index)
                });
            }
        }
        splits
    }
}

fn update_window(tick_datas: &Vec<TickData>, window: &mut OHLCWindow, cur_price: f64, cur_index: usize, window_length: u64) {
    let cur_tick = &tick_datas[cur_index];
    let symbol = &cur_tick.s;
    let begin_tick = &tick_datas[window.begin_index];  // it's safe to use index to fetch the data in vector.
    if cur_tick.T - begin_tick.T > window_length {
        // firstly, try update window begin pos to i-th
        let mut found_new_begin = false;
        for i in window.begin_index + 1..cur_index + 1 {
            if tick_datas[i].s.eq(symbol) && cur_tick.T - tick_datas[i].T < window_length {
                window.begin_index = i;
                window.open = tick_datas[i].price.unwrap();
                window.high = window.open;
                window.low = window.open;
                found_new_begin = true;
                break;
            }
        }
        // all prior same-symbol ticks aged out: reset the window to current tick
        if !found_new_begin {
            window.begin_index = cur_index;
            window.open = cur_price;
            window.high = cur_price;
            window.low = cur_price;
            return;
        }
        // then, update high/low in range i-th to current tick (same symbol only)
        for i in window.begin_index + 1..cur_index + 1 {
            if !tick_datas[i].s.eq(symbol) {
                continue;
            }
            let price = tick_datas[i].price.unwrap();
            if price > window.high {
                window.high = price;
            }
            if price < window.low {
                window.low = price;
            }
        }
    }
    else {
        if cur_price > window.high {
            window.high = cur_price;
        }
        if cur_price < window.low {
            window.low = cur_price;
        }
    }
}

pub fn make_batch_ohlc(
        tick_datas: &Vec<TickData>, // all the tick datas
        window_length: u64, // window length in ms
        range_begin: usize, // begin of the split to compute ohlc
        range_end: usize, // end of the split to compute ohlc
        window_begin: usize // begin of the window to this batch
) -> Vec<OHLCData> {
    // compute one specific batch of ohlc data
    let mut symbel_windows: HashMap<String, OHLCWindow> = HashMap::new();
    let mut ohlc_datas: Vec<OHLCData> = Vec::with_capacity(tick_datas.len());
    for index in window_begin..range_end + 1 {
        let tick = &tick_datas[index];
        let cur_price = tick.price.unwrap();
        if !symbel_windows.contains_key(&tick.s) {
            symbel_windows.insert(tick.s.clone(), OHLCWindow{
                open: cur_price,
                high: cur_price,
                low: cur_price,
                begin_index: index
            });
        }
        else {
            // the interval between each tick may not be the same
            // when we enconter each new line, we need to recompute the window
            update_window(&tick_datas, symbel_windows.get_mut(&tick.s).unwrap(), 
                cur_price, index, window_length);
        }

        if index >= range_begin {
            ohlc_datas.push(OHLCData{
                symbol: tick.s.clone(),
                timestamp: tick.T,
                open: format!("{:.6}", symbel_windows[&tick.s].open),
                high: format!("{:.6}", symbel_windows[&tick.s].high),
                low: format!("{:.6}", symbel_windows[&tick.s].low),
                close: format!("{:.6}", cur_price),
            });
        }
    }
    ohlc_datas
}