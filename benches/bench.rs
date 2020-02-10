#[macro_use]
extern crate criterion;
extern crate emseries;
extern crate rand;
extern crate chrono;
extern crate chrono_tz;
extern crate serde_derive;

use chrono::TimeZone;
use chrono_tz::Etc::UTC;
use criterion::Criterion;
use emseries::{DateTimeTz, Recordable, Series, time_range};
use emseries::indexing::{NoIndex, IndexByTime};
use rand::distributions::{IndependentSample, Range};
use serde_derive::{Deserialize, Serialize};

const DB_SIZES: &[usize] = &[0, 1, 2, 16, 32, 1024, 1024 * 1024];
const START_YEAR: i32 = 1970;
const END_YEAR: i32 = 2100;

#[derive(Clone, Serialize, Deserialize)]
struct S {
    timestamp: DateTimeTz,
}

impl Recordable for S {
    fn timestamp(&self) -> DateTimeTz {
        self.timestamp.clone()
    }

    fn tags(&self) -> Vec<String> {
        Vec::new()
    }
}

fn generate_random_recordables() -> impl Iterator<Item = S> {
    let mut rng = rand::thread_rng();
    let day_range = Range::new(1, 29); // Conservative, but correct
    let month_range = Range::new(1, 13);
    let year_range = Range::new(START_YEAR, END_YEAR);

    std::iter::from_fn(move || {
        let rand_str = format!(
            "{}-{:02}-{:02}T00:00:00+00:00",
            year_range.ind_sample(&mut rng),
            month_range.ind_sample(&mut rng),
            day_range.ind_sample(&mut rng),
        );

        let timestamp = DateTimeTz::from_str(&rand_str).unwrap();//.expect("Generated date should be legal");

        Some(S { timestamp })
    })
}

fn search_time_window(c: &mut Criterion) {
    const INTERVAL_START_YEAR: i32 = 2000;
    const INTERVAL_END_YEAR: i32 = 2050;
    assert!(INTERVAL_START_YEAR >= START_YEAR);
    assert!(INTERVAL_START_YEAR <= END_YEAR);
    assert!(INTERVAL_END_YEAR >= START_YEAR);
    assert!(INTERVAL_END_YEAR <= END_YEAR);
    assert!(INTERVAL_START_YEAR <= INTERVAL_END_YEAR);

    for db_size in DB_SIZES {
        let mut ts_no_indexer = Series::<S, NoIndex>::open("/dev/null").unwrap();
        let mut ts_index_by_time = Series::<S, IndexByTime>::open("/dev/null").unwrap();

        for recordable in generate_random_recordables().take(*db_size) {
            ts_no_indexer.put(recordable.clone()).unwrap();
            ts_index_by_time.put(recordable).unwrap();
        }

        c.bench_function(&format!("search_range_no_index_{}", db_size),
                         move |b| b.iter(|| {
                             match ts_no_indexer.search_sorted(
                                 time_range(
                                     DateTimeTz(UTC.ymd(INTERVAL_START_YEAR, 1, 1).and_hms(0, 0, 0)),
                                     true,
                                     DateTimeTz(UTC.ymd(INTERVAL_END_YEAR, 12, 31).and_hms(23, 59, 59)),
                                     true,
                                 ),
                                 |l, r| l.timestamp().cmp(&r.timestamp()),
                             ) {
                                 Err(err) => assert!(false, err),
                                 Ok(v) => {
                                     criterion::black_box(v);
                                 }
                             };
                         }));

        c.bench_function(&format!("search_range_index_by_time_{}", db_size),
                         move |b| b.iter(|| {
                             match ts_index_by_time.search_range(
                                 DateTimeTz(UTC.ymd(INTERVAL_START_YEAR, 1, 1).and_hms(0, 0, 0))
                                     ..=
                                     DateTimeTz(UTC.ymd(INTERVAL_END_YEAR, 12, 31).and_hms(23, 59, 59)),
                             ) {
                                 Err(err) => assert!(false, err),
                                 Ok(v) => {
                                     criterion::black_box(v.collect::<Vec<_>>());
                                 }
                             }
                         }));
    }
}

criterion_group!(benches, search_time_window);
criterion_main!(benches);
