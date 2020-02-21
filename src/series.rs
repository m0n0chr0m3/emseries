extern crate serde;
extern crate serde_json;
extern crate uuid;

use self::serde::de::DeserializeOwned;
use self::serde::ser::Serialize;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, LineWriter, Write};

use criteria::Criteria;
use types::{DeletableRecord, Error, Recordable, UniqueId};

/// An open time series database.
///
/// Any given database can store only one data type, T. The data type must be determined when the
/// database is opened.
pub struct Series<T: Clone + Recordable + DeserializeOwned + Serialize> {
    //path: String,
    writer: LineWriter<File>,
    records: HashMap<UniqueId, T>,
}

impl<T> Series<T>
where
    T: Clone + Recordable + DeserializeOwned + Serialize,
{
    /// Open a time series database at the specified path. `path` is the full path and filename for
    /// the database.
    pub fn open(path: &str) -> Result<Series<T>, Error> {
        let f = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)
            .map_err(Error::IOError)?;

        let records = Series::load_file(&f)?;

        let writer = LineWriter::new(f);

        Ok(Series {
            //path: String::from(path),
            writer,
            records,
        })
    }

    /// Load a file and return all of the records in it.
    fn load_file(f: &File) -> Result<HashMap<UniqueId, T>, Error> {
        let mut records: HashMap<UniqueId, T> = HashMap::new();
        let reader = BufReader::new(f);
        for line in reader.lines() {
            match line {
                Ok(line_) => {
                    match line_.parse::<DeletableRecord<_>>() {
                        Ok(record) => match record.data {
                            Some(val) => records.insert(
                                record.id.clone(),
                                 val,
                            ),
                            None => records.remove(&record.id.clone()),
                        },
                        Err(err) => return Err(err),
                    };
                }
                Err(err) => return Err(Error::IOError(err)),
            }
        }
        Ok(records)
    }

    /// Put a new record into the database. A unique id will be assigned to the record and
    /// returned.
    pub fn put(&mut self, entry: T) -> Result<UniqueId, Error> {
        let id = UniqueId::new();
        self.update(&id, entry)?;
        Ok(id)
    }

    /// Update the record assigned to an existing id.
    /// The `UniqueId` of the record passed into this function must match the `UniqueId` of a
    /// record already in the database.
    // TODO: Returning the previous `T` that was registered for `uuid` is practically 'free' in
    // case of a real update (i.e., if `uuid` was already known to the `Series`). Return previous?
    // (Note that this would require a change to `Series::put`, since it currently abuses `update`
    // to insert data for a uuid where no previous value existed for.
    pub fn update(&mut self, uuid: &UniqueId, entry: T) -> Result<(), Error> {
        let record = DeletableRecord { id: uuid.clone(), data: Some(entry) };
        match serde_json::to_string(&record) {
            Ok(rec_str) => self
                .writer
                .write_fmt(format_args!("{}\n", rec_str.as_str()))
                .map_err(Error::IOError),
            Err(err) => Err(Error::JSONStringError(err)),
        }?;

        // There's no reason to clone the in-memory representations of `uuid` and `entry`: we know
        // we put them in the `DeletableRecord` and never handed out mutable references to it.
        // Retrieve `id` and `entry` by destructuring the `DeletableRecord`:
        if let DeletableRecord { id, data: Some(entry) } = record {
            self.records.insert(id, entry);

            Ok(())
        } else {
            unreachable!("`DeletableRecord` will contain what we just put in.")
        }
    }

    /// Delete a record from the database
    ///
    /// Future note: while this deletes a record from the view, it only adds an entry to the
    /// database that indicates `data: null`. If record histories ever become important, the record
    /// and its entire history (including this delete) will still be available.
    pub fn delete(&mut self, uuid: &UniqueId) -> Result<(), Error> {
        self.records.remove(uuid);

        let rec = DeletableRecord {
            id: uuid.clone(),
            data: None::<T>,
        };
        match serde_json::to_string(&rec) {
            Ok(rec_str) => self
                .writer
                .write_fmt(format_args!("{}\n", rec_str.as_str()))
                .map_err(Error::IOError),
            Err(err) => Err(Error::JSONStringError(err)),
        }
    }

    /// Retrieve an iterator over all of the records in the database.
    pub fn records<'s>(&'s self) -> Result<impl Iterator<Item=(&'s UniqueId, &'s T)> + 's, Error> {
        Ok(self.records.iter())
    }

    /*  The point of having Search is so that a lot of internal optimizations can happen once the
     *  data sets start getting large. */
    /// Perform a search on the records in a database, based on the given criteria.
    pub fn search<C>(&self, criteria: C) -> Result<impl Iterator<Item = (&UniqueId, &T)>, Error>
    where
        C: Criteria,
    {
        Ok(self.records()?
            .filter(move |tr| criteria.apply(tr.1)))
    }

    /// Perform a search and sort the resulting records based on the comparison.
    pub fn search_sorted<C, CMP>(
        &self,
        criteria: C,
        mut compare: CMP
    ) -> Result<Vec<(&UniqueId, &T)>, Error>
    where
        C: Criteria,
        CMP: FnMut(&T, &T) -> Ordering,
    {
        match self.search(criteria) {
            Ok(records) => {
                let mut records: Vec<_> = records.collect();
                records.sort_unstable_by(move |l, r| compare(l.1, r.1));
                Ok(records)
            }
            Err(err) => Err(err),
        }
    }

    /// Get an exact record from the database based on unique id.
    pub fn get(&self, uuid: &UniqueId) -> Result<Option<T>, Error> {
        Ok(self.records.get(uuid).cloned())
    }

    /*
    pub fn remove(&self, uuid: UniqueId) -> Result<(), Error> {
        unimplemented!()
    }
    */
}

#[cfg(test)]
mod tests {
    extern crate chrono;
    extern crate dimensioned;

    use self::chrono::prelude::*;
    use self::dimensioned::si::{Kilogram, Meter, Second, KG, M, S};
    use chrono_tz::Etc::UTC;
    use date_time_tz::DateTimeTz;
    use std::fs;
    use std::ops;

    use super::*;
    use criteria::*;

    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    struct Distance(Meter<f64>);

    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    struct Duration(Second<f64>);

    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    struct BikeTrip {
        datetime: DateTimeTz,
        distance: Distance,
        duration: Duration,
        comments: String,
    }

    impl Recordable for BikeTrip {
        fn timestamp(&self) -> DateTimeTz {
            self.datetime.clone()
        }
        fn tags(&self) -> Vec<String> {
            Vec::new()
        }
    }

    fn mk_trips() -> [BikeTrip; 5] {
        [
            BikeTrip {
                datetime: DateTimeTz(UTC.ymd(2011, 10, 29).and_hms(0, 0, 0)),
                distance: Distance(58741.055 * M),
                duration: Duration(11040.0 * S),
                comments: String::from("long time ago"),
            },
            BikeTrip {
                datetime: DateTimeTz(UTC.ymd(2011, 10, 31).and_hms(0, 0, 0)),
                distance: Distance(17702.0 * M),
                duration: Duration(2880.0 * S),
                comments: String::from("day 2"),
            },
            BikeTrip {
                datetime: DateTimeTz(UTC.ymd(2011, 11, 02).and_hms(0, 0, 0)),
                distance: Distance(41842.945 * M),
                duration: Duration(7020.0 * S),
                comments: String::from("Do Some Distance!"),
            },
            BikeTrip {
                datetime: DateTimeTz(UTC.ymd(2011, 11, 04).and_hms(0, 0, 0)),
                distance: Distance(34600.895 * M),
                duration: Duration(5580.0 * S),
                comments: String::from("I did a lot of distance back then"),
            },
            BikeTrip {
                datetime: DateTimeTz(UTC.ymd(2011, 11, 05).and_hms(0, 0, 0)),
                distance: Distance(6437.376 * M),
                duration: Duration(960.0 * S),
                comments: String::from("day 5"),
            },
        ]
    }

    fn run_test<T>(test: T) -> ()
    where
        T: FnOnce(tempfile::TempPath),
    {
        let tmp_file = tempfile::NamedTempFile::new().expect("temporary path created");
        let tmp_path = tmp_file.into_temp_path();
        test(tmp_path);
    }

    #[test]
    pub fn can_add_and_retrieve_entries() {
        run_test(|path| {
            let trips = mk_trips();
            let mut ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                .expect("expect the time series to open correctly");
            let uuid = ts.put(trips[0].clone()).expect("expect a successful put");
            let record_res = ts.get(&uuid);

            for trip in &trips[1..=4] {
                ts.put(trip.clone()).expect("expect a successful put");
            }

            match record_res {
                Err(err) => assert!(false, err),
                Ok(None) => assert!(false, "There should have been a value here"),
                Ok(Some(tr)) => {
                    assert_eq!(
                        tr.timestamp(),
                        DateTimeTz(UTC.ymd(2011, 10, 29).and_hms(0, 0, 0))
                    );
                    assert_eq!(tr.duration, Duration(11040.0 * S));
                    assert_eq!(tr.comments, String::from("long time ago"));
                    assert_eq!(tr, trips[0]);
                }
            }
        })
    }

    #[test]
    pub fn can_search_for_an_entry_with_exact_time() {
        run_test(|path| {
            let trips = mk_trips();
            let mut ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                .expect("expect the time series to open correctly");

            for trip in &trips[0..=4] {
                ts.put(trip.clone()).expect("expect a successful put");
            }

            match ts.search(exact_time(DateTimeTz(
                UTC.ymd(2011, 10, 31).and_hms(0, 0, 0),
            ))) {
                Err(err) => assert!(false, err),
                Ok(i) => {
                    let v: Vec<_> = i.collect();
                    assert_eq!(v.len(), 1);
                    assert_eq!(*v[0].1, trips[1]);
                }
            };
        })
    }

    #[test]
    pub fn can_get_entries_in_time_range() {
        run_test(|path| {
            let trips = mk_trips();
            let mut ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                .expect("expect the time series to open correctly");

            for trip in &trips[0..=4] {
                ts.put(trip.clone()).expect("expect a successful put");
            }

            match ts.search_sorted(
                time_range(
                    DateTimeTz(UTC.ymd(2011, 10, 31).and_hms(0, 0, 0)),
                    true,
                    DateTimeTz(UTC.ymd(2011, 11, 04).and_hms(0, 0, 0)),
                    true,
                ),
                |l, r| l.timestamp().cmp(&r.timestamp()),
            ) {
                Err(err) => assert!(false, err),
                Ok(v) => {
                    assert_eq!(v.len(), 3);
                    assert_eq!(*v[0].1, trips[1]);
                    assert_eq!(*v[1].1, trips[2]);
                    assert_eq!(*v[2].1, trips[3]);
                }
            }
        })
    }

    #[test]
    pub fn persists_and_reads_an_entry() {
        run_test(|path| {
            let trips = mk_trips();

            {
                let mut ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                    .expect("expect the time series to open correctly");

                for trip in &trips[0..=4] {
                    ts.put(trip.clone()).expect("expect a successful put");
                }
            }

            {
                let ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                    .expect("expect the time series to open correctly");
                match ts.search_sorted(
                    time_range(
                        DateTimeTz(UTC.ymd(2011, 10, 31).and_hms(0, 0, 0)),
                        true,
                        DateTimeTz(UTC.ymd(2011, 11, 04).and_hms(0, 0, 0)),
                        true,
                    ),
                    |l, r| l.timestamp().cmp(&r.timestamp()),
                ) {
                    Err(err) => assert!(false, err),
                    Ok(v) => {
                        assert_eq!(v.len(), 3);
                        assert_eq!(*v[0].1, trips[1]);
                        assert_eq!(*v[1].1, trips[2]);
                        assert_eq!(*v[2].1, trips[3]);
                    }
                }
            }
        })
    }

    #[test]
    pub fn can_write_to_existing_file() {
        run_test(|path| {
            let trips = mk_trips();

            {
                let mut ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                    .expect("expect the time series to open correctly");

                for trip in &trips[0..=2] {
                    ts.put(trip.clone()).expect("expect a successful put");
                }
            }

            {
                let mut ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                    .expect("expect the time series to open correctly");
                match ts.search_sorted(
                    time_range(
                        DateTimeTz(UTC.ymd(2011, 10, 31).and_hms(0, 0, 0)),
                        true,
                        DateTimeTz(UTC.ymd(2011, 11, 04).and_hms(0, 0, 0)),
                        true,
                    ),
                    |l, r| l.timestamp().cmp(&r.timestamp()),
                ) {
                    Err(err) => assert!(false, err),
                    Ok(v) => {
                        assert_eq!(v.len(), 2);
                        assert_eq!(*v[0].1, trips[1]);
                        assert_eq!(*v[1].1, trips[2]);
                        ts.put(trips[3].clone()).expect("expect a successful put");
                        ts.put(trips[4].clone()).expect("expect a successful put");
                    }
                }
            }

            {
                let ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                    .expect("expect the time series to open correctly");
                match ts.search_sorted(
                    time_range(
                        DateTimeTz(UTC.ymd(2011, 10, 31).and_hms(0, 0, 0)),
                        true,
                        DateTimeTz(UTC.ymd(2011, 11, 05).and_hms(0, 0, 0)),
                        true,
                    ),
                    |l, r| l.timestamp().cmp(&r.timestamp()),
                ) {
                    Err(err) => assert!(false, err),
                    Ok(v) => {
                        assert_eq!(v.len(), 4);
                        assert_eq!(*v[0].1, trips[1]);
                        assert_eq!(*v[1].1, trips[2]);
                        assert_eq!(*v[2].1, trips[3]);
                        assert_eq!(*v[3].1, trips[4]);
                    }
                }
            }
        })
    }

    #[test]
    pub fn can_overwrite_existing_entry() {
        run_test(|path| {
            let trips = mk_trips();

            let mut ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                .expect("expect the time series to open correctly");

            ts.put(trips[0].clone()).expect("expect a successful put");
            ts.put(trips[1].clone()).expect("expect a successful put");
            let trip_id = ts.put(trips[2].clone()).expect("expect a successful put");

            match ts.get(&trip_id) {
                Err(err) => assert!(false, err),
                Ok(None) => assert!(false, "record not found"),
                Ok(Some(mut trip)) => {
                    trip.distance = Distance(50000.0 * M);
                    ts.update(&trip_id, trip).expect("expect record to update");
                }
            };

            match ts.get(&trip_id) {
                Err(err) => assert!(false, err),
                Ok(None) => assert!(false, "record not found"),
                Ok(Some(trip)) => {
                    assert_eq!(
                        trip.datetime,
                        DateTimeTz(UTC.ymd(2011, 11, 02).and_hms(0, 0, 0))
                    );
                    assert_eq!(trip.distance, Distance(50000.0 * M));
                    assert_eq!(trip.duration, Duration(7020.0 * S));
                    assert_eq!(trip.comments, String::from("Do Some Distance!"));
                }
            }
        })
    }

    #[test]
    pub fn record_overwrites_get_persisted() {
        run_test(|path| {
            let trips = mk_trips();

            {
                let mut ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                    .expect("expect the time series to open correctly");

                ts.put(trips[0].clone()).expect("expect a successful put");
                ts.put(trips[1].clone()).expect("expect a successful put");
                let trip_id = ts.put(trips[2].clone()).expect("expect a successful put");

                match ts.get(&trip_id) {
                    Err(err) => assert!(false, err),
                    Ok(None) => assert!(false, "record not found"),
                    Ok(Some(mut trip)) => {
                        trip.distance = Distance(50000.0 * M);
                        ts.update(&trip_id, trip).expect("expect record to update");
                    }
                };
            }

            {
                let ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                    .expect("expect the time series to open correctly");

                match ts.records() {
                    Err(err) => assert!(false, err),
                    Ok(trips) => assert_eq!(trips.count(), 3),
                }

                match ts.search(exact_time(DateTimeTz(
                    UTC.ymd(2011, 11, 02).and_hms(0, 0, 0),
                ))) {
                    Err(err) => assert!(false, err),
                    Ok(trips) => {
                        let trips: Vec<_> = trips.collect();
                        assert_eq!(trips.len(), 1);
                        assert_eq!(
                            trips[0].1.datetime,
                            DateTimeTz(UTC.ymd(2011, 11, 02).and_hms(0, 0, 0))
                        );
                        assert_eq!(trips[0].1.distance, Distance(50000.0 * M));
                        assert_eq!(trips[0].1.duration, Duration(7020.0 * S));
                        assert_eq!(trips[0].1.comments, String::from("Do Some Distance!"));
                    }
                };
            }
        })
    }

    #[test]
    pub fn can_delete_an_entry() {
        run_test(|path| {
            let trips = mk_trips();

            {
                let mut ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                    .expect("expect the time series to open correctly");
                let trip_id = ts.put(trips[0].clone()).expect("expect a successful put");
                ts.put(trips[1].clone()).expect("expect a successful put");
                ts.put(trips[2].clone()).expect("expect a successful put");

                ts.delete(&trip_id).expect("successful delete");

                let recs = ts.records().expect("good record retrieval");
                assert_eq!(recs.count(), 2);
            }

            {
                let ts: Series<BikeTrip> = Series::open(&path.to_string_lossy())
                    .expect("expect the time series to open correctly");
                let recs = ts.records().expect("good record retrieval");
                assert_eq!(recs.count(), 2);
            }
        })
    }

    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    pub struct Weight(Kilogram<f64>);

    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    pub struct WeightRecord {
        pub date: DateTimeTz,
        pub weight: Weight,
    }

    impl Recordable for WeightRecord {
        fn timestamp(&self) -> DateTimeTz {
            self.date.clone()
        }

        fn tags(&self) -> Vec<String> {
            Vec::new()
        }
    }

    #[test]
    pub fn legacy_file_load() {
        let ts: Series<WeightRecord> =
            Series::open("fixtures/weight.json").expect("legacy series should open correctly");

        let uid = "3330c5b0-783f-4919-b2c4-8169c38f65ff"
            .parse()
            .expect("something is wrong with this ID");
        let rec = ts.get(&uid);
        match rec {
            Err(err) => assert!(false, err),
            Ok(None) => assert!(false, "no record found"),
            Ok(Some(rec)) => assert_eq!(rec.weight, Weight(77.79109 * KG)),
        }
    }
}
