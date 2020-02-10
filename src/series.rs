extern crate serde;
extern crate serde_json;
extern crate uuid;

pub mod indexing;

use self::serde::de::DeserializeOwned;
use self::serde::ser::Serialize;
use std::cmp::Ordering;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, LineWriter};

use criteria::Criteria;
use types::{Error, Record, Recordable, DeletableRecord, UniqueId};
use std::collections::hash_map::Entry;
use std::convert::TryInto;
use DateTimeTz;
use series::indexing::{NoIndex, Indexer};
use ahash::AHashMap;

/// An open time series database.
///
/// Any given database can store only one data type, T. The data type must be determined when the
/// database is opened.
pub struct Series<
    T: Clone + Recordable + DeserializeOwned + Serialize,
    I: Indexer = NoIndex
    //I: Indexer = IndexByTime
> {
    writer: LineWriter<File>,
    element_for_key: AHashMap<UniqueId, T>,
    indexer: I,
}


impl<T, I> Series<T, I>
where
    T: Clone + Recordable + DeserializeOwned + Serialize,
    I: Indexer + Default
{
    /// Open a time series database at the specified path, with a default-constructed `Indexer`.
    ///
    /// `path` is the full path and filename for the database.
    pub fn open(path: &str) -> Result<Self, Error> {
        Self::open_with_indexer(path, Default::default())
    }
}

impl<T, I> Series<T, I>
    where
        T: Clone + Recordable + DeserializeOwned + Serialize,
        I: Indexer
{
    /// Open a time series database at the specified path, with the specified `Indexer`.
    ///
    /// `path` is the full path and filename for the database.
    pub fn open_with_indexer(path: &str, indexer: I) -> Result<Self, Error> {
        let f = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)
            .map_err(Error::IOError)?;

        let mut series = Self {
            element_for_key: Self::load_file(&f)?,
            writer: LineWriter::new(f),
            indexer,
        };

        // Populate the index
        for (id, data) in &series.element_for_key {
            series.indexer.insert(id, data);
        }

        Ok(series)
    }

    /// Load a file and return all of the records in it.
    fn load_file(f: &File) -> Result<AHashMap<UniqueId, T>, Error> {
        let mut records: AHashMap<UniqueId, T> = Default::default();
        let reader = BufReader::new(f);
        for line in reader.lines() {
            let line= line.map_err(Error::IOError)?;
            let record: DeletableRecord<T> = line.as_str().try_into()?;
            match record.data {
                Some(val) => records.insert(record.id, val),
                None => records.remove(&record.id),
            };
        }
        Ok(records)
    }

    /// Put a new record into the database. A unique id will be assigned to the record and
    /// returned.
    pub fn put(&mut self, entry: T) -> Result<UniqueId, Error> {
        let record = Record::new(entry);

        match self.element_for_key.entry(record.id) {
            Entry::Vacant(ve) => {
                // Insert into main in-memory store
                ve.insert(record.data.clone());

                // Insert into index
                self.indexer.insert(&record.id, &record.data);

                // Write to file
                DeletableRecord {
                    id: record.id,
                    data: Some(record.data)
                }.write_line(&mut self.writer)?;

                Ok(record.id)
            },
            Entry::Occupied(_) => {
                Err(Error::IOError(std::io::Error::from(std::io::ErrorKind::AlreadyExists)))
            }
        }
    }

    /// Update an existing record. The `UniqueId` of the record passed into this function must match
    /// the `UniqueId` of a record already in the database.
    pub fn update(&mut self, record: Record<T>) -> Result<(), Error> {
        match self.element_for_key.entry(record.id) {
            Entry::Vacant(_) => {
                Err(Error::IOError(std::io::Error::from(std::io::ErrorKind::NotFound)))
            },
            Entry::Occupied(mut oe) => {
                // Update main in-memory store
                oe.insert(record.data.clone());

                // Update index
                self.indexer.update(&record.id, oe.get(), &record.data);

                // Write to file
                DeletableRecord {
                    id: record.id,
                    data: Some(record.data)
                }.write_line(&mut self.writer)
            }
        }
    }

    /// Delete a record from the database
    ///
    /// Future note: while this deletes a record from the view, it only adds an entry to the
    /// database that indicates `data: null`. If record histories ever become important, the record
    /// and its entire history (including this delete) will still be available.
    /// TODO: Large overlap between put, update, and delete. Partially caused by the whole
    /// `Deletablerecord`+`Record` story, whose purpose I don't yet fully understand.
    /// TODO: Returning deleted item on successful deletion is more-or-less free, but changes API.
    pub fn delete(&mut self, uuid: &UniqueId) -> Result<(), Error> {
        // Remove from main in-memory store
        if let Some(prev_val) = self.element_for_key.remove(uuid) {
            // Remove from index
            self.indexer.remove(&uuid, &prev_val);

            // Write to file
            DeletableRecord::<T> {
                id: *uuid,
                data: None
            }.write_line(&mut self.writer)
        } else {
            Err(Error::IOError(std::io::Error::from(std::io::ErrorKind::NotFound)))
        }
    }

    /// Get all of the records in the database.
    #[deprecated(note = "Use the `records` method to get an iterator instead")]
    pub fn all_records(&self) -> Result<Vec<Record<T>>, Error> {
        self.records().map(|rs| rs.collect())
    }

    /// Constructs an iterator over all of the records in the database.
    pub fn records<'s>(&'s self) -> Result<impl Iterator<Item = Record<T>> + 's, Error> {
        Ok(self.element_for_key.iter().map(|(&id, el)| Record { id, data: el.clone() }))
    }

    /*  The point of having Search is so that a lot of internal optimizations can happen once the
     *  data sets start getting large. */
    /// Perform a search on the records in a database, based on the given criteria.
    pub fn search<C>(&self, criteria: C) -> Result<Vec<Record<T>>, Error>
    where
        C: Criteria,
    {
        let results: Vec<Record<T>> = self.element_for_key
            .iter()
            .filter(|&tr| criteria.apply(tr.1))
            .map(|(&id, el)| Record { id, data: el.clone() })
            .collect();
        Ok(results)
    }

    /// Perform a search and sort the resulting records based on the comparison.
    pub fn search_sorted<C, CMP>(&self, criteria: C, compare: CMP) -> Result<Vec<Record<T>>, Error>
    where
        C: Criteria,
        CMP: FnMut(&Record<T>, &Record<T>) -> Ordering,
    {
        match self.search(criteria) {
            Ok(mut records) => {
                records.sort_by(compare);
                Ok(records)
            }
            Err(err) => Err(err),
        }
    }

    /// Get an exact record from the database based on unique id.
    // TODO: Figure out why the return type is the way it is. What Err-condition is anticipated?
    pub fn get(&self, uuid: &UniqueId) -> Result<Option<Record<T>>, Error> {
        let val = self.element_for_key.get(uuid);

        Ok(val.map(|el| Record { id: *uuid, data: el.clone() }))
    }

    #[deprecated(note = "Never-stable API to experiment with indexing")]
    pub fn search_range<'s>(&'s self, range: impl std::ops::RangeBounds<DateTimeTz> + 's)
                            -> Result<Box<dyn Iterator<Item = (&'s UniqueId, &'s T)> + 's>, Error> {
        self.indexer.retrieve_range(&self.element_for_key, range)
    }
}


#[cfg(test)]
mod tests {
    extern crate chrono;
    extern crate dimensioned;

    use chrono_tz::Etc::UTC;
    use self::chrono::prelude::*;
    use self::dimensioned::si::{M, Meter, S, Second, KG, Kilogram};
    use std::fs;
    use std::ops;
    use date_time_tz::DateTimeTz;

    use super::*;
    use criteria::*;
    use std::str::FromStr;
    use series::indexing::{IndexByTime, IndexByAllTags, IndexBySelectedTags};

    #[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
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
            let mut tags = Vec::new();
            if self.distance >= Distance(25_000. * M) {
                tags.push("Long!".to_owned())
            }
            tags
        }
    }

    struct SeriesFileCleanup(String);

    impl SeriesFileCleanup {
        fn new(path: &str) -> SeriesFileCleanup {
            SeriesFileCleanup(String::from(path))
        }
    }

    impl ops::Drop for SeriesFileCleanup {
        fn drop(&mut self) {
            fs::remove_file(&self.0).expect("failed to remove time series file");
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

    #[test]
    pub fn can_add_and_retrieve_entries() {
        let _series_remover = SeriesFileCleanup::new("var/can_add_and_retrieve_entries.json");
        let trips = mk_trips();
        let mut ts: Series<BikeTrip> = Series::open("var/can_add_and_retrieve_entries.json")
            .expect("expect the time series to open correctly");
        let uuid = ts.put(trips[0].clone()).expect("expect a successful put");
        let record_res = ts.get(&uuid);

        for trip in &trips[1..] {
            ts.put(trip.clone()).expect("expect a successful put");
        }

        match record_res {
            Err(err) => assert!(false, err),
            Ok(None) => assert!(false, "There should have been a value here"),
            Ok(Some(tr)) => {
                assert_eq!(tr.id, uuid);
                assert_eq!(
                    tr.timestamp(),
                    DateTimeTz(UTC.ymd(2011, 10, 29).and_hms(0, 0, 0))
                );
                assert_eq!(tr.data.duration, Duration(11040.0 * S));
                assert_eq!(tr.data.comments, String::from("long time ago"));
                assert_eq!(tr.data, trips[0]);
            }
        }
    }

    #[test]
    pub fn can_retrieve_entries_iterator() {
        let _series_remover = SeriesFileCleanup::new("var/can_retrieve_entries_iterator.json");
        let trips = mk_trips();
        let mut ts: Series<BikeTrip> = Series::open("var/can_retrieve_entries_iterator.json")
            .expect("expect the time series to open correctly");

        for trip in &trips {
            ts.put(trip.clone()).expect("expect a successful put");
        }

        let as_vec = ts.all_records().expect("retrieval is currently infallible");
        let as_iter = ts.records().expect("retrieval is currently infallible");

        for (from_vec, from_iter) in as_vec.iter().zip(as_iter) {
            assert_eq!(from_iter.id, from_vec.id);
            assert_eq!(from_iter.data, from_vec.data);
        }
    }

    #[test]
    pub fn can_search_for_an_entry_with_exact_time() {
        let _series_remover =
            SeriesFileCleanup::new("var/can_search_for_an_entry_with_exact_time.json");
        let trips = mk_trips();
        let mut ts: Series<BikeTrip> = Series::open(
            "var/can_search_for_an_entry_with_exact_time.json",
        ).expect("expect the time series to open correctly");
        for trip in &trips {
            ts.put(trip.clone()).expect("expect a successful put");
        }

        match ts.search(exact_time(
            DateTimeTz(UTC.ymd(2011, 10, 31).and_hms(0, 0, 0)),
        )) {
            Err(err) => assert!(false, err),
            Ok(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].data, trips[1]);
            }
        }
    }


    #[test]
    pub fn can_get_entries_in_time_range() {
        let _series_remover = SeriesFileCleanup::new("var/can_get_entries_in_time_range.json");
        let trips = mk_trips();
        let mut ts: Series<BikeTrip, IndexByTime> = Series::open("var/can_get_entries_in_time_range.json")
            .expect("expect the time series to open correctly");
        for trip in &trips {
            ts.put(trip.clone()).expect("expect a successful put");
        }

        // Using search_sorted
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
                assert_eq!(v[0].data, trips[1]);
                assert_eq!(v[1].data, trips[2]);
                assert_eq!(v[2].data, trips[3]);
            }
        }

        // The same case, this time using search_range
        match ts.search_range(
            DateTimeTz(UTC.ymd(2011, 10, 31).and_hms(0, 0, 0))
                ..=
                DateTimeTz(UTC.ymd(2011, 11, 04).and_hms(0, 0, 0))
        ) {
            Err(err) => assert!(false, err),
            Ok(it) => {
                let v: Vec<_> = it.collect();
                assert_eq!(v.len(), 3);
                assert_eq!(*v[0].1, trips[1]);
                assert_eq!(*v[1].1, trips[2]);
                assert_eq!(*v[2].1, trips[3]);
            }
        };
    }


    #[test]
    pub fn can_get_entries_with_specific_tag() {
        let _series_remover = SeriesFileCleanup::new(
            "var/can_get_entries_with_specific_tag__no_index.json");
        let mut ts_noindex: Series<BikeTrip, NoIndex> = Series::open(
            "var/can_get_entries_with_specific_tag__no_index.json"
        ).expect("expect the time series to open correctly");

        let _series_remover = SeriesFileCleanup::new(
            "var/can_get_entries_with_specific_tag__by_time.json");
        let mut ts_by_time: Series<BikeTrip, IndexByTime> = Series::open(
            "var/can_get_entries_with_specific_tag__by_time.json"
        ).expect("expect the time series to open correctly");

        let _series_remover = SeriesFileCleanup::new(
            "var/can_get_entries_with_specific_tag__by_all_tag.json");
        let mut ts_by_all_tag: Series<BikeTrip, IndexByAllTags> = Series::open(
            "var/can_get_entries_with_specific_tag__by_all_tag.json"
        ).expect("expect the time series to open correctly");

        let _series_remover = SeriesFileCleanup::new(
            "var/can_get_entries_with_specific_tag__by_some_tag.json");
        let mut ts_by_some_tag: Series<BikeTrip, IndexBySelectedTags> = Series::open_with_indexer(
            "var/can_get_entries_with_specific_tag__by_some_tag.json",
                IndexBySelectedTags::for_tags(vec!["Long!".to_owned()])
        ).expect("expect the time series to open correctly");

        let trips = mk_trips();
        for trip in &trips {
            ts_noindex.put(trip.clone()).expect("expect a successful put");
            ts_by_time.put(trip.clone()).expect("expect a successful put");
            ts_by_all_tag.put(trip.clone()).expect("expect a successful put");
            ts_by_some_tag.put(trip.clone()).expect("expect a successful put");
        }

        fn check_result<I: Indexer>(trips: &[BikeTrip], series: Series<BikeTrip, I>) {
            // FIXME: Manually using the indexer methods; bad form!
            // Need to come up with proper way of checking `Criteria` from `Indexer`s
            match series.indexer.retrieve_tagged(&series.element_for_key, "Long!") {
                Err(err) => assert!(false, err),
                Ok(it) => {
                    let mut v: Vec<_> = it.collect();
                    v.sort_unstable_by_key(|t| t.1.timestamp());
                    assert_eq!(v.len(), 3);
                    assert_eq!(*v[0].1, trips[0]);
                    assert_eq!(*v[1].1, trips[2]);
                    assert_eq!(*v[2].1, trips[3]);
                }
            };
        }

        check_result(&trips, ts_noindex);
        check_result(&trips, ts_by_time);
        check_result(&trips, ts_by_all_tag);
        check_result(&trips, ts_by_some_tag);
    }


    #[test]
    pub fn persists_and_reads_an_entry() {
        let _series_remover = SeriesFileCleanup::new("var/persists_and_reads_an_entry.json");
        let trips = mk_trips();

        {
            let mut ts: Series<BikeTrip> = Series::open("var/persists_and_reads_an_entry.json")
                .expect("expect the time series to open correctly");

            for trip in &trips {
                ts.put(trip.clone()).expect("expect a successful put");
            }
        }

        {
            let ts: Series<BikeTrip> = Series::open("var/persists_and_reads_an_entry.json")
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
                    assert_eq!(v[0].data, trips[1]);
                    assert_eq!(v[1].data, trips[2]);
                    assert_eq!(v[2].data, trips[3]);
                }
            }
        }
    }


    #[test]
    pub fn can_write_to_existing_file() {
        let _series_remover = SeriesFileCleanup::new("var/can_write_to_existing_file.json");
        let trips = mk_trips();

        {
            let mut ts: Series<BikeTrip> = Series::open("var/can_write_to_existing_file.json")
                .expect("expect the time series to open correctly");

            for trip in &trips[0..=2] {
                ts.put(trip.clone()).expect("expect a successful put");
            }
        }

        {
            let mut ts: Series<BikeTrip> = Series::open("var/can_write_to_existing_file.json")
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
                    assert_eq!(v[0].data, trips[1]);
                    assert_eq!(v[1].data, trips[2]);
                    ts.put(trips[3].clone()).expect("expect a successful put");
                    ts.put(trips[4].clone()).expect("expect a successful put");
                }
            }
        }

        {
            let ts: Series<BikeTrip> = Series::open("var/can_write_to_existing_file.json").expect(
                "expect the time series to open correctly",
            );
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
                    assert_eq!(v[0].data, trips[1]);
                    assert_eq!(v[1].data, trips[2]);
                    assert_eq!(v[2].data, trips[3]);
                    assert_eq!(v[3].data, trips[4]);
                }
            }
        }
    }

    #[test]
    pub fn can_overwrite_existing_entry() {
        let _series_remover = SeriesFileCleanup::new("var/can_overwrite_existing_entry.json");
        let trips = mk_trips();

        let mut ts: Series<BikeTrip> = Series::open("var/can_overwrite_existing_entry.json")
            .expect("expect the time series to open correctly");

        ts.put(trips[0].clone()).expect("expect a successful put");
        ts.put(trips[1].clone()).expect("expect a successful put");
        let trip_id = ts.put(trips[2].clone()).expect("expect a successful put");

        match ts.get(&trip_id) {
            Err(err) => assert!(false, err),
            Ok(None) => assert!(false, "record not found"),
            Ok(Some(mut trip)) => {
                trip.data.distance = Distance(50000.0 * M);
                ts.update(trip).expect("expect record to update");
            }
        };

        match ts.get(&trip_id) {
            Err(err) => assert!(false, err),
            Ok(None) => assert!(false, "record not found"),
            Ok(Some(trip)) => {
                assert_eq!(
                    trip.data.datetime,
                    DateTimeTz(UTC.ymd(2011, 11, 02).and_hms(0, 0, 0))
                );
                assert_eq!(trip.data.distance, Distance(50000.0 * M));
                assert_eq!(trip.data.duration, Duration(7020.0 * S));
                assert_eq!(trip.data.comments, String::from("Do Some Distance!"));
            }
        }
    }

    #[test]
    pub fn record_overwrites_get_persisted() {
        let _series_remover = SeriesFileCleanup::new("var/record_overwrites_get_persisted.json");
        let trips = mk_trips();

        {
            let mut ts: Series<BikeTrip> = Series::open("var/record_overwrites_get_persisted.json")
                .expect("expect the time series to open correctly");

            ts.put(trips[0].clone()).expect("expect a successful put");
            ts.put(trips[1].clone()).expect("expect a successful put");
            let trip_id = ts.put(trips[2].clone()).expect("expect a successful put");

            match ts.get(&trip_id) {
                Err(err) => assert!(false, err),
                Ok(None) => assert!(false, "record not found"),
                Ok(Some(mut trip)) => {
                    trip.data.distance = Distance(50000.0 * M);
                    ts.update(trip).expect("expect record to update");
                }
            };
        }

        {
            let ts: Series<BikeTrip> = Series::open("var/record_overwrites_get_persisted.json")
                .expect("expect the time series to open correctly");

            match ts.all_records() {
                Err(err) => assert!(false, err),
                Ok(trips) => assert_eq!(trips.len(), 3),
            }

            match ts.search(exact_time(
                DateTimeTz(UTC.ymd(2011, 11, 02).and_hms(0, 0, 0)),
            )) {
                Err(err) => assert!(false, err),
                Ok(trips) => {
                    assert_eq!(trips.len(), 1);
                    assert_eq!(
                        trips[0].data.datetime,
                        DateTimeTz(UTC.ymd(2011, 11, 02).and_hms(0, 0, 0))
                    );
                    assert_eq!(trips[0].data.distance, Distance(50000.0 * M));
                    assert_eq!(trips[0].data.duration, Duration(7020.0 * S));
                    assert_eq!(trips[0].data.comments, String::from("Do Some Distance!"));
                }
            }
        }
    }


    #[test]
    pub fn time_index_is_populated_on_load() {
        let _series_remover = SeriesFileCleanup::new("var/time_index_is_restored_on_load.json");
        let trips = mk_trips();

        {
            let mut ts: Series<BikeTrip> = Series::open("var/time_index_is_restored_on_load.json")
                .expect("expect the time series to open correctly");

            for trip in &trips {
                ts.put(trip.clone()).expect("expect a successful put");
            }
        }

        {
            let ts: Series<BikeTrip, IndexByTime> = Series::open("var/time_index_is_restored_on_load.json")
                .expect("expect the time series to open correctly");
            match ts.search_range(
                    DateTimeTz(UTC.ymd(2011, 10, 31).and_hms(0, 0, 0))
                        ..=
                    DateTimeTz(UTC.ymd(2011, 11, 04).and_hms(0, 0, 0))
            ) {
                Err(err) => assert!(false, err),
                Ok(it) => {
                    let v: Vec<_> = it.collect();
                    assert_eq!(v.len(), 3);
                    assert_eq!(*v[0].1, trips[1]);
                    assert_eq!(*v[1].1, trips[2]);
                    assert_eq!(*v[2].1, trips[3]);
                }
            };
        }
    }

    #[test]
    pub fn can_delete_an_entry() {
        let _series_remover = SeriesFileCleanup::new("var/record_deletes.json");
        let trips = mk_trips();

        {
            let mut ts: Series<BikeTrip> = Series::open("var/record_deletes.json").expect(
                "expect the time series to open correctly",
            );
            let trip_id = ts.put(trips[0].clone()).expect("expect a successful put");
            ts.put(trips[1].clone()).expect("expect a successful put");
            ts.put(trips[2].clone()).expect("expect a successful put");

            ts.delete(&trip_id).expect("successful delete");

            let recs = ts.all_records().expect("good record retrieval");
            assert_eq!(recs.len(), 2);
        }

        {
            let ts: Series<BikeTrip> = Series::open("var/record_deletes.json").expect(
                "expect the time series to open correctly",
            );
            let recs = ts.all_records().expect("good record retrieval");
            assert_eq!(recs.len(), 2);
        }

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

        let uid = UniqueId::from_str("3330c5b0-783f-4919-b2c4-8169c38f65ff")
            .expect("something is wrong with this ID");
        let rec = ts.get(&uid);
        match rec {
            Err(err) => assert!(false, err),
            Ok(None) => assert!(false, "no record found"),
            Ok(Some(rec)) => assert_eq!(rec.data.weight, Weight(77.79109 * KG)),
        }
    }

}
