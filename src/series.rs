extern crate serde;
extern crate serde_json;
extern crate uuid;

use self::serde::de::DeserializeOwned;
use self::serde::ser::Serialize;
use std::collections::HashMap;
use std::cmp::Ordering;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, LineWriter};

use criteria::Criteria;
use types::{Error, Record, Recordable, DeletableRecord, UniqueId};
use std::collections::hash_map::Entry;
use std::convert::TryInto;

/// An open time series database.
///
/// Any given database can store only one data type, T. The data type must be determined when the
/// database is opened.
pub struct Series<T: Clone + Recordable + DeserializeOwned + Serialize> {
    writer: LineWriter<File>,
    element_for_key: HashMap<UniqueId, T>,
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

        let element_for_key = Series::load_file(&f)?;

        let writer = LineWriter::new(f);

        Ok(Series {
            writer,
            element_for_key,
        })
    }

    /// Load a file and return all of the records in it.
    fn load_file(f: &File) -> Result<HashMap<UniqueId, T>, Error> {
        let mut records: HashMap<UniqueId, T> = HashMap::new();
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
                ve.insert(record.data.clone());
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
                oe.insert(record.data.clone());
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
        if let Some(_prev_val) = self.element_for_key.remove(uuid) {
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

        ts.put(trips[1].clone()).expect("expect a successful put");
        ts.put(trips[2].clone()).expect("expect a successful put");
        ts.put(trips[3].clone()).expect("expect a successful put");
        ts.put(trips[4].clone()).expect("expect a successful put");

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

        ts.put(trips[0].clone()).expect("expect a successful put");
        ts.put(trips[1].clone()).expect("expect a successful put");
        ts.put(trips[2].clone()).expect("expect a successful put");
        ts.put(trips[3].clone()).expect("expect a successful put");
        ts.put(trips[4].clone()).expect("expect a successful put");

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
        ts.put(trips[0].clone()).expect("expect a successful put");
        ts.put(trips[1].clone()).expect("expect a successful put");
        ts.put(trips[2].clone()).expect("expect a successful put");
        ts.put(trips[3].clone()).expect("expect a successful put");
        ts.put(trips[4].clone()).expect("expect a successful put");

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
        let mut ts: Series<BikeTrip> = Series::open("var/can_get_entries_in_time_range.json")
            .expect("expect the time series to open correctly");
        ts.put(trips[0].clone()).expect("expect a successful put");
        ts.put(trips[1].clone()).expect("expect a successful put");
        ts.put(trips[2].clone()).expect("expect a successful put");
        ts.put(trips[3].clone()).expect("expect a successful put");
        ts.put(trips[4].clone()).expect("expect a successful put");

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


    #[test]
    pub fn persists_and_reads_an_entry() {
        let _series_remover = SeriesFileCleanup::new("var/persists_and_reads_an_entry.json");
        let trips = mk_trips();

        {
            let mut ts: Series<BikeTrip> = Series::open("var/persists_and_reads_an_entry.json")
                .expect("expect the time series to open correctly");

            ts.put(trips[0].clone()).expect("expect a successful put");
            ts.put(trips[1].clone()).expect("expect a successful put");
            ts.put(trips[2].clone()).expect("expect a successful put");
            ts.put(trips[3].clone()).expect("expect a successful put");
            ts.put(trips[4].clone()).expect("expect a successful put");
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

            ts.put(trips[0].clone()).expect("expect a successful put");
            ts.put(trips[1].clone()).expect("expect a successful put");
            ts.put(trips[2].clone()).expect("expect a successful put");
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
