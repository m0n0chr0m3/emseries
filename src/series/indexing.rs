use ::{DateTimeTz, UniqueId};
use ::{Recordable};
use Error;
use ahash::{AHashMap};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::ops::RangeBounds;


pub trait Indexer {
    /// Insert a new `Recordable` into the `Indexer` for the given `id`.
    ///
    /// The `Indexer` may assume no `Recordable` with the same `id` is in the index. Inserting an
    /// `id` which is already in the index may either result in duplicate elements, or overwriting.
    fn insert(&mut self, id: &UniqueId, recordable: &impl Recordable);

    /// Updates which `Recordable` is stored in the `Indexer` for the given `id`.
    ///
    /// The `Indexer` may assume that `old` is currently in the index with the specified `id`.
    /// Calling this method with an `old` that isn't in the index with the specified `id` may
    ///(erroneously) cause an arbitrary `Recordable` to be evicted from the index.
    fn update(&mut self, id: &UniqueId, old: &impl Recordable, new: &impl Recordable);

    /// Removes the `Recordable` which is stored in the `Indexer` for the given `id`.
    ///
    /// The `Indexer` may assume that the `Recordable` is currently in the index with the specified
    /// `id`. Calling this method with a `Recordable` that isn't in the index with the specified
    /// `id` may (erroneously) cause an arbitrary `Recordable` to be evicted from the index.
    fn remove(&mut self, id: &UniqueId, recordable: &impl Recordable);

    // TODO: Document other trait methods

    // TODO: Generalize for `Criteria`
    fn retrieve_range< 's, T: Clone + Recordable + DeserializeOwned + Serialize> (
        &'s self,
        element_for_key: &'s AHashMap<UniqueId, T>,
        criteria: impl std::ops::RangeBounds<DateTimeTz> + 's
    ) -> Result<Box<dyn Iterator<Item = (&'s UniqueId, &'s T)> + 's>, crate::Error>;
    // TODO: Merge with `retrieve_range`, using `Criteria`
    fn retrieve_tagged< 's, T: Clone + Recordable + DeserializeOwned + Serialize> (
        &'s self,
        element_for_key: &'s AHashMap<UniqueId, T>,
        criteria: &'s str,
    ) -> Result<Box<dyn Iterator<Item = (&'s UniqueId, &'s T)> + 's>, crate::Error>;
}

mod index_by_time;
pub use self::index_by_time::IndexByTime;

mod index_by_all_tags;
pub use self::index_by_all_tags::IndexByAllTags;

mod index_selected_tags;
pub use self::index_selected_tags::IndexBySelectedTags;

#[derive(Default)]
pub struct NoIndex;

impl Indexer for NoIndex {
    fn insert(&mut self, _id: &UniqueId, _recordable: &impl Recordable) {
        // NoIndex has no work to do on insert
    }

    fn update(&mut self, _id: &UniqueId, _old: &impl Recordable, _new: &impl Recordable) {
        // NoIndex has no work to do on update
    }

    fn remove(&mut self, _id: &UniqueId, _recordable: &impl Recordable) {
        // NoIndex has no work to do on remove
    }

    fn retrieve_range<'s, T: Clone + Recordable + DeserializeOwned + Serialize> (
        &'s self,
        element_for_key: &'s AHashMap<UniqueId, T>,
        criteria: impl RangeBounds<DateTimeTz> + 's
    ) -> Result<Box<dyn Iterator<Item = (&'s UniqueId, &'s T)> + 's>, Error> {
        let mut tmp: Vec<_> = element_for_key
            .iter()
            .filter(move |&(_id, data)| {
                criteria.contains(&data.timestamp())
            })
            .collect();
        tmp.sort_unstable_by_key(|tr| tr.1.timestamp());

        Ok(Box::new(tmp.into_iter()))
    }

    fn retrieve_tagged<'s, T: Clone + Recordable + DeserializeOwned + Serialize>(
        &'s self,
        element_for_key: &'s AHashMap<UniqueId, T>,
        criteria: &'s str
    ) -> Result<Box<dyn Iterator<Item=(&'s UniqueId, &'s T)> + 's>, Error> {
        Ok(Box::new(element_for_key
            .iter()
            .filter(move |&(_id, data)| {
                data.tags().iter().any(|tag| tag == criteria)
            })))
    }
}

