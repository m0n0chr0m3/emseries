use std::collections::BTreeMap;
use ahash::{AHashMap};
use ::{DateTimeTz, UniqueId};
use Error;
use indexing::Indexer;
use Recordable;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::ops::RangeBounds;
use series::indexing::NoIndex;

#[derive(Default)]
pub struct IndexByTime {
    ids_by_time: BTreeMap<DateTimeTz, Vec<UniqueId>>,
}

impl Indexer for IndexByTime {
    fn insert(&mut self, id: &UniqueId, recordable: &impl Recordable) {
        self.insert_raw(id, recordable.timestamp())
    }

    fn update(&mut self, id: &UniqueId, old: &impl Recordable, new: &impl Recordable) {
        // Update index only if necessary
        if old.timestamp() != new.timestamp() {
            self.remove(id, old);
            self.insert(id, new);
        }
    }

    fn remove(&mut self, id: &UniqueId, recordable: &impl Recordable) {
        self.remove_raw(id, &recordable.timestamp())
    }

    fn retrieve_range<'s, T: Clone + Recordable + DeserializeOwned + Serialize> (
        &'s self,
        element_for_key: &'s AHashMap<UniqueId, T>,
        criteria: impl RangeBounds<DateTimeTz> + 's,
    ) -> Result<Box<dyn Iterator<Item = (&'s UniqueId, &'s T)> + 's>, Error> {
        Ok(Box::new(self.ids_by_time
            .range(criteria)
            .flat_map(|(_, ids)| ids.iter())
            .map(move |id| {
                (id,
                 element_for_key.get(id)
                     .unwrap_or_else(||
                         unreachable!("Elements in index should be in in-memory store too")))
            })))
    }

    fn retrieve_tagged<'s, T: Clone + Recordable + DeserializeOwned + Serialize>(
        &'s self,
        element_for_key: &'s AHashMap<UniqueId, T>,
        criteria: &'s str
    ) -> Result<Box<dyn Iterator<Item=(&'s UniqueId, &'s T)> + 's>, Error> {
        NoIndex::retrieve_tagged(&NoIndex, element_for_key, criteria)
    }
}

impl IndexByTime {
    /// Inserts UniqueId into time-ordered index
    fn insert_raw(&mut self, id: &UniqueId, timestamp: DateTimeTz) {
        let new_bucket = self.ids_by_time
            .entry(timestamp)
            .or_default();
        let idx = new_bucket.binary_search(id).unwrap_or_else(|i|i);
        new_bucket.insert(idx, *id);
    }

    /// Removes UniqueId from time-ordered index
    fn remove_raw(&mut self, id: &UniqueId, timestamp: &DateTimeTz) {
        let old_bucket = self.ids_by_time.get_mut(timestamp)
            .expect("Elements in in-memory store should be in index too");
        let idx = old_bucket
            .binary_search(id)
            .expect("Elements in in-memory store should be in index too");
        let prev_id = old_bucket.remove(idx);
        debug_assert_eq!(&prev_id, id);
    }

}
