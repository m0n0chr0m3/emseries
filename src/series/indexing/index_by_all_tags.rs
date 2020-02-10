use ahash::{AHashMap};
use series::indexing::{Indexer, NoIndex};
use ::{UniqueId, Error};
use std::collections::HashMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use ::{DateTimeTz, Recordable};
use std::ops::RangeBounds;

// TODO: Is it worth maintaining a separate type for this, or would it be better to add a
// configuration option to IndexBySelectedTags which determines whether empty buckets are ignored
// or created?

#[derive(Default)]
pub struct IndexByAllTags {
    ids_by_tag: HashMap<Box<str>, Vec<UniqueId>>,
}

impl Indexer for IndexByAllTags {
    fn insert(&mut self, id: &UniqueId, recordable: &impl Recordable) {
        for tag in recordable.tags() {
            self.insert_raw(id, tag.into_boxed_str())
        }
    }

    fn update(&mut self, id: &UniqueId, old: &impl Recordable, new: &impl Recordable) {
        // Update index only if necessary
        if old.tags() != new.tags() {
            self.remove(id, old);
            self.insert(id, new);
        }
    }

    fn remove(&mut self, id: &UniqueId, recordable: &impl Recordable) {
        for tag in recordable.tags() {
            self.remove_raw(id, &tag)
        }
    }

    fn retrieve_range<'s, T: Clone + Recordable + DeserializeOwned + Serialize>(
        &'s self,
        element_for_key: &'s AHashMap<UniqueId, T>,
        criteria: impl RangeBounds<DateTimeTz> + 's,
    ) -> Result<Box<dyn Iterator<Item=(&'s UniqueId, &'s T)> + 's>, Error> {
        NoIndex::retrieve_range(&NoIndex, element_for_key, criteria)
    }

    fn retrieve_tagged<'s, T: Clone + Recordable + DeserializeOwned + Serialize>(
        &'s self,
        element_for_key: &'s AHashMap<UniqueId, T>,
        criteria: &'s str
    ) -> Result<Box<dyn Iterator<Item=(&'s UniqueId, &'s T)> + 's>, Error> {
        if let Some(bucket) = self.ids_by_tag.get(criteria) {
            Ok(Box::new(bucket
                .iter()
                .map(move |id| {
                    (id,
                     element_for_key.get(id)
                         .unwrap_or_else(||
                             unreachable!("Elements in index should be in in-memory store too")))
                })))
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }
}

impl IndexByAllTags {
    /// Insert UniqueId into tag-index
    fn insert_raw(&mut self, id: &UniqueId, tag: Box<str>) {
        let new_bucket = self.ids_by_tag
            .entry(tag)
            .or_default();
        let idx = new_bucket.binary_search(id).unwrap_or_else(|i|i);
        new_bucket.insert(idx, *id);
    }

    /// Removes UniqueId from tag-index
    fn remove_raw(&mut self, id: &UniqueId, tag: &str) {
        let old_bucket = self.ids_by_tag.get_mut(tag)
            .expect("Elements in in-memory store should be in index too");
        let idx = old_bucket
            .binary_search(id)
            .expect("Elements in in-memory store should be in index too");
        let prev_id = old_bucket.remove(idx);
        debug_assert_eq!(&prev_id, id);
    }

}
