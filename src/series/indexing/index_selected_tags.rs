use ahash::{AHashMap};
use series::indexing::{Indexer, NoIndex};
use ::{UniqueId, Error};
use std::collections::HashMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use ::{DateTimeTz, Recordable};
use std::ops::RangeBounds;

// TODO: Note in documentation: `IndexBySelectedTags` does _not_ implement `Default`, since a
// default-constructed  `IndexBySelectedTags` is useless: it indexes by no tag.
pub struct IndexBySelectedTags {
    ids_by_tag: HashMap<Box<str>, Vec<UniqueId>>,
}

impl Indexer for IndexBySelectedTags {
    fn insert(&mut self, id: &UniqueId, recordable: &impl Recordable) {
        for tag in recordable.tags() {
            self.insert_raw(id, tag.as_ref())
        }
    }

    fn update(&mut self, id: &UniqueId, old: &impl Recordable, new: &impl Recordable) {
        // TODO: update more intelligently?
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
            NoIndex.retrieve_tagged(element_for_key, criteria)
        }
    }
}

impl IndexBySelectedTags {
    /// TODO: Document
    pub fn for_tags(tags: Vec<String>) -> Self {
        IndexBySelectedTags{
            ids_by_tag: tags.into_iter()
                .map(|tag| (tag.into_boxed_str(), Vec::new()))
                .collect()
        }
    }
    
    /// Insert UniqueId into tag-index
    fn insert_raw(&mut self, id: &UniqueId, tag: &str) {
        if let Some(new_bucket) = self.ids_by_tag.get_mut(tag) {
            let idx = new_bucket.binary_search(id).unwrap_or_else(|i| i);
            new_bucket.insert(idx, *id);
        }
    }

    /// Removes UniqueId from tag-index
    fn remove_raw(&mut self, id: &UniqueId, tag: &str) {
        if let Some(old_bucket) = self.ids_by_tag.get_mut(tag) {
            let idx = old_bucket
                .binary_search(id)
                .expect("Elements in in-memory store should be in index too");
            let prev_id = old_bucket.remove(idx);
            debug_assert_eq!(&prev_id, id);
        }
    }
}

