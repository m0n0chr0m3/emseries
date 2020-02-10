/*! An Embedded Time Series Database

This library provides a low-intensity time series database meant to be embedded inside of an
application.

From the signature of the series

```text
pub struct Series<T: Clone + Recordable + DeserializeOwned + Serialize> {
```

you can know that you must parameterize the series over the data type that you want to store,
which must also have several traits implemented.

```text
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct BikeTrip {
    datetime: DateTime<Utc>,
    distance: Distance,
    duration: Duration,
    comments: String,
}

impl Recordable for BikeTrip {
    fn timestamp(&self) -> DateTime<Utc> {
        self.datetime
    }
    fn tags(&self) -> Vec<String> {
        Vec::new()
    }
}
```

Recordable requires implementations for `timestamp` and `tags`, both of which can be used for
searching for records, and both of which may be used for indexing in the future.

The series can only store a single data type, but you can always store multiple data types by
wrapping them into a single enum.

Open the series:

```text
let mut ts: Series<BikeTrip> = Series::open("var/bike_trips.json")
    .expect("expect the time series to open correctly");
```

The series file will be created if it does not already exist. If it does already exist, the existing data will be read into memory and made available.

Note: all of the data is read into memory at once. For human-scale things, this probably takes up very little memory, but this software is not optimized for IoT scale deployments. Additionally, this library assumes only one process is writing to the file. Behavior from more than one process writing to the file is currently undefined.
*/

#[macro_use]
extern crate serde_derive;
extern crate chrono;
extern crate chrono_tz;
extern crate serde;
extern crate ahash;

mod criteria;
mod date_time_tz;
mod series;
mod types;

pub use date_time_tz::DateTimeTz;
pub use criteria::*;
pub use series::Series;
pub use types::{Error, Record, Recordable, UniqueId};
pub use series::indexing;
