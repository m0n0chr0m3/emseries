extern crate chrono;
extern crate serde;
extern crate serde_json;
extern crate uuid;

use self::serde::de::DeserializeOwned;
use self::serde::ser::Serialize;
use self::uuid::Uuid;
use crate::date_time_tz::DateTimeTz;
use std::error;
use std::fmt;
use std::io;
use std::str;

/// Errors for the database
#[derive(Debug)]
pub enum Error {
    /// Indicates that the UUID specified is invalid and cannot be parsed
    UUIDParseError(uuid::ParseError),

    /// Indicates an error in the JSON serialization
    JSONStringError(serde_json::error::Error),

    /// Indicates an error in the JSON deserialization
    JSONParseError(serde_json::error::Error),

    /// Indicates a general IO error
    IOError(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::UUIDParseError(err) => write!(f, "UUID failed to parse: {}", err),
            Error::JSONStringError(err) => write!(f, "Error generating a JSON string: {}", err),
            Error::JSONParseError(err) => write!(f, "Error parsing JSON: {}", err),
            Error::IOError(err) => write!(f, "IO Error: {}", err),
        }
    }
}

impl error::Error for Error {
    fn cause(&self) -> Option<&dyn error::Error> {
        match self {
            Error::UUIDParseError(ref err) => Some(err),
            Error::JSONStringError(ref err) => Some(err),
            Error::JSONParseError(ref err) => Some(err),
            Error::IOError(ref err) => Some(err),
        }
    }
}

/// Any element to be put into the database needs to be Recordable. This is the common API that
/// will aid in searching and later in indexing records.
pub trait Recordable {
    /// The timestamp for the record.
    fn timestamp(&self) -> DateTimeTz;

    /// A list of string tags that can be used for indexing. This list defined per-type.
    fn tags(&self) -> Vec<String>;
}

/// Uniquely identifies a record.
///
/// This is a wrapper around a basic uuid with some extra convenience methods.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct UniqueId(Uuid);

impl UniqueId {
    /// Create a new V4 UUID (this is the most common type in use these days).
    pub fn new() -> UniqueId {
        let id = Uuid::new_v4();
        UniqueId(id)
    }
}

impl str::FromStr for UniqueId {
    type Err = Error;

    /// Parse a UniqueId from a string. Raise UUIDParseError if the parsing fails.
    fn from_str(val: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(val)
            .map(UniqueId)
            .map_err(|err| Error::UUIDParseError(err))
    }
}

impl fmt::Display for UniqueId {
    /// Convert to a hyphenated string
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0.hyphenated().to_string())
    }
}

/// Every record contains a unique ID and then the primary data, which itself must implementd the
/// Recordable trait.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Record<T: Clone + Recordable> {
    pub id: UniqueId,
    pub data: T,
}

impl<T> Record<T>
where
    T: Clone + Recordable,
{
    pub fn new(data: T) -> Record<T> {
        let id = UniqueId::new();
        Record { id, data }
    }
}

impl<T> Recordable for Record<T>
where
    T: Clone + Recordable,
{
    fn timestamp(&self) -> DateTimeTz {
        self.data.timestamp()
    }
    fn tags(&self) -> Vec<String> {
        self.data.tags()
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct DeletableRecord<T: Clone + Recordable> {
    pub id: UniqueId,
    pub data: Option<T>,
}

impl<T> str::FromStr for DeletableRecord<T>
where
    T: Clone + Recordable + DeserializeOwned + Serialize,
{
    type Err = Error;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(&line).map_err(|err| {
            println!("deserialization error: {}", err);
            Error::JSONParseError(err)
        })
    }
}

#[cfg(test)]
mod test {
    extern crate dimensioned;
    extern crate serde_json;

    use self::dimensioned::si::{Kilogram, KG};
    use super::{DeletableRecord, Recordable};
    use crate::date_time_tz::DateTimeTz;
    use chrono::TimeZone;
    use chrono_tz::Etc::UTC;
    use chrono_tz::US::Central;

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

    const WEIGHT_ENTRY: &str = "{\"data\":{\"weight\":77.79109,\"date\":\"2003-11-10T06:00:00.000000000000Z\"},\"id\":\"3330c5b0-783f-4919-b2c4-8169c38f65ff\"}";

    #[test]
    pub fn legacy_deserialization() {
        let rec: DeletableRecord<WeightRecord> = WEIGHT_ENTRY
            .parse()
            .expect("should successfully parse the record");
        assert_eq!(
            rec.id,
            "3330c5b0-783f-4919-b2c4-8169c38f65ff".parse().unwrap()
        );
        assert_eq!(
            rec.data,
            Some(WeightRecord {
                date: DateTimeTz(UTC.with_ymd_and_hms(2003, 11, 10, 6, 0, 0).unwrap()),
                weight: Weight(77.79109 * KG),
            })
        );
    }

    #[test]
    pub fn serialization_output() {
        let rec = WeightRecord {
            date: DateTimeTz(UTC.with_ymd_and_hms(2003, 11, 10, 6, 0, 0).unwrap()),
            weight: Weight(77.0 * KG),
        };
        assert_eq!(
            serde_json::to_string(&rec).unwrap(),
            "{\"date\":\"2003-11-10T06:00:00Z\",\"weight\":77.0}"
        );

        let rec2 = WeightRecord {
            date: DateTimeTz(Central.with_ymd_and_hms(2003, 11, 10, 0, 0, 0).unwrap()),
            weight: Weight(77.0 * KG),
        };
        assert_eq!(
            serde_json::to_string(&rec2).unwrap(),
            "{\"date\":\"2003-11-10T06:00:00Z US/Central\",\"weight\":77.0}"
        );
    }
}
