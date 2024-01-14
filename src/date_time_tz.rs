extern crate chrono;
extern crate chrono_tz;

use chrono::SecondsFormat;
use chrono_tz::Etc::UTC;
use serde::de::{self, Deserialize, Deserializer, Visitor};
use serde::ser::{Serialize, Serializer};
use std::fmt;

/// This is a wrapper around date time objects, using timezones from the chroon-tz database and
/// providing string representation and parsing of the form "<RFC3339> <Timezone Name>", i.e.,
/// "2019-05-15T14:30:00Z US/Central". The to_string method, and serde serialization will
/// produce a string of this format. The parser will accept an RFC3339-only string of the forms
/// "2019-05-15T14:30:00Z", "2019-05-15T14:30:00+00:00", and also an "RFC3339 Timezone Name"
/// string.
///
/// The function here is to generate as close to unambiguous time/date strings, (for earth's
/// gravitational frame of reference), as possible. Clumping together the time, offset from UTC,
/// and the named time zone allows future parsers to know the exact interpretation of the time in
/// the frame of reference of the original recording.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DateTimeTz(pub chrono::DateTime<chrono_tz::Tz>);

impl DateTimeTz {
    pub fn map<F>(&self, f: F) -> DateTimeTz
    where
        F: FnOnce(chrono::DateTime<chrono_tz::Tz>) -> chrono::DateTime<chrono_tz::Tz>,
    {
        DateTimeTz(f(self.0))
    }

    pub fn to_string(&self) -> String {
        if self.0.timezone() == UTC {
            self.0.to_rfc3339_opts(SecondsFormat::Secs, true)
        } else {
            format!(
                "{} {}",
                self.0
                    .with_timezone(&chrono_tz::Etc::UTC)
                    .to_rfc3339_opts(SecondsFormat::Secs, true,),
                self.0.timezone().name()
            )
        }
    }

    pub fn from_str(s: &str) -> Result<DateTimeTz, chrono::ParseError> {
        let v: Vec<&str> = s.split_terminator(" ").collect();
        if v.len() == 2 {
            let tz = v[1].parse::<chrono_tz::Tz>().unwrap();
            chrono::DateTime::parse_from_rfc3339(v[0]).map(|ts| DateTimeTz(ts.with_timezone(&tz)))
        } else {
            chrono::DateTime::parse_from_rfc3339(v[0]).map(|ts| DateTimeTz(ts.with_timezone(&UTC)))
        }
    }
}

struct DateTimeTzVisitor;

impl<'de> Visitor<'de> for DateTimeTzVisitor {
    type Value = DateTimeTz;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string date time representation that can be parsed")
    }

    fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
        DateTimeTz::from_str(s).or(Err(E::custom(format!(
            "string is not a parsable datetime representation"
        ))))
    }
}

impl Serialize for DateTimeTz {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DateTimeTz {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(DateTimeTzVisitor)
    }
}

#[cfg(test)]
mod test {
    extern crate serde_json;

    use crate::date_time_tz::DateTimeTz;
    use chrono::TimeZone;
    use chrono_tz::America::Phoenix;
    use chrono_tz::Etc::UTC;
    use chrono_tz::US::{Arizona, Central};

    #[test]
    fn it_creates_timestamp_with_z() {
        let t = DateTimeTz(UTC.with_ymd_and_hms(2019, 5, 15, 12, 0, 0).unwrap());
        assert_eq!(t.to_string(), "2019-05-15T12:00:00Z");
    }

    #[test]
    fn it_parses_utc_rfc3339_z() {
        let t = DateTimeTz::from_str("2019-05-15T12:00:00Z").unwrap();
        assert_eq!(
            t,
            DateTimeTz(UTC.with_ymd_and_hms(2019, 5, 15, 12, 0, 0).unwrap())
        );
    }

    #[test]
    fn it_parses_rfc3339_with_offset() {
        let t = DateTimeTz::from_str("2019-05-15T12:00:00-06:00").unwrap();
        assert_eq!(
            t,
            DateTimeTz(UTC.with_ymd_and_hms(2019, 5, 15, 18, 0, 0).unwrap())
        );
    }

    #[test]
    fn it_parses_rfc3339_with_tz() {
        let t = DateTimeTz::from_str("2019-06-15T19:00:00Z US/Arizona").unwrap();
        assert_eq!(
            t,
            DateTimeTz(UTC.with_ymd_and_hms(2019, 6, 15, 19, 0, 0).unwrap())
        );
        assert_eq!(
            t,
            DateTimeTz(Arizona.with_ymd_and_hms(2019, 6, 15, 12, 0, 0).unwrap())
        );
        assert_eq!(
            t,
            DateTimeTz(Central.with_ymd_and_hms(2019, 6, 15, 14, 0, 0).unwrap())
        );
        assert_eq!(t.to_string(), "2019-06-15T19:00:00Z US/Arizona");
    }

    #[derive(Serialize)]
    struct DemoStruct {
        id: String,
        dt: DateTimeTz,
    }

    // I used Arizona here specifically because large parts of Arizona do not honor DST, and so
    // that adds in more ambiguity of the -0700 offset with Pacific time.
    #[test]
    fn it_json_serializes() {
        let t = DateTimeTz::from_str("2019-06-15T19:00:00Z America/Phoenix").unwrap();
        assert_eq!(
            serde_json::to_string(&t).unwrap(),
            "\"2019-06-15T19:00:00Z America/Phoenix\""
        );

        let demo = DemoStruct {
            id: String::from("abcdefg"),
            dt: t,
        };
        assert_eq!(
            serde_json::to_string(&demo).unwrap(),
            "{\"id\":\"abcdefg\",\"dt\":\"2019-06-15T19:00:00Z America/Phoenix\"}"
        );
    }

    #[test]
    fn it_json_parses() {
        let t =
            serde_json::from_str::<DateTimeTz>("\"2019-06-15T19:00:00Z America/Phoenix\"").unwrap();
        assert_eq!(
            t,
            DateTimeTz(Phoenix.with_ymd_and_hms(2019, 6, 15, 12, 0, 0).unwrap())
        );
    }
}
