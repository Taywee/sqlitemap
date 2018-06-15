#[cfg(test)]
extern crate serde;
#[cfg(test)]
extern crate flate2;
#[cfg(test)]
extern crate serde_json;
#[cfg(test)]
#[macro_use]
extern crate serde_derive;
extern crate rusqlite;

mod map;
pub use map::SqliteMap;

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite;
    use rusqlite::Connection;
    use rusqlite::Error;
    use rusqlite::types::{ToSql, FromSql, ValueRef, FromSqlError, FromSqlResult, ToSqlOutput};

    #[test]
    fn it_works() {
        let connection = Connection::open_in_memory().unwrap();
        let mut book_reviews: SqliteMap<String, String, &str, &str> = SqliteMap::new(&connection, "map", "TEXT", "TEXT").unwrap();

        assert!(book_reviews.is_empty().unwrap());

        // review some books.
        book_reviews.insert("Adventures of Huckleberry Finn",    "My favorite book.").unwrap();
        book_reviews.insert("Grimms' Fairy Tales",               "Masterpiece.").unwrap();
        book_reviews.insert("The Adventures of Sherlock Holmes", "Eye lyked it alot.").unwrap();
        assert_eq!(book_reviews.get("The Adventures of Sherlock Holmes").unwrap(), Some(String::from("Eye lyked it alot.")));
        assert_eq!(book_reviews.get("The Adventures of Somebody Else").unwrap(), None);

        // Test replacement
        assert_eq!(book_reviews.insert("Pride and Prejudice", "Very enjoyable.").unwrap(), None);
        assert_eq!(book_reviews.insert("Pride and Prejudice", "Just terrible.").unwrap(), Some(String::from("Very enjoyable.")));
        assert_eq!(book_reviews.get("Pride and Prejudice").unwrap(), Some(String::from("Just terrible.")));

        assert!(!book_reviews.is_empty().unwrap());

        assert_eq!(book_reviews.len().unwrap(), 4);

        // check for a specific one.
        assert!(!book_reviews.contains_key("Les Misérables").unwrap());

        // oops, this review has a lot of spelling mistakes, let's delete it.
        assert_eq!(book_reviews.remove("The Adventures of Sherlock Holmes").unwrap(), Some(String::from("Eye lyked it alot.")));
        assert_eq!(book_reviews.remove("The Adventures of Sherlock Holmes").unwrap(), None);

        let x: Result<Vec<String>, Error> = book_reviews.keys().unwrap().collect();
        assert_eq!(x.unwrap(), ["Adventures of Huckleberry Finn", "Grimms' Fairy Tales", "Pride and Prejudice"]);

        assert_eq!(book_reviews.len().unwrap(), 3);
    }

    use std::cmp::{PartialEq, Eq};

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Foo {
        x: i32,
        y: String,
    }

    impl ToSql for Foo {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
            let string = serde_json::to_string(self).unwrap();
            Ok(ToSqlOutput::from(string))
        }
    }

    impl FromSql for Foo {
        fn column_result(value: ValueRef) -> FromSqlResult<Self> {
            match value {
                ValueRef::Text(s) => serde_json::from_str(s),
                ValueRef::Blob(b) => serde_json::from_slice(b),
                _ => return Err(FromSqlError::InvalidType),
            }.map_err(|err| FromSqlError::Other(Box::new(err)))
        }
    }


    #[test]
    fn json_output() {
        let connection = Connection::open_in_memory().unwrap();
        let mut book_reviews: SqliteMap<String, Foo, &str, Foo> = SqliteMap::new(&connection, "map", "TEXT", "TEXT").unwrap();
        let foo = Foo{
            x: 8,
            y: String::from("This is a test string"),
        };

        assert_eq!(book_reviews.insert("foo", foo.clone()).unwrap(), None);
        assert_eq!(book_reviews.insert("foo", foo.clone()).unwrap(), Some(foo));
    }

    use std::io::prelude::*;
    use flate2::Compression;
    use flate2::write::ZlibEncoder;
    use flate2::read::ZlibDecoder;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct ZipFoo(Foo);

    impl ToSql for ZipFoo {
        fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
            let mut e = ZlibEncoder::new(Vec::new(), Compression::default());
            let string = serde_json::to_vec(self).unwrap();
            e.write_all(&string);
            Ok(ToSqlOutput::from(e.finish().unwrap()))
        }
    }

    impl FromSql for ZipFoo {
        fn column_result(value: ValueRef) -> FromSqlResult<Self> {
            let mut s = String::new();
            match value {
                ValueRef::Blob(b) => {
                    let mut z = ZlibDecoder::new(b);
                    z.read_to_string(&mut s);
                    serde_json::from_str(&s)
                },
                _ => return Err(FromSqlError::InvalidType),
            }.map_err(|err| FromSqlError::Other(Box::new(err)))
        }
    }

    #[test]
    fn zip_json_output() {
        let connection = Connection::open_in_memory().unwrap();
        let mut book_reviews: SqliteMap<String, ZipFoo, &str, ZipFoo> = SqliteMap::new(&connection, "map", "BLOB", "BLOB").unwrap();
        let foo = Foo{
            x: 8,
            y: String::from("This is a test string"),
        };

        assert_eq!(book_reviews.insert("foo", ZipFoo(foo.clone())).unwrap(), None);
        assert_eq!(book_reviews.insert("foo", ZipFoo(foo.clone())).unwrap(), Some(ZipFoo(foo)));
    }
}
