extern crate rusqlite;

mod map;
pub use map::SqliteMap;

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use rusqlite::Error;

    #[test]
    fn it_works() {
        // type inference lets us omit an explicit type signature (which
        // would be `HashMap<&str, &str>` in this example).
        let connection = Connection::open_in_memory().unwrap();
        let mut book_reviews: SqliteMap<String, String, &str, &str> = SqliteMap::new(&connection, "map", "TEXT", "TEXT").unwrap();

        assert!(book_reviews.is_empty().unwrap());

        // review some books.
        book_reviews.insert("Adventures of Huckleberry Finn",    "My favorite book.").unwrap();
        book_reviews.insert("Grimms' Fairy Tales",               "Masterpiece.").unwrap();
        book_reviews.insert("The Adventures of Sherlock Holmes", "Eye lyked it alot.").unwrap();

        // Test replacement
        assert_eq!(book_reviews.insert("Pride and Prejudice", "Very enjoyable.").unwrap(), None);
        assert_eq!(book_reviews.insert("Pride and Prejudice", "Just terrible.").unwrap(), Some(String::from("Very enjoyable.")));
        assert_eq!(book_reviews.get("Pride and Prejudice").unwrap(), Some(String::from("Just terrible.")));

        assert!(!book_reviews.is_empty().unwrap());

        assert_eq!(book_reviews.get("The Adventures of Sherlock Holmes").unwrap(), Some(String::from("Eye lyked it alot.")));
        assert_eq!(book_reviews.get("The Adventures of Somebody Else").unwrap(), None);

        assert_eq!(book_reviews.len().unwrap(), 4);

        // check for a specific one.
        assert!(!book_reviews.contains_key("Les Misérables").unwrap());

        // oops, this review has a lot of spelling mistakes, let's delete it.
        book_reviews.remove("The Adventures of Sherlock Holmes").unwrap();

        let x: Result<Vec<String>, Error> = book_reviews.keys().unwrap().collect();
        assert_eq!(x.unwrap(), ["Adventures of Huckleberry Finn", "Grimms' Fairy Tales", "Pride and Prejudice"]);

        assert_eq!(book_reviews.len().unwrap(), 3);
    }
}
