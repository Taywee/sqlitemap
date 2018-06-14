use std::os::raw::c_int;
use std::marker::PhantomData;

use rusqlite::Row;
use rusqlite::MappedRows;
use rusqlite::Connection;
use rusqlite::Statement;
use rusqlite::Result;
use rusqlite::types::{ToSql, FromSql};

pub struct SqliteMap<'a, GetKey, GetValue, PutKey, PutValue> {
    get_key: PhantomData<GetKey>,
    get_value: PhantomData<GetValue>,
    put_key: PhantomData<PutKey>,
    put_value: PhantomData<PutValue>,

    insert_key_value: Statement<'a>,
    select_value: Statement<'a>,
    select_key: Statement<'a>,
    select_keys: Statement<'a>,
    delete_key: Statement<'a>,
    select_count: Statement<'a>,
    select_one: Statement<'a>,
}

impl<'a, GetKey, GetValue, PutKey, PutValue> SqliteMap<'a, GetKey, GetValue, PutKey, PutValue>
    where GetKey: FromSql,
          GetValue: FromSql, 
          PutKey: ToSql,
          PutValue: ToSql {
    pub fn new(connection: &'a Connection, tablename: &str) -> Self {
        connection.execute(&format!("
            CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY,
                key TEXT UNIQUE NOT NULL,
                value TEXT NOT NULL
            )", tablename), &[]).unwrap();
        let insert_key_value = connection.prepare(&format!("INSERT OR REPLACE INTO {} (key, value) VALUES (?, ?)", tablename)).unwrap();
        let select_value = connection.prepare(&format!("SELECT value FROM {} WHERE key=?", tablename)).unwrap();
        let select_key = connection.prepare(&format!("SELECT 1 FROM {} WHERE key=?", tablename)).unwrap();
        let select_keys = connection.prepare(&format!("SELECT key FROM {}", tablename)).unwrap();
        let delete_key = connection.prepare(&format!("DELETE FROM {} WHERE key=?", tablename)).unwrap();
        let select_count = connection.prepare(&format!("SELECT COUNT(*) FROM {}", tablename)).unwrap();
        let select_one = connection.prepare(&format!("SELECT 1 FROM {}", tablename)).unwrap();

        Self {
            get_key: PhantomData,
            get_value: PhantomData,
            put_key: PhantomData,
            put_value: PhantomData,

            insert_key_value,
            select_value,
            select_key,
            select_keys,
            delete_key,
            select_count,
            select_one,
        }
    }
    pub fn insert(&mut self, key: PutKey, value: PutValue) -> Result<c_int> {
        self.insert_key_value.execute(&[&key, &value])
    }

    pub fn get(&mut self, key: PutKey) -> Option<GetValue> {
        let mut rows = match self.select_value.query(&[&key]) {
            Ok(rows) => rows,
            _ => return None,
        };
        let row = match rows.next() {
            Some(Ok(row)) => row,
            _ => return None,
        };
        match row.get_checked(0) {
            Ok(value) => Some(value),
            _ => None,
        }
    }

    pub fn keys(&mut self) -> MappedRows<impl FnMut(&Row) -> GetKey> {
        self.select_keys.query_map(&[], |row| row.get(0)).unwrap()
    }

    pub fn contains_key(&mut self, key: PutKey) -> bool {
        let mut rows = match self.select_key.query(&[&key]) {
            Ok(rows) => rows,
            _ => return false,
        };
        match rows.next() {
            Some(Ok(_)) => true,
            _ => false,
        }
    }

    pub fn len(&mut self) -> usize {
        let mut rows = match self.select_count.query(&[]) {
            Ok(rows) => rows,
            _ => return 0,
        };
        let row = match rows.next() {
            Some(Ok(row)) => row,
            _ => return 0,
        };
        let size: isize = match row.get_checked(0) {
            Ok(value) => value,
            _ => 0,
        };
        size as usize
    }

    pub fn remove(&mut self, key: PutKey) -> Option<GetValue> {
        let mut rows = match self.select_value.query(&[&key]) {
            Ok(rows) => rows,
            _ => return None,
        };
        let row = match rows.next() {
            Some(Ok(row)) => row,
            _ => return None,
        };
        match row.get_checked(0) {
            Ok(value) => {
                self.delete_key.execute(&[&key]).unwrap();
                Some(value)
            },
            _ => None,
        }
    }

    pub fn is_empty(&mut self) -> bool {
        let mut rows = match self.select_one.query(&[]) {
            Ok(rows) => rows,
            _ => return true,
        };
        match rows.next() {
            Some(Ok(row)) => false,
            _ => return true,
        }
    }
}
