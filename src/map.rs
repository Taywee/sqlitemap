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

    replace_key_value: Statement<'a>,
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
    pub fn new(connection: &'a Connection, tablename: &str, keytype: &str, valuetype: &str) -> Result<Self> {
        connection.execute(&format!("
            CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY,
                key {} UNIQUE NOT NULL,
                value {} NOT NULL
            )", tablename, keytype, valuetype), &[])?;
        let replace_key_value = connection.prepare(&format!("INSERT OR REPLACE INTO {} (key, value) VALUES (?, ?)", tablename))?;
        let select_value = connection.prepare(&format!("SELECT value FROM {} WHERE key=?", tablename))?;
        let select_key = connection.prepare(&format!("SELECT 1 FROM {} WHERE key=?", tablename))?;
        let select_keys = connection.prepare(&format!("SELECT key FROM {}", tablename))?;
        let delete_key = connection.prepare(&format!("DELETE FROM {} WHERE key=?", tablename))?;
        let select_count = connection.prepare(&format!("SELECT COUNT(*) FROM {}", tablename))?;
        let select_one = connection.prepare(&format!("SELECT 1 FROM {}", tablename))?;

        Ok(Self {
            get_key: PhantomData,
            get_value: PhantomData,
            put_key: PhantomData,
            put_value: PhantomData,

            replace_key_value,
            select_value,
            select_key,
            select_keys,
            delete_key,
            select_count,
            select_one,
        })
    }
    pub fn insert(&mut self, key: PutKey, value: PutValue) -> Result<Option<GetValue>> {
        let mut rows = self.select_value.query(&[&key])?;
        let output = match rows.next() {
            Some(row) => row?.get_checked(0)?,
            None => None,
        };
        self.replace_key_value.execute(&[&key, &value])?;
        Ok(output)
    }

    pub fn get(&mut self, key: PutKey) -> Result<Option<GetValue>> {
        let mut rows = self.select_value.query(&[&key])?;
        let row = match rows.next() {
            Some(row) => row?,
            None => return Ok(None),
        };
        Ok(Some(row.get_checked(0)?))
    }

    pub fn keys(&mut self) -> Result<MappedRows<impl FnMut(&Row) -> GetKey>> {
        self.select_keys.query_map(&[], |row| row.get(0))
    }

    pub fn contains_key(&mut self, key: PutKey) -> Result<bool> {
        let mut rows = self.select_key.query(&[&key])?;
        Ok(match rows.next() {
            Some(Ok(_)) => true,
            Some(Err(x)) => return Err(x),
            None => false,
        })
    }

    pub fn len(&mut self) -> Result<usize> {
        let mut rows = self.select_count.query(&[])?;
        let row = match rows.next() {
            Some(row) => row?,
            None => return Ok(0),
        };
        let size: isize = row.get_checked(0)?;
        Ok(size as usize)
    }

    pub fn remove(&mut self, key: PutKey) -> Result<Option<GetValue>> {
        let mut rows = self.select_value.query(&[&key])?;
        let row = match rows.next() {
            Some(row) => row?,
            None => return Ok(None),
        };
        match row.get_checked(0) {
            Ok(value) => {
                self.delete_key.execute(&[&key])?;
                Ok(Some(value))
            },
            Err(x) => Err(x)
        }
    }

    pub fn is_empty(&mut self) -> Result<bool> {
        let mut rows = self.select_one.query(&[])?;
        Ok(match rows.next() {
            Some(Ok(_)) => false,
            Some(Err(x)) => return Err(x),
            None => true,
        })
    }
}
