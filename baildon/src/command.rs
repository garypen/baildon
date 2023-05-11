//! Command Functions
//!
//! Used in the WAL (Write Ahead Log)

use anyhow::Result;
use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::btree::baildon::BaildonKey;
use crate::btree::baildon::BaildonValue;
use crate::BINCODER;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum Command<K, V> {
    Upsert(K, V),
    Delete(K),
}

impl<K, V> Command<K, V>
where
    K: BaildonKey,
    V: BaildonValue,
{
    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        let s_cmd = BINCODER.serialize(&self)?;
        Ok(s_cmd)
    }

    pub(crate) fn deserialize(buf: &[u8]) -> Result<Self> {
        BINCODER.deserialize(buf).map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_serializes_upsert_command() {
        let upsert = Command::Upsert("this".to_string(), "that".to_string());
        let s_upsert = upsert.serialize().expect("serializes");
        let new_upsert = Command::deserialize(&s_upsert).expect("deserializes");
        assert_eq!(upsert, new_upsert);
    }

    #[test]
    fn it_serializes_delete_command() {
        let delete_: Command<String, usize> = Command::Delete("this".to_string());
        let s_delete = delete_.serialize().expect("serializes");
        let new_delete = Command::deserialize(&s_delete).expect("deserializes");
        assert_eq!(delete_, new_delete);
    }
}
