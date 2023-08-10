// using this file to connect to the redis

use std::time::SystemTime;
// use hex_literal::hex;

use bson::{Document, Bson, JavaScriptCodeWithScope};
use redis;
use serde_json::{json , Value};
use chrono::{TimeZone, Utc};
pub struct RedisDb {
    pub db: redis::Client,
}


pub trait RedisSerializer {
    fn to_redis(&self) -> Value;
}








    
impl RedisSerializer for Bson {
    fn to_redis(&self) -> Value {
        match self {
            Bson::Int32(i) => json!(i),
            Bson::Int64(i) => json!(i),
            Bson::Double(f) => {
                json!({ "$f": f })
            },
            Bson::DateTime(date) => {
                json!({ "$d": date.timestamp_millis() })
            },
            // convert the array to redis supported format
            Bson::Array(arr) => {
                Value::Array(arr.into_iter().map(Bson::to_redis).collect())
            },
            Bson::Document(arr) => {
                Value::Object(arr.into_iter().map(|(k,v)| (k.clone() , v.to_redis())).collect::<serde_json::Map<String, Value>>())
            },
            Bson::JavaScriptCode(code) => json!({
                "$j": code
            }),
            Bson::JavaScriptCodeWithScope(JavaScriptCodeWithScope { code, scope }) => json!({
                "$j": code,
                "s": serde_json::to_string(&scope).unwrap(),
            }),
            Bson::RegularExpression(bson::Regex { pattern, options }) => {
                let mut chars: Vec<_> = options.chars().collect();
                chars.sort_unstable();
                let options: String = chars.into_iter().collect();
                json!({
                    "$regex": pattern,
                    "$options": options,
                })
            },
            Bson::ObjectId(v) => json!({"$o": v.to_hex()}),
            other => other.clone().into_relaxed_extjson(),

        }
    }


    // we are converting bson to redis value
    // fn from_redis_value( &self, v: &redis::Value ) -> redis::RedisResult<Self> {
    //     match self {
    //         Bson::Int32(e) => FromRedisValue::from_redis_value(v).map(|v| Bson::Int32(v)),
    //         Bson::Int64(e) => FromRedisValue::from_redis_value(v).map(|v| Bson::Int64(v)),
    //         Bson::Double(e) => FromRedisValue::from_redis_value(v).map(|v| Bson::Double(v)),
    //         Bson::DateTime(e) => FromRedisValue::from_redis_value(v).map(|v| Bson::DateTime(v)),
    //         Bson::Array(e) => FromRedisValue::from_redis_value(v).map(|v| Bson::Array(v)),
    //     }
    // }
}




pub trait RedisDeserializer {
    fn from_redis(&self) -> Bson;
}

impl RedisDeserializer for Value {
    fn from_redis(&self) -> Bson {
        match self {
            serde_json::Value::String(s) => Bson::String(s.to_string()),
            serde_json::Value::Number(n) => {
                let s = n.to_string();
                if s.contains(".") {
                    Bson::Double(n.as_f64().unwrap())
                } else {
                    if let Some(n) = n.as_i64() {
                        Bson::Int32(n.try_into().unwrap())
                    } else if let Some(n) = n.as_f64() {
                        Bson::Double(n)
                    } else {
                        panic!("Invalid number");
                    }
                }
            }
            serde_json::Value::Bool(b) => Bson::Boolean(b.to_owned()),
            serde_json::Value::Null => Bson::Null,
            serde_json::Value::Array(arr) => {
                Bson::Array(arr.into_iter().map(|v| v.from_redis()).collect())
            }
            serde_json::Value::Object(o) => {
                if o.contains_key("$o") {
                    return Bson::ObjectId(
                        bson::oid::ObjectId::parse_str(o["$o"].as_str().unwrap().to_string()).unwrap());
                }
                if o.contains_key("$d") {
                    let t: chrono::DateTime<Utc> = Utc.timestamp_millis_opt(o["$d"].as_i64().unwrap()).unwrap();
                    let systime = SystemTime::from(t);
                    let bson_datetime = bson::DateTime::from(systime);
                    return Bson::DateTime(bson_datetime);
                }
                if o.contains_key("$f") {
                    return Bson::Double(o["$f"].as_f64().unwrap());
                }
                if o.contains_key("$j") {
                    if o.contains_key("s") {
                        return Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
                            code: o["$j"].as_str().unwrap().to_string(),
                            scope: serde_json::from_str(o["s"].as_str().unwrap()).unwrap(),
                        
                        });
                    } else {
                        return Bson::JavaScriptCode(o["$j"].as_str().unwrap().to_string());
                    }
                }
                if o.contains_key("$r") {
                    return Bson::RegularExpression(
                        bson::Regex { pattern: o["$r"].as_str().unwrap().to_string(), options: o["$o"].as_str().unwrap().to_string() }
                    );
                }
                let mut m = bson::Document::new();
                for (k, v) in o {
                    m.insert(k, v.from_redis());
                }
                Bson::Document(m)
            }
        }
    }
}





impl RedisDb {
    pub fn new() -> Self {
        // use uri from env
        let uri = std::env::var("REDIS_URI").unwrap();
        let client = redis::Client::open(uri).unwrap();
        // let con = client.get_connection().unwrap();
        RedisDb { db: client }
    }
    pub fn new_with_uri(uri: &str) -> Self {
        let client = redis::Client::open(uri).unwrap();
        // let con = client.get_connection().unwrap();
        RedisDb { db: client }
    }

    // a hash generator for redis on the basis of the mongodb filter
   
   





   
}
#[derive(Debug, Clone)]
pub struct RedisParams {
    pub db: String,
    pub collection: String,
}

impl RedisParams {
    pub fn new(db: &str, collection: &str) -> Self {
        RedisParams {
            db: db.to_string(),
            collection: collection.to_string(),
        }
    }
    pub fn from(doc: &Document, col_attr: &str) -> Self {
        Self::new(&doc.get_str("$db").unwrap().to_string(), &doc.get_str(col_attr).unwrap().to_string())
    }

    
    

}