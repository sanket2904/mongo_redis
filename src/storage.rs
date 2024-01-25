// use std::collections::HashMap;
// use std::sync::{Arc, Mutex};

// use bson::Document;
// pub struct Storage {
//     pub data: std::sync::Arc<Mutex<HashMap<String, bson::Document>>>,
// }


// impl Storage {
//     pub fn new() -> Self {
//         Storage {
//             data: Arc::new(Mutex::new(HashMap::new())),
//         }
//     }
//     pub fn clone(&self) -> Self {
//         Storage {
//             data: Arc::clone(&self.data)
//         }
//     }
//     pub fn get_storage(&self) -> &Arc<Mutex<HashMap<String, bson::Document>>> {
//         &self.data

//     }

//     pub fn insert(&self, key:String, data: Document) -> Result<(),String> {
//         println!("inserting into cache");
//         let mut db = match self.data.lock() {
//             Ok(db) => db,
//             Err(poi) => poi.into_inner(),
//         };
//         db.insert(key, data);
//         Ok(())

//     }

//     pub fn find(&self, key:String) -> Option<Document> {
//         println!("using cache");
//         let db = match self.data.lock() {
//             Ok(db) => db,
//             Err(poi) => poi.into_inner(),
//         };
//         let data = db.get(&key);
//         if data.is_none() {
//             return None;
//         }
//         return Some(data.unwrap().clone());        
//     }
// }
