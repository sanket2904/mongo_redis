// use super::Handler;

// use bson::doc;

// pub struct Count {}

// impl Handler for Count {
//     fn new() -> Self {
//         Count {}
//     }
//     fn handle(&self,request: &crate::handler::Request,msg: &Vec<bson::Document>,) -> Result<bson::Document, crate::handler::CommandExecutionError> {
//         let doc = &msg[0];
//         let db = doc.get_str("$db").unwrap();
//         let collection = doc.get_str("count").unwrap();
//         let filter = if doc.contains_key("filter") {
//             doc.get_document("filter").unwrap().clone()
//         } else {
//             doc! {}
//         };
        
//     }
// }



// async fn get_count() {
//     let 
// }