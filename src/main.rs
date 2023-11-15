mod parser;
mod indexer;
mod utils;
mod disk_io;
mod external_sorter;
mod bin_indexer;
mod term_query_processor;

use std::fs;
use std::path::Path;
use disk_io::{process_gzip_file, merge_sorted_postings};
use bin_indexer::build_bin_index;
use crate::term_query_processor::TermQueryProcessor;
use actix_files;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use std::sync::{Arc, Mutex};
use serde::Deserialize;


// Function to clean up the postings_data folder
fn cleanup_postings_data_folder() -> std::io::Result<()> {
    let dir = Path::new("postings_data");
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    Ok(())
}

fn build_index() {
    // Assignment 2: Build the Inverted Index

    if let Err(e) = cleanup_postings_data_folder() {
        eprintln!("Error cleaning up postings_data folder: {}", e);
    }

    let file_path = "data/msmarco-docs.trec.gz";
    if let Err(e) = process_gzip_file(file_path) {
        eprintln!("Error processing file: {}", e);
    }

    // After processing the file, apply the external merge sort on the batches
    if let Err(e) = merge_sorted_postings() {
        eprintln!("Error merging sorted postings: {}", e);
    }

    // Build binary inverted index and store in 'data/' directory
    if let Err(e) = build_bin_index("data/merged_postings.data", "data/bin_index.data",
                                    "data/bin_lexicon.data", "data/bin_directory.data") {
        eprintln!("Error building binary inverted index: {}", e);
    }
}

struct AppState {
    query_processor: Arc<Mutex<TermQueryProcessor>>,
}

#[derive(Deserialize)]
struct QueryParams {
    query: String,
}

async fn handle_conjunctive_query(
    data: web::Data<AppState>,
    query: web::Query<QueryParams>,
) -> impl Responder {
    let mut processor = data.query_processor.lock().unwrap();

    match processor.conjunctive_query(&query.query) {
        Ok(json) => HttpResponse::Ok().content_type("application/json").body(json),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

async fn handle_disjunctive_query(
    data: web::Data<AppState>,
    query: web::Query<QueryParams>,
) -> impl Responder {
    let mut processor = data.query_processor.lock().unwrap();

    match processor.disjunctive_query(&query.query) {
        Ok(json) => HttpResponse::Ok().content_type("application/json").body(json),
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    // Create your TermQueryProcessor instance here
    let tqp = Arc::new(Mutex::new(TermQueryProcessor::new("data/bin_index.data", "data/bin_lexicon.data", "data/bin_directory.data",
                                                          "data/doc_metadata.data")));

    HttpServer::new(move || {
        let app_data = web::Data::new(AppState {
            query_processor: tqp.clone(),
        });

        App::new()
            .app_data(app_data)
            .service(web::resource("/conjunctive_query").route(web::get().to(handle_conjunctive_query)))
            .service(web::resource("/disjunctive_query").route(web::get().to(handle_disjunctive_query)))
            // Serve static files
            .service(actix_files::Files::new("/", "./static").index_file("index.html"))
    })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}