use marple_db::{MarpleDB, PushFileOptions};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("MDB_URL")?;
    let token = std::env::var("MDB_TOKEN")?;
    let db = MarpleDB::new(&url, &token)?;
    let stream = db.get_stream("runs").await?;

    let dataset = db
        .push_file(stream.id, "run.csv", PushFileOptions::default())
        .await?;

    println!("uploaded dataset {}", dataset.id);
    Ok(())
}
