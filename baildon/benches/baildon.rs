use std::fs::File;
use std::io::Read;

use anyhow::Result;
use futures::StreamExt;

use baildon::btree::Baildon;
use baildon::btree::Direction;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::{rng, Rng};

const TEST_DB: &str = "test.db";

// Utility function for creating a database to use with tests
async fn create_database() -> Result<Baildon<String, String>> {
    let mut contents = String::new();
    const DATA: &[&str] = &["data/1984.txt", "data/sun-rising.txt"];
    File::open(DATA[0])
        .unwrap()
        .read_to_string(&mut contents)
        .unwrap();

    let db = Baildon::try_new(TEST_DB, 7).await?;

    for (index, word) in contents
        .split(|c: char| c.is_whitespace())
        .map(|s| s.to_string())
        .enumerate()
    {
        let _ = db.insert(index.to_string(), word).await;
    }

    Ok(db)
}

// Utility function for getting keys from a database
async fn get_keys(db: &Baildon<String, String>, len: usize) -> Vec<String> {
    db.keys(Direction::Ascending)
        .await
        .take(len)
        .collect::<Vec<String>>()
        .await
}

fn baildon_delete(c: &mut Criterion) {
    // Shared source database (contains approx 10,000 entries)

    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt
        .block_on(async { create_database().await })
        .expect("task spawn failed");

    drop(db);

    let mut group = c.benchmark_group("delete");
    for size in [64, 128, 256, 512, 1024, 2048, 4096, 8192].iter() {
        // Benchmark baildon
        let (db, words) = rt
            .block_on(async {
                let db = Baildon::<String, String>::try_open(TEST_DB).await?;
                let words = get_keys(&db, 8192).await;
                // Close the database to clean out the cache and then open it again.
                drop(db);
                let db = Baildon::<String, String>::try_open(TEST_DB).await?;
                Ok::<(Baildon<String, String>, Vec<String>), anyhow::Error>((db, words))
            })
            .expect("task spawn failed");
        group.bench_with_input(
            BenchmarkId::new("baildon delete", size),
            &words,
            |b, words| {
                b.to_async(tokio::runtime::Runtime::new().expect("build tokio runtime"))
                    .iter(|| async {
                        let word = &words[rng().random_range(0..*size)];
                        let _ = db.delete(word).await;
                    })
            },
        );
    }
}

fn baildon_get(c: &mut Criterion) {
    // Shared source database (contains approx 10,000 entries)

    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt
        .block_on(async { create_database().await })
        .expect("task spawn failed");

    drop(db);

    let mut group = c.benchmark_group("get");
    for size in [64, 128, 256, 512, 1024, 2048, 4096, 8192].iter() {
        // Benchmark baildon
        let (db, words) = rt
            .block_on(async {
                let db = Baildon::<String, String>::try_open(TEST_DB).await?;
                let words = get_keys(&db, 8192).await;
                // Close the database to clean out the cache and then open it again.
                drop(db);
                let db = Baildon::<String, String>::try_open(TEST_DB).await?;
                Ok::<(Baildon<String, String>, Vec<String>), anyhow::Error>((db, words))
            })
            .expect("task spawn failed");
        group.bench_with_input(BenchmarkId::new("baildon get", size), &words, |b, words| {
            b.to_async(tokio::runtime::Runtime::new().expect("build tokio runtime"))
                .iter(|| async {
                    let word = &words[rng().random_range(0..*size)];
                    let _ = db.get(word).await;
                })
        });
    }
}

fn baildon_upsert(c: &mut Criterion) {
    // Shared source database (contains approx 10,000 entries)

    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt
        .block_on(async { create_database().await })
        .expect("task spawn failed");

    drop(db);

    let mut group = c.benchmark_group("upsert");
    for size in [64, 128, 256, 512, 1024, 2048, 4096, 8192].iter() {
        // Benchmark baildon
        let (db, words) = rt
            .block_on(async {
                let db = Baildon::<String, String>::try_open(TEST_DB).await?;
                let words = get_keys(&db, 8192).await;
                // Close the database to clean out the cache and then open it again.
                drop(db);
                let db = Baildon::<String, String>::try_open(TEST_DB).await?;
                Ok::<(Baildon<String, String>, Vec<String>), anyhow::Error>((db, words))
            })
            .expect("task spawn failed");
        group.bench_with_input(
            BenchmarkId::new("baildon upsert", size),
            &words,
            |b, words| {
                b.to_async(tokio::runtime::Runtime::new().expect("build tokio runtime"))
                    .iter(|| async {
                        let word = &words[rng().random_range(0..*size)];
                        let _ = db.insert(word.to_string(), "value".to_string()).await;
                    })
            },
        );
    }
}

criterion_group!(benches, baildon_delete, baildon_get, baildon_upsert);
criterion_main!(benches);
