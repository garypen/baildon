use std::env;
use std::fs::metadata;
use std::ops::ControlFlow;
use std::path::PathBuf;

use anyhow::Result;
use baildon::btree::Direction;
use clap::Parser;
use gluesql::prelude::*;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

mod glue;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Database location
    database: String,

    /// Create a new database (will overwrite existing file)
    #[arg(short, long, default_value_t = false)]
    create: bool,
}

fn get_history_file() -> Option<PathBuf> {
    dirs::preference_dir()
        .and_then(|mut base| {
            base.push("baildon-glue");
            // Note: Not create_dir_all(), because we don't want to create preference
            // dirs if they don't exist.
            if metadata(base.clone()).ok().is_none() {
                std::fs::create_dir(base.clone()).ok()?
            }
            Some(base)
        })
        .map(|mut base| {
            base.push("history.txt");
            base
        })
}

#[tokio::main]
async fn main() -> Result<()> {
    let isatty = unsafe { libc::isatty(0) };

    let cli = Cli::parse();

    let log_dir = match env::var("TMPDIR") {
        Ok(d) => d,
        Err(_e) => ".".to_string(),
    };

    let file_appender = tracing_appender::rolling::daily(log_dir, "baildon-glue.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt().with_writer(non_blocking).init();

    let storage: glue::BaildonGlue = if cli.create {
        glue::BaildonGlue::new(&cli.database).await?
    } else {
        glue::BaildonGlue::open(&cli.database).await?
    };

    // let storage = SharedMemoryStorage::new();
    let mut glue = Glue::new(storage);

    // `()` can be used when no completer is required
    let mut rl = DefaultEditor::new()?;
    if isatty == 1 {
        if let Some(file_location) = get_history_file() {
            if let Err(e) = rl.load_history(&file_location) {
                println!("error loading history: {e}");
            }
        }
    }

    println!("terminate with ctrl-c or ctrl-d");
    loop {
        let readline = rl.readline("sql> ");
        match readline {
            Ok(line) => {
                if line.is_empty() {
                    continue;
                }
                let output = match glue.execute_async(&line).await {
                    Ok(out) => {
                        if isatty == 0 {
                            format!("{out:?}")
                        } else {
                            format!("out> {out:?}")
                        }
                    }
                    Err(err) => format!("err> {err}"),
                };
                println!("{output}");
                if isatty == 1 {
                    rl.add_history_entry(line.as_str())?;
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("terminating...");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("terminating...");
                break;
            }
            Err(err) => {
                println!("error: {err:?}");
                break;
            }
        }
    }

    if isatty == 1 {
        if let Some(file_location) = get_history_file() {
            if let Err(e) = rl.save_history(&file_location) {
                println!("error saving history: {e}");
            }
        }
    }

    glue.storage.save().await.expect("It will save");

    glue.storage.print_tables().await?;

    let mut sep = "";
    let callback = |(key, value)| {
        print!("{sep}{key:?}:{value:?}");
        sep = ", ";
        ControlFlow::Continue(())
    };
    glue.storage.schemas.traverse_entries(Direction::Ascending, callback).await;
    println!("\nutilization: {}", glue.storage.schemas.utilization().await);
    println!();

    Ok(())
}
