use std::env;
use std::fs::metadata;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use baildon::btree::Baildon;
use baildon::btree::Direction;
use clap::CommandFactory;
use clap::Parser;
use clap::Subcommand;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use strum::EnumString;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Store location
    store: String,

    /// Create a new store (will overwrite existing file)
    #[arg(short, long, default_value_t = false)]
    create: bool,

    #[command(subcommand)]
    parameter: Option<Parameter>,
}

#[derive(Debug, EnumString, Subcommand)]
#[strum(ascii_case_insensitive)]
enum Parameter {
    /// Does our store contain this key
    Contains { key: String },
    /// Clear store entries
    Clear,
    /// Display B+Tree entry count
    Count,
    /// Delete this key
    Delete { key: String },
    /// List store entries
    Entries {
        /// Direction (Descending or Ascending)
        direction: Option<Direction>,
    },
    /// Get this key
    Get { key: String },
    /// Interactive Help
    Help,
    /// Insert key value pair
    Insert { key: String, value: String },
    /// List store keys
    Keys {
        /// Direction (Descending or Ascending)
        direction: Option<Direction>,
    },
    /// List store nodes
    Nodes {
        /// Direction (Descending or Ascending)
        direction: Option<Direction>,
    },
    /// Node Utilization
    Utilization,
    /// List store values
    Values {
        /// Direction (Descending or Ascending)
        direction: Option<Direction>,
    },
    /// Verify store
    Verify,
}

fn get_history_file() -> Option<PathBuf> {
    dirs::preference_dir()
        .and_then(|mut base| {
            base.push("baildon");
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

async fn interactive(btree: Baildon<String, String>) -> Result<()> {
    // `()` can be used when no completer is required
    let mut rl = DefaultEditor::new()?;
    if let Some(file_location) = get_history_file() {
        if let Err(e) = rl.load_history(&file_location) {
            println!("error loading history: {e}");
        }
    }
    println!("terminate with ctrl-c or ctrl-d");
    loop {
        let readline = rl.readline("word: ");
        match readline {
            Ok(line) => {
                if line.is_empty() {
                    continue;
                }
                // EnumString doesn't deal with variant parameters, so...
                let words = line.split_whitespace().collect::<Vec<&str>>();
                let parameter = match Parameter::from_str(words[0]) {
                    Ok(p) => {
                        // Can't think of a better way of doing this...
                        match p {
                            Parameter::Contains { key: _ } => {
                                if words.len() != 2 {
                                    println!("usage: contains <key>");
                                    continue;
                                }
                                Parameter::Contains {
                                    key: words[1].to_string(),
                                }
                            }
                            Parameter::Delete { key: _ } => {
                                if words.len() != 2 {
                                    println!("usage: delete <key>");
                                    continue;
                                }
                                Parameter::Delete {
                                    key: words[1].to_string(),
                                }
                            }
                            Parameter::Get { key: _ } => {
                                if words.len() != 2 {
                                    println!("usage: get <key>");
                                    continue;
                                }
                                Parameter::Get {
                                    key: words[1].to_string(),
                                }
                            }
                            Parameter::Insert { key: _, value: _ } => {
                                if words.len() != 3 {
                                    println!("usage: insert <key> <value>");
                                    continue;
                                }
                                Parameter::Insert {
                                    key: words[1].to_string(),
                                    value: words[2].to_string(),
                                }
                            }
                            Parameter::Keys { direction: _ } => match words.len() {
                                1 => Parameter::Keys { direction: None },
                                2 => {
                                    // Try to process the parameter
                                    let direction = Direction::from_str(words[1]).ok();
                                    if direction.is_none() {
                                        println!("usage: keys [<direction>]");
                                        continue;
                                    }
                                    Parameter::Keys { direction }
                                }
                                _ => {
                                    println!("usage: keys [<direction>]");
                                    continue;
                                }
                            },
                            Parameter::Entries { direction: _ } => match words.len() {
                                1 => Parameter::Entries { direction: None },
                                2 => {
                                    // Try to process the parameter
                                    let direction = Direction::from_str(words[1]).ok();
                                    if direction.is_none() {
                                        println!("usage: entries [<direction>]");
                                        continue;
                                    }
                                    Parameter::Entries { direction }
                                }
                                _ => {
                                    println!("usage: entries [<direction>]");
                                    continue;
                                }
                            },
                            Parameter::Nodes { direction: _ } => match words.len() {
                                1 => Parameter::Nodes { direction: None },
                                2 => {
                                    // Try to process the parameter
                                    let direction = Direction::from_str(words[1]).ok();
                                    if direction.is_none() {
                                        println!("usage: nodes [<direction>]");
                                        continue;
                                    }
                                    Parameter::Nodes { direction }
                                }
                                _ => {
                                    println!("usage: nodes [<direction>]");
                                    continue;
                                }
                            },
                            Parameter::Values { direction: _ } => match words.len() {
                                1 => Parameter::Values { direction: None },
                                2 => {
                                    // Try to process the parameter
                                    let direction = Direction::from_str(words[1]).ok();
                                    if direction.is_none() {
                                        println!("usage: values [<direction>]");
                                        continue;
                                    }
                                    Parameter::Values { direction }
                                }
                                _ => {
                                    println!("usage: values [<direction>]");
                                    continue;
                                }
                            },
                            _ => p,
                        }
                    }
                    Err(e) => {
                        println!("error: {e}");
                        continue;
                    }
                };
                process_parameter(&btree, &parameter).await;
                rl.add_history_entry(line.as_str())?;
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
                println!("Error: {err:?}");
                break;
            }
        }
    }
    if let Some(file_location) = get_history_file() {
        if let Err(e) = rl.save_history(&file_location) {
            println!("error saving history: {e}");
        }
    }
    Ok(())
}

async fn process_parameter(btree: &Baildon<String, String>, parameter: &Parameter) {
    match parameter {
        Parameter::Contains { key } => {
            if btree.contains(key).await {
                println!("true");
            } else {
                println!("false");
            }
        }
        Parameter::Clear => match btree.clear().await {
            Ok(_) => println!("cleared"),
            Err(e) => println!("error: {e}"),
        },
        Parameter::Count => println!("count: {}", btree.count().await),
        Parameter::Delete { key } => match btree.delete(key).await {
            Ok(opt_value) => match opt_value {
                Some(value) => {
                    println!("deleted: {key}: {value}");
                }
                None => {
                    println!("not found");
                }
            },
            Err(err) => {
                println!("delete failed: {err}");
            }
        },
        Parameter::Get { key } => match btree.get(key).await {
            Some(value) => {
                println!("{value}");
            }
            None => {
                println!("not found");
            }
        },
        Parameter::Help => {
            let help = Cli::command().render_help().to_string();

            let mut print_it = false;

            for line in help.lines() {
                if line.starts_with("Arguments:") {
                    print_it = false;
                }
                if print_it && !line.is_empty() {
                    println!("{}", line);
                }
                if line.starts_with("Commands:") {
                    print_it = true;
                }
            }
        }
        Parameter::Insert { key, value } => match btree.insert(key.clone(), value.clone()).await {
            Ok(opt_value) => match opt_value {
                Some(old) => {
                    println!("old value: {old}");
                }
                None => {
                    println!("inserted: {key}: {value}");
                }
            },
            Err(err) => {
                println!("insert failed: {err}");
            }
        },
        Parameter::Keys { direction } => {
            if let Some(dir) = direction {
                btree.print_keys(*dir).await
            } else {
                btree.print_keys(Direction::Ascending).await
            }
        }
        Parameter::Entries { direction } => {
            if let Some(dir) = direction {
                btree.print_entries(*dir).await
            } else {
                btree.print_entries(Direction::Ascending).await
            }
        }
        Parameter::Nodes { direction } => {
            if let Some(dir) = direction {
                btree.print_nodes(*dir).await
            } else {
                btree.print_nodes(Direction::Ascending).await
            }
        }
        Parameter::Utilization => {
            println!("Utilization: {:.1}%", 100.0 * btree.utilization().await);
        }
        Parameter::Verify => match btree.verify(Direction::Ascending).await {
            Ok(_) => println!("Ok"),
            Err(e) => println!("Verification failed: {e}"),
        },
        Parameter::Values { direction } => {
            if let Some(dir) = direction {
                btree.print_values(*dir).await
            } else {
                btree.print_values(Direction::Ascending).await
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let log_dir = match env::var("TMPDIR") {
        Ok(d) => d,
        Err(_e) => ".".to_string(),
    };

    let file_appender = tracing_appender::rolling::daily(log_dir, "baildon.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt().with_writer(non_blocking).init();

    let btree: Baildon<String, String> = if cli.create {
        Baildon::<String, String>::try_new(&cli.store, 13).await?
    } else {
        Baildon::<String, String>::try_open(&cli.store).await?
    };

    match cli.parameter {
        Some(parameter) => process_parameter(&btree, &parameter).await,
        None => interactive(btree).await?,
    }
    Ok(())
}
