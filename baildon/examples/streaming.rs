use baildon::btree::Baildon;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a tree with a branching factor of 7
    let tree = Baildon::<String, usize>::try_new("hello.db", 7).await?;

    // Make sure we can't find "something" in our tree
    assert!(!tree.contains(&"something".to_string()).await);
    assert_eq!(tree.get(&"something".to_string()).await, None);

    tree.insert("something".to_string(), 3).await?;

    // Make sure we can find "something" in our tree
    assert!(tree.contains(&"something".to_string()).await);
    assert_eq!(tree.get(&"something".to_string()).await, Some(3));

    std::fs::remove_file("hello.db").expect("cleanup");
    Ok(())
}
