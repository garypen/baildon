use anyhow::Result;

use baildon::btree::Baildon;

#[tokio::main]
async fn main() -> Result<()> {
    let key = "something".to_string();
    let value = 3;

    // Create a tree with a branching factor of 7
    let tree = Baildon::<String, usize>::try_new("hello.db", 7).await?;

    // Make sure we can't find "something" in our tree
    assert!(!tree.contains(&key).await);
    assert_eq!(tree.get(&key).await, None);

    // Insert "something" with a value of 3
    tree.insert(key.clone(), value).await?;

    // Make sure we can find "something" in our tree
    assert!(tree.contains(&key).await);
    assert_eq!(tree.get(&key).await, Some(value));

    // Remove "something"
    tree.delete(&key).await?;
    assert!(!tree.contains(&key).await);
    assert_eq!(tree.get(&key).await, None);

    std::fs::remove_file("hello.db")?;
    Ok(())
}
