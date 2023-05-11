use super::*;

use rand::Rng;

#[tokio::test]
async fn it_creates_tree() {
    let _tree = Baildon::<String, usize>::try_new("create.db", 5)
        .await
        .expect("creates tree file");
    std::fs::remove_file("create.db").expect("cleanup");
}

#[tokio::test]
async fn it_opens_tree() {
    let tree = Baildon::<String, usize>::try_new("open.db", 5)
        .await
        .expect("creates tree file");
    drop(tree);
    let _tree = Baildon::<String, usize>::try_open("open.db")
        .await
        .expect("opens tree file");
    std::fs::remove_file("open.db").expect("cleanup");
}

#[tokio::test]
async fn it_searches_empty_tree() {
    let tree = Baildon::<String, usize>::try_new("search_empty.db", 5)
        .await
        .expect("creates tree file");
    assert!(!tree.contains(&"something".to_string()).await);
    std::fs::remove_file("search_empty.db").expect("cleanup");
}

#[test_log::test(tokio::test)]
async fn it_inserts_into_empty_tree_random_usize() {
    let tree = Baildon::<usize, usize>::try_new("insert_empty_random_usize.db", 8)
        .await
        .expect("creates tree file");
    let mut input = vec![];
    for _i in 0..400 {
        let i = rand::thread_rng().gen_range(0..100_000);
        tree.insert(i, i).await.expect("insert worked");
        input.push(i);
    }
    for i in input {
        assert!(tree.contains(&i).await);
    }
    tree.info().await;
    std::fs::remove_file("insert_empty_random_usize.db").expect("cleanup");
}

#[test_log::test(tokio::test)]
async fn it_inserts_into_empty_tree_usize() {
    let tree = Baildon::<usize, usize>::try_new("insert_empty_usize.db", 157)
        .await
        .expect("creates tree file");
    for i in 0..4_000 {
        let callback = |node: &Node<usize, usize>| {
            tracing::debug!(?node, "leaf");
            ControlFlow::Continue(())
        };
        tree.traverse_leaf_nodes(Direction::Ascending, callback)
            .await;
        tree.insert(i, i).await.expect("insert worked");
        tree.traverse_leaf_nodes(Direction::Ascending, callback)
            .await;
    }
    for i in 0..4_000 {
        assert!(tree.contains(&i).await);
    }
    tree.info().await;
    std::fs::remove_file("insert_empty_usize.db").expect("cleanup");
}

#[test_log::test(tokio::test)]
async fn it_inserts_into_empty_tree_reverse_usize() {
    let tree = Baildon::<usize, usize>::try_new("insert_empty_reverse_usize.db", 5)
        .await
        .expect("creates tree file");
    for i in (0..400).rev() {
        tree.insert(i, i).await.expect("insert worked");
    }
    for i in 0..400 {
        assert!(tree.contains(&i).await);
    }
    tree.info().await;
    std::fs::remove_file("insert_empty_reverse_usize.db").expect("cleanup");
}

#[test_log::test(tokio::test)]
async fn it_inserts_and_clears_into_empty_tree_random_usize() {
    let tree = Baildon::<usize, usize>::try_new("insert_and_clear_empty_random_usize.db", 5)
        .await
        .expect("creates tree file");
    let mut input = vec![];
    for _i in 0..200 {
        let i = rand::thread_rng().gen_range(0..200);
        tree.insert(i, i).await.expect("insert worked");
        input.push(i);
    }
    for i in &input {
        assert!(tree.contains(i).await);
    }
    tree.info().await;
    tree.clear().await.expect("tree cleared");
    for i in input {
        assert!(!tree.contains(&i).await);
    }
    tree.info().await;
    std::fs::remove_file("insert_and_clear_empty_random_usize.db").expect("cleanup");
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn it_inserts_into_empty_tree_string() {
    let tree = Baildon::<String, usize>::try_new("insert_empty_string.db", 30)
        .await
        .expect("creates tree file");
    tree.info().await;
    for i in (0..400).rev() {
        let key = format!("something_{i}");
        let callback = |node: &Node<String, usize>| {
            tracing::debug!(?node, "node");
            ControlFlow::Continue(())
        };
        tree.traverse_nodes(Direction::Ascending, callback).await;
        tree.insert(key, i).await.expect("insert worked");
        tree.traverse_nodes(Direction::Ascending, callback).await;
    }
    tree.info().await;
    assert!(tree.contains(&"something_0".to_string()).await);
    assert!(tree.contains(&"something_13".to_string()).await);
    assert!(tree.contains(&"something_319".to_string()).await);
    tree.inner_flush_to_disk(false)
        .await
        .expect("FLUSHING DATA");

    drop(tree);
    tracing::debug!("ABOUT TO RE-OPEN");
    let new_tree = Baildon::<String, usize>::try_open("insert_empty_string.db")
        .await
        .expect("opens tree file");
    tracing::debug!("ABOUT TO PRINT NEW-INFO");
    new_tree.info().await;
    assert!(new_tree.contains(&"something_0".to_string()).await);
    assert!(new_tree.contains(&"something_13".to_string()).await);
    assert!(new_tree.contains(&"something_319".to_string()).await);
    for i in 400..800 {
        let key = format!("something_{i}");
        let callback = |node: &Node<String, usize>| {
            tracing::debug!(?node, "node");
            ControlFlow::Continue(())
        };
        new_tree
            .traverse_nodes(Direction::Ascending, callback)
            .await;
        new_tree.insert(key, i).await.expect("insert worked");
        new_tree
            .traverse_nodes(Direction::Ascending, callback)
            .await;
    }
    tracing::debug!("ABOUT TO PRINT NODES");
    assert!(new_tree.contains(&"something_400".to_string()).await);
    assert!(new_tree.contains(&"something_413".to_string()).await);
    assert!(new_tree.contains(&"something_719".to_string()).await);
    drop(new_tree);
    std::fs::remove_file("insert_empty_string.db").expect("cleanup");
}

#[test_log::test(tokio::test)]
async fn it_inserts_into_empty_tree_example_usize() {
    let tree = Baildon::<usize, usize>::try_new("insert_empty_example_usize.db", 3)
        .await
        .expect("creates tree file");
    let input = vec![5, 15, 25, 35, 45];
    for i in &input {
        let callback = |node: &Node<usize, usize>| {
            tracing::debug!(?node, "node");
            ControlFlow::Continue(())
        };
        tree.traverse_nodes(Direction::Ascending, callback).await;
        tree.insert(*i, *i).await.expect("insert worked");
        tree.traverse_nodes(Direction::Ascending, callback).await;
    }
    for i in &input {
        assert!(tree.contains(i).await);
    }
    tree.info().await;
    std::fs::remove_file("insert_empty_example_usize.db").expect("cleanup");
}

#[test_log::test(tokio::test)]
async fn it_deletes_from_populated_tree_example_usize() {
    let tree = Baildon::<usize, usize>::try_new("delete_populated_example_usize.db", 3)
        .await
        .expect("creates tree file");
    let input = vec![5, 15, 20, 25, 30, 35, 40, 45, 55];
    for i in &input {
        let callback = |node: &Node<usize, usize>| {
            tracing::debug!(?node, "node");
            ControlFlow::Continue(())
        };
        tree.traverse_nodes(Direction::Ascending, callback).await;
        tree.insert(*i, *i).await.expect("insert worked");
        tree.traverse_nodes(Direction::Ascending, callback).await;
    }
    for i in &input {
        assert!(tree.contains(i).await);
    }
    assert_eq!(tree.first_key().await.unwrap(), 5);
    assert_eq!(tree.last_key().await.unwrap(), 55);
    tree.info().await;
    tree.inner_flush_to_disk(false)
        .await
        .expect("FLUSHING DATA");
    println!("What is 55: {:?}", tree.get(&55).await);
    tree.delete(&55).await.expect("delete worked");
    println!("What is 55: {:?}", tree.get(&55).await);
    std::fs::remove_file("delete_populated_example_usize.db").expect("cleanup");
}

#[test_log::test(tokio::test)]
async fn it_deletes_from_populated_tree_you_tube_example_usize() {
    let tree = Baildon::<usize, usize>::try_new("delete_populated_you_tube_example_usize.db", 3)
        .await
        .expect("creates tree file");
    let input = vec![
        7, 8, 14, 20, 21, 27, 34, 42, 43, 47, 48, 52, 64, 72, 90, 91, 93, 94, 97,
    ];
    for i in &input {
        tree.insert(*i, *i).await.expect("insert worked");
    }

    std::fs::remove_file("delete_populated_you_tube_example_usize.db").expect("cleanup");
}

#[test_log::test(tokio::test)]
async fn it_can_retrieve_keys_from_empty_tree() {
    let tree = Baildon::<usize, usize>::try_new("retrieve_keys_from_empty_tree.db", 3)
        .await
        .expect("creates tree file");

    let keys = tree
        .keys(Direction::Ascending)
        .await
        .collect::<Vec<usize>>()
        .await;
    assert!(keys.is_empty());

    std::fs::remove_file("retrieve_keys_from_empty_tree.db").expect("cleanup");
}

#[test_log::test(tokio::test)]
async fn it_can_retrieve_keys_from_tree() {
    let tree = Baildon::<usize, usize>::try_new("retrieve_keys_from_tree.db", 3)
        .await
        .expect("creates tree file");

    let input = vec![
        7, 8, 14, 20, 21, 27, 34, 42, 43, 47, 48, 52, 64, 72, 90, 91, 93, 94, 97,
    ];
    for i in &input {
        tree.insert(*i, *i).await.expect("insert worked");
    }

    let keys = tree
        .keys(Direction::Ascending)
        .await
        .collect::<Vec<usize>>()
        .await;

    assert_eq!(input, keys);

    std::fs::remove_file("retrieve_keys_from_tree.db").expect("cleanup");
}
