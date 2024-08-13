use std::collections::HashMap;

#[derive(Debug, Default)]
struct TrieNode {
    children: HashMap<char, TrieNode>,
    is_end_of_key: bool,
}

#[derive(Debug, Default)]
pub(crate) struct Trie {
    root: TrieNode,
}

impl Trie {
    pub(crate) fn new() -> Self {
        Self {
            root: TrieNode::default(),
        }
    }

    pub(crate) fn insert(&mut self, key: &str) {
        let mut node = &mut self.root;
        for ch in key.chars() {
            node = node.children.entry(ch).or_insert_with(TrieNode::default);
        }
        node.is_end_of_key = true;
    }

    pub(crate) fn remove(&mut self, key: &str) {
        Trie::remove_recursive(&mut self.root, key, 0);
    }

    fn remove_recursive(node: &mut TrieNode, key: &str, depth: usize) -> bool {
        if depth == key.len() {
            if !node.is_end_of_key {
                return false;
            }
            node.is_end_of_key = false;
            return node.children.is_empty();
        }

        let ch = key.chars().nth(depth).unwrap();
        if let Some(child_node) = node.children.get_mut(&ch) {
            if Trie::remove_recursive(child_node, key, depth + 1) {
                node.children.remove(&ch);
                return !node.is_end_of_key && node.children.is_empty();
            }
        }
        false
    }

    pub(crate) fn search(&self, prefix: &str) -> Vec<String> {
        let mut node = &self.root;
        for ch in prefix.chars() {
            if let Some(next_node) = node.children.get(&ch) {
                node = next_node;
            } else {
                return Vec::new();
            }
        }
        let mut keys = Vec::new();
        self.collect_keys(node, prefix.to_string(), &mut keys);
        keys
    }

    fn collect_keys(&self, node: &TrieNode, prefix: String, keys: &mut Vec<String>) {
        if node.is_end_of_key {
            keys.push(prefix.clone());
        }
        for (ch, child_node) in &node.children {
            let mut new_prefix = prefix.clone();
            new_prefix.push(*ch);
            self.collect_keys(child_node, new_prefix, keys);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_search() {
        let mut trie = Trie::new();
        trie.insert("/cluster/brokers/load/broker1");
        trie.insert("/cluster/brokers/load/broker2");
        trie.insert("/cluster/brokers/policy/broker1");
        trie.insert("/cluster/brokers/policy/broker2");

        let mut result = trie.search("/cluster/brokers/load");
        result.sort();
        assert_eq!(
            result,
            vec![
                "/cluster/brokers/load/broker1",
                "/cluster/brokers/load/broker2"
            ]
        );

        let mut result = trie.search("/cluster/brokers/policy");
        result.sort();
        assert_eq!(
            result,
            vec![
                "/cluster/brokers/policy/broker1",
                "/cluster/brokers/policy/broker2"
            ]
        );
    }

    #[test]
    fn test_search_partitions() {
        let mut trie = Trie::new();
        trie.insert("/namespaces/default/topics/default/topic1-part-1");
        trie.insert("/namespaces/default/topics/default/topic1-part-2");
        trie.insert("/namespaces/default/topics/default/topic2-part-1");
        trie.insert("/namespaces/default/topics/default/topic2-part-2");

        let mut result = trie.search("/namespaces/default/topics/default/topic1");
        result.sort();
        assert_eq!(
            result,
            vec![
                "/namespaces/default/topics/default/topic1-part-1",
                "/namespaces/default/topics/default/topic1-part-2"
            ]
        );
    }

    #[test]
    fn test_remove() {
        let mut trie = Trie::new();
        trie.insert("/cluster/brokers/load/broker1");
        trie.insert("/cluster/brokers/load/broker2");
        trie.insert("/cluster/brokers/policy/broker1");

        trie.remove("/cluster/brokers/load/broker2");

        let result = trie.search("/cluster/brokers/load");
        assert_eq!(result, vec!["/cluster/brokers/load/broker1"]);

        let result = trie.search("/cluster/brokers/policy");
        assert_eq!(result, vec!["/cluster/brokers/policy/broker1"]);

        trie.remove("/cluster/brokers/load/broker1");
        let result = trie.search("/cluster/brokers/load");
        assert!(result.is_empty());
    }

    #[test]
    fn test_search_non_existent_prefix() {
        let mut trie = Trie::new();
        trie.insert("/cluster/brokers/load/broker1");

        let result = trie.search("/cluster/brokers/policy");
        assert!(result.is_empty());
    }

    #[test]
    fn test_insert_same_key_twice() {
        let mut trie = Trie::new();
        trie.insert("/cluster/brokers/load/broker1");
        trie.insert("/cluster/brokers/load/broker1");

        let result = trie.search("/cluster/brokers/load");
        assert_eq!(result, vec!["/cluster/brokers/load/broker1"]);
    }
}
