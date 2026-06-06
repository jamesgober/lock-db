//! Wait-for graph, deadlock detection, and victim selection.
//!
//! When a transaction cannot get a lock because another transaction holds it,
//! the first *waits for* the second. Draw an edge from every waiter to every
//! transaction it is blocked by and you get the **wait-for graph**. A cycle in
//! that graph is a deadlock: a set of transactions each waiting for the next,
//! none able to proceed. The only way out is to abort one of them — the
//! *victim* — so the rest can continue.
//!
//! [`WaitForGraph`] holds the edges and finds cycles; [`pick_victim`] chooses
//! which member of a cycle to abort. The graph is pure data — no locks, no
//! threads — which keeps the detection logic small enough to test exhaustively.
//! [`LockManager`](crate::LockManager) builds one of these from its live wait
//! set, recomputed from the current lock table, so detection never acts on a
//! stale edge and so never aborts a transaction that is not actually deadlocked.
//!
//! [`pick_victim`]: WaitForGraph::pick_victim

use std::collections::HashMap;

use crate::TxnId;

/// How to choose which transaction in a deadlock cycle to abort.
///
/// Both policies break the cycle correctly; they differ only in which member
/// pays. Transaction ids are taken as a proxy for age — a larger id is a
/// transaction that started later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum VictimPolicy {
    /// Abort the youngest transaction in the cycle (the largest [`TxnId`]).
    ///
    /// The default. Aborting the most recently started transaction tends to
    /// waste the least already-done work.
    #[default]
    Youngest,

    /// Abort the oldest transaction in the cycle (the smallest [`TxnId`]).
    ///
    /// Useful when the oldest transaction is the one most likely to be stuck or
    /// holding the most locks.
    Oldest,
}

/// A detected deadlock: the cycle of transactions and the one chosen to abort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deadlock {
    /// The transaction selected for abort to break the cycle.
    pub victim: TxnId,
    /// The transactions forming the cycle, in wait-for order. Each waits for the
    /// next, and the last waits for the first.
    pub cycle: Vec<TxnId>,
}

/// A directed graph of which transactions are waiting for which.
///
/// An edge `a -> b` means transaction `a` is blocked waiting for a lock held by
/// transaction `b`. The graph is a plain adjacency map; build it, then call
/// [`detect_cycle`](Self::detect_cycle) or [`cycle_from`](Self::cycle_from) to
/// find a deadlock.
///
/// # Examples
///
/// ```
/// use lock_db::{TxnId, WaitForGraph, VictimPolicy};
///
/// let mut g = WaitForGraph::new();
/// // T1 waits for T2, T2 waits for T1: a deadlock.
/// g.add_wait(TxnId::new(1), TxnId::new(2));
/// g.add_wait(TxnId::new(2), TxnId::new(1));
///
/// let cycle = g.detect_cycle().expect("a cycle exists");
/// assert_eq!(cycle.len(), 2);
/// assert_eq!(WaitForGraph::pick_victim(&cycle, VictimPolicy::Youngest), Some(TxnId::new(2)));
/// ```
#[derive(Debug, Clone, Default)]
pub struct WaitForGraph {
    edges: HashMap<TxnId, Vec<TxnId>>,
}

impl WaitForGraph {
    /// Creates an empty graph.
    #[must_use]
    pub fn new() -> Self {
        Self {
            edges: HashMap::new(),
        }
    }

    /// Records that `waiter` is blocked waiting for a lock held by `holder`.
    ///
    /// A self-edge (`waiter == holder`) is ignored: a transaction cannot
    /// deadlock against itself.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{TxnId, WaitForGraph};
    ///
    /// let mut g = WaitForGraph::new();
    /// g.add_wait(TxnId::new(1), TxnId::new(2));
    /// assert_eq!(g.waiter_count(), 1);
    /// g.add_wait(TxnId::new(3), TxnId::new(3)); // self-edge ignored
    /// assert_eq!(g.waiter_count(), 1);
    /// ```
    pub fn add_wait(&mut self, waiter: TxnId, holder: TxnId) {
        if waiter == holder {
            return;
        }
        self.edges.entry(waiter).or_default().push(holder);
    }

    /// Records that `waiter` is blocked by every transaction in `holders`.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{TxnId, WaitForGraph};
    ///
    /// let mut g = WaitForGraph::new();
    /// g.add_waits(TxnId::new(1), &[TxnId::new(2), TxnId::new(3)]);
    /// // No cycle: 1 waits for 2 and 3, neither waits back.
    /// assert!(g.detect_cycle().is_none());
    /// ```
    pub fn add_waits(&mut self, waiter: TxnId, holders: &[TxnId]) {
        for &holder in holders {
            self.add_wait(waiter, holder);
        }
    }

    /// Removes every edge originating at `waiter` (it stopped waiting).
    pub fn clear_waiter(&mut self, waiter: TxnId) {
        let _ = self.edges.remove(&waiter);
    }

    /// Removes `txn` from the graph entirely — both its own waits and every edge
    /// pointing at it.
    pub fn remove_txn(&mut self, txn: TxnId) {
        let _ = self.edges.remove(&txn);
        for holders in self.edges.values_mut() {
            holders.retain(|h| *h != txn);
        }
    }

    /// Returns `true` if no transaction is waiting.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Returns the number of transactions that are waiting (have outgoing edges).
    #[must_use]
    pub fn waiter_count(&self) -> usize {
        self.edges.len()
    }

    /// Returns a cycle in the graph if one exists, or `None`.
    ///
    /// The returned vector lists the transactions of one cycle in wait-for
    /// order. When several cycles exist, which one is returned is unspecified.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{TxnId, WaitForGraph};
    ///
    /// let mut g = WaitForGraph::new();
    /// // A chain, no cycle.
    /// g.add_wait(TxnId::new(1), TxnId::new(2));
    /// g.add_wait(TxnId::new(2), TxnId::new(3));
    /// assert!(g.detect_cycle().is_none());
    ///
    /// // Close the loop.
    /// g.add_wait(TxnId::new(3), TxnId::new(1));
    /// assert_eq!(g.detect_cycle().map(|c| c.len()), Some(3));
    /// ```
    #[must_use]
    pub fn detect_cycle(&self) -> Option<Vec<TxnId>> {
        for &start in self.edges.keys() {
            if let Some(cycle) = self.cycle_from(start) {
                return Some(cycle);
            }
        }
        None
    }

    /// Returns a cycle reachable from `start` if one exists, or `None`.
    ///
    /// Used for detection at the moment a new wait is added: the only cycle that
    /// can have just formed is one reachable from the transaction that added the
    /// edge.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{TxnId, WaitForGraph};
    ///
    /// let mut g = WaitForGraph::new();
    /// g.add_wait(TxnId::new(1), TxnId::new(2));
    /// g.add_wait(TxnId::new(2), TxnId::new(1));
    /// assert!(g.cycle_from(TxnId::new(1)).is_some());
    /// assert!(g.cycle_from(TxnId::new(9)).is_none()); // unknown txn
    /// ```
    #[must_use]
    pub fn cycle_from(&self, start: TxnId) -> Option<Vec<TxnId>> {
        // Iterative depth-first search. `state`: 0 unvisited, 1 on the current
        // path, 2 fully explored. A back edge to a node still on the path is a
        // cycle. Iterative rather than recursive so a long wait chain cannot
        // overflow the stack.
        let mut state: HashMap<TxnId, u8> = HashMap::new();
        let mut path: Vec<TxnId> = Vec::new();
        let mut stack: Vec<(TxnId, usize)> = Vec::new();

        let _ = state.insert(start, 1);
        path.push(start);
        stack.push((start, 0));

        while let Some(&(node, idx)) = stack.last() {
            let neighbors: &[TxnId] = self.edges.get(&node).map_or(&[], Vec::as_slice);
            if idx < neighbors.len() {
                if let Some(top) = stack.last_mut() {
                    top.1 += 1;
                }
                let next = neighbors[idx];
                match state.get(&next).copied().unwrap_or(0) {
                    1 => {
                        if let Some(pos) = path.iter().position(|t| *t == next) {
                            return Some(path[pos..].to_vec());
                        }
                    }
                    0 => {
                        let _ = state.insert(next, 1);
                        path.push(next);
                        stack.push((next, 0));
                    }
                    _ => {}
                }
            } else {
                let _ = state.insert(node, 2);
                let _ = path.pop();
                let _ = stack.pop();
            }
        }
        None
    }

    /// Chooses the transaction to abort from a deadlock cycle.
    ///
    /// Returns `None` only for an empty slice. See [`VictimPolicy`] for how the
    /// choice is made.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{TxnId, VictimPolicy, WaitForGraph};
    ///
    /// let cycle = [TxnId::new(3), TxnId::new(7), TxnId::new(5)];
    /// assert_eq!(WaitForGraph::pick_victim(&cycle, VictimPolicy::Youngest), Some(TxnId::new(7)));
    /// assert_eq!(WaitForGraph::pick_victim(&cycle, VictimPolicy::Oldest), Some(TxnId::new(3)));
    /// ```
    #[must_use]
    pub fn pick_victim(cycle: &[TxnId], policy: VictimPolicy) -> Option<TxnId> {
        match policy {
            VictimPolicy::Youngest => cycle.iter().copied().max(),
            VictimPolicy::Oldest => cycle.iter().copied().min(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{VictimPolicy, WaitForGraph};
    use crate::TxnId;

    fn t(id: u64) -> TxnId {
        TxnId::new(id)
    }

    #[test]
    fn test_empty_graph_has_no_cycle() {
        let g = WaitForGraph::new();
        assert!(g.is_empty());
        assert!(g.detect_cycle().is_none());
    }

    #[test]
    fn test_self_edge_ignored() {
        let mut g = WaitForGraph::new();
        g.add_wait(t(1), t(1));
        assert!(g.is_empty());
        assert!(g.detect_cycle().is_none());
    }

    #[test]
    fn test_chain_has_no_cycle() {
        let mut g = WaitForGraph::new();
        g.add_wait(t(1), t(2));
        g.add_wait(t(2), t(3));
        g.add_wait(t(3), t(4));
        assert!(g.detect_cycle().is_none());
    }

    #[test]
    fn test_two_cycle_detected() {
        let mut g = WaitForGraph::new();
        g.add_wait(t(1), t(2));
        g.add_wait(t(2), t(1));
        let cycle = g.detect_cycle().unwrap();
        assert_eq!(cycle.len(), 2);
        assert!(cycle.contains(&t(1)) && cycle.contains(&t(2)));
    }

    #[test]
    fn test_three_cycle_detected() {
        let mut g = WaitForGraph::new();
        g.add_wait(t(1), t(2));
        g.add_wait(t(2), t(3));
        g.add_wait(t(3), t(1));
        let cycle = g.detect_cycle().unwrap();
        assert_eq!(cycle.len(), 3);
    }

    #[test]
    fn test_cycle_from_unknown_txn_is_none() {
        let mut g = WaitForGraph::new();
        g.add_wait(t(1), t(2));
        g.add_wait(t(2), t(1));
        assert!(g.cycle_from(t(99)).is_none());
    }

    #[test]
    fn test_cycle_from_finds_cycle_containing_start() {
        let mut g = WaitForGraph::new();
        g.add_wait(t(1), t(2));
        g.add_wait(t(2), t(3));
        g.add_wait(t(3), t(2)); // cycle 2<->3, reachable from 1
        let cycle = g.cycle_from(t(1)).unwrap();
        assert!(cycle.contains(&t(2)) && cycle.contains(&t(3)));
        assert!(!cycle.contains(&t(1))); // 1 is not in the cycle
    }

    #[test]
    fn test_clear_waiter_breaks_cycle() {
        let mut g = WaitForGraph::new();
        g.add_wait(t(1), t(2));
        g.add_wait(t(2), t(1));
        g.clear_waiter(t(1));
        assert!(g.detect_cycle().is_none());
    }

    #[test]
    fn test_remove_txn_drops_incoming_and_outgoing() {
        let mut g = WaitForGraph::new();
        g.add_wait(t(1), t(2));
        g.add_wait(t(2), t(1));
        g.add_wait(t(3), t(2));
        g.remove_txn(t(2));
        assert!(g.detect_cycle().is_none());
        // t(3)'s edge to the removed t(2) is gone.
        assert!(g.cycle_from(t(3)).is_none());
    }

    #[test]
    fn test_diamond_no_cycle() {
        // 1->2, 1->3, 2->4, 3->4: a DAG, no cycle.
        let mut g = WaitForGraph::new();
        g.add_wait(t(1), t(2));
        g.add_wait(t(1), t(3));
        g.add_wait(t(2), t(4));
        g.add_wait(t(3), t(4));
        assert!(g.detect_cycle().is_none());
    }

    #[test]
    fn test_pick_victim_policies() {
        let cycle = [t(3), t(7), t(5)];
        assert_eq!(
            WaitForGraph::pick_victim(&cycle, VictimPolicy::Youngest),
            Some(t(7))
        );
        assert_eq!(
            WaitForGraph::pick_victim(&cycle, VictimPolicy::Oldest),
            Some(t(3))
        );
        assert_eq!(WaitForGraph::pick_victim(&[], VictimPolicy::Youngest), None);
    }

    #[test]
    fn test_detected_cycle_is_an_actual_cycle() {
        // Every consecutive pair in the returned cycle must be a real edge, and
        // the last must wait for the first.
        let mut g = WaitForGraph::new();
        g.add_wait(t(1), t(2));
        g.add_wait(t(2), t(3));
        g.add_wait(t(3), t(1));
        let cycle = g.detect_cycle().unwrap();
        for i in 0..cycle.len() {
            let from = cycle[i];
            let to = cycle[(i + 1) % cycle.len()];
            let edges = g.edges.get(&from).unwrap();
            assert!(edges.contains(&to), "missing edge {from:?} -> {to:?}");
        }
    }

    #[test]
    fn test_default_policy_is_youngest() {
        assert_eq!(VictimPolicy::default(), VictimPolicy::Youngest);
    }
}
