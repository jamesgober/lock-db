# lock-db &mdash; API Reference

> Complete reference for every public item in `lock-db`, with examples.
> **Status: pre-1.0.** Sections below describe the intended surface as it lands across the 0.x series (see [`dev/ROADMAP.md`](../dev/ROADMAP.md)).

## Table of Contents

- [Overview](#overview)
- [Lock modes](#lock-modes) _(planned)_
- [Multiple granularities](#multiple-granularities) _(planned)_
- [Row and range locks](#row-and-range-locks) _(planned)_
- [Lock table](#lock-table) _(planned)_
- [Deadlock detection](#deadlock-detection) _(planned)_
- [Wait / grant queues](#wait--grant-queues) _(planned)_
- [Feature flags](#feature-flags)

---

## Overview

lock-db is the lock manager for a transactional database: the component that lets many transactions touch shared data at once without corrupting it, and that notices when they have deadlocked and breaks the tie.

---

### Lock modes

_shared (S), exclusive (X), and intention locks (IS, IX, SIX) with a full compatibility matrix. Documented as this lands across the 0.x series._

### Multiple granularities

_database, table, page, and row-level locks with intention-lock coupling. Documented as this lands across the 0.x series._

### Row and range locks

_lock individual keys or key ranges (for predicate / phantom protection). Documented as this lands across the 0.x series._

### Lock table

_a sharded, contention-aware table mapping resources to their lock queues. Documented as this lands across the 0.x series._

### Deadlock detection

_a wait-for graph with cycle detection and configurable victim selection. Documented as this lands across the 0.x series._

### Wait / grant queues

_fair queuing with upgrade handling (S to X). Documented as this lands across the 0.x series._

---

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `std` | yes | Standard library. |
| `serde` | no | Serialization for public types. |

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>.</sub>
