## Overview

This project aims to design and build a **lightweight in-memory data storage system** using pure Java to store and retrieve data efficiently using SQL-like commands. It should also handle concurrent access, utilize efficient indexing, and optionally provide transaction support.

<br>

## Core Components

#### 1. Data Storage Engine

-   Tables stored as in-memory data structures (`HashMap`, `ArrayList`, etc.)
-   Schema definition for each table (column types)
-   Support for basic data types (`INTEGER`, `STRING`, `BOOLEAN`, `VARCHAR`)

#### 2. Query Engine

-   Parser to process SQL-like commands (`INSERT`, `SELECT`, `DELETE`, `UPDATE`)
-   Query execution logic to fetch and modify data efficiently

#### 3. Indexing System

-   Implement a `B-Tree` or `Hash Index` (or anything else) for fast lookups

#### 4. Transaction Manager (ACID compliance)

-   `Atomicity`: Ensure transactions are fully applied or not at all
-   `Consistency`: **Not needed** since you do not need to implement constraints
-   `Isolation`: Use lock-based or MVCC (Multi-Version Concurrency Control) for concurrency.
-   `Durability`: You **do not need** to implement persistence. This can be **optional** (Implement a basic Write-Ahead Log (WAL) for persistence)

#### 5. Concurrency Control

-   Use Read-Write Locks to handle concurrent queries
-   Ensure thread safety when multiple clients access the database

#### 6. Client API

-   Provide a simple command-line interface (CLI) or Java API for interaction

<br>

## Functional Requirements

-   Table creation
-   Data modification
-   Query execution
-   Indexes for optimization
-   Transaction support
-   Concurrency and thread safety

<br>

## Non-functional Requirements

-   The system should maintain low latency for request processing
-   Ensure the system remains stable under load and can handle sudden traffic spikes
-   The design should allow for future scaling options

<br>

## Deliverables

-   A fully implemented solution as a Java codebase
-   Documentation of the design decisions, algorithms used, and any trade-offs made.
-   Test cases demonstrating the system's performance under different load scenarios
-   Performance benchmarks
