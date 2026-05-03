# System Architecture

## Overview
This project implements a distributed synchronization system using the RAFT consensus algorithm.

## Components

### 1. Nodes
- Base Node
- Cache Node
- Queue Node
- Lock Manager

Each node can act as:
- Leader
- Follower
- Candidate

### 2. Communication Layer
Handles:
- Message passing
- Failure detection

### 3. Consensus (RAFT)
Located in:
`src/consensus/raft.py`

Responsibilities:
- Leader election
- Log replication
- Fault tolerance

### 4. Utilities
- Config management
- Metrics tracking

## Architecture Flow

Client → Node API → RAFT Consensus → Data Replication → Other Nodes

## Fault Tolerance
- Uses heartbeat mechanism
- Automatic leader election if leader fails