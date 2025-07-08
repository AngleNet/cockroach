# CockroachDB: An In-Depth Source Code Level Technical Report

## Table of Contents

1. [Executive Summary and Introduction](#1-executive-summary-and-introduction)
2. [System Architecture Deep Dive](#2-system-architecture-deep-dive)
3. [Storage Layer Analysis](#3-storage-layer-analysis)
4. [Distributed Systems Core](#4-distributed-systems-core)
5. [Transaction Processing](#5-transaction-processing)
6. [SQL Layer](#6-sql-layer)
7. [Networking and Communication](#7-networking-and-communication)
8. [Advanced Features](#8-advanced-features)
9. [Performance and Monitoring](#9-performance-and-monitoring)

---

## 1. Executive Summary and Introduction

### 1.1 Overview

CockroachDB represents a groundbreaking approach to distributed SQL databases, designed from the ground up to address the fundamental challenges of building globally distributed, strongly consistent, and highly available data systems. At its core, CockroachDB is a distributed SQL database built on a transactional and strongly-consistent key-value store, implementing a unique architecture that combines the best aspects of traditional relational databases with modern distributed systems principles.

The system's name, inspired by the resilience of cockroaches, reflects its primary design goal: survival. CockroachDB is engineered to tolerate disk, machine, rack, and even datacenter failures with minimal latency disruption and no manual intervention. This resilience is achieved through a sophisticated combination of consensus algorithms, intelligent data distribution, and automatic recovery mechanisms.

### 1.2 Key Design Principles

CockroachDB's architecture is guided by several fundamental design principles that distinguish it from both traditional monolithic databases and first-generation distributed databases:

#### 1.2.1 Symmetric Architecture

Unlike many distributed databases that employ master-slave or primary-secondary architectures, CockroachDB implements a symmetric design where every node in the cluster is functionally identical. This symmetry is reflected in the codebase structure found in `/pkg/server/`, where each node runs the same binary and can assume any role - from serving client SQL queries to participating in distributed consensus.

```go
// From pkg/server/node.go
type Node struct {
    stopper         *stop.Stopper
    clusterID       uuid.UUID
    Descriptor      roachpb.NodeDescriptor
    storeCfg        kvserver.StoreConfig
    stores          *kvserver.Stores
    metrics         nodeMetrics
    recorder        *status.MetricsRecorder
    startedAt       int64
    lastUp          int64
    initialStart    bool
    txnMetrics      kvcoord.TxnMetrics
}
```

This symmetric design eliminates single points of failure and simplifies deployment, as operators need only manage a single binary type regardless of cluster size.

#### 1.2.2 Horizontal Scalability

The system achieves horizontal scalability through its range-based data distribution model. Data is automatically partitioned into ranges (default size: 64MB), which are distributed across the cluster. The implementation in `/pkg/kv/kvserver/` manages these ranges, with each range maintaining multiple replicas for fault tolerance.

#### 1.2.3 Strong Consistency

CockroachDB provides strong consistency guarantees through its implementation of the Raft consensus algorithm. Every write operation goes through Raft consensus among the replicas of the affected range, ensuring that all replicas maintain identical state. The Raft implementation can be found in `/pkg/raft/`, with extensive optimizations for production workloads.

#### 1.2.4 SQL Compatibility

Despite being a distributed system at its core, CockroachDB presents a familiar SQL interface to applications. The SQL layer, implemented in `/pkg/sql/`, supports a significant subset of the PostgreSQL dialect, including complex queries, joins, and secondary indexes. This compatibility is achieved through a sophisticated query planning and execution engine that translates SQL operations into distributed key-value operations.

### 1.3 Architecture Overview

CockroachDB implements a layered architecture that cleanly separates concerns while maintaining performance:

```
┌─────────────────────────────────────────┐
│          SQL Layer                      │
│  (Parser, Planner, Executor)            │
├─────────────────────────────────────────┤
│      Transactional KV Layer             │
│  (MVCC, Transactions, Intents)          │
├─────────────────────────────────────────┤
│      Distribution Layer                 │
│  (Ranges, Replication, Rebalancing)     │
├─────────────────────────────────────────┤
│      Consensus Layer                    │
│  (Raft, Leader Election, Log Repl.)     │
├─────────────────────────────────────────┤
│      Storage Layer                      │
│  (Pebble, SSTs, WAL)                   │
└─────────────────────────────────────────┘
```

Each layer is carefully designed to provide clean abstractions while maintaining the performance characteristics necessary for a production database system.

### 1.4 Technical Innovations

CockroachDB introduces several technical innovations that differentiate it from other distributed databases:

#### 1.4.1 Hybrid Logical Clocks (HLC)

To maintain consistency in a distributed environment without relying on synchronized physical clocks, CockroachDB implements Hybrid Logical Clocks. The HLC implementation in `/pkg/util/hlc/` combines physical timestamps with logical counters to establish a total ordering of events across the cluster:

```go
// From pkg/util/hlc/hlc.go
type Timestamp struct {
    WallTime int64 // Wall time in nanoseconds
    Logical  int32 // Logical component for causality
}
```

This approach allows CockroachDB to provide strong consistency guarantees without the infrastructure requirements of systems like Google Spanner's TrueTime.

#### 1.4.2 Distributed SQL Execution

The DistSQL engine, implemented in `/pkg/sql/distsql/`, enables CockroachDB to distribute query processing across multiple nodes. Complex queries are broken down into a physical plan of processors that can execute in parallel across the cluster, bringing computation to data rather than moving data to computation.

#### 1.4.3 Multi-Version Concurrency Control (MVCC)

CockroachDB implements MVCC at the storage layer, allowing multiple versions of data to coexist. This enables lock-free reads and efficient transaction isolation. The MVCC implementation in `/pkg/storage/mvcc.go` maintains historical versions of all data, with garbage collection removing old versions based on configured retention policies.

### 1.5 Trade-offs and Design Decisions

Every architectural decision in CockroachDB involves trade-offs between competing concerns:

#### 1.5.1 Consistency vs. Availability

CockroachDB chooses consistency over availability in network partition scenarios, following the CP side of the CAP theorem. This means that during network partitions, ranges without a quorum of replicas become unavailable rather than risk data inconsistency. This trade-off is fundamental to maintaining the strong consistency guarantees that many applications require.

#### 1.5.2 Performance vs. Simplicity

The system often chooses implementation simplicity over maximum theoretical performance. For example, the use of Raft for consensus, while not the fastest consensus algorithm available, provides a well-understood and proven foundation for distributed consensus. The implementation includes numerous optimizations like batched proposals and coalesced heartbeats to mitigate performance impacts.

#### 1.5.3 Flexibility vs. Optimization

CockroachDB's design as a general-purpose database means it cannot optimize for specific workload patterns as aggressively as specialized systems. The range-based sharding mechanism, while providing good general-purpose performance, may not be optimal for all access patterns compared to application-specific sharding strategies.

### 1.6 Source Code Organization

The CockroachDB codebase is organized into a modular structure that reflects its layered architecture:

- `/pkg/sql/`: SQL parsing, planning, and execution
- `/pkg/kv/`: Transactional key-value layer
- `/pkg/kvserver/`: Range management and replication  
- `/pkg/storage/`: Low-level storage engine interface
- `/pkg/roachpb/`: Protocol buffer definitions
- `/pkg/util/`: Utility packages for common functionality
- `/pkg/ccl/`: Closed-source enterprise features

This organization facilitates both understanding and contribution, with clear boundaries between components and well-defined interfaces.

### 1.7 Performance Characteristics

CockroachDB's performance profile reflects its design priorities:

- **Write Performance**: Bounded by Raft consensus latency, typically requiring a quorum of replicas to acknowledge writes. The system achieves good throughput through batching and pipelining.

- **Read Performance**: Optimized through range leases that allow consistent reads from a single replica without consensus overhead. The MVCC implementation enables lock-free reads.

- **Scalability**: Near-linear scalability for many workloads as nodes are added, limited primarily by cross-range transaction coordination overhead.

- **Latency**: Predictable latencies for single-range operations, with increased latency for operations spanning multiple ranges or requiring distributed transaction coordination.

### 1.8 Operational Considerations

CockroachDB's design emphasizes operational simplicity:

- **Self-Healing**: Automatic detection and recovery from node failures, with data automatically re-replicated to maintain the desired replication factor.

- **Zero-Downtime Operations**: Support for rolling upgrades, online schema changes, and dynamic cluster resizing without service interruption.

- **Monitoring and Observability**: Comprehensive metrics exposure through Prometheus-compatible endpoints and integrated distributed tracing.

- **Multi-Region Deployment**: Native support for geo-distributed deployments with configurable data placement policies.

This introduction provides the foundation for understanding CockroachDB's architecture and implementation details that will be explored in depth throughout this report. Each subsequent section will dive deep into specific components, analyzing source code, discussing implementation trade-offs, and providing concrete examples of how the system achieves its design goals.

## 2. System Architecture Deep Dive

### 2.1 Layered Architecture Analysis

CockroachDB's architecture is meticulously designed as a series of well-defined layers, each responsible for specific functionality while maintaining clean interfaces with adjacent layers. This layered approach enables modularity, testability, and the ability to reason about system behavior at different levels of abstraction.

#### 2.1.1 The SQL Layer

At the topmost level, the SQL layer (`/pkg/sql/`) serves as the primary interface between client applications and the distributed storage system. This layer is responsible for:

1. **Connection Management**: The pgwire protocol implementation in `/pkg/sql/pgwire/` handles PostgreSQL-compatible client connections:

```go
// From pkg/sql/pgwire/server.go
type Server struct {
    AmbientCtx         log.AmbientContext
    cfg                *base.Config
    SQLServer          *sql.Server
    execCfg            *sql.ExecutorConfig
    
    mu struct {
        syncutil.RWMutex
        connCount      int64
        connections    map[net.Conn]struct{}
        
        // draining is set to true when the server starts draining the SQL 
        // connections.
        draining bool
    }
}
```

2. **Query Parsing and Analysis**: The parser, built using yacc and located in `/pkg/sql/parser/`, transforms SQL text into abstract syntax trees (AST). The parser is designed to be PostgreSQL-compatible while supporting CockroachDB-specific extensions:

```go
// From pkg/sql/parser/parse.go
func Parse(sql string) (Statements, error) {
    return parseWithDepth(1, sql)
}

func parseWithDepth(depth int, sql string) (Statements, error) {
    s := scanner{
        in:    sql,
        depth: depth,
    }
    if yyParse(&s) != 0 {
        return nil, s.lastError
    }
    return s.stmts, nil
}
```

3. **Query Planning**: The planner transforms AST nodes into executable query plans. The planning process involves multiple phases:
   - Semantic analysis and type checking
   - Logical plan construction
   - Physical plan generation
   - Cost-based optimization

The optimizer, located in `/pkg/sql/opt/`, implements a sophisticated cost-based query optimizer inspired by the Cascades framework:

```go
// From pkg/sql/opt/exec/execbuilder/builder.go
type Builder struct {
    factory            exec.Factory
    optimizer          *opt.Optimizer
    evalCtx            *eval.Context
    
    // withExprs is the set of With expressions which are currently being 
    // built.
    withExprs []builtWithExpr
    
    // subqueries tracks the subqueries that are part of scalar expressions
    // we are currently building.
    subqueries []exec.Subquery
}
```

#### 2.1.2 The Transaction Layer

The transaction layer (`/pkg/kv/`) provides ACID guarantees across distributed data. This layer implements:

1. **Transaction Coordination**: Each transaction is managed by a coordinator that tracks write intents and ensures consistency:

```go
// From pkg/kv/txn.go
type Txn struct {
    db              *DB
    typ             TxnType
    gatewayNodeID   roachpb.NodeID
    mu              struct {
        syncutil.Mutex
        
        ID              uuid.UUID
        debugName       string
        sender          TxnSender
        
        // userPriority is the transaction's priority.
        userPriority roachpb.UserPriority
        
        // txnAnchorKey is the key at which to anchor the transaction record.
        txnAnchorKey roachpb.Key
    }
}
```

2. **Distributed Transaction Protocol**: CockroachDB implements a distributed transaction protocol that avoids traditional two-phase commit overhead through the use of write intents and transaction records:

```go
// From pkg/kv/kvserver/batcheval/transaction.go
func updateIntentTxnStatus(
    ctx context.Context,
    readWriter storage.ReadWriter,
    evalCtx EvalContext,
    key roachpb.Key,
    meta *enginepb.MVCCMetadata,
    txn *roachpb.Transaction,
    commit bool,
) error {
    // Implementation handles intent resolution based on transaction status
}
```

#### 2.1.3 The Distribution Layer

The distribution layer (`/pkg/kvserver/`) manages data distribution across the cluster:

1. **Range Management**: Data is divided into ranges, each maintaining its own Raft group:

```go
// From pkg/kvserver/replica.go
type Replica struct {
    RangeID       roachpb.RangeID
    store         *Store
    abortSpan     *abortspan.AbortSpan
    
    mu struct {
        syncutil.RWMutex
        
        // The destroyed status of a replica indicating if it's alive, 
        // destroyed, or pending destruction.
        destroyed destroyStatus
        
        // The state of the Raft state machine.
        state storagepb.ReplicaState
        
        // The lease information.
        lease roachpb.Lease
    }
}
```

2. **Load Balancing**: The system continuously monitors load distribution and rebalances data to maintain optimal performance:

```go
// From pkg/kvserver/allocator/allocator.go
type Allocator struct {
    storePool     *storepool.StorePool
    nodeLatencyFn func(nodeID roachpb.NodeID) (time.Duration, bool)
    
    // scorerOptions configures the range rebalancing scorer.
    scorerOptions *RangeRebalanceOptions
}

func (a *Allocator) ComputeAction(
    ctx context.Context, conf roachpb.SpanConfig, desc *roachpb.RangeDescriptor,
) (AllocatorAction, float64) {
    // Determines whether a range needs rebalancing, upreplication, etc.
}
```

#### 2.1.4 The Consensus Layer

The consensus layer implements the Raft consensus algorithm to ensure consistency across replicas:

```go
// From pkg/raft/raft.go
type raft struct {
    id     uint64
    Term   uint64
    Vote   uint64
    
    // the log
    raftLog *raftLog
    
    state StateType
    
    // isLearner is true if the local raft node is a learner.
    isLearner bool
    
    msgs []pb.Message
    
    // the leader id
    lead uint64
    
    // leadTransferee is id of the leader transfer target when its value is not zero.
    leadTransferee uint64
}
```

The Raft implementation includes numerous optimizations for production use:
- Batched log entries
- Coalesced heartbeats
- Pre-vote phase to prevent disruption
- Joint consensus for configuration changes

#### 2.1.5 The Storage Layer

At the lowest level, the storage layer (`/pkg/storage/`) interfaces with the Pebble storage engine:

```go
// From pkg/storage/pebble.go
type Pebble struct {
    db *pebble.DB
    
    closed      bool
    readOnly    bool
    path        string
    auxDir      string
    ballastPath string
    
    // Relevant options copied over from pebble.Options.
    fs            vfs.FS
    unencryptedFS vfs.FS
    logger        pebble.Logger
}
```

### 2.2 Component Interactions

The interaction between layers follows a strict hierarchy, with each layer only communicating with adjacent layers through well-defined interfaces.

#### 2.2.1 SQL to KV Translation

When a SQL query is executed, it undergoes a complex transformation process:

1. **Query Reception**: The pgwire server receives the query and creates a session context
2. **Parsing**: The SQL text is parsed into an AST
3. **Planning**: The AST is transformed into a logical plan, then optimized into a physical plan
4. **Execution**: The plan is executed, generating KV operations

Example of SQL to KV translation for a simple INSERT:

```sql
INSERT INTO users (id, name) VALUES (1, 'Alice');
```

This translates to KV operations:

```go
// Simplified representation
Put(Key: /Table/53/1/1/0, Value: 'Alice')
// Where: /Table/TableID/IndexID/PrimaryKey/ColumnID
```

#### 2.2.2 Transaction Flow

A typical transaction flow through the layers:

```go
// Client initiates transaction
BEGIN;
INSERT INTO accounts (id, balance) VALUES (1, 100);
UPDATE accounts SET balance = balance - 50 WHERE id = 1;
COMMIT;
```

The flow proceeds as:

1. **SQL Layer**: Creates a transaction object and coordinator
2. **Transaction Layer**: Manages transaction state, tracks write intents
3. **Distribution Layer**: Routes operations to appropriate ranges
4. **Consensus Layer**: Ensures all replicas agree on the operations
5. **Storage Layer**: Persists the data durably

#### 2.2.3 Range Operations

Range operations demonstrate the coordination between layers:

```go
// From pkg/kvserver/replica_send.go
func (r *Replica) Send(
    ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
    // Check lease
    if err := r.checkExecutionCanProceed(ctx, ba); err != nil {
        return nil, kvpb.NewError(err)
    }
    
    // Route to appropriate handler
    if ba.IsWrite() {
        return r.executeWriteBatch(ctx, ba)
    }
    return r.executeReadOnlyBatch(ctx, ba)
}
```

### 2.3 Design Patterns and Trade-offs

CockroachDB's architecture embodies several key design patterns and makes specific trade-offs to achieve its goals.

#### 2.3.1 Design Patterns

1. **Command Pattern**: KV operations are encapsulated as commands that can be serialized, sent over the network, and replayed:

```go
// From pkg/kvpb/api.go
type Request interface {
    Method() Method
    ShallowCopy() Request
}

type GetRequest struct {
    RequestHeader
    KeyLocking lock.Strength
}

type PutRequest struct {
    RequestHeader
    Value   roachpb.Value
    Inline  bool
}
```

2. **Strategy Pattern**: Different execution strategies for different operation types:

```go
// From pkg/sql/exec_factory.go
type Factory interface {
    // ConstructScan creates a node that scans a table.
    ConstructScan(
        table cat.Table,
        index cat.Index,
        needed exec.TableColumnOrdinalSet,
        // ... more parameters
    ) (exec.Node, error)
    
    // ConstructFilter creates a node that filters rows.
    ConstructFilter(n exec.Node, filter tree.TypedExpr) (exec.Node, error)
    
    // ... more construction methods
}
```

3. **Observer Pattern**: The gossip system implements a publish-subscribe mechanism for cluster metadata:

```go
// From pkg/gossip/gossip.go
type Gossip struct {
    mu struct {
        syncutil.RWMutex
        
        // callbacks are invoked when gossip values change.
        callbacks []*callback
        
        // info is the set of gossip values.
        info infoStore
    }
}

func (g *Gossip) RegisterCallback(pattern string, fn Callback) func() {
    // Registers a callback for gossip updates
}
```

#### 2.3.2 Architectural Trade-offs

1. **Consistency vs. Availability**: CockroachDB prioritizes consistency, making ranges unavailable during network partitions if they cannot maintain a quorum. This is implemented through the Raft requirement for majority agreement:

```go
// Simplified quorum check
func hasQuorum(replicas int, available int) bool {
    return available > replicas/2
}
```

2. **Latency vs. Throughput**: The system optimizes for throughput through batching, but this can increase latency for individual operations:

```go
// From pkg/kvserver/store_send.go
type batcher struct {
    // Batches are accumulated here before being sent
    pending []kvpb.BatchRequest
    
    // Maximum time to wait before sending a partial batch
    timeout time.Duration
}
```

3. **Flexibility vs. Performance**: The SQL layer provides flexibility but adds overhead compared to direct KV operations. The system mitigates this through:
   - Distributed SQL execution to push computation to data
   - Vectorized execution engine for analytical queries
   - Plan caching to avoid re-optimization

4. **Simplicity vs. Features**: Some design choices favor simplicity:
   - Single binary deployment (no separate coordinator/worker nodes)
   - Symmetric node architecture (all nodes can serve all roles)
   - Automatic sharding (no manual partition management)

### 2.4 System Initialization and Bootstrap

The system initialization process demonstrates the careful orchestration required to bootstrap a distributed system:

```go
// From pkg/server/init.go
func (s *initServer) Bootstrap(
    ctx context.Context, req *serverpb.BootstrapRequest,
) (*serverpb.BootstrapResponse, error) {
    // Phase 1: Validate cluster not already initialized
    if err := s.checkBootstrapRequest(ctx, req); err != nil {
        return nil, err
    }
    
    // Phase 2: Create initial range descriptors
    initialRanges := s.createInitialRanges()
    
    // Phase 3: Write initial system data
    if err := s.writeInitialClusterData(ctx, initialRanges); err != nil {
        return nil, err
    }
    
    // Phase 4: Start Raft groups for system ranges
    if err := s.startSystemRanges(ctx); err != nil {
        return nil, err
    }
    
    return &serverpb.BootstrapResponse{}, nil
}
```

### 2.5 Runtime Architecture

During runtime, the system maintains several critical subsystems:

#### 2.5.1 Background Tasks

CockroachDB runs numerous background tasks to maintain system health:

```go
// From pkg/server/node.go
func (n *Node) startBackgroundTasks(ctx context.Context) {
    // Start gossip
    n.startGossiping(ctx, n.stopper)
    
    // Start range lease renewal
    n.stores.VisitStores(func(s *kvserver.Store) error {
        s.StartLeaseRenewer(ctx)
        return nil
    })
    
    // Start metrics collection
    n.startComputePeriodicMetrics(n.stopper, defaultMetricInterval)
    
    // Start time series maintenance
    n.startTimeSeriesMaintenance(ctx)
}
```

#### 2.5.2 Request Routing

The system must route requests to the appropriate range lease holders:

```go
// From pkg/kv/kvclient/kvcoord/dist_sender.go
type DistSender struct {
    // Range cache for looking up range descriptors
    rangeCache *rangecache.RangeCache
    
    // Transport for sending RPCs
    transportFactory TransportFactory
    
    // Metrics
    metrics DistSenderMetrics
}

func (ds *DistSender) Send(
    ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
    // Divide batch by ranges
    parts := ds.divideAndSendBatchToRanges(ctx, ba)
    
    // Send to each range in parallel
    return ds.parallelSend(ctx, parts)
}
```

### 2.6 Failure Handling and Recovery

The architecture includes comprehensive failure handling at each layer:

#### 2.6.1 Node Failure Detection

Node failures are detected through gossip heartbeats and Raft leadership:

```go
// From pkg/kvserver/liveness/liveness.go
type NodeLiveness struct {
    mu struct {
        syncutil.RWMutex
        
        // Map of node ID to most recent liveness record
        nodes map[roachpb.NodeID]Record
    }
}

func (nl *NodeLiveness) IsLive(nodeID roachpb.NodeID) (bool, error) {
    rec, exists := nl.mu.nodes[nodeID]
    if !exists {
        return false, ErrNoLivenessRecord
    }
    return rec.IsLive(nl.clock.Now()), nil
}
```

#### 2.6.2 Range Recovery

When a range loses replicas, the system automatically creates new ones:

```go
// From pkg/kvserver/replicate_queue.go
func (rq *replicateQueue) processOneChange(
    ctx context.Context, r *Replica,
) error {
    desc := r.Desc()
    
    // Check if we need to add/remove replicas
    action, _ := rq.allocator.ComputeAction(ctx, r.SpanConfig(), desc)
    
    switch action {
    case AllocatorAddVoter:
        return rq.addReplica(ctx, r, VoterTarget)
    case AllocatorRemoveVoter:
        return rq.removeReplica(ctx, r, VoterTarget)
    // ... other actions
    }
}
```

### 2.7 Performance Considerations

The architecture includes numerous performance optimizations:

#### 2.7.1 Batch Processing

Operations are batched at multiple levels to amortize overhead:

```go
// From pkg/kvserver/replica_write.go
func (r *Replica) proposeBatch(
    ctx context.Context, ba *kvpb.BatchRequest,
) (chan proposalResult, error) {
    // Combine multiple requests into a single Raft proposal
    proposal := &ProposalData{
        command: &kvserverpb.RaftCommand{
            BatchRequest: ba,
        },
    }
    
    return r.propose(ctx, proposal)
}
```

#### 2.7.2 Parallel Execution

The system exploits parallelism wherever possible:

```go
// From pkg/sql/distsql/server.go
func (ds *ServerImpl) setupFlow(
    ctx context.Context, req *execinfrapb.SetupFlowRequest,
) (*execinfrapb.SimpleResponse, error) {
    // Create processors in parallel
    var wg sync.WaitGroup
    for _, proc := range req.Processors {
        wg.Add(1)
        go func(p execinfrapb.ProcessorSpec) {
            defer wg.Done()
            ds.createProcessor(ctx, &p)
        }(proc)
    }
    wg.Wait()
}
```

### 2.8 Security Architecture

Security is integrated throughout the layers:

#### 2.8.1 Authentication and Authorization

The SQL layer implements comprehensive security controls:

```go
// From pkg/sql/authorization.go
type AuthorizationAccessor interface {
    // CheckPrivilege verifies user has required privilege
    CheckPrivilege(
        ctx context.Context,
        descriptor catalog.Descriptor,
        privilege privilege.Kind,
    ) error
    
    // RequireAdminRole verifies user has admin role
    RequireAdminRole(ctx context.Context, action string) error
}
```

#### 2.8.2 Encryption

Data is encrypted at rest and in transit:

```go
// From pkg/storage/engine_key.go
type EngineKey struct {
    Key     roachpb.Key
    Version storage.MVCCKeyVersion
}

// Encryption is handled transparently by the storage layer
func (k EngineKey) Encode() []byte {
    // Returns encrypted byte representation
}
```

This architectural deep dive demonstrates how CockroachDB achieves its design goals through careful layering, clear interfaces, and thoughtful trade-offs. Each layer is designed to be independently testable and maintainable while working together to provide a cohesive distributed SQL database system.