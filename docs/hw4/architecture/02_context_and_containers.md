# Context, containers, and runtime shape

## 1. System context

```mermaid
flowchart LR
    USER["Python user / strategy"]
    DATA[("Databento JSONL data")]
    API["backtest.run + Strategy API"]
    NATIVE["Native C++ backtest engine"]
    RESULT["Result: PnL, fills, order log"]

    USER --> API --> NATIVE
    DATA --> NATIVE
    NATIVE --> RESULT --> USER
```

## 2. Process and thread model

```mermaid
flowchart TB
    subgraph PY["Python layer"]
        RUN["backtest.run"]
        STRAT["Python Strategy subclass"]
        PANDAS["Result pandas views"]
    end

    subgraph CPP["Single C++ process"]
        subgraph BT["Backtest Engine thread"]
            READER["JSONL reader / source iterators"]
            MERGER["Chronological event merger"]
            DISPATCH["Dispatcher + virtual scheduler"]
            STORE["HistoricalLOBStore"]
            READER --> MERGER --> DISPATCH
            DISPATCH --> STORE
        end

        subgraph TT["Trading Engine thread"]
            CONSUMER["MarketDataConsumer"]
            CLOCK["VirtualClock"]
            SIM["SimulatedLOB / EngineView"]
            OM["OrderManager"]
            POS["PositionKeeper"]
            ADAPTER["StrategyAdapter"]
            REC["ResultRecorder"]

            CONSUMER --> CLOCK
            CONSUMER --> SIM
            SIM --> OM --> POS
            OM --> REC
            POS --> REC
            OM --> ADAPTER
        end

        EVTQ[["SPSC engine-event ring"]]
        CMDQ[["SPSC order-command ring"]]
        READY[("atomic processed_seq")]

        DISPATCH --> EVTQ --> CONSUMER
        OM --> CMDQ --> DISPATCH
        CONSUMER --> READY
        READY -. "barrier" .-> DISPATCH
    end

    RUN --> DISPATCH
    ADAPTER <--> STRAT
    REC --> PANDAS
```

## 3. Why two threads

The assignment explicitly distinguishes a backtest engine thread and trading engine thread and requires an atomic ready sequence. The two-thread design demonstrates the intended synchronization while staying small enough for a course project.

- The Backtest Engine owns chronological scheduling and writes the shared historical book.
- The Trading Engine consumes one published event at a time, matches private orders, updates state, and invokes Python.
- The strict barrier means the shared book is stable while the Trading Engine reacts to the current event.

## 4. Strategy location

The strategy implementation is Python-side. C++ defines a Strategy interface/trampoline and calls the Python override from the Trading Engine thread. The “Strategy Logic” box in the source diagram is therefore the runtime callback location, not a requirement to implement strategies in C++.

## 5. Market-event sequence

```mermaid
sequenceDiagram
    participant D as Dispatcher
    participant H as HistoricalLOBStore
    participant Q as Engine event ring
    participant T as Trading Engine
    participant S as SimulatedLOB
    participant O as OrderManager / PositionKeeper
    participant P as Python Strategy

    D->>H: apply atomic historical update/group
    D->>Q: publish event(dispatch_seq, scheduled_ts)
    Q->>T: consume event
    T->>T: virtual_clock = scheduled_ts
    T->>S: re-evaluate resting own orders
    S-->>O: zero or more fills
    O->>O: update order state, position, result buffers
    O->>P: on_fill() for each fill
    T->>P: on_book_update() and/or on_trade()
    P->>O: optional submit/cancel
    O-->>D: enqueue command(arrival_ts)
    T->>D: processed_seq = dispatch_seq
    D->>D: continue with next scheduled event
```

## 6. Order-arrival sequence

```mermaid
sequenceDiagram
    participant P as Python Strategy
    participant O as OrderManager
    participant C as Command ring
    participant D as Dispatcher
    participant T as Trading Engine
    participant S as SimulatedLOB
    participant R as ResultRecorder

    P->>O: submit_limit(instrument, side, price, qty)
    O->>O: allocate ClOrdId and set state = PendingNew
    O->>C: NewOrderCommand(arrival_ts)
    C->>D: drain into chronological scheduler
    D->>T: publish NewOrderArrival
    T->>S: validate and match against stable historical book

    alt rejected
        S-->>O: Reject
        O->>O: PendingNew -> Rejected
        O->>R: append transition
        O->>P: on_reject()
    else full fill
        S-->>O: Fill(s)
        O->>O: PendingNew -> Filled and update position
        O->>R: append fills and transitions
        O->>P: on_fill()
    else partial or no immediate fill
        S-->>O: Fill(s) and/or resting remainder
        O->>O: PendingNew -> PartiallyFilled or Open
        O->>R: append fills and transitions
        O->>P: on_fill() for produced fills
    end

    T->>D: processed_seq
```

## 7. Runtime ownership summary

| State | Sole writer | Readers |
|---|---|---|
| Historical market book | Backtest Engine thread | Trading Engine while dispatcher waits |
| Scheduler heap / command ordering | Backtest Engine thread | none |
| EngineView private overlay | Trading Engine thread | strategy queries on same thread |
| OrderManager / PositionKeeper | Trading Engine thread | strategy callbacks on same thread |
| Result buffers | Trading Engine thread during run | Python after threads join |
| Python strategy object | Python caller owns lifetime; Trading Engine invokes | Python caller after run |

Avoiding multi-writer state is more valuable here than introducing general-purpose locking.
