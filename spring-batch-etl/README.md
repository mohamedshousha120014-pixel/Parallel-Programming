# 🚀 Spring Batch ETL — Multi-Threading & Parallelism Demo

A complete **ETL (Extract → Transform → Load)** pipeline built with **Spring Batch** and **Multi-Threading**, demonstrating parallelism concepts for large-scale data processing.

---

## 📋 What This Project Covers

| Concept | Implementation |
|---|---|
| ✅ **Batch Processing** | Spring Batch with Job → Step → Reader/Processor/Writer |
| ✅ **Multi-Threading** | `ThreadPoolTaskExecutor` (core=4, max=10 threads) |
| ✅ **Parallelism** | Multiple chunks processed simultaneously |
| ✅ **Database Integration** | JPA + H2 (easy to switch to MySQL/PostgreSQL) |
| ✅ **ETL Pipeline** | CSV → Transform → Database |
| ✅ **Design Patterns** | Builder, Factory, Strategy, Observer, Repository, DTO, Template Method |
| ✅ **Fault Tolerance** | Skip bad records, retry on failure |
| ✅ **REST API** | Trigger and monitor jobs via HTTP |

---

## 🏗️ Project Structure

```
spring-batch-etl/
├── src/main/java/com/example/etl/
│   ├── EtlApplication.java          ← Main entry point
│   ├── model/
│   │   ├── EmployeeCSV.java         ← DTO: raw CSV data (INPUT)
│   │   ├── Employee.java            ← Entity: processed data (OUTPUT)
│   │   └── EmployeeRepository.java  ← Repository pattern (DB access)
│   ├── reader/
│   │   └── EmployeeCSVReader.java   ← EXTRACT: reads CSV file
│   ├── processor/
│   │   └── EmployeeProcessor.java   ← TRANSFORM: validates & enriches
│   ├── writer/
│   │   └── EmployeeDBWriter.java    ← LOAD: saves to database
│   ├── listener/
│   │   ├── JobCompletionListener.java   ← Job lifecycle events
│   │   └── ChunkProcessingListener.java ← Chunk lifecycle events
│   └── config/
│       ├── BatchConfig.java         ← Wires everything together
│       └── EtlController.java       ← REST API endpoints
├── src/main/resources/
│   ├── application.properties       ← Configuration
│   └── data/employees.csv           ← Sample input data
└── pom.xml
```

---

## 🔄 ETL Pipeline Flow

```
┌─────────────────────────────────────────────────────────┐
│                    ETL PIPELINE                         │
│                                                         │
│  📄 CSV File                                            │
│       │                                                 │
│       ▼                                                 │
│  ┌─────────┐    SynchronizedItemStreamReader            │
│  │ READER  │ ←  (thread-safe file reading)              │
│  └────┬────┘                                            │
│       │  chunk of N records                             │
│       ▼                                                 │
│  ┌─────────────────────────────────────┐                │
│  │         THREAD POOL                 │                │
│  │  Thread-1: chunk [1-10]             │  ← PARALLEL    │
│  │  Thread-2: chunk [11-20]            │  ← PARALLEL    │
│  │  Thread-3: chunk [21-30]            │  ← PARALLEL    │
│  └───────────────┬─────────────────────┘                │
│                  │                                       │
│       ▼                                                 │
│  ┌───────────┐                                          │
│  │ PROCESSOR │ ← validate, compute, enrich              │
│  └─────┬─────┘                                          │
│        │                                                │
│        ▼                                                │
│  ┌────────┐                                             │
│  │ WRITER │ ← batch insert to database                  │
│  └────────┘                                             │
└─────────────────────────────────────────────────────────┘
```

---

## 🧵 Multi-Threading Details

### ThreadPoolTaskExecutor Configuration
```properties
etl.thread-pool.core-size=4       # Always-alive threads
etl.thread-pool.max-size=10       # Max under heavy load
etl.thread-pool.queue-capacity=50 # Buffer for waiting tasks
```

### How Parallelism Works
1. Spring Batch reads a **chunk** of records (e.g., 10 records)
2. Submits the chunk to the **thread pool** as a task
3. While Thread-1 processes chunk-1, Thread-2 processes chunk-2 simultaneously
4. Each employee record is tagged with the **thread name** that processed it
5. You can see the proof via `GET /api/etl/threads`

### Thread Safety Strategy
| Component | Thread Safety | How |
|---|---|---|
| Reader | ✅ Safe | `SynchronizedItemStreamReader` wrapper |
| Processor | ✅ Safe | Stateless (no shared mutable state) |
| Writer | ✅ Safe | Spring Data JPA handles transactions |

---

## 🔄 ETL Transformations (Processor)

| Input (CSV) | Transformation | Output (DB) |
|---|---|---|
| `firstName` + `lastName` | Concatenate | `fullName` |
| `salary` (String) | Parse + classify | `salary` (BigDecimal) + `salaryGrade` |
| `joinDate` (String) | Parse + compute | `joinDate` (LocalDate) + `yearsOfService` |
| — | System time | `processedAt` |
| — | Thread name | `processedByThread` |

### Salary Grade Rules
```
< $70,000   → Junior
$70-84,999  → Mid  
$85-99,999  → Senior
≥ $100,000  → Lead
```

---

## 🎨 Design Patterns Used

| Pattern | Where | Why |
|---|---|---|
| **Builder** | `JobBuilder`, `StepBuilder` | Construct complex objects step-by-step |
| **Factory** | `@Bean` methods in `BatchConfig` | Centralize object creation |
| **Strategy** | `ItemReader`, `ItemProcessor`, `ItemWriter` | Swap implementations without changing the Step |
| **Observer** | `JobCompletionListener`, `ChunkProcessingListener` | React to lifecycle events without coupling |
| **Repository** | `EmployeeRepository` | Abstract database access |
| **DTO** | `EmployeeCSV` → `Employee` | Separate input model from output model |
| **Template Method** | `FlatFileItemReader` | Framework provides skeleton, we fill in specifics |

---

## 🚀 How to Run

### Prerequisites
- Java 17+
- Maven 3.8+

### Steps
```bash
# 1. Clone / navigate to project
cd spring-batch-etl

# 2. Build
mvn clean install

# 3. Run
mvn spring-boot:run
```

### Trigger the ETL Job
```bash
# Start the ETL job
curl -X POST http://localhost:8080/api/etl/run

# View results
curl http://localhost:8080/api/etl/results

# View department stats
curl http://localhost:8080/api/etl/stats

# View thread breakdown (PROVES MULTI-THREADING)
curl http://localhost:8080/api/etl/threads
```

### View Database
Open: http://localhost:8080/h2-console
- JDBC URL: `jdbc:h2:mem:etldb`
- Username: `sa`
- Password: (empty)

---

## 🔧 Configuration

All tuneable parameters are in `application.properties`:

```properties
# Thread pool
etl.thread-pool.core-size=4
etl.thread-pool.max-size=10
etl.thread-pool.queue-capacity=50

# Batch
etl.chunk-size=10

# Input file
etl.input-file=classpath:data/employees.csv
```

---

## 🧪 Running Tests

```bash
mvn test
```

Tests verify:
- ✅ Job completes with `COMPLETED` status
- ✅ All records saved to DB
- ✅ Transformations applied correctly (fullName, salaryGrade, yearsOfService)
- ✅ Multiple threads used (parallelism confirmed)

---

## 🔌 Switching to MySQL/PostgreSQL

Change `application.properties`:
```properties
# Remove H2, add MySQL
spring.datasource.url=jdbc:mysql://localhost:3306/etldb
spring.datasource.username=root
spring.datasource.password=yourpassword
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
spring.jpa.database-platform=org.hibernate.dialect.MySQL8Dialect
```

And add MySQL dependency to `pom.xml`:
```xml
<dependency>
    <groupId>com.mysql</groupId>
    <artifactId>mysql-connector-j</artifactId>
</dependency>
```

---

## 👥 Team Discussion Points

1. **Why SynchronizedItemStreamReader?** → File reading is sequential but processing is parallel
2. **Why chunk-based processing?** → Memory efficiency + transactional safety
3. **Why ThreadPoolTaskExecutor?** → Better than `SimpleAsyncTaskExecutor` (reuses threads, bounded)
4. **How to prove parallelism?** → `/api/etl/threads` shows multiple thread names
5. **What if a record fails?** → `faultTolerant().skipLimit(5)` skips bad records
