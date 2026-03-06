# Parallel-Programming
Employee ETL Pipeline using Spring Batch &amp; Multi-Threading


# Employee ETL Pipeline using Spring Batch & Multi-Threading
# نظام معالجة بيانات الموظفين باستخدام Spring Batch و Multi-Threading

---

## Project Idea

System that reads employee data from a CSV file,
processes it, and saves it to a database using
parallel processing to handle large amounts of data fast.

## How it works 


- Reads a CSV file with thousands of employee records
- Splits the work across multiple threads simultaneously
- Each thread processes a chunk of records at the same time
- Saves the processed data to MySQL Database

## Concepts Applied

1. Multi-Threading using ThreadPoolTaskExecutor
2. Batch Processing using Spring Batch
3. Parallel Processing (multiple chunks at the same time)
4. ETL Pipeline: Extract → Transform → Load
5. Design Patterns: Builder, Strategy, Observer, Repository

## Project Steps



### Phase 1: Setup
- Create Spring Boot project
- Add Spring Batch + JPA + MySQL dependencies

### Phase 2: Extract (Reader)
- Read employee data from CSV file
- Make reader thread-safe using SynchronizedItemStreamReader

### Phase 3: Transform (Processor)
- Validate each record
- Compute full name, salary grade, years of service
- Tag each record with the thread name that processed it

### Phase 4: Load (Writer)
- Save processed records to MySQL Database
- Use batch insert for efficiency

### Phase 5: Multi-Threading
- Configure ThreadPoolTaskExecutor (4 to 10 threads)
- Assign thread pool to the Step
- Prove parallelism: each record shows which thread processed it

### Phase 6: Testing
- REST API to trigger the job
- Verify multiple threads worked simultaneously


## Tech Stack 

| Technology | Version |
|---|---|
| Java | 17 |
| Spring Boot | 3.2 |
| Spring Batch | 5.x |
| Spring Data JPA | 3.2 |
| MySQL | 8.x |
| Maven | 3.8+ |
