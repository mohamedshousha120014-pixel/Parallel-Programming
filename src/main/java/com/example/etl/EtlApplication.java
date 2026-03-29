package com.example.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║          SPRING BATCH ETL — Main Application                 ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║                                                              ║
 * ║  This project demonstrates:                                  ║
 * ║                                                              ║
 * ║  1️⃣  BATCH PROCESSING with Spring Batch                      ║
 * ║      - Job → Step → Reader / Processor / Writer              ║
 * ║      - Chunk-oriented processing (configurable chunk size)   ║
 * ║      - Fault tolerance (skip bad records)                    ║
 * ║      - Job metadata stored in DB (restartable jobs)          ║
 * ║                                                              ║
 * ║  2️⃣  MULTI-THREADING / PARALLELISM                           ║
 * ║      - ThreadPoolTaskExecutor (core=4, max=10)               ║
 * ║      - Multiple chunks processed simultaneously              ║
 * ║      - Thread-safe reader (SynchronizedItemStreamReader)     ║
 * ║      - Stateless Processor (safe across threads)             ║
 * ║      - Each record tagged with the thread that processed it  ║
 * ║                                                              ║
 * ║  3️⃣  ETL PIPELINE (Extract → Transform → Load)               ║
 * ║      - Extract : Read employees from CSV file                ║
 * ║      - Transform: Validate + enrich + compute derived fields ║
 * ║      - Load    : Batch insert to H2/relational database      ║
 * ║                                                              ║
 * ║  4️⃣  DESIGN PATTERNS                                          ║
 * ║      - Builder Pattern  (JobBuilder, StepBuilder)            ║
 * ║      - Factory Pattern  (@Bean configuration methods)        ║
 * ║      - Strategy Pattern (pluggable Reader/Processor/Writer)  ║
 * ║      - Observer Pattern (Job & Chunk Listeners)              ║
 * ║      - Repository Pattern (EmployeeRepository)               ║
 * ║      - DTO Pattern (EmployeeCSV → Employee)                  ║
 * ║      - Template Method (FlatFileItemReader)                  ║
 * ║                                                              ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║  HOW TO RUN:                                                 ║
 * ║    mvn spring-boot:run                                       ║
 * ║    Then: POST http://localhost:8080/api/etl/run              ║
 * ║    Then: GET  http://localhost:8080/api/etl/stats            ║
 * ║    Then: GET  http://localhost:8080/api/etl/threads          ║
 * ║    DB:   http://localhost:8080/h2-console                    ║
 * ╚══════════════════════════════════════════════════════════════╝
 */
@SpringBootApplication
public class EtlApplication {

    private static final Logger log = LoggerFactory.getLogger(EtlApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(EtlApplication.class, args);
        log.info("╔══════════════════════════════════════════╗");
        log.info("║  🚀 ETL Application is ready!            ║");
        log.info("║  POST /api/etl/run    → Start ETL job    ║");
        log.info("║  GET  /api/etl/stats  → View results     ║");
        log.info("║  GET  /api/etl/threads→ Thread breakdown ║");
        log.info("║  GET  /h2-console     → View database    ║");
        log.info("╚══════════════════════════════════════════╝");
    }
}
