package com.example.etl.config;

import com.example.etl.listener.ChunkProcessingListener;
import com.example.etl.listener.JobCompletionListener;
import com.example.etl.model.Employee;
import com.example.etl.model.EmployeeCSV;
import com.example.etl.processor.EmployeeProcessor;
import com.example.etl.reader.EmployeeCSVReader;
import com.example.etl.writer.EmployeeDBWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * BATCH CONFIGURATION — Wires the entire ETL pipeline together
 *
 * This class defines:
 *   1. ThreadPoolTaskExecutor  → Multi-threading engine
 *   2. Reader                  → CSV file reader (thread-safe wrapped)
 *   3. Processor               → Business logic & transformations
 *   4. Writer                  → Database writer
 *   5. Step                    → Combines Reader + Processor + Writer
 *   6. Job                     → Contains the Step(s)
 *
 * ╔══════════════════════════════════════════════════╗
 * ║            SPRING BATCH FLOW                     ║
 * ║                                                  ║
 * ║  CSV File                                        ║
 * ║     │                                            ║
 * ║     ▼                                            ║
 * ║  [Reader] ──── reads chunk of N records ──────▶  ║
 * ║     │           Thread 1: records 1-10           ║
 * ║     │           Thread 2: records 11-20   (PARALLEL) ║
 * ║     │           Thread 3: records 21-30          ║
 * ║     ▼                                            ║
 * ║  [Processor] ── transforms each record ────────▶ ║
 * ║     │           validate, enrich, compute        ║
 * ║     ▼                                            ║
 * ║  [Writer] ──── batch insert to DB ─────────────▶ ║
 * ║                                                  ║
 * ╚══════════════════════════════════════════════════╝
 *
 * Design Patterns used:
 *   - Builder Pattern    : JobBuilder, StepBuilder
 *   - Factory Pattern    : @Bean methods
 *   - Strategy Pattern   : Pluggable Reader/Processor/Writer
 *   - Observer Pattern   : Listeners (Job, Chunk)
 *   - Template Method    : FlatFileItemReader skeleton
 */
@Configuration
public class BatchConfig {

    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);

    // ---- Properties from application.properties ----
    @Value("${etl.thread-pool.core-size}")
    private int corePoolSize;

    @Value("${etl.thread-pool.max-size}")
    private int maxPoolSize;

    @Value("${etl.thread-pool.queue-capacity}")
    private int queueCapacity;

    @Value("${etl.thread-pool.thread-name-prefix}")
    private String threadNamePrefix;

    @Value("${etl.chunk-size}")
    private int chunkSize;

    @Value("${etl.input-file}")
    private Resource inputFile;

    // ══════════════════════════════════════════════
    //  1. MULTI-THREADING ENGINE
    // ══════════════════════════════════════════════

    /**
     * ThreadPoolTaskExecutor — manages a pool of worker threads.
     *
     * Why ThreadPoolTaskExecutor over SimpleAsyncTaskExecutor?
     * - Reuses threads (no overhead of creating new thread per task)
     * - Bounded: won't spawn unlimited threads
     * - Production-grade: proper queue and rejection handling
     *
     * How it works:
     *   1. Spring Batch submits each chunk to the executor
     *   2. Executor assigns chunks to available threads
     *   3. Multiple chunks processed simultaneously = PARALLELISM
     */
    @Bean
    public TaskExecutor etlTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        executor.setCorePoolSize(corePoolSize);      // Always-alive threads
        executor.setMaxPoolSize(maxPoolSize);         // Max threads under heavy load
        executor.setQueueCapacity(queueCapacity);     // Buffer for waiting tasks
        executor.setThreadNamePrefix(threadNamePrefix); // Name shown in logs
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();

        log.info("ThreadPool initialized: core={}, max={}, queue={}",
                corePoolSize, maxPoolSize, queueCapacity);

        return executor;
    }

    // ══════════════════════════════════════════════
    //  2. READER (Thread-Safe)
    // ══════════════════════════════════════════════

    /**
     * SynchronizedItemStreamReader wraps EmployeeCSVReader to make it thread-safe.
     *
     * Problem: FlatFileItemReader is NOT thread-safe (multiple threads reading
     *          the same file at the same time = race condition)
     *
     * Solution: SynchronizedItemStreamReader adds synchronized keyword to read()
     *           so only ONE thread reads at a time, but processing still happens
     *           in parallel after reading.
     */
    @Bean
    public SynchronizedItemStreamReader<EmployeeCSV> employeeReader() {
        EmployeeCSVReader csvReader = new EmployeeCSVReader(inputFile);
        csvReader.setSaveState(false);

        return new SynchronizedItemStreamReaderBuilder<EmployeeCSV>()
                .delegate(csvReader)
                .build();
    }

    // ══════════════════════════════════════════════
    //  3. PROCESSOR
    // ══════════════════════════════════════════════

    @Bean
    public EmployeeProcessor employeeProcessor() {
        return new EmployeeProcessor();
        // Stateless → safe to share across threads ✅
    }

    // ══════════════════════════════════════════════
    //  4. WRITER
    // ══════════════════════════════════════════════

    @Bean
    public EmployeeDBWriter employeeWriter() {
        return new EmployeeDBWriter();
    }

    // ══════════════════════════════════════════════
    //  5. LISTENERS
    // ══════════════════════════════════════════════

    @Bean
    public JobCompletionListener jobCompletionListener() {
        return new JobCompletionListener();
    }

    @Bean
    public ChunkProcessingListener chunkProcessingListener() {
        return new ChunkProcessingListener();
    }

    // ══════════════════════════════════════════════
    //  6. STEP — Combines everything
    // ══════════════════════════════════════════════

    /**
     * The Step ties Reader → Processor → Writer together.
     *
     * chunk(chunkSize): Process N records per transaction.
     *   - Read 10 → Process 10 → Write 10 → Commit → Repeat
     *   - If write fails, only that chunk is rolled back (not the whole job)
     *
     * taskExecutor(etlTaskExecutor()): Enables multi-threading.
     *   - Spring Batch will process multiple chunks simultaneously
     *
     * faultTolerant(): Enables skip/retry behavior.
     *   - skipLimit(5): Skip up to 5 bad records before failing the job
     */
    @Bean
    public Step etlStep(JobRepository jobRepository,
                        PlatformTransactionManager transactionManager,
                        TaskExecutor etlTaskExecutor,
                        SynchronizedItemStreamReader<EmployeeCSV> employeeReader,
                        EmployeeProcessor employeeProcessor,
                        EmployeeDBWriter employeeWriter) {
        return new StepBuilder("etl-step", jobRepository)
                .<EmployeeCSV, Employee>chunk(chunkSize, transactionManager)
                .reader(employeeReader)
                .processor(employeeProcessor)
                .writer(employeeWriter)
                // ← MULTI-THREADING: assign the thread pool to this step
                .taskExecutor(etlTaskExecutor)
                .throttleLimit(maxPoolSize) // Allow maximum parallel chunks
                .listener(chunkProcessingListener())
                // Fault tolerance: skip bad records instead of failing
                .faultTolerant()
                .skipLimit(0) // قلل الرقم لـ 0 مؤقتاً لاكتشاف أي خطأ خفي في البيانات
                .skip(Exception.class)
                .build();
    }

    // ══════════════════════════════════════════════
    //  7. JOB
    // ══════════════════════════════════════════════

    @Bean
    public Job employeeEtlJob(JobRepository jobRepository,
                              Step etlStep) {
        return new JobBuilder("employee-etl-job", jobRepository)
                .listener(jobCompletionListener())
                .start(etlStep)
                .build();
    }
}
