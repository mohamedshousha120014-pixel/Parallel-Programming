package com.example.etl.listener;

import com.example.etl.model.EmployeeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * JOB LISTENER — Runs before and after the entire Job
 *
 * - beforeJob: Log that job is starting
 * - afterJob:  Print a full report of results
 *
 * Design Pattern: Observer Pattern
 * - Spring Batch notifies this listener about job lifecycle events
 * - Listener reacts without being tightly coupled to the Job itself
 */
public class JobCompletionListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionListener.class);

    @Autowired
    private EmployeeRepository employeeRepository;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("╔══════════════════════════════════════════════╗");
        log.info("║     🚀 ETL JOB STARTED                       ║");
        log.info("╚══════════════════════════════════════════════╝");
        log.info("Job Name    : {}", jobExecution.getJobInstance().getJobName());
        log.info("Job ID      : {}", jobExecution.getJobId());
        log.info("Start Time  : {}", jobExecution.getStartTime());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("╔══════════════════════════════════════════════╗");
            log.info("║     ✅ ETL JOB COMPLETED SUCCESSFULLY        ║");
            log.info("╚══════════════════════════════════════════════╝");
            printReport(jobExecution);
        } else {
            log.error("╔══════════════════════════════════════════════╗");
            log.error("║     ❌ ETL JOB FAILED: {}  ║", jobExecution.getStatus());
            log.error("╚══════════════════════════════════════════════╝");
            jobExecution.getAllFailureExceptions()
                    .forEach(ex -> log.error("Failure: ", ex));
        }
    }

    private void printReport(JobExecution jobExecution) {
        long totalSaved = employeeRepository.count();

        // Department breakdown
        List<Object[]> deptStats = employeeRepository.getDepartmentStats();

        // Which threads did the processing (proves multi-threading worked)
        List<String> threads = employeeRepository.getProcessingThreads();

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("📊 ETL REPORT");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("Total Records Saved : {}", totalSaved);
        log.info("");
        log.info("📂 Department Breakdown:");
        deptStats.forEach(row ->
                log.info("   {:<15} → {} employees | Avg Salary: ${,.0f}",
                        row[0], row[1], row[2])
        );
        log.info("");
        log.info("🧵 Threads That Processed the Data (Multi-Threading Proof):");
        threads.forEach(thread -> log.info("   → {}", thread));
        log.info("");
        log.info("⏱️ Duration: {} ms",
                jobExecution.getEndTime() != null && jobExecution.getStartTime() != null
                        ? java.time.Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis()
                        : "N/A");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }
}
