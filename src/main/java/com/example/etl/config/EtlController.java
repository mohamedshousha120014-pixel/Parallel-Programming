package com.example.etl.config;

import com.example.etl.model.Employee;
import com.example.etl.model.EmployeeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * REST Controller to trigger and monitor the ETL job via HTTP endpoints.
 *
 * Endpoints:
 *   POST /api/etl/run          → Trigger the ETL job
 *   GET  /api/etl/results      → View all processed employees
 *   GET  /api/etl/stats        → View department stats & thread usage
 *   GET  /api/etl/threads      → View which threads processed data
 */
@RestController
@RequestMapping("/api/etl")
public class EtlController {

    private static final Logger log = LoggerFactory.getLogger(EtlController.class);

    private final JobLauncher jobLauncher;
    private final Job employeeEtlJob;
    private final EmployeeRepository employeeRepository;

    public EtlController(JobLauncher jobLauncher, Job employeeEtlJob, EmployeeRepository employeeRepository) {
        this.jobLauncher = jobLauncher;
        this.employeeEtlJob = employeeEtlJob;
        this.employeeRepository = employeeRepository;
    }

    /**
     * POST /api/etl/run
     * Triggers the ETL job asynchronously
     */
    @PostMapping("/run")
    public ResponseEntity<Map<String, Object>> runEtlJob() {
        Map<String, Object> response = new HashMap<>();
        try {
            // Unique parameter ensures each run is treated as a new job instance
            JobParameters params = new JobParametersBuilder()
                    .addLong("run.id", System.currentTimeMillis())
                    .toJobParameters();

            log.info("Launching ETL Job via REST API...");
            JobExecution execution = jobLauncher.run(employeeEtlJob, params);

            response.put("status", execution.getStatus().toString());
            response.put("jobId", execution.getJobId());
            response.put("startTime", execution.getStartTime());
            response.put("message", "ETL Job triggered successfully");
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Failed to launch ETL job", e);
            response.put("status", "FAILED");
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * GET /api/etl/results
     * Returns all processed employees from the database
     */
    @GetMapping("/results")
    public ResponseEntity<Map<String, Object>> getResults() {
        List<Employee> employees = employeeRepository.findAll();
        Map<String, Object> response = new HashMap<>();
        response.put("totalRecords", employees.size());
        response.put("employees", employees);
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/etl/stats
     * Returns department statistics (proves ETL transformations worked)
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        List<Object[]> deptStats = employeeRepository.getDepartmentStats();
        List<String> threads = employeeRepository.getProcessingThreads();

        List<Map<String, Object>> departments = deptStats.stream().map(row -> {
            Map<String, Object> dept = new HashMap<>();
            dept.put("department", row[0]);
            dept.put("employeeCount", row[1]);
            Double avg = (row[2] != null) ? (Double) row[2] : 0.0;
            dept.put("avgSalary", String.format("$%.0f", avg));
            return dept;
        }).collect(Collectors.toList());

        Map<String, Object> response = new HashMap<>();
        response.put("totalEmployees", employeeRepository.count());
        response.put("departmentBreakdown", departments);
        response.put("threadsUsed", threads);
        response.put("threadCount", threads.size());
        response.put("message", threads.size() > 1
                ? "✅ Multi-threading Active: " + threads.size() + " threads worked in parallel."
                : "ℹ️ Single thread used. Add a delay in Processor to see parallelism.");

        return ResponseEntity.ok(response);
    }

    /**
     * DELETE /api/etl/reset
     * Clears the database to allow for a clean re-run of the ETL job
     */
    @DeleteMapping("/reset")
    public ResponseEntity<Void> resetData() {
        employeeRepository.deleteAll();
        return ResponseEntity.noContent().build();
    }

    /**
     * GET /api/etl/threads
     * Shows which thread processed each employee (proves parallelism)
     */
    @GetMapping("/threads")
    public ResponseEntity<Map<String, Object>> getThreadBreakdown() {
        List<Employee> employees = employeeRepository.findAll();

        Map<String, List<String>> threadToEmployees = employees.stream()
                .collect(Collectors.groupingBy(
                        Employee::getProcessedByThread,
                        Collectors.mapping(Employee::getFullName, Collectors.toList())
                ));

        Map<String, Object> response = new HashMap<>();
        response.put("threadBreakdown", threadToEmployees);
        response.put("totalThreadsUsed", threadToEmployees.size());
        return ResponseEntity.ok(response);
    }
}
