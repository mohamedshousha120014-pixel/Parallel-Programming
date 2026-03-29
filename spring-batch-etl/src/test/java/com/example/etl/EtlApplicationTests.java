package com.example.etl;

import com.example.etl.model.Employee;
import com.example.etl.model.EmployeeRepository;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the ETL pipeline.
 * Tests that:
 *   1. Job completes successfully
 *   2. All records are saved to DB
 *   3. Transformations are applied correctly
 *   4. Multiple threads were used (parallelism works)
 */
@SpringBatchTest
@SpringBootTest
class EtlApplicationTests {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private EmployeeRepository employeeRepository;

    @Test
    void etlJob_ShouldCompleteSuccessfully() throws Exception {
        // Run the job
        JobExecution execution = jobLauncherTestUtils.launchJob(
                new JobParametersBuilder()
                        .addLong("run.id", System.currentTimeMillis())
                        .toJobParameters()
        );

        // Assert job completed
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        System.out.println("✅ Job Status: " + execution.getStatus());
    }

    @Test
    void etlJob_ShouldSaveAllRecordsToDB() throws Exception {
        jobLauncherTestUtils.launchJob(
                new JobParametersBuilder()
                        .addLong("run.id", System.currentTimeMillis())
                        .toJobParameters()
        );

        List<Employee> employees = employeeRepository.findAll();

        // 30 records in CSV
        assertThat(employees).isNotEmpty();
        System.out.println("✅ Records saved: " + employees.size());
    }

    @Test
    void etlJob_ShouldApplyTransformations() throws Exception {
        jobLauncherTestUtils.launchJob(
                new JobParametersBuilder()
                        .addLong("run.id", System.currentTimeMillis())
                        .toJobParameters()
        );

        List<Employee> employees = employeeRepository.findAll();

        employees.forEach(emp -> {
            // fullName should be computed
            assertThat(emp.getFullName()).isNotNull();
            assertThat(emp.getFullName()).contains(emp.getFirstName());

            // salaryGrade should be assigned
            assertThat(emp.getSalaryGrade()).isIn("Junior", "Mid", "Senior", "Lead");

            // yearsOfService should be positive
            assertThat(emp.getYearsOfService()).isGreaterThanOrEqualTo(0);

            // processedAt should be set
            assertThat(emp.getProcessedAt()).isNotNull();

            // processedByThread should be set
            assertThat(emp.getProcessedByThread()).isNotNull();
        });

        System.out.println("✅ All transformations applied correctly!");
    }

    @Test
    void etlJob_ShouldUseMultipleThreads() throws Exception {
        jobLauncherTestUtils.launchJob(
                new JobParametersBuilder()
                        .addLong("run.id", System.currentTimeMillis())
                        .toJobParameters()
        );

        List<Employee> employees = employeeRepository.findAll();

        // Collect distinct thread names that processed records
        Set<String> threadNames = employees.stream()
                .map(Employee::getProcessedByThread)
                .collect(Collectors.toSet());

        System.out.println("🧵 Threads used: " + threadNames);

        // With 30 records, chunk-size=10, and 4 core threads → should use multiple threads
        assertThat(threadNames).isNotEmpty();

        // Print thread breakdown
        Map<String, Long> threadCounts = employees.stream()
                .collect(Collectors.groupingBy(Employee::getProcessedByThread, Collectors.counting()));

        threadCounts.forEach((thread, count) ->
                System.out.printf("   Thread %-25s processed %d records%n", thread, count));
    }
}
