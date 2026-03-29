package com.example.etl.writer;

import com.example.etl.model.Employee;
import com.example.etl.model.EmployeeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * WRITER — Step 3 of ETL Pipeline (the "L" in ETL = Load)
 *
 * Saves a chunk of processed Employee records to the database.
 * Spring Batch calls write() once per chunk (not once per record).
 *
 * Example: chunk-size=10 → write() receives a list of 10 employees
 * This is more efficient than saving one record at a time (batch insert).
 *
 * Design Pattern: Repository Pattern (via EmployeeRepository)
 * - Writer delegates all DB operations to the repository
 * - Writer only knows "save this list", not HOW to save it
 *
 * Thread Safety:
 * - Spring Data JPA's saveAll() is thread-safe ✅
 * - Each thread gets its own transaction ✅
 */
public class EmployeeDBWriter implements ItemWriter<Employee> {

    private static final Logger log = LoggerFactory.getLogger(EmployeeDBWriter.class);

    @Autowired
    private EmployeeRepository employeeRepository;

    @Override
    public void write(Chunk<? extends Employee> chunk) throws Exception {
        List<? extends Employee> employees = chunk.getItems();
        String currentThread = Thread.currentThread().getName();

        // Group by thread to show which thread is writing
        Map<String, Long> threadCounts = employees.stream()
                .collect(Collectors.groupingBy(Employee::getProcessedByThread, Collectors.counting()));

        log.info("[{}] Writing chunk of {} employees to DB | Processed by threads: {}",
                currentThread, employees.size(), threadCounts);

        // Batch insert — saves all records in one DB round-trip (efficient!)
        employeeRepository.saveAll(employees);

        log.info("[{}] ✅ Successfully saved {} employees to database.", currentThread, employees.size());
    }
}
