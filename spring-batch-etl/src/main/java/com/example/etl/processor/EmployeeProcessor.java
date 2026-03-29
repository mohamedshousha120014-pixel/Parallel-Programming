package com.example.etl.processor;

import com.example.etl.model.Employee;
import com.example.etl.model.EmployeeCSV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.lang.NonNull;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;

/**
 * PROCESSOR — Step 2 of ETL Pipeline (the "T" in ETL = Transform)
 *
 * Transforms raw EmployeeCSV data into a clean Employee entity.
 * This is where all business logic, validation, and enrichment happens.
 *
 * Transformations applied:
 *   1. Parse salary String → BigDecimal
 *   2. Parse joinDate String → LocalDate
 *   3. Compute fullName = firstName + " " + lastName
 *   4. Compute salaryGrade based on salary range
 *   5. Compute yearsOfService from joinDate
 *   6. Record which thread processed this record (proves multi-threading)
 *
 * Design Pattern: Strategy Pattern
 * - ItemProcessor is the strategy interface
 * - EmployeeProcessor is the concrete strategy
 * - BatchConfig injects the strategy into the Step
 *
 * Thread Safety:
 * - This class is STATELESS (no instance variables that change)
 * - Safe to use with multiple threads simultaneously ✅
 */
public class EmployeeProcessor implements ItemProcessor<EmployeeCSV, Employee> {

    private static final Logger log = LoggerFactory.getLogger(EmployeeProcessor.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public Employee process(@NonNull EmployeeCSV raw) throws Exception {
        String currentThread = Thread.currentThread().getName();
        log.debug("[{}] Processing employee ID: {} - {} {}",
                currentThread, raw.getId(), raw.getFirstName(), raw.getLastName());

        // --- VALIDATE ---
        if (raw.getId() == null || raw.getFirstName() == null || raw.getEmail() == null) {
            log.warn("[{}] Skipping invalid record: {}", currentThread, raw);
            return null;  // Returning null tells Spring Batch to skip this record
        }

        // --- TRANSFORM ---
        BigDecimal salary = parseSalary(raw.getSalary());
        LocalDate joinDate = parseDate(raw.getJoinDate());

        Employee employee = Employee.builder()
                .employeeId(Integer.parseInt(raw.getId().trim()))
                .firstName(raw.getFirstName().trim())
                .lastName(raw.getLastName().trim())
                // Transform 1: Compute full name
                .fullName(raw.getFirstName().trim() + " " + raw.getLastName().trim())
                .email(raw.getEmail().trim().toLowerCase())
                .department(raw.getDepartment().trim())
                .salary(salary)
                // Transform 2: Compute salary grade
                .salaryGrade(computeSalaryGrade(salary))
                .joinDate(joinDate)
                // Transform 3: Compute years of service
                .yearsOfService(computeYearsOfService(joinDate))
                // Metadata: timestamp and thread name (demonstrates multi-threading)
                .processedAt(LocalDateTime.now())
                .processedByThread(currentThread)
                .build();

        log.debug("[{}] Transformed → {} | Grade: {} | Years: {}",
                currentThread, employee.getFullName(),
                employee.getSalaryGrade(), employee.getYearsOfService());

        return employee;
    }

    // ---- Private helper methods ----

    private BigDecimal parseSalary(String salaryStr) {
        try {
            return new BigDecimal(salaryStr.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid salary value: '{}', defaulting to 0", salaryStr);
            return BigDecimal.ZERO;
        }
    }

    private LocalDate parseDate(String dateStr) {
        try {
            return LocalDate.parse(dateStr.trim(), DATE_FORMATTER);
        } catch (Exception e) {
            log.warn("Invalid date value: '{}', defaulting to today", dateStr);
            return LocalDate.now();
        }
    }

    /**
     * Business Rule: Classify employees by salary
     * Junior  < $70,000
     * Mid     $70,000 – $84,999
     * Senior  $85,000 – $99,999
     * Lead    >= $100,000
     */
    private String computeSalaryGrade(BigDecimal salary) {
        double amount = salary.doubleValue();
        if (amount < 70_000)  return "Junior";
        if (amount < 85_000)  return "Mid";
        if (amount < 100_000) return "Senior";
        return "Lead";
    }

    /**
     * Compute how many full years since joinDate
     */
    private int computeYearsOfService(LocalDate joinDate) {
        return Period.between(joinDate, LocalDate.now()).getYears();
    }
}
