package com.example.etl.model;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Database access layer for Employee entity.
 *
 * Design Pattern: Repository Pattern
 * - Abstracts all database operations
 * - Spring Data JPA provides implementation automatically
 */
@Repository
public interface EmployeeRepository extends JpaRepository<Employee, Long> {

    List<Employee> findByDepartment(String department);

    @Query("SELECT e.department, COUNT(e), AVG(e.salary) FROM Employee e GROUP BY e.department")
    List<Object[]> getDepartmentStats();

    @Query("SELECT DISTINCT e.processedByThread FROM Employee e ORDER BY e.processedByThread")
    List<String> getProcessingThreads();

    long countByDepartment(String department);
}
