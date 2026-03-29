package com.example.etl.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * PROCESSED Employee entity saved to the Database.
 * This is the OUTPUT model in our ETL pipeline.
 *
 * Design Pattern: Entity (JPA)
 * - Represents a database table row
 * - Contains validated, transformed data
 */
@Entity
@Table(name = "employees")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Employee {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "employee_id", unique = true, nullable = false)
    private Integer employeeId;

    @Column(name = "first_name", nullable = false)
    private String firstName;

    @Column(name = "last_name", nullable = false)
    private String lastName;

    @Column(name = "full_name")
    private String fullName;    // Computed: firstName + " " + lastName (ETL Transform)

    @Column(name = "email", unique = true)
    private String email;

    @Column(name = "department")
    private String department;

    @Column(name = "salary", precision = 10, scale = 2)
    private BigDecimal salary;

    @Column(name = "salary_grade")
    private String salaryGrade;  // Computed: Junior/Mid/Senior based on salary (ETL Transform)

    @Column(name = "join_date")
    private LocalDate joinDate;

    @Column(name = "years_of_service")
    private Integer yearsOfService;  // Computed from joinDate (ETL Transform)

    @Column(name = "processed_at")
    private LocalDateTime processedAt;  // Timestamp when this record was processed

    @Column(name = "processed_by_thread")
    private String processedByThread;  // Which thread processed this record (shows multi-threading)
}
