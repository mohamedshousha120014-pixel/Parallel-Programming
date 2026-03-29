package com.example.etl.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * RAW Employee data read from CSV.
 * This is the INPUT model in our ETL pipeline.
 *
 * Design Pattern: DTO (Data Transfer Object)
 * - Used to carry raw data from the source (CSV file)
 * - No business logic, just holds data
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EmployeeCSV {

    private String id;
    private String firstName;
    private String lastName;
    private String email;
    private String department;
    private String salary;      // Raw String — will be parsed/validated in Processor
    private String joinDate;    // Raw String — will be parsed in Processor
}
