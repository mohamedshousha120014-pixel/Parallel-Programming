package com.example.etl.reader;

import com.example.etl.model.EmployeeCSV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.Resource;

/**
 * READER — Step 1 of ETL Pipeline
 *
 * Reads employee records from a CSV file line by line.
 * Spring Batch handles file reading in chunks (defined by chunk-size).
 *
 * Design Pattern: Template Method Pattern
 * - FlatFileItemReader provides the skeleton algorithm
 * - We configure the specific parsing logic (CSV columns, delimiter, target class)
 *
 * Thread Safety:
 * - FlatFileItemReader is NOT thread-safe by itself
 * - We use SynchronizedItemStreamReader wrapper in BatchConfig to make it safe
 */
public class EmployeeCSVReader extends FlatFileItemReader<EmployeeCSV> {

    private static final Logger log = LoggerFactory.getLogger(EmployeeCSVReader.class);

    public EmployeeCSVReader(Resource inputResource) {
        log.info("Initializing EmployeeCSVReader for file: {}", inputResource.getFilename());

        // Set the CSV file to read
        setResource(inputResource);

        // Skip the header line
        setLinesToSkip(1);

        // Configure how to parse each line
        setLineMapper(buildLineMapper());

        setName("employeeCSVReader");
    }

    /**
     * Builds the line mapper: CSV line → EmployeeCSV object
     */
    private DefaultLineMapper<EmployeeCSV> buildLineMapper() {
        DefaultLineMapper<EmployeeCSV> lineMapper = new DefaultLineMapper<>();

        // Step 1: Tokenize the line by comma
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setNames("id", "firstName", "lastName", "email", "department", "salary", "joinDate");

        // Step 2: Map tokens to EmployeeCSV fields
        BeanWrapperFieldSetMapper<EmployeeCSV> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(EmployeeCSV.class);

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }
}
