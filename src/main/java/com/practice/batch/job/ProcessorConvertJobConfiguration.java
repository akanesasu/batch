package com.practice.batch.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class ProcessorConvertJobConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    private static final int chunkSize = 10;

    @Bean
    public Job processConvertJob() throws Exception {
        return jobBuilderFactory.get("ProcessorConvertBatch")
                .preventRestart()
                .start(processConvertStep())
                .build();
    }

    @Bean
    @JobScope
    public Step processConvertStep() throws Exception {
        return stepBuilderFactory.get("ProcessorConvertBatchStep")
                .<Teacher, String>chunk(chunkSize)
                .reader(processConvertReader())
                .processor(processConvertProcessor())
                .writer(processConvertWriter())
                .build();
    }

    private ItemWriter<? super String> processConvertWriter() {
        return items -> {
            for (String item : items) {
                log.info("Teacher Name = {}", item);
            }
        };
    }

    @Bean
    public ItemProcessor<Teacher, String> processConvertProcessor() {
        return Teacher::getName;
    }

    @Bean
    public JdbcPagingItemReader<Teacher> processConvertReader() throws Exception {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("id", 5);

        return new JdbcPagingItemReaderBuilder<Teacher>()
                .pageSize(chunkSize)
                .fetchSize(chunkSize)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(Teacher.class))
                .queryProvider(processorConvertQueryProvider())
                .parameterValues(parameterValues)
                .name("processConvertReader")
                .build();
    }

    @Bean
    public PagingQueryProvider processorConvertQueryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);
        queryProvider.setSelectClause("id, name");
        queryProvider.setFromClause("from teacher");
        queryProvider.setWhereClause("where id >= :id");

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);
        return queryProvider.getObject();
    }
}
