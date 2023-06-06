package com.practice.batch.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.weaver.ast.Or;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
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
public class ProcessorNullJobConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    private static final int chunkSize = 10;
    
    @Bean
    public Job processorNullJob() throws Exception {
        return jobBuilderFactory.get("processorNullJob")
                .start(processorNullStep())
                .build();
    }

    @Bean
    public Step processorNullStep() throws Exception {
        return stepBuilderFactory.get("processorNullStep")
                .<Teacher, Teacher>chunk(chunkSize)
                .reader(processorNullReader())
                .processor(processorNullProcessor())
                .writer(processorNullWriter())
                .build();
    }

    public ItemWriter<Teacher> processorNullWriter() {
        return items -> {
            for (Teacher item : items) {
                log.info("Teacher Name = {}", item.getName());
            }
        };
    }

    @Bean
    public ItemProcessor<Teacher, Teacher> processorNullProcessor() {
        return teacher -> {
            boolean isIgnoreTest = teacher.getId() % 2 == 0L;
            if (isIgnoreTest) {
                log.info(">>>>>>>>> Teacher Name = {}, isIgnoreTest = {}", teacher.getName(), isIgnoreTest);
                return null;
            }

            return teacher;
        };
    }

    @Bean
    public JdbcPagingItemReader<Teacher> processorNullReader() throws Exception {
        return new JdbcPagingItemReaderBuilder<Teacher>()
                .pageSize(chunkSize)
                .fetchSize(chunkSize)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(Teacher.class))
                .queryProvider(processorNullQueryProvider())
                .name("processorNullReader")
                .build();
    }

    public PagingQueryProvider processorNullQueryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);
        queryProvider.setSelectClause("id, name");
        queryProvider.setFromClause("from teacher");

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);
        return queryProvider.getObject();
    }
}
