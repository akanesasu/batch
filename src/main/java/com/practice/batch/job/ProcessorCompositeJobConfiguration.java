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
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class ProcessorCompositeJobConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    private static final int chunkSize = 10;

    @Bean
    public Job processorCompositeJob() throws Exception {
        return jobBuilderFactory.get("processorCompositeJob")
                .start(processorCompositeStep())
                .build();
    }

    @Bean
    @JobScope
    public Step processorCompositeStep() throws Exception {
        return stepBuilderFactory.get("processorCompositeStep")
                .<Teacher, String>chunk(chunkSize)
                .reader(processCompositeReader())
                .processor(compositeItemProcessor())
                .writer(processCompositeWriter())
                .build();
    }

    private ItemWriter<String> processCompositeWriter() {
        return items -> {
            for (String item : items) {
                log.info("Teacher Name = {}", item);
            }
        };
    }

    @Bean
    public CompositeItemProcessor compositeItemProcessor() {
        List<ItemProcessor> delegates = new ArrayList<>(2);
        delegates.add(processCompositeProcessor1());
        delegates.add(processCompositeProcessor2());

        CompositeItemProcessor processor = new CompositeItemProcessor();
        processor.setDelegates(delegates);
        return processor;
    }

    @Bean
    public ItemProcessor<Teacher, String> processCompositeProcessor1() {
        return Teacher::getName;
    }

    @Bean
    public ItemProcessor<String, String> processCompositeProcessor2() {
        return name -> "안녕하세요. "+ name + "입니다.";
    }

    @Bean
    public JdbcPagingItemReader<Teacher> processCompositeReader() throws Exception {
        return new JdbcPagingItemReaderBuilder<Teacher>()
            .pageSize(chunkSize)
            .fetchSize(chunkSize)
            .dataSource(dataSource)
            .rowMapper(new BeanPropertyRowMapper<>(Teacher.class))
            .queryProvider(processorCompositeQueryProvider())
            .name("processCompositeReader")
            .build();
    }

    @Bean
    public PagingQueryProvider processorCompositeQueryProvider() throws Exception {
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
