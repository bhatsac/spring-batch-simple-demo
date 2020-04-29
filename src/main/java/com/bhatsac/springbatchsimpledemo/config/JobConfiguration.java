package com.bhatsac.springbatchsimpledemo.config;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.bhatsac.springbatchsimpledemo.impl.Customer;
import com.bhatsac.springbatchsimpledemo.impl.CustomerRowMapper;
import com.bhatsac.springbatchsimpledemo.impl.Orders;
import com.github.javafaker.Faker;

@Configuration
public class JobConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Autowired
	public JdbcTemplate jdbctemplate;

	//
	// @Bean
	// public JdbcCursorItemReader<Customer> cursorItemReader() {
	// JdbcCursorItemReader<Customer> reader = new JdbcCursorItemReader<>();
	//
	// reader.setSql("select id, firstName, lastName, birthdate from customer
	// order by lastName, firstName");
	// reader.setDataSource(this.dataSource);
	// reader.setRowMapper(new CustomerRowMapper());
	//
	// return reader;
	// }

	@Bean
	public JdbcPagingItemReader<Customer> pagingItemReader() {
		JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();

		reader.setDataSource(this.dataSource);
		reader.setFetchSize(10);
		reader.setRowMapper(new CustomerRowMapper());

		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("id, firstName, lastName, birthdate");
		queryProvider.setFromClause("from customer");

		Map<String, Order> sortKeys = new HashMap<>(1);

		sortKeys.put("id", Order.ASCENDING);

		queryProvider.setSortKeys(sortKeys);

		reader.setQueryProvider(queryProvider);

		return reader;
	}

	@Bean
	public ItemWriter<Customer> customerItemWriter() {
		return (List<? extends Customer> items) -> {
			processWriter(items);
		};
	}

	@Bean
	public ItemProcessor<Customer, Customer> itemProcessor() {
		return (Customer item) -> {
			String sql = "SELECT * from ORDERS WHERE customer_id=" + item.getId() + " ORDER BY customer_id";
			jdbctemplate.setFetchSize(2);
			jdbctemplate.query(sql, (rs) -> {
				System.out.println(rs.getLong("id") + "---------" + rs.getString("item"));
			});
			System.out.println("-------------------------------------------------------------");
			return item;
		};

	}

	public void processWriter(List<? extends Customer> items) throws IllegalAccessException {

		for (Customer item : items) {

			/*
			 * jdbctemplate.update(
			 * "INSERT INTO new_customer (firstName, lastName,birthDate) VALUES (?, ?,?)"
			 * ,item.getFirstName(),item.getLastName(),item.getBirthdate());
			 */

			int order_size = (int) ((Math.random() * 10));
			List<Orders> orderList = new ArrayList<>();
			for (int i = 0; i < order_size; i++) {
				
				Faker faker = new Faker();
				Orders order =new Orders();
				order.setItem(faker.commerce().material());
				order.setQuantity( String.valueOf(((int) (Math.random() * 10))));
				order.setCustomer_id(item.getId());
				orderList.add(order);
				/*
				 * String print=
				 * "INSERT INTO customer (item,quantity,customer_id) " +
				 * "VALUES ('"+faker.commerce().material()+
				 * "','"+((int)(Math.random()*10))+"','"+item.getId()+"')";
				 * System.out.println(print);
				 */
			}
			batchInsert(orderList, orderList.size());
			/*
			 * if( item.getId()==40 || item.getId()==60 ) throw new
			 * IllegalAccessException("I am just an exception"+ item.getId());
			 * 
			 * if( item.getId()==45) throw new RuntimeException(
			 * "I am just an exception"+ item.getId());
			 */
		}
	}
	
	  public int[][] batchInsert(List<Orders> cuss, int batchSize) {

	        int[][] updateCounts = jdbctemplate.batchUpdate(
	                "INSERT INTO orders (item,quantity,customer_id) VALUES (?, ?,?)",
	                cuss,
	                batchSize,
	                new ParameterizedPreparedStatementSetter<Orders>() {
	                    public void setValues(PreparedStatement ps, Orders argument)
							throws SQLException {
	                        ps.setString(1, argument.getItem());
	                        ps.setString(2, argument.getQuantity());
	                        ps.setLong(3, argument.getCustomer_id());
	                    }
	                });
	        return updateCounts;

	    }

	@Bean
	public ItemWriter<Customer> customerItemWriter2() {
		return (List<? extends Customer> items) -> {
		
		};
	}

	@Bean
	public Step step1() {
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		threadPoolTaskExecutor.setCorePoolSize(20);
		threadPoolTaskExecutor.setMaxPoolSize(20);
		threadPoolTaskExecutor.setThreadNamePrefix("chunk_thread_");
		threadPoolTaskExecutor.initialize();
		return stepBuilderFactory.get("step1").<Customer, Customer> chunk(10).reader(pagingItemReader())
				// .processor()
				.writer(customerItemWriter())
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}

	@Bean
	public Step step2() {
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		threadPoolTaskExecutor.setCorePoolSize(2);
		threadPoolTaskExecutor.setMaxPoolSize(2);
		threadPoolTaskExecutor.setThreadNamePrefix("chunk_thread_");
		threadPoolTaskExecutor.initialize();
		return stepBuilderFactory.get("step2").<Customer, Customer> chunk(10).reader(pagingItemReader())
				.processor(itemProcessor())
				.writer(customerItemWriter2())
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}

	@Bean
	public Job job() {
		return jobBuilderFactory.get("job").start(step1()).next(step2()).build();
	}
}