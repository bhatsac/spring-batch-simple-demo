package com.bhatsac.springbatchsimpledemo;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing

public class SpringBatchSimpleDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchSimpleDemoApplication.class, args);
	}

}
