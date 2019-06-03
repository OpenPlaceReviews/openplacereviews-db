package org.openplacereviews.db;

//import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class ApplicationConfiguration {

	@Value("${http.read.timeout}")
	private Integer readTimeoutMs;

	@Value("${http.connect.timeout}")
	private Integer connectTimeoutMs;

	@Bean
	@Autowired
	public RestTemplate restTemplate(RestTemplateBuilder restTemplateBuilder) {
		return restTemplateBuilder
				.setReadTimeout(readTimeoutMs)
				.setConnectTimeout(connectTimeoutMs)
				.build();

	}

	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper();
	}

}
