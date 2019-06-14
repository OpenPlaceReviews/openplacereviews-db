package org.openplacereviews.db;


import org.openplacereviews.opendb.OpenDBServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("org.openplacereviews")
public class OpenPlaceReviewsDbBoot extends OpenDBServer {

	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenPlaceReviewsDbBoot.class, args);
	}

}
