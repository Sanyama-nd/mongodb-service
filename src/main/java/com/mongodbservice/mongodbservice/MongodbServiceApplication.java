package com.mongodbservice.mongodbservice;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@SpringBootApplication
@RestController
public class MongodbServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(MongodbServiceApplication.class, args);
	}

	@Autowired
	MongodbService service;
	@GetMapping()
	String appRunning() {
		return "Running";
	}
	@PostMapping(value = "/smartviewrequest/v2/", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	String getSmartView(@RequestParam("tenant_id") int tenantId, @RequestParam("start_date") String startDate, @RequestParam("end_date") String endDate,
						@RequestBody String payload) {
		return service.getSmartView(payload, tenantId, startDate, endDate);
	}

	@GetMapping(value = "/getResults")
	String getResults() {
		return service.getResults();
	}


}
