package com.github.jenkinsx.quickstarts.springboot.rest.prometheus;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import se.af.iris.kafka.ThreadedConsumerExample;

import java.util.Map;

import static java.util.Collections.singletonMap;

@SpringBootApplication
@Controller
public class RestPrometheusApplication {

	@Autowired
	private MeterRegistry registry;

	@GetMapping(path = "/", produces = "application/json")
	@ResponseBody
	public Map<String, Object> landingPage() {
		Counter.builder("mymetric").tag("foo", "bar").register(registry).increment();
        return singletonMap("hello", "kafka avro nexus world");
	}

	public static void main(String[] args)  throws InterruptedException {
		SpringApplication.run(RestPrometheusApplication.class, args);
		ThreadedConsumerExample consumerExample = new ThreadedConsumerExample();
		consumerExample.startConsuming();
		Thread.sleep(60000 * 10); //Run for one minute
		consumerExample.stopConsuming();
	}

}
