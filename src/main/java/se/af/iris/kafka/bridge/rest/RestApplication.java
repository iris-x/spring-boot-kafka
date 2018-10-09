package se.af.iris.kafka.bridge.rest;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import se.af.iris.kafka.bridge.consumer.AnnonsvisningConsumer;

import java.util.Map;

import static java.util.Collections.singletonMap;

@SpringBootApplication
@Controller
public class RestApplication {

	@Autowired
	private MeterRegistry registry;

	private static AnnonsvisningConsumer annonsvisningConsumer = new AnnonsvisningConsumer();

	@GetMapping(path = "/", produces = "application/json")
	@ResponseBody
	public Map<String, Object> landingPage() {
		Counter.builder("mymetric").tag("foo", "bar").register(registry).increment();
        return singletonMap("Messages forwarded: ", annonsvisningConsumer.getMessageCount());
	}

	public static void main(String[] args)  throws InterruptedException {
		SpringApplication.run(RestApplication.class, args);
		annonsvisningConsumer.startConsuming();
		Thread.sleep(60000 * 60); //Run for one minute
		annonsvisningConsumer.stopConsuming();
	}

}

