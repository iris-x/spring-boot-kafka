package se.af.iris.kafka;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class ThreadedConsumerExample {

    private volatile boolean doneConsuming = false;
    private int numberPartitions;
    private ExecutorService executorService;

    private static String topic_name = "iris_platsannons_visning";
    //private static String topic_name = "platsbanken_inloggningar";


    public ThreadedConsumerExample(int numberPartitions) {
        this.numberPartitions = numberPartitions;
    }


    public void startConsuming() {
        executorService = Executors.newFixedThreadPool(numberPartitions);
        Properties properties = getConsumerProps();

        for (int i = 0; i < numberPartitions; i++) {
            Runnable consumerThread = getConsumerThread(properties);
            executorService.submit(consumerThread);
        }
    }

    private Runnable getConsumerThread(Properties properties) {
        return () -> {
            Consumer<String, String> consumer = null;
            try {
                consumer = new KafkaConsumer<>(properties);
                consumer.subscribe(Collections.singletonList(topic_name));
                while (!doneConsuming) {
                    ConsumerRecords<String, String> records = consumer.poll(5000);
                    for (ConsumerRecord<String, String> record : records) {
                        String message = String.format("Consumed: key = %s value = %s with offset = %d partition = %d",
                                record.key(), record.value(), record.offset(), record.partition());
                        System.out.println(message);
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        };
    }

    public void stopConsuming() throws InterruptedException {
        doneConsuming = true;
        executorService.awaitTermination(10000, TimeUnit.MILLISECONDS);
        executorService.shutdownNow();
    }


    private Properties getConsumerProps() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "sauron.ws.ams.se:9092");
        properties.put("group.id", "simple-consumer-example");
        properties.put("auto.offset.reset", "latest");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "3000");
        //properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://sauron.ws.ams.se:8081");
        properties.put("key.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        properties.put("value.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);


        return properties;

    }


    public static void main(String[] args) throws InterruptedException {
        ThreadedConsumerExample consumerExample = new ThreadedConsumerExample(2);
        consumerExample.startConsuming();
        Thread.sleep(60000 * 10); //Run for one minute
        consumerExample.stopConsuming();
    }

}


