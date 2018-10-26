package se.af.iris.kafka.bridge.consumer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import se.af.iris.kafka.bridge.producer.AnnonsvisningProducer;
import se.af.iris.kafka.bridge.producer.AnnonsvisningRestProducer;
import se.arbetsformedlingen.kafka.Annonsvisning;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AnnonsvisningConsumer {

    private volatile boolean doneConsuming = false;
    private int numberPartitions;
    private ExecutorService executorService;

    private int messageCount;

    private static String topic_name = "iris_platsannons_visning";

    private static AnnonsvisningRestProducer annonsvisningProducer = new AnnonsvisningRestProducer();


    public AnnonsvisningConsumer() {

    }


    public void startConsuming() {

        System.out.println("AnnonsvisningConsumer/startConsuming");
        numberPartitions = 2;

        executorService = Executors.newFixedThreadPool(numberPartitions);
        Properties properties = getConsumerProps();

        for (int i = 0; i < numberPartitions; i++) {
            Runnable consumerThread = getConsumerThread(properties);
            executorService.submit(consumerThread);
        }
    }

    public int getMessageCount() {
        return messageCount;
    }

    private Runnable getConsumerThread(Properties properties) {
        return () -> {
            Consumer<String, GenericRecord> consumer = null;
            try {
                consumer = new KafkaConsumer<>(properties);
                consumer.subscribe(Collections.singletonList(topic_name));
                while (!doneConsuming) {
                    ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(5));
                    for (ConsumerRecord<String, GenericRecord> record : records) {
                        GenericRecord recordValue = record.value();

                        Annonsvisning annonsvisning = Annonsvisning.newBuilder()
                                .setDeviceId(recordValue.get("device_id").toString())
                                .setSessionId(recordValue.get("session_id").toString())
                                .setAnnonsId(recordValue.get("annons_id").toString())
                                .setTidpunkt(Long.parseLong(recordValue.get("tidpunkt").toString()))
                                .setAnvId(recordValue.get("anv_id") != null ? recordValue.get("anv_id").toString() : null)
                                .build();

                        Annonsvisning annonsvisning2 = ((Annonsvisning) SpecificData.get().deepCopy(Annonsvisning.getClassSchema(), recordValue));

                        System.out.println(annonsvisning);
                        System.out.println(annonsvisning2);
                        annonsvisningProducer.sendMessage(annonsvisning);

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
        properties.put("group.id", "stams2-annonsvisning-consumer");
        properties.put("auto.offset.reset", "latest");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "3000");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://sauron.ws.ams.se:8081");
        properties.put("key.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        properties.put("value.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        return properties;

    }

    public static void main(String[] args) throws InterruptedException {

        AnnonsvisningConsumer consumerExample = new AnnonsvisningConsumer();

        consumerExample.startConsuming();
        Thread.sleep(60000 * 600); //Run for one minute
        consumerExample.stopConsuming();
    }

}


