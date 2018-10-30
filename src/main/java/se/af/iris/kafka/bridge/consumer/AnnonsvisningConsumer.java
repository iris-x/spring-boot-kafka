package se.af.iris.kafka.bridge.consumer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import se.af.iris.kafka.bridge.producer.AnnonsvisningProducer;
import se.af.iris.kafka.bridge.producer.AnnonsvisningRestProducer;
import se.arbetsformedlingen.kafka.Annonsvisning;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    //private static AnnonsvisningRestProducer annonsvisningProducer = new AnnonsvisningRestProducer();

    private static AnnonsvisningProducer annonsvisningProducer =
            new AnnonsvisningProducer("omegateam.se", 19092, "test_annonsvisningar");


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
                    ConsumerRecords<String, GenericRecord> consumerRecords = consumer.poll(Duration.ofSeconds(5));

                    for (ConsumerRecord<String, GenericRecord> record : consumerRecords) {
                        annonsvisningProducer.sendRecord(record.key(),  record.value());
                        System.out.println(record.key() + " : " + record.value());
                    }

                    /* AnnonsvisningRestProducer

                    List<SpecificRecordBase> annonsvisningar = getAnnonsvisningar(consumerRecords);
                    if (annonsvisningar.size() > 0) {
                        System.out.println("Sending annonsvisningar: " + annonsvisningar.size());
                        annonsvisningProducer.sendMessages(annonsvisningar);
                    }*/

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

    public List<SpecificRecordBase> getAnnonsvisningar(ConsumerRecords<String, GenericRecord> consumerRecords) {

        List<SpecificRecordBase> annonsvisningar = new ArrayList<SpecificRecordBase>();
        for (ConsumerRecord<String, GenericRecord> consumerRecord : consumerRecords) {
            GenericRecord recordValue = consumerRecord.value();
            Annonsvisning annonsvisning = ((Annonsvisning) SpecificData.get().deepCopy(Annonsvisning.getClassSchema(), recordValue));
            annonsvisningar.add(annonsvisning);
        }
        return annonsvisningar;
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


