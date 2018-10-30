package se.af.iris.kafka.bridge.producer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import se.af.iris.kafka.bridge.consumer.AnnonsvisningConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class OmegaProducer {

    final Producer<String, String> producer = createProducer(getProperties());



    public static void main(String[] args) throws InterruptedException {

        OmegaProducer omegaProducer = new OmegaProducer();

        omegaProducer.sendRecord("123", "1234567");
    }

    public void sendRecord(String key, String value) {


        ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", key, value);
        RecordMetadata metadata = null;
        try {
            metadata = producer.send(record).get();
        } catch (InterruptedException e) {
            System.out.println(metadata);
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }


    private static Properties getProperties() {
        String host = "omegateam.se";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", host + ":19092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://" + host + ":8081");
        properties.put("key.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put("value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put("acks", "1");
        properties.put("retries", "3");
        return properties;
    }

    private static Producer<String, String> createProducer(Properties properties) {
        return new KafkaProducer<>(properties) ;
    }



}
