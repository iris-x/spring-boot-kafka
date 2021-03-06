package se.af.iris.kafka.bridge.producer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AnnonsvisningProducer {

    private Producer<String, GenericRecord> producer;
    private String topic;

    public AnnonsvisningProducer(String host, int port, String topic) {
        this.producer = createProducer(getProperties(host, port));
        this.topic = topic;
    }

    public void sendRecord(String key, GenericRecord value) {

        ProducerRecord<String, GenericRecord> record = getRecord(this.topic, key, value);
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

    private ProducerRecord<String, GenericRecord> getRecord(String topic, String key, GenericRecord value) {
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, key, value);
        return record;
    }

    private static Properties getProperties(String host, int port) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", host + ":" + port);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://" + host + ":8081");
        properties.put("key.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put("value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put("acks", "1");
        properties.put("retries", "3");
        return properties;
    }

    private static Producer<String, GenericRecord> createProducer(Properties properties) {
        return new KafkaProducer<>(properties) ;
    }

}
