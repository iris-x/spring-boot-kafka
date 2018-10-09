package se.af.iris.kafka.bridge.producer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AnnonsvisningProducer {

    final Producer<String, GenericRecord> producer = createProducer(getProperties());


    public void sendRecord(String key, GenericRecord value) {

        ProducerRecord<String, GenericRecord> record = getRecord("platsannons_visning_test", key, value);
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

    // ClusterIP K8s 10.110.215.78
    private static Properties getProperties() {
        //String host = "k8s.arbetsformedlingen.se";
        String host = "localhost";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", host + ":9092");
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
