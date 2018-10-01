package se.af.iris.kafka;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AnnonsvisningProducer {

    final Producer<String, GenericRecord> producer = createProducer(getProperties());


    public static void main(String[] args) {

        AnnonsvisningProducer annonsvisningProducer = new AnnonsvisningProducer();

        //annonsvisningProducer.sendRecord("12345", new GenericData.Record());


    }

    public void sendRecord(String key, GenericRecord value) {

        ProducerRecord<String, GenericRecord> record = getRecord("platsannons_visning", key, value);

        try {
            RecordMetadata metadata = producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    private ProducerRecord<String, GenericRecord> getRecord(String topic, String key, GenericRecord value) {

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, key, value);
        return record;
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:29092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
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
