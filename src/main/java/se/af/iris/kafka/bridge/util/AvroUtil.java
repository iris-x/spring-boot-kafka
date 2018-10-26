package se.af.iris.kafka.bridge.util;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroUtil {

    public static <T extends SpecificRecordBase> String avroToJson(T obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder enc = EncoderFactory.get().jsonEncoder(obj.getSchema(), out);
        DatumWriter<T> writer = new SpecificDatumWriter<T>(obj.getSchema());
        writer.write(obj, enc);
        enc.flush();
        return out.toString();
    }

    public static <T extends SpecificRecordBase> T avroFromJson(String json, Schema schema) throws IOException {
        Decoder dec = DecoderFactory.get().jsonDecoder(schema, json);
        DatumReader<T> reader = new SpecificDatumReader<T>(schema);
        return reader.read(null, dec);
    }


    public static <T extends SpecificRecordBase> byte[] avroToBinary(T obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder enc = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<T> writer = new SpecificDatumWriter<T>(obj.getSchema());
        writer.write(obj, enc);
        enc.flush();
        return out.toByteArray();
    }

    public static <T extends SpecificRecordBase> T avroFromBinary(byte[] bytes, Schema schema) throws IOException {
        Decoder dec = DecoderFactory.get().binaryDecoder(bytes, null);
        DatumReader<T> reader = new SpecificDatumReader<T>(schema);
        return reader.read(null, dec);
    }
}
