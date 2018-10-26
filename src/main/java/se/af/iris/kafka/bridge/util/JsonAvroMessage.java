package se.af.iris.kafka.bridge.util;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JsonAvroMessage {

    private List<SpecificRecordBase> specificRecords = new ArrayList<SpecificRecordBase>();

    public JsonAvroMessage(SpecificRecordBase specificRecord) {
        this.specificRecords.add(specificRecord);
    }

    public JsonAvroMessage(List<SpecificRecordBase> specificRecords) {
        this.specificRecords = specificRecords;
    }

    public Schema getSchema() {
        return (specificRecords.size() > 0 ? specificRecords.get(0).getSchema() : null);
    }

    public String getJsonMessage() {
        String dataMessage = "{" + getSchemaMessage() + ", " + getDataMessage() + "}";
        return dataMessage;
    }

    public String getSchemaMessage() {
        String schema = (getSchema() != null ? getSchema().toString() : "");
        schema = schema.replace("\"","\\\"");
        String schemaMessage = "\"value_schema\": \"" + schema + "\"";
        return schemaMessage;
    }

    public String getDataMessage() {
        String valueMessages = "";
        for (SpecificRecordBase specificRecord: specificRecords ) {
            if (valueMessages.isEmpty() == false) {
                valueMessages += ", ";
            }
            valueMessages += getValueMessage(specificRecord);

        }
        return "\"records\":[" + valueMessages + "]";
    }

    private String getValueMessage(SpecificRecordBase specificRecord) {

       try {
           return "{\"value\": "+ AvroUtil.avroToJson(specificRecord) + "}";
       } catch (IOException e) {
           e.printStackTrace();
       }

       throw new RuntimeException("AvroToJsonException");
     }



}
