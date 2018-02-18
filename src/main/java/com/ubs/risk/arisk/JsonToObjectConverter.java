package com.ubs.risk.arisk;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;

public class JsonToObjectConverter {

 //String jsonString = "{\"name\": \"PositionKeySchema\",\"fields\": [  {\"fieldName\": \"positionId\", \"fieldType\": \"string\", \"optional\": \"false\" },  {       \"fieldName\": \"positionName\", \"fieldType\": \"string\",\"optional\": \"false\"   } ]}";

public static SchemaObject convertReadSchema(){
    String jsonString = "{\"schemaName\": \"PositionKeySchema\",\"fields\": [  {\"fieldName\": \"positionId\", \"fieldType\": \"string\", \"fieldOptional\": \"false\" },  {       \"fieldName\": \"positionName\", \"fieldType\": \"string\",\"fieldOptional\": \"false\"   } ]}";
    ObjectMapper objectMapper = new ObjectMapper();
    SchemaObject schemaObject = null;

    //convert json string to object
    try {
        schemaObject = objectMapper.readValue(jsonString, SchemaObject.class);
        if(schemaObject!=null){
            System.out.println("schema object is not null "+schemaObject.getSchemaName() + "\n fields list "+ schemaObject.getFields().size() );
        }
    } catch (IOException e) {
        e.printStackTrace();
    }
    return schemaObject;
}


}
