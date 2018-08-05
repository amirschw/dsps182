package dsps.ass1.localapp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SQSMessage {
    
    @Override
    public String toString() {
        String json = null;
        try {
            json = new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException ex) {
            System.err.println("Caught an exception while parsing to Json String");
            ex.printStackTrace();
        }
        return json;
    }

}
