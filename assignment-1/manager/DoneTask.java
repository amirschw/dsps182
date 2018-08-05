package dsps.ass1.manager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DoneTask extends SQSMessage {
	
    private String bucket;
    private String key;
   
    @JsonCreator
    public DoneTask(@JsonProperty("bucket") String bucket, @JsonProperty("key") String key) {
        this.bucket = bucket;
        this.key = key;
    }
    public String getBucket() {
        return bucket;
    }
    public void setBucket(String bucket) {
        this.bucket = bucket;
    }
    public String getKey() {
        return key;
    }
    public void setKey(String key) {
        this.key = key;
    }

}
