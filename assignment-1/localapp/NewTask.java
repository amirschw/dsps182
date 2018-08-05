package dsps.ass1.localapp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NewTask extends SQSMessage {
	private String bucket;
	private String key;
    private int imagesPerWorker;
    private String localAppId;

    @JsonCreator
	public NewTask(@JsonProperty("bucket") String bucket,
	        @JsonProperty("key") String key,
	        @JsonProperty("imagesPerWorker") int imagesPerWorker,
	        @JsonProperty("localAppId") String localAppId) {
		this.bucket = bucket;
		this.key = key;
        this.imagesPerWorker = imagesPerWorker;
        this.localAppId = localAppId;
	}

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucketName) {
        this.bucket = bucketName;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getImagesPerWorker() {
        return imagesPerWorker;
    }

    public void setImagesPerWorker(int imagesPerWorker) {
        this.imagesPerWorker = imagesPerWorker;
    }

    public String getLocalAppId() {
        return localAppId;
    }

    public void setLocalAppId(String localAppId) {
        this.localAppId = localAppId;
    }
    	
}
