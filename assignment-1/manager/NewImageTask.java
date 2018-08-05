package dsps.ass1.manager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NewImageTask extends SQSMessage {
    
    private String filename;
	private String url;
	
	@JsonCreator
	public NewImageTask(@JsonProperty("url") String url, @JsonProperty("filename") String filename) {
        this.url = url;
        this.filename = filename;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

}
