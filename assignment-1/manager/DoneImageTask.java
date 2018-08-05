package dsps.ass1.manager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DoneImageTask extends SQSMessage {

    private String filename;
	private String url;
	private String recognition;
	private String worker;

	@JsonCreator
	public DoneImageTask(@JsonProperty("url") String url, @JsonProperty("recognition") String recognition,
	        @JsonProperty("filename") String filename, @JsonProperty("worker") String worker) {
		this.recognition = recognition;
		this.url = url;
		this.filename = filename;
		this.worker = worker;
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

    public String getRecognition() {
        return recognition;
    }

    public void setRecognition(String recognition) {
        this.recognition = recognition;
    }

    public String getWorker() {
        return worker;
    }

    public void setWorker(String worker) {
        this.worker = worker;
    }

}
