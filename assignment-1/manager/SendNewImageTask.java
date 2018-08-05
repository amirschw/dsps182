package dsps.ass1.manager;

public class SendNewImageTask implements Runnable {
    
    private String url;
    private String key;

    public SendNewImageTask(String url, String key) {
        this.url = url;
        this.key = key;
    }

    @Override
    public void run() {
        SQSHelper.sendSQSMessage(
                Manager.M2Wsqs, Manager.M2WUrl, new NewImageTask(url, key).toString());
    }
}
