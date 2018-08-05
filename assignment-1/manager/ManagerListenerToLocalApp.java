package dsps.ass1.manager;

import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ManagerListenerToLocalApp implements MessageListener {
    
    private ObjectMapper mapper;

    public ManagerListenerToLocalApp() {
        mapper = new ObjectMapper();
    }

    @Override
    public void onMessage(Message msg) {
        // Convert message to NewTask object
        NewTask newTask = parseMsg(msg);
        
        // Create a task to download and parse S3 file content
        Manager.downloadPool.execute(new DownloadAndParse(newTask));
        
        // Tell SQS to delete the message
        try {
            msg.acknowledge();
        } catch (JMSException ex) {
            System.err.println("Caught an exception while acknowledging message");
        }
    }

    private NewTask parseMsg(Message msg) {
        NewTask task = null;
        try {
            String message = ((TextMessage) msg).getText();
            System.out.println("Handling message from local app:\n" + message);
            task = mapper.readValue(message, NewTask.class);
        } catch (IOException | JMSException ex) {
            System.err.println("Caught an exception while parsing message");
            ex.printStackTrace();
            System.exit(1);
        }
        return task;
    }

}
