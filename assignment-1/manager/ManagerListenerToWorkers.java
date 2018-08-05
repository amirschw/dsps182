package dsps.ass1.manager;

import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ManagerListenerToWorkers implements MessageListener {
    
    private ObjectMapper mapper;

    public ManagerListenerToWorkers() {
        mapper = new ObjectMapper();
    }

    @Override
    public void onMessage(Message msg) {
        // Convert message to DoneImageTask object
        DoneImageTask dit = parseMsg(msg);
        
        // Create task to append OCR to file
        Manager.fileEditionPool.execute(new AppendRecognitionToFile(dit));
        
        // Tell SQS to delete the message
        try {
            msg.acknowledge();
        } catch (JMSException ex) {
            System.err.println("Caught an exception while acknowledging message");
        }        
    }

    private DoneImageTask parseMsg(Message msg) {
        DoneImageTask dit = null;
        try {
            String message = ((TextMessage) msg).getText();
            System.out.println("Handling message from worker:\n" + message);
            dit = mapper.readValue(message, DoneImageTask.class);
        } catch (IOException | JMSException ex) {
            System.err.println("Caught an exception while parsing message");
            ex.printStackTrace();
            System.exit(1);
        }
        return dit;
    }

}
