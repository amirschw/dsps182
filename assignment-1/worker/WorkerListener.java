package dsps.ass1.worker;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WorkerListener implements MessageListener {

	private ObjectMapper objectMapper;
	
	public WorkerListener() {
		this.objectMapper = new ObjectMapper();

	}

	// Called whenever a message enters the sqs
	public void onMessage(Message msg) {
		System.out.println("Message received, start analyizing");
		try {
			// Parse the message
		    NewImageTask newImageTask = parseMsg(msg);
			
			// analyze
			handleRequest(newImageTask);

			// tell the sqs to delete the message  
			msg.acknowledge();
		} catch (Exception e) {
			System.err.println("Caught an exception while trying to analyze:\n" + e);
			return;
		}
	}
	
	private NewImageTask parseMsg(Message msg) throws Exception {
		
		String json = ((TextMessage) msg).getText();
		NewImageTask task = objectMapper.readValue(json, NewImageTask.class);
		System.out.println("Successfully parsed message:\n" + json);
		
		return task;
	}
	
	private void handleRequest(NewImageTask task) {
		
		try {
	        System.out.println("Starting handling");
	        String recognition = MyOcr.process(task.getUrl());

	        // create and parse to json response
	        DoneImageTask response = new DoneImageTask(
	                task.getUrl(), recognition, task.getFilename(),
	                Worker.instanceId);

		    String outMsg = response.toString();
	        System.out.println("Finished analyzing, the result is:\n" + outMsg);
	        
	        // send the response to manager
	        SQSHelper.sendSQSMessage(Worker.sqs, Worker.W2MUrl, outMsg);
		} catch (Exception ex) {
            System.err.println("Caught an exception while trying to analyze:\n" + ex);
		    SQSHelper.sendSQSMessage(Worker.sqs, Worker.W2MUrl, new DoneImageTask(
		            task.getUrl(),
		            "<error: failed to analyze image from url: " + task.getUrl() + ">",
		            task.getFilename(), Worker.instanceId).toString());
		}
	}

}
