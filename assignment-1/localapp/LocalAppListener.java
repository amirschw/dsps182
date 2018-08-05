package dsps.ass1.localapp;

import static j2html.TagCreator.body;
import static j2html.TagCreator.br;
import static j2html.TagCreator.div;
import static j2html.TagCreator.each;
import static j2html.TagCreator.head;
import static j2html.TagCreator.html;
import static j2html.TagCreator.img;
import static j2html.TagCreator.p;
import static j2html.TagCreator.title;

import java.beans.XMLEncoder;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;


public class LocalAppListener implements MessageListener {
    
    private ObjectMapper mapper;

    public LocalAppListener() {
        mapper = new ObjectMapper();
    }

    @Override
    public void onMessage(Message msg) {
    	System.out.println("got massage on localApp");
    	
        // Convert message to DoneTask object
        DoneTask doneTask = parseMsg(msg);
        
                    
        // Download file, convert content to a list of DoneImageTask objects and delete file
        ArrayList<DoneImageTask> summary = downloadAndParseFileContent(doneTask);
        
        // Create html output
        createHTML(summary);
        
        // Create XML file with distribution of jobs between workers
        CheckBalance(summary);
        
        // Tell SQS to delete the message
        try {
            msg.acknowledge();
        } catch (JMSException ex) {
            System.err.println("Caught an exception while acknowledging message");
        }
        
        // Notify local app that job is complete
        LocalApp.stopListening();
    }
    
    private ArrayList<DoneImageTask> downloadAndParseFileContent(DoneTask task) {
        
        // Download file from S3
        S3Object object = S3Helper.downloadObject(
                LocalApp.s3, task.getBucket(), task.getKey());
        
        // Read results from file
        ArrayList<DoneImageTask> images = new ArrayList<DoneImageTask>();
        try (BufferedReader buffer = new BufferedReader(
                new InputStreamReader(object.getObjectContent()))) {
            String line;
            while ((line = buffer.readLine()) != null) {
                images.add(mapper.readValue(line, DoneImageTask.class));
            }
            // Delete s3 file and enclosing bucket
            S3Helper.deleteS3Bucket(LocalApp.s3, task.getBucket());
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while deleting summary file from S3");
            ex.printStackTrace();
        } catch (IOException ex) {
            System.err.println("Caught an exception while parsing S3 response file");
            ex.printStackTrace();
            System.exit(1);
        }
        return images;
    }

    private DoneTask parseMsg(Message msg) {
        DoneTask doneTask = null;
        try {
            String message = ((TextMessage) msg).getText();
            System.out.println("Handling message from manager:\n" + message);
            doneTask = mapper.readValue(message, DoneTask.class);
        } catch (IOException | JMSException ex) {
            System.err.println("Caught an exception while parsing message");
            ex.printStackTrace();
            System.exit(1);
        }
        return doneTask;
    }
    
    private void CheckBalance(List<DoneImageTask> list) {
    	HashMap<String, Integer> workersJobs = new HashMap<String, Integer>();
    	for (DoneImageTask task: list) {
    		String worker = task.getWorker();
    		Integer counter = workersJobs.get(worker);
    		if (counter == null) {
    			counter = 1;
    		} else {
    			counter++;	
    		}
    		workersJobs.put(worker, counter);
    	}

		try (FileOutputStream fos = new FileOutputStream("workersJobs.xml");
		        XMLEncoder encoder = new XMLEncoder(fos)) {
		    encoder.writeObject(workersJobs);
		} catch (IOException e1) {
            System.err.println("Caught an exception while creating XML file");
			e1.printStackTrace();
		}
    	System.out.println("XML file with name 'workersJobs.xml' created.");    	
    }
    
    private void createHTML(List<DoneImageTask> list) {
    	
        try (PrintWriter out = new PrintWriter("output.html")) {
            String html = html(
                    head(
                            title("Optical Character Recognition")
                        ),
                        body(
                                each(list, image ->
                                   div(img().withSrc(image.getUrl()),
                                       p(image.getRecognition()),
                                       br()
                                   )
                                )
                        )
                    ).renderFormatted();
            out.println(html);
        } catch (IOException ex) {
            System.err.println("Caught an exception while creating html output");
            ex.printStackTrace();
        }
        System.out.println("Html output file created");
    }
    
}
