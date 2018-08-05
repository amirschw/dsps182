package dsps.ass1.manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.amazonaws.services.s3.model.S3Object;

public class DownloadAndParse implements Runnable {
    private NewTask task;
    
    public DownloadAndParse(NewTask task) {
        this.task = task;
    }

    @Override
    public void run() {
        // Add file to file manager
        Manager.fileManager.addFile(task.getKey(), task.getLocalAppId());
        
        // Parse file content and distribute tasks
        int numOfImagesInFile = downloadAndParseFileContent(task);
                
        // Update number of images in file manager
        Manager.fileManager.setNumOfImagesInFile(task.getKey(), numOfImagesInFile);
        
        // Compute and update the number of needed workers
        int numOfWorkers = (int)Math.ceil((double)numOfImagesInFile / task.getImagesPerWorker());
        synchronized (MonitorWorkers.lock) {
            if (MonitorWorkers.workersNeeded < numOfWorkers) {
                MonitorWorkers.workersNeeded = numOfWorkers;
            }
        }
        
        // Create a task to create and start worker processes
        Manager.nodesCreationPool.execute(new CreateWorkers(numOfWorkers));
    }

    private int downloadAndParseFileContent(NewTask task) {
        // Download file from S3
        S3Object object = S3Helper.downloadObject(
                Manager.s3, task.getBucket(), task.getKey());
        
        // Parse file and create a task to send an SQS message for each URL
        int numOfImages = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(object.getObjectContent()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Manager.distributionPool.execute(new SendNewImageTask(
                        line.trim(), task.getKey()));
                numOfImages++;
            }
        } catch (IOException ex) {
            System.err.println("Caught an exception while parsing images list file from S3");
            ex.printStackTrace();
            System.exit(1);
        }
        return numOfImages;
    }

}
