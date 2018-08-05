package dsps.ass1.manager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

public class SummaryFile {
    private String filename;
    private String localAppId;
    private File file;
    private int numOfImages;
    private CopyOnWriteArrayList<String> dataList;
    
    public SummaryFile(File file, String filename, String localAppId) {
        this.filename = filename;
        System.out.println("SummaryFile - filename: " + this.filename);
        this.localAppId = localAppId;
        System.out.println("localAppId - localAppId: " + this.localAppId);
        this.file = file;
        this.numOfImages = -1;
        this.dataList = new CopyOnWriteArrayList<String>();
    }
    
    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getLocalAppId() {
        return localAppId;
    }

    public void setLocalAppId(String localAppId) {
        this.localAppId = localAppId;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public int getNumOfImages() {
        return numOfImages;
    }

    public void setNumOfImages(int numOfImages) {
        this.numOfImages = numOfImages;
    }

    public void addRecognition(String data) {
    	dataList.add(data);
    }
    
    public boolean isDone() {
        return dataList.size() == numOfImages;
    }
    
    public void writeRecognitionsToFile() {
    	
            try (BufferedWriter out = new BufferedWriter(new FileWriter(file, true))) {
            	for (String data : this.dataList)
            		out.write(data + "\n");
            } catch (IOException ex) {
                System.err.println("Caught an exception while trying " +
                        "to write to file: " + filename);
                ex.printStackTrace();
            }

    }
    
}
