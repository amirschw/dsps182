package dsps.ass1.manager;

public class AppendRecognitionToFile implements Runnable {
    
    private DoneImageTask dit;

    public AppendRecognitionToFile(DoneImageTask dit) {
        this.dit = dit;
    }

    @Override
    public void run() {
        Manager.fileManager.appendRecognitionToFile(dit.getFilename(), dit.toString());
    }

}
