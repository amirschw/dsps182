package dsps.ass1.worker;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Paths;

public class SaveImageFromUrl {

	static int uniqueId = 0;
	
	public static String download(String imageUrl) throws Exception {
		URL url = new URL(imageUrl);
		String destinationFile = uniqueId + Paths.get(url.getPath()).getFileName().toString();
        saveImage(url, destinationFile);
	    uniqueId++;
	    return destinationFile;
	}

    public static void saveImage(URL url, String destinationFile) throws Exception {
    
       try (InputStream is = url.openStream();
               OutputStream os = new FileOutputStream(destinationFile)) {
           byte[] b = new byte[2048];
           int length;
        
           while ((length = is.read(b)) != -1) {
               os.write(b, 0, length);
           }
       }
    }

}
