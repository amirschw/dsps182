package dsps.ass1.worker;

import java.io.File;

import com.asprise.ocr.Ocr;

public class MyOcr
{
	public static Ocr ocr;

	public static void init() {
    	Ocr.setUp(); // one time setup
    	ocr = new Ocr(); // create a new OCR engine
    	ocr.startEngine("eng", Ocr.SPEED_FASTEST); // English
	}

	public static String process(String url) {
	    String filePath = null;
	    try {
	        filePath = SaveImageFromUrl.download(url);
	    } catch (Exception ex) {
	        return "<error: failed to download image from url: " + url + ">";
	    }
    	String recognition = ocr.recognize(
    	        new File[] {new File(filePath)}, Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT);
    	
    	if (recognition.isEmpty()) {
	        return "<error: failed to recognize image from url: " + url + ">";
    	}
    	System.out.println("Result: " + recognition);
    	return recognition;
    	
	}

}