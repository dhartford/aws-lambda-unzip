package kornell;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;


//handler name: kornell.S3EventProcessorUnzip

public class S3EventProcessorUnzip implements RequestHandler<S3Event, String> {

static final    Pattern pattern = Pattern.compile(".*\\.([^\\.]*)");

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
//        byte[] buffer = new byte[1024];
        byte[] buffer = new byte[8192];
        try {
            for (S3EventNotificationRecord record: s3Event.getRecords()) {
                final String srcBucket = record.getS3().getBucket().getName();

                // Object key may have spaces or unicode non-ASCII characters.
                String srcKey = record.getS3().getObject().getKey()
                        .replace('+', ' ');
                srcKey = URLDecoder.decode(srcKey, "UTF-8");

                // Detect file type
//                Pattern pattern = Pattern.compile(".*\\.([^\\.]*)");
                final Matcher matcher = pattern.matcher(srcKey);
                if (!matcher.matches()) {
                    System.out.println("CODE: Unable to detect file type for key " + srcKey);
                    return "";
                }
                final String extension = matcher.group(1).toLowerCase();
//                final String foldername = matcher.group(0);
              final String foldername = srcKey.replace("." + extension, "");
                
                final String destBucket = srcBucket + "/" + foldername;
                
                
                if (!"zip".equals(extension)) {
                    System.out.println("CODE: Skipping non-zip file " + srcKey + " with extension " + extension);
                    return "";
                }
                System.out.println("CODE: Extracting zip file " + srcBucket + "/" + srcKey);
                
                
                // Download the zip from S3 into a stream
                AmazonS3 s3Client = new AmazonS3Client();
                final S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
                final ZipInputStream zis = new ZipInputStream(s3Object.getObjectContent());
                ZipEntry entry = zis.getNextEntry();

                while(entry != null) {
                    String fileName = entry.getName();
                    System.out.println("CODE: Extracting " + fileName + ", compressed: " + entry.getCompressedSize() + " bytes, extracted: " + entry.getSize() + " bytes");
                    
                    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    int len;
//                    while ((len = zis.read(buffer)) > 0) {
//                        outputStream.write(buffer, 0, len);
//                    }
                    final GZIPOutputStream zipStream =
                            new GZIPOutputStream(outputStream);
                    try{
	                    while ((len = zis.read(buffer)) > 0) {
	                    	zipStream.write(buffer, 0, len);
	                    }
                      }
                      finally
                      {
                        zipStream.close();
                      }
                      
                    
                    final InputStream is = new ByteArrayInputStream(outputStream.toByteArray());
                    final ObjectMetadata meta = new ObjectMetadata();
                    meta.setContentLength(outputStream.size());
                    //TODO evaluate modifying filename when doesn't comply with S3 naming conventions.
//                    s3Client.putObject(srcBucket, fileName + ".gz", is, meta);
                    s3Client.putObject(destBucket, fileName + ".gz", is, meta);
                    
                    is.close();
                    outputStream.close();
                    entry = zis.getNextEntry();
                }
                zis.closeEntry();
                zis.close();
                
                //delete zip file when done
                s3Client.deleteObject(new DeleteObjectRequest(srcBucket, srcKey));
                System.out.println("CODE: Deleted zip file " + srcBucket + "/" + srcKey);
            }
            return "Ok";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    


}
