# aws-lambda-unzip
Function for AWS Lambda to extract zip files uploaded to S3. This version converts them to individual GZIP compressed files.

NOTE: Fork from Craftware/aws-lambda-unzip, thank you!!!!

LICENSE: Apache License, see license file.


BEHAVIOR (note, very different from original aws-lambda-unzip): Zipfilename will be used to create a directory beside zipfile location, then each file within the zip file will be moved underneath that directory and GZIP compressed (.gz extension).

INTENDED USAGE: most cluster/cloud computing/splitting operations do not recognize nor understand ZIP files, particularly since the content of a zip file could be any number of other files.  However, gzip is well understood and with the proper setup is a splittable file format and provide much greater throughput (for any good-sized files) than left uncompressed.

The zipfile is deleted at the end of the operation.

## Necessary permissions
In order to remove the uploaded zip file, the role configured in your Lambda function should have a policy looking like this:
```
{
        "Effect": "Allow",
        "Action": [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject"
        ],
        "Resource": [
            "arn:aws:s3:::mybucket"
	]
}
```

## Packaging for deployment
Maven is already configured to package the .jar file correctly for deployment into Lambda. Just run
```
mvn clean package
```
The packaged file will be present in your `target/` folder. Upload the non-original named JAR file (the larger one).

## Configuring AWS 
Lambda Configuration of Handler: *kornell.S3EventProcessorUnzip*

Lambda memory sizing: Recommend highest, 1536MB, since the pricing and associated performance gains appear to be linear as of 2016-01-29.

S3 bucket: properties, an Event for S3 put/post/copy (depending on desired usage), and a filter like .zip, and select the newly setup Lambda function.  When udpating the Lambda function, may need to delete and setup the S3 bucket event again.

Random 8MB ZIP file with CSV content file for different Lambda sizing comparisons with S3 put event to individual GZIP compressed file versions as of 2016-01-29:
```
--duration 28-38 seconds at 256MB lambda sizing
--duration 13-18 seconds at 512MB lambda sizing
--duration 7-9 seconds at 1024MB lambda sizing
--duration 6-7 seconds at 1536MB lambda sizing
--actual memory used for all consistently 80-120MB (more on the 85MB side)
```

Unofficial/random note with no promises, gaurantees, warranties, or anything other than take-at-your-own-risk knowledge.  A 10MB zip file with 1 text file inside at 1536MB would take on average 7-8 seconds would have a cost of:
```
$0.0002 per 10MB file to process.
```


