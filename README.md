# Signals

Signals provides parallel and streaming upload and download of S3 objects.

## Features

- *Streaming*: Parallel and streaming upload and download for efficient operations.
- *Integrity checks*: Integrity checks are done during multi-part upload.
- *Retries*: Every call to s3 are retried according to Amazon S3 recommendations.
- *Memory conscious*: Signals tries to make a conscious usage of memory during upload and download.


## Usage

```kotlin
File("data.txt").inputStream().use { file -> 
    S3OutputStream(bucket = "bucket", key = "data.txt").use { s3 ->
        file.copyTo(s3)
    }
}

File("data.txt").outputStream().use { file -> 
    S3InputStream(bucket = "bucket", key = "data.txt").use { s3 -> 
        s3.copyTo(file)
    }
}
```

## Part size calculations

The size of each part follow the same logic for [both upload and download](https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance-guidelines.html#optimizing-performance-guidelines-get-range). It starts at 5MB, and grow up to 3.6GB to allow to upload up to 5TB, to match with [AWS S3 limits](https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html).


![size](https://user-images.githubusercontent.com/44535/71637075-8e221d80-2bef-11ea-9a79-db01e7b87bc7.png)

_Part size growth_

![sum](https://user-images.githubusercontent.com/44535/71637076-92e6d180-2bef-11ea-9335-88e36efa1a0b.png)

_Total size growth_


## See also

* [aws/s3](https://github.com/cyberdelia/aws/tree/master/s3)
