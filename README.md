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

## See also

* [aws/s3](https://github.com/cyberdelia/aws/tree/master/s3)
