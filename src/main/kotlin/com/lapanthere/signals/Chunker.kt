package com.lapanthere.signals

/**
 * A chunker that returns values through its iterator.
 *
 * Each values indicates the size of a chunk to be uploaded or downloaded, each chunk,
 * the sum of each chunk size and the number of chunks has to comply to S3 limitations.
 *
 * @see [Amazon S3 multipart upload limits](https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html)
 *
 */
public fun interface Chunker {
    /**
     * Returns an [Iterator] that returns the each chunk size from the Chunker.
     */
    public fun iterator(): Iterator<Long>
}
