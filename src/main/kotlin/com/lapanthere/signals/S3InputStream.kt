package com.lapanthere.signals

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import java.io.InputStream
import java.io.SequenceInputStream
import java.util.Enumeration
import kotlin.math.min

internal val AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors()

/**
 * Download a file from S3 using multi-part download.
 *
 * @param bucket The name of the bucket containing the object.
 * @param key Key of the object to download.
 * @param parallelism The number of parts to download at a time.
 * @param s3 The S3 client to be used during the download.
 * @param mutator The function that mutates the request given to the S3 client.
 *
 */
class S3InputStream(
    bucket: String,
    key: String,
    parallelism: Int = AVAILABLE_PROCESSORS,
    s3: S3AsyncClient = S3AsyncClient.create(),
    mutator: (GetObjectRequest.Builder) -> Unit = {}
) : InputStream() {
    private val scope = CoroutineScope(Dispatchers.IO)
    private val streams by lazy {
        val size = s3.headObject(
            HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build()
        ).get().contentLength()
        val chunkSize = min(MIN_PART_SIZE, size)
        (0 until size step chunkSize).mapIndexed { i, begin ->
            scope.async(CoroutineName("chunk-${i + 1}"), CoroutineStart.LAZY) {
                s3.getObject(
                    GetObjectRequest.builder()
                        .applyMutation(mutator)
                        .bucket(bucket)
                        .key(key)
                        .range("bytes=$begin-${begin + chunkSize - 1}")
                        .build(), AsyncResponseTransformer.toBytes()
                ).await().asInputStream()
            }
        }.toMutableList()
    }
    private val buffer = SequenceInputStream(object : Enumeration<InputStream> {
        private val iterator = streams.iterator()

        override fun hasMoreElements(): Boolean {
            // Starts downloading the next chunks ahead.
            streams.take(parallelism).forEach { it.start() }
            return iterator.hasNext()
        }

        override fun nextElement(): InputStream = runBlocking {
            iterator.use { it.await() }
        }
    })

    override fun read(): Int {
        return buffer.read()
    }

    override fun close() {
        buffer.close()
    }
}

internal inline fun <T, R> MutableIterator<T>.use(block: (T) -> R): R {
    try {
        return block(this.next())
    } finally {
        this.remove()
    }
}
