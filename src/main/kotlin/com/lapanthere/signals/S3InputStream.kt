package com.lapanthere.signals

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
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
 *
 */
// TODO: Allow to pass others options.
class S3InputStream(
    bucket: String,
    key: String,
    parallelism: Int = AVAILABLE_PROCESSORS,
    s3: S3AsyncClient = S3AsyncClient.create()
) : InputStream() {
    private val scope = CoroutineScope(Dispatchers.IO)
    private val semaphore = Semaphore(parallelism)

    // Could be replaced by a Flow, once parallel execution is supported.
    private val streams by lazy {
        val size = s3.headObject(
            HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build()
        ).get().contentLength()
        val chunkSize = min(MIN_PART_SIZE, size)
        (0 until size step chunkSize).map { begin ->
            scope.async(Dispatchers.IO) {
                semaphore.acquire()
                s3.getObject(
                    GetObjectRequest.builder()
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
            return iterator.hasNext()
        }

        override fun nextElement(): InputStream = runBlocking {
            semaphore.release()
            val stream = iterator.next().await()
            iterator.remove() // Making sure the stream get released.
            stream
        }
    })

    override fun read(): Int {
        return buffer.read()
    }

    override fun close() {
        buffer.close()
    }
}
