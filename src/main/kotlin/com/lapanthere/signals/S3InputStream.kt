package com.lapanthere.signals

import com.lapanthere.signals.transformers.InputStreamAsyncResponseTransformer
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import java.io.InputStream
import java.io.SequenceInputStream
import java.time.Instant
import java.util.Enumeration
import kotlin.coroutines.CoroutineContext

internal val AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors()

/**
 * Download a file from S3 using multi-part download.
 *
 * @param bucket The name of the bucket containing the object.
 * @param key Key of the object to download.
 * @param parallelism The number of parts to download at a time.
 * @param s3 A [software.amazon.awssdk.services.s3.S3AsyncClient] to be used during the download.
 * @param chunker A [Chunker] that provides each chunk sizes.
 * @param mutator A function that mutates the request given to the S3 client.
 *
 */
public class S3InputStream(
    bucket: String,
    key: String,
    parallelism: Int = AVAILABLE_PROCESSORS,
    s3: S3AsyncClient = S3AsyncClient.create(),
    chunker: Chunker = DefaultChunker(),
    mutator: (GetObjectRequest.Builder) -> Unit = {},
) : InputStream(), CoroutineScope {
    private val s3Object =
        s3.headObject(
            HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build(),
        ).get()
    private val parts = byteRange(chunker, s3Object.contentLength())
    private val streams =
        parts.mapIndexed { i, (begin, end) ->
            async(CoroutineName("chunk-${i + 1}"), CoroutineStart.LAZY) {
                s3.getObject(
                    GetObjectRequest.builder()
                        .applyMutation(mutator)
                        .bucket(bucket)
                        .key(key)
                        .range("bytes=$begin-$end")
                        .build(),
                    InputStreamAsyncResponseTransformer(),
                ).await()
            }
        }.toMutableList()
    private val buffer: SequenceInputStream by lazy {
        SequenceInputStream(
            object : Enumeration<InputStream> {
                private val iterator = streams.iterator()

                override fun hasMoreElements(): Boolean {
                    // Starts downloading the next chunks ahead.
                    streams.take(parallelism).forEach { it.start() }
                    return iterator.hasNext()
                }

                override fun nextElement(): InputStream =
                    runBlocking {
                        iterator.use { it.await() }
                    }
            },
        )
    }

    public val eTag: String? = s3Object.eTag()
    public val contentLength: Long? = s3Object.contentLength()
    public val lastModified: Instant? = s3Object.lastModified()
    public val metadata: Map<String, String> = s3Object.metadata()
    public val contentType: String? = s3Object.contentType()
    public val contentEncoding: String? = s3Object.contentEncoding()
    public val contentDisposition: String? = s3Object.contentDisposition()
    public val contentLanguage: String? = s3Object.contentLanguage()
    public val versionId: String? = s3Object.versionId()
    public val cacheControl: String? = s3Object.cacheControl()
    public val expires: Instant? = s3Object.expires()

    override fun read(): Int = buffer.read()

    override fun close() {
        buffer.close()
    }

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO
}

internal inline fun <T, R> MutableIterator<T>.use(block: (T) -> R): R {
    try {
        return block(this.next())
    } finally {
        this.remove()
    }
}
