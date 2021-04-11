package com.lapanthere.signals

import com.lapanthere.signals.transformers.InputStreamAsyncResponseTransformer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.future.await
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import java.io.IOException
import java.io.InputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.time.Instant

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
    mutator: (GetObjectRequest.Builder) -> Unit = {}
) : InputStream() {
    private val s3Object = s3.headObject(
        HeadObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .build()
    ).get()
    private val scope = CoroutineScope(Dispatchers.IO)
    private val parts = byteRange(chunker, s3Object.contentLength())
    private val pipe = PipedInputStream()
    private val pipeline = flow {
        parts.forEach { (begin, end) ->
            val inputStream = s3.getObject(
                GetObjectRequest.builder()
                    .applyMutation(mutator)
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=$begin-$end")
                    .build(),
                InputStreamAsyncResponseTransformer()
            ).await()
            emit(inputStream)
        }
    }

    private var cancellationException: Throwable? = null

    init {
        val outputStream = PipedOutputStream(pipe)
        pipeline.buffer(parallelism)
            .onEach { it.copyTo(outputStream) }
            .catch { cancellationException = it }
            .onCompletion { outputStream.close() }
            .launchIn(scope)
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

    override fun read(): Int = run {
        pipe.read()
    }

    override fun read(b: ByteArray, off: Int, len: Int): Int = run {
        pipe.read(b, off, len)
    }

    override fun close(): Unit = finally {
        pipe.close()
    }

    private inline fun finally(block: () -> Unit) {
        block()
        if (cancellationException != null) {
            throw IOException(cancellationException)
        }
    }

    private inline fun <T> run(block: () -> T): T {
        if (cancellationException != null) {
            throw IOException(cancellationException)
        }
        return block()
    }
}

internal inline fun <T, R> MutableIterator<T>.use(block: (T) -> R): R {
    try {
        return block(this.next())
    } finally {
        this.remove()
    }
}
