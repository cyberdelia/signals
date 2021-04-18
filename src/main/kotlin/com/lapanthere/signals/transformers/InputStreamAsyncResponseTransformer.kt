package com.lapanthere.signals.transformers

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.reactive.asFlow
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import java.io.IOException
import java.io.InputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.nio.ByteBuffer
import java.util.Arrays
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.cancellation.CancellationException

public class InputStreamAsyncResponseTransformer(
    override val coroutineContext: CoroutineContext = Dispatchers.IO
) : AsyncResponseTransformer<GetObjectResponse, InputStream>, CoroutineScope {
    private val future = CompletableFuture<InputStream>()
    private val pipe = PipedOutputStream()
    private lateinit var pipeline: Job

    override fun prepare(): CompletableFuture<InputStream> = future

    override fun onResponse(response: GetObjectResponse) {}

    override fun onStream(publisher: SdkPublisher<ByteBuffer>) {
        val inputStream = CancellablePipedInputStream(pipe)
        pipeline = publisher.asFlow()
            .onEach { pipe.write(it.toByteArray()) }
            .catch { inputStream.cancel(it) }
            .onCompletion {
                pipe.flush()
                pipe.close()
            }
            .launchIn(this)
        future.complete(inputStream)
    }

    override fun exceptionOccurred(error: Throwable) {
        future.completeExceptionally(error)
        if (::pipeline.isInitialized) {
            pipeline.cancel(CancellationException(cause = error))
        }
    }
}

internal class CancellablePipedInputStream(
    private val inputStream: PipedInputStream
) : InputStream() {
    constructor(pipedOutputStream: PipedOutputStream) : this(PipedInputStream(pipedOutputStream))

    private var cancellationException: Throwable? = null

    internal fun cancel(throwable: Throwable) {
        cancellationException = throwable
    }

    private inline fun <T> run(block: () -> T): T {
        if (cancellationException != null) {
            throw IOException(cancellationException)
        }
        return block()
    }

    private inline fun finally(block: () -> Unit) {
        block()
        if (cancellationException != null) {
            throw IOException(cancellationException)
        }
    }

    override fun available(): Int = run {
        inputStream.available()
    }

    override fun read(b: ByteArray): Int = run {
        inputStream.read(b)
    }

    override fun read(b: ByteArray, off: Int, len: Int): Int = run {
        inputStream.read(b, off, len)
    }

    override fun read(): Int = run {
        inputStream.read()
    }

    override fun skip(n: Long): Long = run {
        inputStream.skip(n)
    }

    override fun close() = finally {
        inputStream.close()
    }
}

internal fun ByteBuffer.toByteArray(): ByteArray {
    if (hasArray()) {
        return Arrays.copyOfRange(
            array(),
            arrayOffset() + position(),
            arrayOffset() + limit()
        )
    }

    val bytes = ByteArray(remaining())
    asReadOnlyBuffer().get(bytes)
    return bytes
}
