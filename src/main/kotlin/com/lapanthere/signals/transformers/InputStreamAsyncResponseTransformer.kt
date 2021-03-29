package com.lapanthere.signals.transformers

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.reactive.asFlow
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import java.io.InputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.CoroutineContext

public class InputStreamAsyncResponseTransformer :
    AsyncResponseTransformer<GetObjectResponse, InputStream>,
    CoroutineScope {
    private val future = CompletableFuture<InputStream>()
    private val pipe = PipedOutputStream()

    override fun prepare(): CompletableFuture<InputStream> = future

    override fun onResponse(response: GetObjectResponse) {}

    override fun onStream(publisher: SdkPublisher<ByteBuffer>) {
        future.complete(PipedInputStream(pipe))
        publisher.asFlow()
            .onEach { pipe.write(it.array()) }
            .onCompletion { pipe.close() }
            .launchIn(this)
    }

    override fun exceptionOccurred(error: Throwable) {
        future.completeExceptionally(error)
    }

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO
}
