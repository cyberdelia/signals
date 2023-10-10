package com.lapanthere.signals.transformers

import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import java.io.IOException
import java.io.OutputStream
import java.nio.ByteBuffer
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

internal class InputStreamAsyncResponseTransformerTest {
    @Test
    fun `stream error are propagated`() {
        val transformer = InputStreamAsyncResponseTransformer()
        val future = transformer.prepare()
        transformer.onResponse(GetObjectResponse.builder().build())
        transformer.onStream { subscriber: Subscriber<in ByteBuffer> ->
            subscriber.onSubscribe(
                object : Subscription {
                    override fun request(l: Long) {}

                    override fun cancel() {}
                },
            )
            subscriber.onError(RuntimeException("unexpected exception"))
        }
        val inputStream = future.get()
        assertFailsWith(IOException::class) {
            inputStream.use {
                it.copyTo(nullOutputStream)
            }
        }
    }

    @Test
    fun `completes exceptionally if request failed`() {
        val transformer = InputStreamAsyncResponseTransformer()
        val future = transformer.prepare()
        transformer.exceptionOccurred(RuntimeException("unexpected exception"))
        assertTrue(future.isCompletedExceptionally)
    }

    @Test
    fun `propagates bytes to the reader`() {
        val transformer = InputStreamAsyncResponseTransformer()
        val future = transformer.prepare()
        transformer.onResponse(GetObjectResponse.builder().build())
        transformer.onStream { subscriber: Subscriber<in ByteBuffer> ->
            subscriber.onSubscribe(
                object : Subscription {
                    override fun request(l: Long) {
                        subscriber.onNext(ByteBuffer.wrap(Random.nextBytes(30_000)))
                        subscriber.onComplete()
                    }

                    override fun cancel() {}
                },
            )
        }
        val inputStream = future.get()
        inputStream.use {
            val bytes = it.readBytes()
            assertTrue(bytes.isNotEmpty())
            assertEquals(30_000, bytes.size)
        }
    }
}

internal val nullOutputStream =
    object : OutputStream() {
        override fun write(b: Int) {}
    }
