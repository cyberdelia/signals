package com.lapanthere.signals

import com.lapanthere.signals.transformers.InputStreamAsyncResponseTransformer
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.time.Instant
import java.util.concurrent.CompletableFuture
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

internal class S3InputStreamTest {
    private val bucket = "bucket"
    private val key = "key"
    private val date = Instant.now()
    private val s3: S3AsyncClient = mockk {
        every {
            headObject(
                HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
        } returns CompletableFuture.completedFuture(
            HeadObjectResponse.builder()
                .contentLength(6_291_456)
                .contentType("application/json")
                .contentEncoding("gzip")
                .contentDisposition("inline")
                .cacheControl("no-cache")
                .expires(date)
                .lastModified(date)
                .contentLanguage("de-DE")
                .versionId("L4kqtJlcpXroDTDmpUMLUo")
                .eTag("d41d8cd98f00b204e9800998ecf8427e-2")
                .build()
        )
        every {
            getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=0-5242879")
                    .build(),
                any<InputStreamAsyncResponseTransformer>()
            )
        } returns CompletableFuture.completedFuture(
            ByteArrayInputStream(ByteArray(32))
        )
        every {
            getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=5242880-10489185")
                    .build(),
                any<InputStreamAsyncResponseTransformer>()
            )
        } returns CompletableFuture.completedFuture(
            ByteArrayInputStream(ByteArray(32))
        )
    }

    @Test
    fun `download a file`() {
        ByteArrayOutputStream().use { target ->
            S3InputStream(bucket = bucket, key = key, s3 = s3).use { stream ->
                stream.copyTo(target)
            }
        }
        verify(exactly = 1) {
            s3.headObject(
                HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
        }
        verify(exactly = 1) {
            s3.getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=0-5242879")
                    .build(),
                any<ByteArrayAsyncResponseTransformer<GetObjectResponse>>()
            )
        }
        verify(exactly = 1) {
            s3.getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=5242880-10489185")
                    .build(),
                any<ByteArrayAsyncResponseTransformer<GetObjectResponse>>()
            )
        }

        confirmVerified(s3)
    }

    @Test
    fun `provides metadata`() {
        val stream = S3InputStream(bucket = bucket, key = key, s3 = s3)
        verify(exactly = 1) {
            s3.headObject(
                HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
        }
        assertEquals("application/json", stream.contentType)
        assertEquals("gzip", stream.contentEncoding)
        assertEquals("inline", stream.contentDisposition)
        assertEquals("de-DE", stream.contentLanguage)
        assertEquals(6_291_456, stream.contentLength)
        assertEquals(date, stream.lastModified)
        assertEquals(date, stream.expires)
        assertEquals("L4kqtJlcpXroDTDmpUMLUo", stream.versionId)
        assertEquals("d41d8cd98f00b204e9800998ecf8427e-2", stream.eTag)
        assertEquals("no-cache", stream.cacheControl)
        assertEquals(emptyMap(), stream.metadata)
    }

    @Test
    fun `downloading starts when reading starts`() {
        S3InputStream(bucket = bucket, key = key, s3 = s3)
        verify(exactly = 1) {
            s3.headObject(
                HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
        }
        verify(exactly = 0) {
            s3.getObject(any<GetObjectRequest>(), any<ByteArrayAsyncResponseTransformer<GetObjectResponse>>())
        }
        confirmVerified(s3)
    }

    @Test
    fun `handle exception on failure`() {
        every {
            s3.getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=0-5242879")
                    .build(),
                any<ByteArrayAsyncResponseTransformer<GetObjectResponse>>()
            )
        } throws SdkClientException.create("read timeout")

        assertFailsWith<SdkClientException> {
            ByteArrayOutputStream().use { target ->
                S3InputStream(bucket = bucket, key = key, s3 = s3).use { stream ->
                    stream.copyTo(target)
                }
            }
        }
    }
}
