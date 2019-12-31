package com.lapanthere.signals

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import java.io.ByteArrayOutputStream
import java.util.concurrent.CompletableFuture
import kotlin.test.Test
import kotlin.test.assertFailsWith

class S3InputStreamTest {
    private val bucket = "bucket"
    private val key = "key"
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
                .build()
        )
        every {
            getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=0-5242879")
                    .build(), any<ByteArrayAsyncResponseTransformer<GetObjectResponse>>()
            )
        } returns CompletableFuture.completedFuture(
            ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), ByteArray(32))
        )
        every {
            getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=5242880-10491001")
                    .build(), any<ByteArrayAsyncResponseTransformer<GetObjectResponse>>()
            )
        } returns CompletableFuture.completedFuture(
            ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), ByteArray(32))
        )
    }

    @Test
    fun testDownload() {
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
                    .build(), any<ByteArrayAsyncResponseTransformer<GetObjectResponse>>()
            )
        }
        verify(exactly = 1) {
            s3.getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=5242880-10491001")
                    .build(), any<ByteArrayAsyncResponseTransformer<GetObjectResponse>>()
            )
        }
    }

    @Test
    fun testFailure() {
        every {
            s3.getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=0-5242879")
                    .build(), any<ByteArrayAsyncResponseTransformer<GetObjectResponse>>()
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
