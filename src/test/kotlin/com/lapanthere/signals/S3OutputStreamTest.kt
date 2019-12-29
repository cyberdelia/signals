package com.lapanthere.signals

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import software.amazon.awssdk.services.s3.model.UploadPartResponse
import java.io.ByteArrayInputStream
import java.util.concurrent.CompletableFuture
import kotlin.test.Test
import kotlin.test.assertFailsWith

class S3OutputStreamTest {
    private val uploadID = "upload-id"
    private val bucket = "bucket"
    private val key = "key"
    private val s3: S3AsyncClient = mockk {
        every {
            createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()
            )
        } returns CompletableFuture.completedFuture(
            CreateMultipartUploadResponse.builder()
                .uploadId(uploadID)
                .build()
        )
        every {
            uploadPart(
                UploadPartRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadID)
                    .contentLength(32)
                    .contentMD5("cLyPS3KoaSFGi/joRB3OUQ==")
                    .partNumber(1)
                    .build(), any<AsyncRequestBody>()
            )
        } returns CompletableFuture.completedFuture(
            UploadPartResponse.builder()
                .eTag("70bc8f4b72a86921468bf8e8441dce51")
                .build()
        )
        every {
            completeMultipartUpload(
                CompleteMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadID)
                    .multipartUpload(
                        CompletedMultipartUpload.builder()
                            .parts(
                                CompletedPart.builder()
                                    .partNumber(1)
                                    .eTag("70bc8f4b72a86921468bf8e8441dce51")
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
        } returns CompletableFuture.completedFuture(
            CompleteMultipartUploadResponse.builder()
                .bucket(bucket)
                .key(key)
                .eTag("057ab97180cd57d1c51ff5280884cbf8-1")
                .build()
        )
        every {
            abortMultipartUpload(
                AbortMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadID)
                    .build()
            )
        } returns CompletableFuture.completedFuture(
            AbortMultipartUploadResponse.builder()
                .build()
        )
    }

    @Test
    fun testWrite() {
        ByteArrayInputStream(ByteArray(32)).use { target ->
            S3OutputStream(bucket = bucket, key = key, s3 = s3).use { stream ->
                target.copyTo(stream)
            }
        }
        verify(exactly = 0) {
            s3.abortMultipartUpload(
                AbortMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadID)
                    .build()
            )
        }
    }

    @Test
    fun testFailure() {
        every {
            s3.uploadPart(
                UploadPartRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadID)
                    .contentLength(32)
                    .contentMD5("cLyPS3KoaSFGi/joRB3OUQ==")
                    .partNumber(1)
                    .build(), any<AsyncRequestBody>()
            )
        } throws SdkClientException.create("write timeout")

        assertFailsWith<SdkClientException> {
            ByteArrayInputStream(ByteArray(32)).use { target ->
                S3OutputStream(bucket = bucket, key = key, s3 = s3).use { stream ->
                    target.copyTo(stream)
                }
            }
        }

        verify(exactly = 1) {
            s3.abortMultipartUpload(
                AbortMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadID)
                    .build()
            )
        }
    }
}
