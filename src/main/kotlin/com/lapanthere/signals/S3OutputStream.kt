package com.lapanthere.signals

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import software.amazon.awssdk.services.s3.model.UploadPartResponse
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.OutputStream
import java.security.DigestOutputStream
import java.security.MessageDigest
import java.util.Base64

/**
 *
 * Upload a file to S3 using multi-part upload.
 *
 * @param bucket The name of the bucket to which to initiate the upload.
 * @param key Object key for which the multipart upload is to be initiated.
 * @param parallelism The number of parts to upload at a time.
 * @param s3 The S3 client to be used during the upload.
 * @param mutator The function that mutates the request given to the S3 client.
 *
 */
public class S3OutputStream(
    private val bucket: String,
    private val key: String,
    parallelism: Int = AVAILABLE_PROCESSORS,
    private val s3: S3AsyncClient = S3AsyncClient.create(),
    mutator: (CreateMultipartUploadRequest.Builder) -> Unit = {}
) : OutputStream() {
    private val scope = CoroutineScope(Dispatchers.IO)
    private val semaphore = Semaphore(parallelism)
    private val parts = mutableListOf<Deferred<CompletedPart>>()
    private val buffer = ByteArrayOutputStream(MIN_PART_SIZE.toInt())
    private val digest = DigestOutputStream(buffer, MessageDigest.getInstance("MD5"))
    private val partSize = SizeIterator()
    private val uploadID = s3.createMultipartUpload(
        CreateMultipartUploadRequest.builder()
            .applyMutation(mutator)
            .bucket(bucket)
            .key(key)
            .build()
    ).get().uploadId()

    override fun write(b: Int) {
        digest.write(b)
        if (buffer.size() >= partSize.value) {
            uploadPart()
        }
    }

    private fun uploadPart() = runBlocking {
        semaphore.acquire()
        partSize.next()
        val part = Part(uploadID, parts.size + 1, digest, buffer)
        parts.add(
            scope.async(CoroutineName("part-${part.partNumber}")) {
                val response = s3.uploadPart(
                    UploadPartRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .partNumber(part.partNumber)
                        .uploadId(uploadID)
                        .contentMD5(part.contentMD5)
                        .contentLength(part.buffer.size.toLong())
                        .build(),
                    AsyncRequestBody.fromBytes(part.buffer)
                ).await()
                if (response.eTag != part.eTag) {
                    throw IOException("mismatching checksum: ${response.eTag} != ${part.eTag}")
                }
                semaphore.release()
                part.toCompletedPart()
            }
        )
        buffer.reset()
    }

    override fun close() {
        uploadPart()
        complete()
        digest.close()
    }

    private fun complete() = runBlocking {
        try {
            val parts = parts.awaitAll()
            val response = s3.completeMultipartUpload(
                CompleteMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadID)
                    .multipartUpload(
                        CompletedMultipartUpload.builder()
                            .parts(parts)
                            .build()
                    )
                    .build()
            ).await()
            if (response.eTag != parts.eTag) {
                throw IOException("mismatching checksum: ${response.eTag} != ${parts.eTag}")
            }
            if (response.count != parts.size) {
                throw IOException("unexpected parts count: ${response.count} != ${parts.size}")
            }
        } catch (e: Exception) {
            abort()
            throw e
        }
    }

    private suspend fun abort() {
        s3.abortMultipartUpload(
            AbortMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadID)
                .build()
        ).await()
    }
}

internal val DigestOutputStream.digest: ByteArray
    get() = this.messageDigest.digest()

internal fun ByteArray.encodeToString(): String {
    return Base64.getEncoder().encodeToString(this)
}

internal fun ByteArray.toHex(): String {
    return this.joinToString("") { "%02x".format(it) }
}

internal val UploadPartResponse.eTag: String
    get() = this.eTag().trim('"')

internal val CompleteMultipartUploadResponse.eTag: String
    get() = this.eTag().trim('"').split('-').first()

internal val CompleteMultipartUploadResponse.count: Int
    get() = this.eTag().trim('"').split('-').last().toInt()

internal val List<CompletedPart>.eTag: String
    get() {
        val digest = MessageDigest.getInstance("MD5")
        this.forEach { digest.update(it.digest) }
        return digest.digest().toHex()
    }

internal val CompletedPart.digest: ByteArray
    get() = this.eTag().chunked(2).map { it.toUpperCase().toInt(16).toByte() }.toByteArray()
