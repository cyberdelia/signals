@file:JvmMultifileClass
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
import kotlin.math.min

internal const val MIN_PART_SIZE: Long = 5_242_880L
internal const val MAX_PART_SIZE: Long = 5_368_709_120L

internal data class Part(
    val uploadID: String,
    val partNumber: Int,
    val digest: ByteArray,
    val buffer: ByteArray
) {
    val eTag: String = digest.toHex()
    val contentMD5: String = digest.encodeToString()

    constructor(uploadID: String, partNumber: Int, digest: DigestOutputStream, buffer: ByteArrayOutputStream) : this(
        uploadID,
        partNumber,
        digest.digest,
        buffer.toByteArray()
    )

    fun toCompletedPart(): CompletedPart = CompletedPart.builder()
        .eTag(eTag)
        .partNumber(partNumber)
        .build()

    override fun toString(): String {
        return "Part(uploadID=$uploadID, partNumber=$partNumber, eTag=$eTag}"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Part

        if (uploadID != other.uploadID) return false
        if (partNumber != other.partNumber) return false
        if (!digest.contentEquals(other.digest)) return false
        if (!buffer.contentEquals(other.buffer)) return false
        if (eTag != other.eTag) return false
        if (contentMD5 != other.contentMD5) return false

        return true
    }

    override fun hashCode(): Int {
        var result = uploadID.hashCode()
        result = 31 * result + partNumber
        result = 31 * result + digest.contentHashCode()
        result = 31 * result + buffer.contentHashCode()
        result = 31 * result + eTag.hashCode()
        result = 31 * result + contentMD5.hashCode()
        return result
    }
}

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
class S3OutputStream(
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
    private var partSize: Long = MIN_PART_SIZE
    private val uploadID = s3.createMultipartUpload(
        CreateMultipartUploadRequest.builder()
            .applyMutation(mutator)
            .bucket(bucket)
            .key(key)
            .build()
    ).get().uploadId()

    override fun write(b: Int) {
        digest.write(b)
        if (buffer.size() >= partSize) {
            uploadPart()
        }
    }

    private fun uploadPart() = runBlocking {
        semaphore.acquire()
        partSize = min(partSize + partSize / 1000, MAX_PART_SIZE)
        val part = Part(uploadID, parts.size + 1, digest, buffer)
        parts.add(scope.async(CoroutineName("part-${part.partNumber}")) {
            val response = s3.uploadPart(
                UploadPartRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .partNumber(part.partNumber)
                    .uploadId(uploadID)
                    .contentMD5(part.contentMD5)
                    .contentLength(part.buffer.size.toLong())
                    .build(), AsyncRequestBody.fromBytes(part.buffer)
            ).await()
            if (response.eTag != part.eTag) {
                throw IOException("mismatching checksum: ${response.eTag} != ${part.eTag}")
            }
            semaphore.release()
            part.toCompletedPart()
        })
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
