@file:JvmMultifileClass

package com.lapanthere.signals

import software.amazon.awssdk.services.s3.model.CompletedPart
import java.io.ByteArrayOutputStream
import java.security.DigestOutputStream
import kotlin.math.min

public const val MIN_PART_SIZE: Long = 5_242_880L
public const val MAX_PART_SIZE: Long = 5_368_709_120L
private const val GROWTH_FACTOR: Long = 1530L

/**
 * DefaultChunker provides a [Chunker] that provides sane
 * defaults for small to medium file sizes.
 */
public class DefaultChunker : Chunker {
    override fun iterator(): Iterator<Long> = sequence {
        var chunkSize = MIN_PART_SIZE
        while (true) {
            yield(chunkSize)
            chunkSize = min(chunkSize + chunkSize / GROWTH_FACTOR, MAX_PART_SIZE)
        }
    }.iterator()
}

internal class SizeIterator(chunker: Chunker) {
    private val iterator = chunker.iterator()
    private var chunkSize = iterator.next()

    val value: Long
        get() = chunkSize

    fun next() {
        chunkSize = iterator.next()
    }
}

internal fun byteRange(chunker: Chunker, size: Long) = sequence {
    val iterator = chunker.iterator()
    var begin = 0L
    while (begin < size) {
        val chunkSize = iterator.next()
        yield(Pair(begin, begin + chunkSize - 1))
        begin += chunkSize
    }
}

internal data class Part(
    val uploadID: String,
    val partNumber: Int,
    val digest: ByteArray,
    val buffer: ByteArray,
) {
    val eTag: String = digest.toHex()
    val contentMD5: String = digest.encodeToString()

    constructor(
        uploadID: String,
        partNumber: Int,
        digest: DigestOutputStream,
        buffer: ByteArrayOutputStream,
    ) : this(
        uploadID,
        partNumber,
        digest.digest,
        buffer.toByteArray(),
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
