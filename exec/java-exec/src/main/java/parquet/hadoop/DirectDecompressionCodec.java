package parquet.hadoop;


import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * This class encapsulates a codec which can decompress direct bytebuffers.
 * From org.apache.hadoop.io.compress.DirectDecompressionCodec
 */

public interface DirectDecompressionCodec extends CompressionCodec {
  /**
   * Create a new {@link DirectDecompressor} for use by this {@link DirectDecompressionCodec}.
   *
   * @return a new direct decompressor for use by this codec
   */
  DirectDecompressor createDirectDecompressor();
}
