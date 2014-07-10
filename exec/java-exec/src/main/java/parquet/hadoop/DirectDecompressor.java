package parquet.hadoop;


import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Specification of a direct ByteBuffer 'de-compressor'.
 */
public interface DirectDecompressor {
  /*
   * From org.apache.hadoop.io.compress.DirectDecompressor
   *
   * This exposes a direct interface for record decompression with direct byte
   * buffers.
   *
   * The decompress() function need not always consume the buffers provided,
   * it will need to be called multiple times to decompress an entire buffer
   * and the object will hold the compression context internally.
   *
   * Codecs such as {@link SnappyCodec} may or may not support partial
   * decompression of buffers and will need enough space in the destination
   * buffer to decompress an entire block.
   *
   * The operation is modelled around dst.put(src);
   *
   * The end result will move src.position() by the bytes-read and
   * dst.position() by the bytes-written. It should not modify the src.limit()
   * or dst.limit() to maintain consistency of operation between codecs.
   *
   * @param src Source direct {@link ByteBuffer} for reading from. Requires src
   * != null and src.remaining() > 0
   *
   * @param dst Destination direct {@link ByteBuffer} for storing the results
   * into. Requires dst != null and dst.remaining() to be > 0
   *
   * @throws IOException if compression fails
   */
  public void decompress(ByteBuffer src, ByteBuffer dst) throws IOException;
}