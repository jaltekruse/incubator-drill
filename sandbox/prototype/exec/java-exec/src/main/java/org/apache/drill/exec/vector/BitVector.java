package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;

import java.util.Random;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;
/**
 * Bit implements a vector of bit-width values.  Elements in the vector are accessed
 * by position from the logical start of the vector.
 *   The width of each element is 1 bit.
 *   The equivalent Java primitive is an int containing the value '0' or '1'.
 *
 * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
 */
public final class BitVector extends BaseDataValueVector implements FixedWidthVector{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitVector.class);

  private int valueCapacity;
  
  public BitVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  private int getSizeFromCount(int valueCount) {
    return (int) Math.ceil(valueCount / 8.0);
  }
  
  /**
   * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
   *
   * @param valueCount  The number of values which can be contained within this vector.
   */
  public void allocateNew(int valueCount) {
    clear();
    valueCapacity = valueCount;
    int valueSize = getSizeFromCount(valueCount);
    data = allocator.buffer(valueSize);
    for (int i = 0; i < getSizeFromCount(valueCount); i++) {
      data.setByte(i, 0);
    }
  }
  
  @Override
  public int load(int valueCount, ByteBuf buf){
    clear();
    this.recordCount = valueCount;
    int len = getSizeFromCount(valueCount);
    data = buf.slice(0, len);
    data.retain();
    return len;
  }
  
  @Override
  public void load(FieldMetadata metadata, ByteBuf buffer) {
    assert this.field.getDef().equals(metadata.getDef());
    int loaded = load(metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }
  
  @Override
  public int getValueCapacity() {
    return valueCapacity;
  }

  public Mutator getMutator() {
    return new Mutator();
  }

  public Accessor getAccessor(){
    return new Accessor();
  }

  @Override
  public FieldMetadata getMetadata() {
    return FieldMetadata.newBuilder()
        .setDef(getField().getDef())
        .setValueCount(recordCount)
        .setBufferLength((int) Math.ceil(recordCount / 8.0))
        .build();
  }
  
  
  public class Accessor extends BaseAccessor{

    /**
     * Get the byte holding the desired bit, then mask all other bits.  Iff the result is 0, the
     * bit was not set.
     *
     * @param  index   position of the bit in the vector
     * @return 1 if set, otherwise 0
     */
    public int get(int index) {
      // logger.debug("BIT GET: index: {}, byte: {}, mask: {}, masked byte: {}",
      //             index,
      //             data.getByte((int)Math.floor(index/8)),
      //             (int)Math.pow(2, (index % 8)),
      //             data.getByte((int)Math.floor(index/8)) & (int)Math.pow(2, (index % 8)));
      return ((data.getByte((int)Math.floor(index/8)) & (int)Math.pow(2, (index % 8))) == 0) ? 0 : 1;
    }
    
    @Override
    public Object getObject(int index) {
      return new Boolean(get(index) != 0);
    }
    
    public int getRecordCount() {
      return recordCount;
    }
    
  }
  
  /**
   * MutableBit implements a vector of bit-width values.  Elements in the vector are accessed
   * by position from the logical start of the vector.  Values should be pushed onto the vector
   * sequentially, but may be randomly accessed.
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public class Mutator extends BaseMutator{

    private Mutator(){}
    
    /**
     * Set the bit at the given index to the specified value.
     *
     * @param index   position of the bit to set
     * @param value   value to set (either 1 or 0)
     */
    public void set(int index, int value) {
      byte currentByte = data.getByte((int)Math.floor(index/8));
      if (value != 0) {
        // true
        currentByte |= (byte) Math.pow(2, (index % 8));
      }
      else if ((currentByte & (byte) Math.pow(2, (index % 8))) == (byte) Math.pow(2, (index % 8))) {
        // false, and bit was previously set
        currentByte -= (byte) Math.pow(2, (index % 8));
      }
      data.setByte((int) Math.floor(index/8), currentByte);
    }

    public void setValueCount(int recordCount) {
      BitVector.this.recordCount = recordCount;
      data.writerIndex(getSizeFromCount(recordCount));
    }

    @Override
    public void randomizeData() {
      if (data != DeadBuf.DEAD_BUFFER) {
        Random r = new Random();
        for (int i = 0; i < data.capacity() - 1; i++) {
          byte[] bytes = new byte[1];
          r.nextBytes(bytes);
          data.setByte(i, bytes[0]);
        }
      }
    }

  }
}