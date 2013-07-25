package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;

public abstract class BaseDataValueVector extends BaseValueVector{

  public ByteBuf data = DeadBuf.DEAD_BUFFER;
  protected int valueCount;
  
  public BaseDataValueVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    
  }

  /**
   * Release the underlying ByteBuf and reset the ValueVector
   */
  @Override
  public void clear() {
    if (data != DeadBuf.DEAD_BUFFER) {
      data.release();
      data = DeadBuf.DEAD_BUFFER;
      valueCount = 0;
    }
  }

  public int getValueCount(){
    return valueCount;
  }

  public void setValueCount(int count){
    valueCount = count;
  }
  
  @Override
  public ByteBuf[] getBuffers(){
    return new ByteBuf[]{data};
  }
  
  public int getBufferSize() {
    return data.writerIndex();
  }

  @Override
  public FieldMetadata getMetadata() {
    return null;
  }
  
  
}
