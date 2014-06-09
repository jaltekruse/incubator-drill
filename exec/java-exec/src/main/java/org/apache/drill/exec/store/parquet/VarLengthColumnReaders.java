/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.exec.store.parquet;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.DecimalUtility;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal28SparseHolder;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.format.ConvertedType;
import parquet.column.Encoding;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.io.api.Binary;

import java.io.IOException;
import java.math.BigDecimal;

public class VarLengthColumnReaders {

  public static abstract class VarLengthColumn<V extends ValueVector> extends ColumnReader {

    Binary currDictVal;

    VarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v,
                    SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
        usingDictionary = true;
      }
      else {
        usingDictionary = false;
      }
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
      boolean success = setSafe(valuesReadInCurrentPass, pageReadStatus.pageDataByteArray,
          (int) pageReadStatus.readPosInBytes + 4, dataTypeLengthInBits);
      assert success;
      pageReadStatus.readPosInBytes += dataTypeLengthInBits + 4;
      bytesReadInCurrentPass += dataTypeLengthInBits + 4;
      pageReadStatus.valuesRead++;
      valuesReadInCurrentPass++;
    }

    public abstract boolean setSafe(int index, byte[] bytes, int start, int length);

    public abstract int capacity();

    public void reset() {
      bytesReadInCurrentPass = 0;
      valuesReadInCurrentPass = 0;
    }
    /**
     * Determines the size of a single value in a variable column.
     *
     * Return value indicates if we have finished a row group and should stop reading
     *
     * @param recordsReadInCurrentPass
     * @param lengthVarFieldsInCurrentRecord
     * @return
     * @throws IOException
     */
    public boolean determineSize(long recordsReadInCurrentPass, Integer lengthVarFieldsInCurrentRecord) throws IOException {

      if (recordsReadInCurrentPass == valueVec.getValueCapacity()){
        return true;
      }
      if (pageReadStatus.currentPage == null
          || pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
        totalValuesRead += pageReadStatus.valuesRead;
        if (!pageReadStatus.next()) {
          return true;
        }
      }

      // re-purposing this field here for length in BYTES to prevent repetitive multiplication/division
      dataTypeLengthInBits = BytesUtils.readIntLittleEndian(pageReadStatus.pageDataByteArray,
          (int) pageReadStatus.readPosInBytes);
      lengthVarFieldsInCurrentRecord += dataTypeLengthInBits;

      if (bytesReadInCurrentPass + dataTypeLengthInBits > capacity()) {
        // TODO - determine if we need to add this back somehow
        //break outer;
        return false;
      }
      return false;
    }

  }

  public static abstract class NullableVarLengthColumn<V extends ValueVector> extends VarLengthColumn<V> {

    int nullsRead;
    boolean currentValNull = false;
    Binary currDictVal;

    NullableVarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v,
                            SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    }

    public abstract boolean setSafe(int index, byte[] value, int start, int length);

    public abstract int capacity();

    public void reset() {
      bytesReadInCurrentPass = 0;
      valuesReadInCurrentPass = 0;
      nullsRead = 0;
    }

    public boolean determineSize(long recordsReadInCurrentPass, Integer lengthVarFieldsInCurrentRecord) throws IOException {
      // check to make sure there is capacity for the next value (for nullables this is a check to see if there is
      // still space in the nullability recording vector)
      if (recordsReadInCurrentPass == valueVec.getValueCapacity()){
        return true;
      }
      if (pageReadStatus.currentPage == null
          || pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
        if (!pageReadStatus.next()) {
          return true;
        } else {
          currDictVal = null;
        }
      }
      // we need to read all of the lengths to determine if this value will fit in the current vector,
      // as we can only read each definition level once, we have to store the last one as we will need it
      // at the start of the next read if we decide after reading all of the varlength values in this record
      // that it will not fit in this batch
      if ( currDefLevel == -1 ) {
        currDefLevel = pageReadStatus.definitionLevels.readInteger();
      }
      if ( columnDescriptor.getMaxDefinitionLevel() > currDefLevel){
        currentValNull = true;
        dataTypeLengthInBits = 0;
        nullsRead++;
        return false;// field is null, no length to add to data vector
      }

      if (usingDictionary) {
        if (currDictVal == null) {
          currDictVal = pageReadStatus.valueReader.readBytes();
        }
        // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
        dataTypeLengthInBits = currDictVal.length();
      }
      else {
        // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
        dataTypeLengthInBits = BytesUtils.readIntLittleEndian(pageReadStatus.pageDataByteArray,
            (int) pageReadStatus.readPosInBytes);
      }
      lengthVarFieldsInCurrentRecord += dataTypeLengthInBits;

      if (bytesReadInCurrentPass + dataTypeLengthInBits > capacity()) {
        // TODO - determine if we need to add this back somehow
        //break outer;
        return false;
      }
      return false;
    }
    
    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
      if (!currentValNull && dataTypeLengthInBits > 0){
        boolean success = setSafe(valuesReadInCurrentPass, pageReadStatus.pageDataByteArray,
            (int) pageReadStatus.readPosInBytes + 4, dataTypeLengthInBits);
        assert success;
      }
      currentValNull = false;
      currDefLevel = -1;
      if (dataTypeLengthInBits > 0){
        pageReadStatus.readPosInBytes += dataTypeLengthInBits + 4;
        bytesReadInCurrentPass += dataTypeLengthInBits + 4;
      }
      pageReadStatus.valuesRead++;
      valuesReadInCurrentPass++;
      if ( pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
        totalValuesRead += pageReadStatus.valuesRead;
        // I do not believe this is needed
        //pageReadStatus.next();
      }
      currDictVal = null;
    }
  }

  public static class Decimal28Column extends VarLengthColumn<Decimal28SparseVector> {

    protected Decimal28SparseVector decimal28Vector;

    Decimal28Column(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                   ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Decimal28SparseVector v,
                   SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      this.decimal28Vector = v;
    }

    @Override
    public boolean setSafe(int index, byte[] bytes, int start, int length) {
      int width = Decimal28SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromByteArray(bytes, start, length, schemaElement.getScale());
      if (index >= decimal28Vector.getValueCapacity()) {
        return false;
      }
      DecimalUtility.getSparseFromBigDecimal(intermediate, decimal28Vector.getData(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal28SparseHolder.nDecimalDigits);
      return true;
    }

    @Override
    public int capacity() {
      return decimal28Vector.getData().capacity();
    }
  }

  public static class NullableDecimal28Column extends NullableVarLengthColumn<NullableDecimal28SparseVector> {

    protected NullableDecimal28SparseVector nullableDecimal28Vector;

    NullableDecimal28Column(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableDecimal28SparseVector v,
                    SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      nullableDecimal28Vector = v;
    }

    @Override
    public boolean setSafe(int index, byte[] bytes, int start, int length) {
      int width = Decimal28SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromByteArray(bytes, start, length, schemaElement.getScale());
      if (index >= nullableDecimal28Vector.getValueCapacity()) {
        return false;
      }
      DecimalUtility.getSparseFromBigDecimal(intermediate, nullableDecimal28Vector.getData(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal28SparseHolder.nDecimalDigits);
      nullableDecimal28Vector.getMutator().setIndexDefined(index);
      return true;
    }

    @Override
    public int capacity() {
      return nullableDecimal28Vector.getData().capacity();
    }
  }

  public static class Decimal38Column extends VarLengthColumn<Decimal38SparseVector> {

    protected Decimal38SparseVector decimal28Vector;

    Decimal38Column(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, Decimal38SparseVector v,
                    SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      decimal28Vector = v;
    }

    @Override
    public boolean setSafe(int index, byte[] bytes, int start, int length) {
      int width = Decimal38SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromByteArray(bytes, start, length, schemaElement.getScale());
      if (index >= decimal28Vector.getValueCapacity()) {
        return false;
      }
      DecimalUtility.getSparseFromBigDecimal(intermediate, decimal28Vector.getData(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal38SparseHolder.nDecimalDigits);
      return true;
    }

    @Override
    public int capacity() {
      return decimal28Vector.getData().capacity();
    }
  }

  public static class NullableDecimal38Column extends NullableVarLengthColumn<NullableDecimal38SparseVector> {

    protected NullableDecimal38SparseVector nullableDecimal38Vector;

    NullableDecimal38Column(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableDecimal38SparseVector v,
                            SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      nullableDecimal38Vector = v;
    }

    @Override
    public boolean setSafe(int index, byte[] bytes, int start, int length) {
      int width = Decimal38SparseHolder.WIDTH;
      BigDecimal intermediate = DecimalUtility.getBigDecimalFromByteArray(bytes, start, length, schemaElement.getScale());
      if (index >= nullableDecimal38Vector.getValueCapacity()) {
        return false;
      }
      DecimalUtility.getSparseFromBigDecimal(intermediate, nullableDecimal38Vector.getData(), index * width, schemaElement.getScale(),
              schemaElement.getPrecision(), Decimal38SparseHolder.nDecimalDigits);
      nullableDecimal38Vector.getMutator().setIndexDefined(index);
      return true;
    }

    @Override
    public int capacity() {
      return nullableDecimal38Vector.getData().capacity();
    }
  }


  public static class VarCharColumn extends VarLengthColumn <VarCharVector> {

    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    protected VarCharVector varCharVector;

    VarCharColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                  ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarCharVector v,
                  SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      varCharVector = v;
    }

    @Override
    public boolean setSafe(int index, byte[] bytes, int start, int length) {
      boolean success;
      if(index >= varCharVector.getValueCapacity()) return false;

      if (usingDictionary) {
        success = varCharVector.getMutator().setSafe(valuesReadInCurrentPass, currDictVal.getBytes(),
            0, currDictVal.length());
      }
      else {
        success = varCharVector.getMutator().setSafe(index, bytes, start, length);
      }
      return success;
    }

    @Override
    public int capacity() {
      return varCharVector.getData().capacity();
    }
  }

  public static class NullableVarCharColumn extends NullableVarLengthColumn <NullableVarCharVector> {

    int nullsRead;
    boolean currentValNull = false;
    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    protected NullableVarCharVector nullableVarCharVector;

    NullableVarCharColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                          ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableVarCharVector v,
                          SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      nullableVarCharVector = v;
    }

    public boolean setSafe(int index, byte[] value, int start, int length) {
      boolean success;
      if(index >= nullableVarCharVector.getValueCapacity()) return false;

      if (usingDictionary) {
        success = nullableVarCharVector.getMutator().setSafe(valuesReadInCurrentPass, currDictVal.getBytes(),
            0, currDictVal.length());
      }
      else {
        success = nullableVarCharVector.getMutator().setSafe(index, value, start, length);
      }
      return success;
    }

    @Override
    public int capacity() {
      return nullableVarCharVector.getData().capacity();
    }
  }

  public static class VarBinaryColumn extends VarLengthColumn <VarBinaryVector> {

    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    protected VarBinaryVector varBinaryVector;

    VarBinaryColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, VarBinaryVector v,
                    SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      varBinaryVector = v;
    }

    @Override
    public boolean setSafe(int index, byte[] bytes, int start, int length) {
      boolean success;
      if(index >= varBinaryVector.getValueCapacity()) return false;

      if (usingDictionary) {
        success = varBinaryVector.getMutator().setSafe(valuesReadInCurrentPass, currDictVal.getBytes(),
            0, currDictVal.length());
      }
      else {
        success = varBinaryVector.getMutator().setSafe(index, bytes, start, length);
      }
      return success;
    }

    @Override
    public int capacity() {
      return varBinaryVector.getData().capacity();
    }
  }

  public static class NullableVarBinaryColumn extends NullableVarLengthColumn <NullableVarBinaryVector> {

    int nullsRead;
    boolean currentValNull = false;
    // store a hard reference to the vector (which is also stored in the superclass) to prevent repetitive casting
    protected org.apache.drill.exec.vector.NullableVarBinaryVector nullableVarBinaryVector;

    NullableVarBinaryColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                            ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, NullableVarBinaryVector v,
                            SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      nullableVarBinaryVector = v;
    }

    public boolean setSafe(int index, byte[] value, int start, int length) {
      boolean success;
      if(index >= nullableVarBinaryVector.getValueCapacity()) return false;

      if (usingDictionary) {
        success = nullableVarBinaryVector.getMutator().setSafe(valuesReadInCurrentPass, currDictVal.getBytes(),
            0, currDictVal.length());
      }
      else {
        success = nullableVarBinaryVector.getMutator().setSafe(index, value, start, length);
      }
      return  success;
    }

    @Override
    public int capacity() {
      return nullableVarBinaryVector.getData().capacity();
    }

  }
}
