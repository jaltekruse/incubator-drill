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

import java.math.BigDecimal;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.DecimalUtility;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.vector.Decimal28SparseVector;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.NullableDecimal28SparseVector;
import org.apache.drill.exec.vector.NullableDecimal38SparseVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.RepeatedFixedWidthVector;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.io.api.Binary;

import java.io.IOException;
import java.math.BigDecimal;

public class VarLengthColumnReaders {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VarLengthColumnReaders.class);

  public static abstract class VarLengthColumn<V extends ValueVector> extends ColumnReader {

    Binary currDictVal;

    VarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                          ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v,
                          SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      // TODO - figure out if repeated columns can use dictionary
//      if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
//        usingDictionary = true;
//      }
//      else {
//        usingDictionary = false;
//      }
    }

    public abstract boolean determineSize(long recordsReadInCurrentPass, Integer lengthVarFieldsInCurrentRecord) throws IOException;

    protected void readRecords(int recordsToRead) {
      for (int i = 0; i < recordsToRead; i++) {
        readField(i, null);
      }
      pageReadStatus.valuesRead += recordsToRead;
    }

    public abstract void updatePosition();

    public abstract void updateReadyToReadPosition();

    public void reset() {
      bytesReadInCurrentPass = 0;
      valuesReadInCurrentPass = 0;
      pageReadStatus.valuesReadyToRead = 0;
    }

    public abstract boolean setSafe(int index, byte[] bytes, int start, int length);

    public abstract int capacity();

  }

  public static abstract class VarLengthValuesColumn<V extends ValueVector> extends VarLengthColumn {

    Binary currDictVal;
    VariableWidthVector variableWidthVector;

    VarLengthValuesColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                          ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v,
                          SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      variableWidthVector = (VariableWidthVector) valueVec;
      if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
        usingDictionary = true;
      }
      else {
        usingDictionary = false;
      }
    }

    @Override
    protected void readField(long recordToRead, ColumnReader firstColumnStatus) {
      dataTypeLengthInBits = variableWidthVector.getAccessor().getValueLength(valuesReadInCurrentPass);
      // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
      boolean success = setSafe((int) valuesReadInCurrentPass, pageReadStatus.pageDataByteArray,
          (int) pageReadStatus.readPosInBytes + 4, dataTypeLengthInBits);
      assert success;
      updatePosition();
    }

    public void updateReadyToReadPosition() {
      pageReadStatus.readyToReadPosInBytes += dataTypeLengthInBits + 4;
      pageReadStatus.valuesReadyToRead++;
      currDictVal = null;
      byte[] buf = new byte[100];
//      System.arraycopy(pageReadStatus.pageDataByteArray, (int) Math.max(0, (pageReadStatus.readyToReadPosInBytes - 50)), buf, 0, 100);
    }

    public void updatePosition() {
      pageReadStatus.readPosInBytes += dataTypeLengthInBits + 4;
      bytesReadInCurrentPass += dataTypeLengthInBits;
      valuesReadInCurrentPass++;
    }

    /**
     * Determines the size of a single value in a variable column.
     *
     * Return value indicates if we have finished a row group and should stop reading
     *
     * @param recordsReadInCurrentPass
     * @param lengthVarFieldsInCurrentRecord
     * @return - true if we should stop reading
     * @throws IOException
     */
    public boolean determineSize(long recordsReadInCurrentPass, Integer lengthVarFieldsInCurrentRecord) throws IOException {

      if (recordsReadInCurrentPass == valueVec.getValueCapacity()){
        return true;
      }
      if (pageReadStatus.currentPage == null
          || pageReadStatus.valuesRead + pageReadStatus.valuesReadyToRead == pageReadStatus.currentPage.getValueCount()) {
        readRecords(pageReadStatus.valuesReadyToRead);
        totalValuesRead += pageReadStatus.valuesRead;
        if (!pageReadStatus.next()) {
          return true;
        }
        pageReadStatus.valuesReadyToRead = 0;
      }

      // re-purposing this field here for length in BYTES to prevent repetitive multiplication/division
      dataTypeLengthInBits = BytesUtils.readIntLittleEndian(pageReadStatus.pageDataByteArray,
          (int) pageReadStatus.readyToReadPosInBytes);

      // this should not fail
      if (!variableWidthVector.getMutator().setValueLengthSafe((int) valuesReadInCurrentPass + pageReadStatus.valuesReadyToRead,
          dataTypeLengthInBits)) {
        return true;
      }
      lengthVarFieldsInCurrentRecord += dataTypeLengthInBits;

      if (bytesReadInCurrentPass + dataTypeLengthInBits > capacity()) {
        // TODO - determine if we need to add this back somehow
        //break outer;
        logger.debug("Reached the capacity of the data vector in a variable length value vector.");
        return true;
      }
      return false;
    }
  }

  public static abstract class NullableVarLengthValuesColumn<V extends ValueVector> extends VarLengthValuesColumn<V> {

    int nullsRead;
    boolean currentValNull = false;

    NullableVarLengthValuesColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
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
      pageReadStatus.valuesReadyToRead = 0;
    }

    public boolean determineSize(long recordsReadInCurrentPass, Integer lengthVarFieldsInCurrentRecord) throws IOException {
      // check to make sure there is capacity for the next value (for nullables this is a check to see if there is
      // still space in the nullability recording vector)
      if (recordsReadInCurrentPass == valueVec.getValueCapacity()){
        return true;
      }
      if (pageReadStatus.currentPage == null
          || pageReadStatus.valuesRead + pageReadStatus.valuesReadyToRead == pageReadStatus.currentPage.getValueCount()) {
        readRecords(pageReadStatus.valuesReadyToRead);
        totalValuesRead += pageReadStatus.valuesRead;
        if (!pageReadStatus.next()) {
          return true;
        } else {
          currDictVal = null;
        }
        pageReadStatus.valuesReadyToRead = 0;
      }
      // we need to read all of the lengths to determine if this value will fit in the current vector,
      // as we can only read each definition level once, we have to store the last one as we will need it
      // at the start of the next read if we decide after reading all of the varlength values in this record
      // that it will not fit in this batch
      currentValNull = false;
      if ( currDefLevel == -1 ) {
        try {
        currDefLevel = pageReadStatus.definitionLevels.readInteger();
        } catch (Exception ex) {
          throw ex;
        }
      }
      if ( columnDescriptor.getMaxDefinitionLevel() > currDefLevel){
        nullsRead++;
        // set length of zero, each index in the vector defaults to null so no need to set the nullability
        variableWidthVector.getMutator().setValueLengthSafe(
            valuesReadInCurrentPass + pageReadStatus.valuesReadyToRead, 0);
        currentValNull = true;
        return false;// field is null, no length to add to data vector
      }

      if (usingDictionary) {
        if (currDictVal == null) {
          currDictVal = pageReadStatus.dictionaryLengthDeterminingReader.readBytes();
        }
        // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
        dataTypeLengthInBits = currDictVal.length();
      }
      else {
        try {
        // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
        dataTypeLengthInBits = BytesUtils.readIntLittleEndian(pageReadStatus.pageDataByteArray,
            (int) pageReadStatus.readyToReadPosInBytes);
        } catch (Exception ex) {
          throw ex;
        }
      }
      // I think this also needs to happen if it is null for the random access
      if (! variableWidthVector.getMutator().setValueLengthSafe((int) valuesReadInCurrentPass + pageReadStatus.valuesReadyToRead, dataTypeLengthInBits)) {
        return true;
      }
      // TODO - replace with with a new interface to allow the nullability to be set for each value
      // this is a hack that is allowing a distinction between null values and empty strings
      // the value has length zero, but it is not null (this case is handled above), record that is is defined with zero length
      // this this condition should be changed to (! currValNull)
//      if ( dataTypeLengthInBits == 0 ) {
      try {
        boolean success = setSafe(valuesReadInCurrentPass + pageReadStatus.valuesReadyToRead, pageReadStatus.pageDataByteArray,
            (int) pageReadStatus.readyToReadPosInBytes + 4, dataTypeLengthInBits);
        assert success;
      } catch (Throwable t) {
        byte[] buf = new byte[100];
        System.arraycopy(pageReadStatus.pageDataByteArray, (int) (pageReadStatus.readyToReadPosInBytes - 50), buf, 0, 100);
        throw t;
      }
//      }
//      if ( ! currentValNull) {
//        // this should not fail
//        if (!variableWidthVector.getMutator().setValueLengthSafe((int) valuesReadInCurrentPass, dataTypeLengthInBits)) {
//          return true;
//        }
//      }
      lengthVarFieldsInCurrentRecord += dataTypeLengthInBits;

      if (bytesReadInCurrentPass + dataTypeLengthInBits > capacity()) {
        // TODO - determine if we need to add this back somehow
        //break outer;
        logger.debug("Reached the capacity of the data vector in a variable length value vector.");
        return true;
      }
      return false;
    }

    public void updateReadyToReadPosition() {
      if (! currentValNull){
        pageReadStatus.readyToReadPosInBytes += dataTypeLengthInBits + 4;
      }
      pageReadStatus.valuesReadyToRead++;
      currDictVal = null;
      byte[] buf = new byte[100];
//      System.arraycopy(pageReadStatus.pageDataByteArray, (int) Math.max(0, (pageReadStatus.readyToReadPosInBytes - 50)), buf, 0, 100);
    }

    public void updatePosition() {
      if (! currentValNull){
        pageReadStatus.readPosInBytes += dataTypeLengthInBits + 4;
        bytesReadInCurrentPass += dataTypeLengthInBits;
      }
      valuesReadInCurrentPass++;
    }
    
    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      if (usingDictionary) {
        currDictVal = pageReadStatus.dictionaryValueReader.readBytes();
        // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
      }
      dataTypeLengthInBits = variableWidthVector.getAccessor().getValueLength(valuesReadInCurrentPass);
      currentValNull = variableWidthVector.getAccessor().getObject(valuesReadInCurrentPass) == null;
      // TODO - recall nullability into currentValNull
      // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
      if (! currentValNull){
        boolean success = setSafe(valuesReadInCurrentPass, pageReadStatus.pageDataByteArray,
            (int) pageReadStatus.readPosInBytes + 4, dataTypeLengthInBits);
        assert success;
      }
      updatePosition();
      currDefLevel = -1;
      currentValNull = false;
      if ( pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
        totalValuesRead += pageReadStatus.valuesRead;
        // I do not believe this is needed
        //pageReadStatus.next();
      }
      currDictVal = null;
    }
  }

  public static class FixedWidthRepeatedReader extends VarLengthColumn {

    RepeatedFixedWidthVector castedRepeatedVector;
    ColumnReader dataReader;
    int dataTypeLengthInBytes;
    // we can do a vector copy of the data once we figure out how much we need to copy
    // this tracks the number of values to transfer (the dataReader will translate this to a number
    // of bytes to transfer and re-use the code from the non-repeated types)
    int valuesToRead;
    int repeatedGroupsReadInCurrentPass;
    // stores the number of
    int repeatedValuesInCurrentList;
    // empty lists are notated by definition levels, to stop reading at the correct time, we must keep
    // track of the number of empty lists as well as the length of all of the defined lists together
    int definitionLevelsRead;

    FixedWidthRepeatedReader(ParquetRecordReader parentReader, ColumnReader dataReader, int dataTypeLengthInBytes, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector valueVector, SchemaElement schemaElement) throws ExecutionSetupException {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, valueVector, schemaElement);
      castedRepeatedVector = (RepeatedFixedWidthVector) valueVector;
      this.dataTypeLengthInBytes = dataTypeLengthInBytes;
      this.dataReader = dataReader;
      this.dataReader.pageReadStatus = this.pageReadStatus;
    }

    public void reset() {
      bytesReadInCurrentPass = 0;
      valuesReadInCurrentPass = 0;
      pageReadStatus.valuesReadyToRead = 0;
      dataReader.vectorData = castedRepeatedVector.getMutator().getDataVector().getData();
      repeatedGroupsReadInCurrentPass = 0;
    }

    public int getRecordsReadInCurrentPass() {
      return repeatedGroupsReadInCurrentPass;
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    public void updateReadyToReadPosition() {
      valuesToRead += repeatedValuesInCurrentList;
      pageReadStatus.valuesReadyToRead += repeatedValuesInCurrentList;
      repeatedGroupsReadInCurrentPass++;
      currDictVal = null;
      repeatedValuesInCurrentList = -1;
    }

    public void updatePosition() {
      pageReadStatus.readPosInBytes += dataTypeLengthInBits;
      bytesReadInCurrentPass += dataTypeLengthInBits;
      valuesReadInCurrentPass++;
    }


    public boolean determineSize(long recordsReadInCurrentPass, Integer lengthVarFieldsInCurrentRecord) throws IOException {

      if (recordsReadInCurrentPass == valueVec.getValueCapacity()){
        return true;
      }
      if (pageReadStatus.currentPage == null
          || definitionLevelsRead == pageReadStatus.currentPage.getValueCount()) {
        readRecords(pageReadStatus.valuesReadyToRead);
        totalValuesRead += definitionLevelsRead;
        if (!pageReadStatus.next()) {
          pageReadStatus.valuesReadyToRead = 0;
          definitionLevelsRead = 0;
          return true;
        }
        repeatedValuesInCurrentList = -1;
        pageReadStatus.valuesReadyToRead = 0;
        definitionLevelsRead = 0;
      }

      int repLevel;
      if ( currDefLevel == -1 ) {
        try {
          currDefLevel = pageReadStatus.definitionLevels.readInteger();
          definitionLevelsRead++;
        } catch (Exception ex) {
          throw ex;
        }
      }
      if ( columnDescriptor.getMaxDefinitionLevel() == currDefLevel){
        if (repeatedValuesInCurrentList == -1) {
          repeatedValuesInCurrentList = 1;
          do {
            repLevel = pageReadStatus.repetitionLevels.readInteger();
            if (repLevel > 0) {
              repeatedValuesInCurrentList++;
              currDefLevel = pageReadStatus.definitionLevels.readInteger();
              definitionLevelsRead++;
            }
          } while (repLevel != 0);
        }
      }
      else {
        repeatedValuesInCurrentList = 0;
      }
      // this should not fail
      if (!castedRepeatedVector.getMutator().setRepetitionAtIndexSafe(repeatedGroupsReadInCurrentPass,
          repeatedValuesInCurrentList)) {
        return true;
      }
      // again going to make this the length in BYTES to avoid repetitive multiplication/division
      lengthVarFieldsInCurrentRecord += repeatedValuesInCurrentList * dataTypeLengthInBytes;

      if (valuesReadInCurrentPass + pageReadStatus.valuesReadyToRead + repeatedValuesInCurrentList >= valueVec.getValueCapacity()) {
        return true;
      }
      if (bytesReadInCurrentPass + dataTypeLengthInBits > capacity()) {
        // TODO - determine if we need to add this back somehow
        //break outer;
        logger.debug("Reached the capacity of the data vector in a {}", castedRepeatedVector.getMutator().getDataVector().getClass());
        return true;
      }
      return false;
    }

    protected void readRecords(int valuesToRead) {
      if (valuesToRead == 0) return;
      dataReader.readField(valuesToRead, null);
      //pageReadStatus.valuesRead += definitionLevelsRead;
      valuesReadInCurrentPass += valuesToRead;
      try {
      castedRepeatedVector.getMutator().setValueCounts(repeatedGroupsReadInCurrentPass, valuesReadInCurrentPass);
      } catch (Throwable t) {
        throw t;
      }
    }

    @Override
    public boolean setSafe(int index, byte[] bytes, int start, int length) {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int capacity() {
      return castedRepeatedVector.getMutator().getDataVector().getData().capacity();
    }
  }

  public static class Decimal28Column extends VarLengthValuesColumn<Decimal28SparseVector> {

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

  public static class NullableDecimal28Column extends NullableVarLengthValuesColumn<NullableDecimal28SparseVector> {

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

  public static class Decimal38Column extends VarLengthValuesColumn<Decimal38SparseVector> {

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

  public static class NullableDecimal38Column extends NullableVarLengthValuesColumn<NullableDecimal38SparseVector> {

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


  public static class VarCharColumn extends VarLengthValuesColumn<VarCharVector> {

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

  public static class NullableVarCharColumn extends NullableVarLengthValuesColumn<NullableVarCharVector> {

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

  public static class VarBinaryColumn extends VarLengthValuesColumn<VarBinaryVector> {

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
        success = varBinaryVector.getMutator().setSafe(index, currDictVal.getBytes(),
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

  public static class NullableVarBinaryColumn extends NullableVarLengthValuesColumn<NullableVarBinaryVector> {

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
        success = nullableVarBinaryVector.getMutator().setSafe(index, currDictVal.getBytes(),
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
