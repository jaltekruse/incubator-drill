/**
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
 */
package org.apache.drill.exec.vector.complex.fn;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.google.common.base.Charsets;

public class JsonReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonReader.class);


  private final JsonFactory factory = new JsonFactory();
  private JsonParser parser;
  private List<SchemaPath> columns;

  public JsonReader() throws IOException {
    this(null);
  }

  public JsonReader(List<SchemaPath> columns) throws JsonParseException, IOException {
    factory.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    factory.configure(Feature.ALLOW_COMMENTS, true);
    this.columns = columns;
  }

  private boolean fieldSelected(SchemaPath field){
    if (this.columns != null){
      for (SchemaPath expr : this.columns){
        if ( field.containedBy(expr)){
          return true;
        }
      }
      return false;
    }
    return true;
  }

  public boolean write(Reader reader, ComplexWriter writer) throws JsonParseException, IOException {

    parser = factory.createJsonParser(reader);
    reader.mark(1024*128);
    JsonToken t = parser.nextToken();
    while(!parser.hasCurrentToken()) t = parser.nextToken();


    switch (t) {
    case START_OBJECT:
      writeData(writer.rootAsMap());
      break;
    case START_ARRAY:
      writeData(writer.rootAsList());
      break;
    case NOT_AVAILABLE:
      return false;
    default:
      throw new JsonParseException(
          String.format("Failure while parsing JSON.  Found token of [%s]  Drill currently only supports parsing "
              + "json strings that contain either lists or maps.  The root object cannot be a scalar.",
              t),
          parser.getCurrentLocation());
    }

    return true;
  }


  private void writeData(MapWriter map) throws JsonParseException, IOException {
    //
    map.start();
    outside: while(true){
      JsonToken t = parser.nextToken();
      if(t == JsonToken.NOT_AVAILABLE || t == JsonToken.END_OBJECT) return;

      assert t == JsonToken.FIELD_NAME : String.format("Expected FIELD_NAME but got %s.", t.name());
      final String fieldName = parser.getText();
      SchemaPath path = null;
      if (map.getField().getPath().getRootSegment().getPath().equals("")) {
        path = new SchemaPath(new PathSegment.NameSegment(fieldName));
      } else {
        path = map.getField().getPath().getChild(fieldName);
      }
      if ( ! fieldSelected(path) ) {
        switch(parser.nextToken()){
          case START_ARRAY:
            int arrayCounter = 1;
            skipArrayLoop: while (true) {
              switch(parser.nextToken()) {
                case START_ARRAY:
                  arrayCounter++;
                  break;
                case END_ARRAY:
                  arrayCounter--;
                  if (arrayCounter == 0) {
                    break skipArrayLoop;
                  }
                  break;
              }
            }
            continue outside;
          case START_OBJECT:
            int objectCounter = 1;
            skipObjectLoop: while (true) {
              switch(parser.nextToken()) {
                case START_OBJECT:
                  objectCounter++;
                  break;
                case END_OBJECT:
                  objectCounter--;
                  if (objectCounter == 0) {
                    break skipObjectLoop;
                  }
                  break;
              }
            }
            continue outside;
          default:
            // hit a single value, do nothing as the token was already read
            // in the switch statement
            continue outside;
        }
      }

      switch(parser.nextToken()){
      case START_ARRAY:
        writeData(map.list(fieldName));
        break;
      case START_OBJECT:
        writeData(map.map(fieldName));
        break;
      case END_OBJECT:
        break outside;

      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE: {
        BitHolder h = new BitHolder();
        h.value = 0;
        map.bit(fieldName).write(h);
        break;
      }
      case VALUE_TRUE: {
        BitHolder h = new BitHolder();
        h.value = 1;
        map.bit(fieldName).write(h);
        break;
      }
      case VALUE_NULL:
        // do nothing as we don't have a type.
        break;
      case VALUE_NUMBER_FLOAT:
        Float8Holder fh = new Float8Holder();
        fh.value = parser.getDoubleValue();
        map.float8(fieldName).write(fh);
        break;
      case VALUE_NUMBER_INT:
        BigIntHolder bh = new BigIntHolder();
        bh.value = parser.getLongValue();
        map.bigInt(fieldName).write(bh);
        break;
      case VALUE_STRING:
        VarCharHolder vh = new VarCharHolder();
        map.varChar(fieldName).write(prepareVarCharHolder(vh, parser));
        break;

      default:
        throw new IllegalStateException("Unexpected token " + parser.getCurrentToken());

      }

    }
    map.end();

  }

  private VarCharHolder prepareVarCharHolder(VarCharHolder vh, JsonParser parser) throws IOException {
    String value = parser.getText();
    byte[] b = value.getBytes(Charsets.UTF_8);
    ByteBuf d = UnpooledByteBufAllocator.DEFAULT.buffer(b.length);
    d.setBytes(0, b);
    vh.buffer = d;
    vh.start = 0;
    vh.end = b.length;
    return vh;
  }

  private void writeData(ListWriter list) throws JsonParseException, IOException {
    list.start();
    outside: while(true){

      switch(parser.nextToken()){
      case START_ARRAY:
        writeData(list.list());
        break;
      case START_OBJECT:
        writeData(list.map());
        break;
      case END_ARRAY:
      case END_OBJECT:
        break outside;

      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE:{
        BitHolder h = new BitHolder();
        h.value = 0;
        list.bit().write(h);
        break;
      }
      case VALUE_TRUE: {
        BitHolder h = new BitHolder();
        h.value = 1;
        list.bit().write(h);
        break;
      }
      case VALUE_NULL:
        // do nothing as we don't have a type.
        break;
      case VALUE_NUMBER_FLOAT:
        Float8Holder fh = new Float8Holder();
        fh.value = parser.getDoubleValue();
        list.float8().write(fh);
        break;
      case VALUE_NUMBER_INT:
        BigIntHolder bh = new BigIntHolder();
        bh.value = parser.getLongValue();
        list.bigInt().write(bh);
        break;
      case VALUE_STRING:
        VarCharHolder vh = new VarCharHolder();
        list.varChar().write(prepareVarCharHolder(vh, parser));
        break;
      default:
        throw new IllegalStateException("Unexpected token " + parser.getCurrentToken());
      }
    }
    list.end();
    
    
  }
}
