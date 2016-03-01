/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.sys;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.server.DrillbitContext;

import java.util.Collection;

public class FragmentIterator {
  //Drillbit, queryid, major fragmentid, minorfragmentid, coordinate, memory usage, rows processed, start time
  private final DrillbitContext drillbitContext;

  public FragmentIterator(FragmentContext c) {
    this.drillbitContext = c.getDrillbitContext();
    Collection<CoordinationProtos.DrillbitEndpoint> bits = drillbitContext.getBits();

  }

  public static class FragmentInfo {
    public String hostname;
    public String queryId;
    public int majorFragmentId;
    public int minorFragmentId;
    public int coordinate;
    public long memoryUsage;
    public long rowsProcessed;
    // TODO - a datetime currently appears in the Version table, but
    // it is returned as a String
//    public DateTime startTime;
    public long startTime;
  }
}
