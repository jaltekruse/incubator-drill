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
package org.apache.drill.exec.expr.fn.impl;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;
import java.util.Iterator;

public class DirectoryExplorers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectoryExplorers.class);

  public static final String MAXDIR_NAME = "maxdir";

  @FunctionTemplate(name = MAXDIR_NAME, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class MaxDir implements DrillSimpleFunc {

    @Param VarCharHolder plugin;
    @Param  VarCharHolder workspace;
    @Param  VarCharHolder partition;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;
    @Inject org.apache.drill.exec.store.PartitionExplorer partitionExplorer;

    public void setup() {
    }

    public void eval() {
      Iterable<String> subPartitions = null;
      try {
        subPartitions = partitionExplorer.getSubPartitions(
            StringFunctionHelpers.getStringFromVarCharHolder(plugin) + "." +
            StringFunctionHelpers.getStringFromVarCharHolder(workspace),
            Lists.newArrayList("dir0"),
            Lists.newArrayList(StringFunctionHelpers.getStringFromVarCharHolder(partition)));
      } catch (org.apache.drill.exec.store.PartitionNotFoundException e) {
        throw new RuntimeException(
            String.format("Error in %s function: Partition `%s`.`%s` does not exist in storage plugin %s ",
              org.apache.drill.exec.expr.fn.impl.DirectoryExplorers.MAXDIR_NAME,
              org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(workspace),
              org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(plugin),
              org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(partition)
              )
        );
      }
      Iterator<String> partitionIterator = subPartitions.iterator();
      if (!partitionIterator.hasNext()) {
        throw new RuntimeException(
            String.format("Error in %s function: Partition `%s`.`%s` in storage plugin %s does not contain sub-partitions.",
                org.apache.drill.exec.expr.fn.impl.DirectoryExplorers.MAXDIR_NAME,
                org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(workspace),
                org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(plugin),
                org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(partition)
            )
        );
      }
      String subPartitionStr = partitionIterator.next();
      String curr;
      // find the maximum directory in the list using a case-insensitive string comparison
      while (partitionIterator.hasNext()){
        curr = partitionIterator.next();
        if (subPartitionStr.compareToIgnoreCase(curr) < 0) {
          subPartitionStr = curr;
        }
      }
      String[] subPartitionParts = subPartitionStr.split(java.io.File.separator);
      subPartitionStr = subPartitionParts[subPartitionParts.length - 1];
      byte[] result = subPartitionStr.getBytes();
      out.buffer = buffer = buffer.reallocIfNeeded(result.length);

      out.buffer.setBytes(0, subPartitionStr.getBytes(), 0, result.length);
      out.start = 0;
      out.end = result.length;
    }
  }
}
