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
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

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
      String[] subPartitions = null;
      String pluginStr = null;
      String workspaceStr = null;
      String partitionStr = null;
      byte[] temp;
      VarCharHolder currentInput;
      try {
        currentInput = plugin;
        temp = new byte[currentInput.end - currentInput.start];
        currentInput.buffer.getBytes(0, temp, 0, currentInput.end - currentInput.start);
        pluginStr = new String(temp, "UTF-8");

        currentInput = workspace;
        temp = new byte[currentInput.end - currentInput.start];
        currentInput.buffer.getBytes(0, temp, 0, currentInput.end - currentInput.start);
        workspaceStr = new String(temp, "UTF-8");

        currentInput = partition;
        temp = new byte[currentInput.end - currentInput.start];
        currentInput.buffer.getBytes(0, temp, 0, currentInput.end - currentInput.start);
        partitionStr = new String(temp, "UTF-8");

      } catch (java.io.UnsupportedEncodingException ex) {
        // should not happen, UTF-8 encoding should be available
        throw new RuntimeException(ex);
      }
      try {
        subPartitions = partitionExplorer.getSubPartitions(plugin, workspace, partition);
      } catch (org.apache.drill.exec.store.PartitionNotFoundException e) {
        throw new RuntimeException(
            "Error in %s function: " + org.apache.drill.exec.expr.fn.impl.DirectoryExplorers.MAXDIR_NAME +
                "Partition `" +  workspaceStr + "`.`" + pluginStr + "`" +
                " does not exist in storage plugin " + partitionStr);
      }

      if (subPartitions.length == 0) {
        throw new RuntimeException(
            "Error in %s function: " + org.apache.drill.exec.expr.fn.impl.DirectoryExplorers.MAXDIR_NAME +
             "Partition `" +  workspaceStr + "`.`" + pluginStr + "`" +
                " in storage plugin " + partitionStr + "  does not contain sub-partitions.");
      }
      String subPartitionStr = subPartitions[0];
      // find the maximum directory in the list using a case-insensitive string comparison
      for (int i = 1; i < subPartitions.length; i++) {
        if (subPartitionStr.compareToIgnoreCase(subPartitions[i]) < 0) {
          subPartitionStr = subPartitions[i];
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
