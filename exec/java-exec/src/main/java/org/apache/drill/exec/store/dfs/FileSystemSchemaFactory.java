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
package org.apache.drill.exec.store.dfs;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.hydromatic.optiq.Function;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;

import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.PartitionNotFoundException;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.WorkspaceSchema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;


/**
 * This is the top level schema that responds to root level path requests. Also supports
 */
public class FileSystemSchemaFactory implements SchemaFactory{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemSchemaFactory.class);

  private List<WorkspaceSchemaFactory> factories;
  private String schemaName;
  private final String defaultSchemaName = "default";


  public FileSystemSchemaFactory(String schemaName, List<WorkspaceSchemaFactory> factories) {
    super();
    this.schemaName = schemaName;
    this.factories = factories;
  }

  @Override
  public void registerSchemas(UserSession session, SchemaPlus parent) {
    FileSystemSchema schema = new FileSystemSchema(schemaName, session);
    SchemaPlus plusOfThis = parent.add(schema.getName(), schema);
    schema.setPlus(plusOfThis);
  }

  public class FileSystemSchema extends AbstractSchema {

    private final WorkspaceSchema defaultSchema;
    private final Map<String, WorkspaceSchema> schemaMap = Maps.newHashMap();

    public FileSystemSchema(String name, UserSession session) {
      super(ImmutableList.<String>of(), name);
      for(WorkspaceSchemaFactory f :  factories){
        WorkspaceSchema s = f.createSchema(getSchemaPath(), session);
        schemaMap.put(s.getName(), s);
      }

      defaultSchema = schemaMap.get(defaultSchemaName);
    }

    void setPlus(SchemaPlus plusOfThis){
      for(WorkspaceSchema s : schemaMap.values()){
        plusOfThis.add(s.getName(), s);
      }
    }

    private class SubDirectoryList implements Iterable<String>{
      final List<FileStatus> fileStatuses;

      SubDirectoryList(List<FileStatus> fileStatuses) {
        this.fileStatuses = fileStatuses;
      }

      @Override
      public Iterator<String> iterator() {
        return new SubDirectoryIterator(fileStatuses.iterator());
      }

      private class SubDirectoryIterator implements Iterator<String> {

        final Iterator<FileStatus> fileStatusIterator;

        SubDirectoryIterator(Iterator<FileStatus> fileStatusIterator) {
          this.fileStatusIterator = fileStatusIterator;
        }

        @Override
        public boolean hasNext() {
          return fileStatusIterator.hasNext();
        }

        @Override
        public String next() {
          return fileStatusIterator.next().getPath().toUri().toString();
        }

        /**
         * This class is designed specifically for use in conjunction with the
         * {@link org.apache.drill.exec.store.PartitionExplorer} interface.
         * This is only designed for accessing partition information, not
         * modifying it. To avoid confusing users of the interface this
         * method throws UnsupportedOperationException.
         *
         * @throws UnsupportedOperationException - this is not useful here, the
         *           list being iterated over should not be used in a way that
         *           removing an element would be meaningful.
         */
        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      }
    }

    public Iterable<String> getSubPartitions(String workspace, String partition) throws PartitionNotFoundException {

      final Path p = new Path(config.workspaces.get(workspace).getLocation() + File.separator + partition);
      List<FileStatus> fileStatuses;
      try {
        // if the path passed is a file, return an empty list of sub-partitions
        if (fs.isFile(p)) {
          return new SubDirectoryList(new ArrayList<FileStatus>());
        }
        fileStatuses = fs.list(false, p);
      } catch (IOException e) {
        // TODO - figure out if we can separate out the case of a partition not being found, or at least
        // take a look at what the error message comes out looking like to a user.
        throw new PartitionNotFoundException("Error trying to read sub-partitions." , e);
      }
      return new SubDirectoryList(fileStatuses);
    }

    @Override
    public boolean showInInformationSchema() {
      return false;
    }

    @Override
    public String getTypeName() {
      return FileSystemConfig.NAME;
    }

    @Override
    public Table getTable(String name) {
      return defaultSchema.getTable(name);
    }

    @Override
    public Collection<Function> getFunctions(String name) {
      return defaultSchema.getFunctions(name);
    }

    @Override
    public Set<String> getFunctionNames() {
      return defaultSchema.getFunctionNames();
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      return schemaMap.get(name);
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return schemaMap.keySet();
    }

    @Override
    public Set<String> getTableNames() {
      return defaultSchema.getTableNames();
    }

    @Override
    public boolean isMutable() {
      return defaultSchema.isMutable();
    }

    @Override
    public CreateTableEntry createNewTable(String tableName) {
      return defaultSchema.createNewTable(tableName);
    }

    @Override
    public AbstractSchema getDefaultSchema() {
      return defaultSchema;
    }
  }
}
