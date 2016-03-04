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

import com.beust.jcommander.internal.Lists;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.server.rest.profile.ProfileResources;

import java.util.Iterator;
import java.util.List;

public class QueryIterator implements Iterator<Object> {

  private final FragmentContext context;
  private final Iterator<ProfileResources.ProfileInfo> queryIter;

  public QueryIterator(final FragmentContext context) {
    this.context = context;
    try {
      ProfileResources.QProfiles queryLists = ProfileResources.getProfiles(
          context.getDrillbitContext().getStoreProvider(),
          context.getDrillbitContext().getClusterCoordinator(),
          new DrillUserPrincipal("admin", true, new DrillClient()));
      List<ProfileResources.ProfileInfo> allQueries = Lists.newArrayList();
      allQueries.addAll(queryLists.getRunningQueries());
      allQueries.addAll(queryLists.getFinishedQueries());
      this.queryIter = allQueries.iterator();
    } catch (StoreException e) {
      throw new RuntimeException("Error setting up query list system table.", e);
    }
  }

  @Override
  public boolean hasNext() {
    return queryIter.hasNext();
  }

  @Override
  public ProfileResources.ProfileInfo next() {
    return queryIter.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
