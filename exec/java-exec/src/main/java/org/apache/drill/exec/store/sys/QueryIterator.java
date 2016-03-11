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
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.server.rest.profile.FragmentWrapper;
import org.apache.drill.exec.server.rest.profile.ProfileResources;
import org.apache.drill.exec.server.rest.profile.ProfileWrapper;
import org.apache.drill.exec.work.foreman.Foreman;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

// queries: Foreman, QueryId, User, SQL, Start Time, rows processed, query plan, # nodes involved, number of running fragments, memory consumed
//        - work manager as well?
public class QueryIterator implements Iterator<Object> {

  private final Iterator<ProfileResources.ProfileInfo> allQueries;
  ProfileResources.ProfileInfo nextToReturn;
  private final FragmentContext context;

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
      this.allQueries = allQueries.iterator();
      this.nextToReturn = null;
    } catch (StoreException e) {
      throw new RuntimeException("Error setting up active query system table.", e);
    }
  }

  @Override
  public boolean hasNext() {
    findNextActiveQuery();
    if (nextToReturn == null) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public QueryInfo next() {
    findNextActiveQuery();
    if (nextToReturn != null) {
      ProfileResources.ProfileInfo ret = nextToReturn;
      Foreman f = context.getDrillbitContext().getWorkManager().getBee().getForemanForQueryId(QueryIdHelper.getQueryIdFromString(ret.queryId));
      UserBitShared.QueryProfile queryProfile = f.getQueryManager().getQueryProfile();
      ProfileWrapper profileWrapper = new ProfileWrapper(queryProfile);
      long totalRowsProcessed = 0;
      for (FragmentWrapper fragmentWrapper : profileWrapper.getFragmentProfiles()) {
        totalRowsProcessed += fragmentWrapper.getRowsProcessed();
      }
      // TODO - This is currently minor fragments, is that right?
      long totalRunningFragments = 0;
      for (FragmentWrapper fragmentWrapper : profileWrapper.getFragmentProfiles()) {
        totalRunningFragments += fragmentWrapper.getNumberRunningFragments();
      }
      nextToReturn = null;
      return new QueryInfo(ret, queryProfile.getPlan(), totalRowsProcessed, totalRunningFragments);
    } else {
      throw new NoSuchElementException();
    }
  }

  private void findNextActiveQuery() {
    while (nextToReturn == null && allQueries.hasNext()) {
      nextToReturn = allQueries.next();
      if (nextToReturn.state.equals("COMPLETED") ||
          nextToReturn.state.equals("FAILED") ||
          nextToReturn.state.equals("CANCELED")) {
        nextToReturn = null;
      }
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  // If we use something closer to Jackson to determine what the Pojo reader exposes this could be done with
  // composition instead of inheritance
  public static class QueryInfo {

    public final String plan;
    public String queryId;
    public Date time;
    public String location;
    public String foreman;
    public String query;
    public String state;
    public String user;
    public long rowsProcessed;
    public long runningFragments;

    public QueryInfo(ProfileResources.ProfileInfo profileInfo, String plan, long rowsProcessed, long runningFragments) {
      this(profileInfo.queryId, profileInfo.time.getTime(), profileInfo.foreman, profileInfo.query,
          profileInfo.state, profileInfo.user, plan, rowsProcessed, runningFragments);
    }

    public QueryInfo(String queryId, long time, String foreman, String query, String state, String user,
                     String plan, long rowsProcessed, long runningFragments) {
      this.queryId = queryId;
      this.time = new Date(time);
      this.foreman = foreman;
      this.location = "http://localhost:8047/profile/" + queryId + ".json";
      // in ProfileInfo this is truncated, don't think that is the desired behavior here
      // over there it should probably be done in the front-end anyway
      this.query = query;
      this.state = state;
      this.user = user;
      this.plan = plan;
      this.rowsProcessed = rowsProcessed;
      this.runningFragments = runningFragments;
    }
  }
}
