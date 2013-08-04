package org.apache.drill.exec.physical.config;

import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.ReadEntryFromHDFS;
import org.apache.drill.exec.physical.base.AbstractScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: sphillips
 * Date: 8/4/13
 * Time: 1:00 AM
 * To change this template use File | Settings | File Templates.
 */
public class ParquetRowGroupScan extends AbstractScan {
  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Scan<?> getSpecificScan(int minorFragmentId) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public ParquetRowGroupScan (LinkedList<ParquetRowGroupReadEntry> readEntries) {
    super(readEntries);
  }
  public static class ParquetRowGroupReadEntry extends ReadEntryFromHDFS {
    public ParquetRowGroupReadEntry(String path, long start, long length) {
      super(path,start,length);
    }
  }
}
