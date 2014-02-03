
package org.apache.drill.client.jdbc;

import java.sql.Connection;

import org.apache.drill.exec.rpc.RpcException;

public interface DrillConnection extends AvaticaConnection{
  public void connect() throws RpcException;
  public void closeDrillClient();
}

