package org.apache.drill.jdbc;

import java.util.Properties;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaFactory;
import net.hydromatic.avatica.Meta;
import net.hydromatic.avatica.UnregisteredDriver;

public abstract class DrillConnection extends AvaticaConnection{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConnection.class);

  protected DrillConnection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info) {
    super(driver, factory, url, info);
  }

  @Override
  protected Meta createMeta() {
    return super.createMeta();
  }

  
}
