package org.apache.drill.exec.store;

import net.hydromatic.optiq.SchemaPlus;

/**
 * Helper class to provide parent schema after initialization given Optiq's backwards schema build model.
 */
public class SchemaHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaHolder.class);
  
  private SchemaPlus schema;

  public SchemaHolder(){}
  
  public SchemaHolder(SchemaPlus schema){
    this.schema = schema;
  }
  
  public SchemaPlus getSchema() {
    return schema;
  }

  public void setSchema(SchemaPlus schema) {
    this.schema = schema;
  }
   
}
