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
package org.apache.drill.jdbc.test;

import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import org.apache.drill.jdbc.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Foo {
    public net.hydromatic.linq4j.Enumerable execute(final net.hydromatic.optiq.DataContext root) {
        return org.apache.drill.optiq.EnumerableDrill.of("{\"head\":{\"type\":\"APACHE_DRILL_LOGICAL\",\"version\":\"1\",\"generator\":{\"type\"" +
                ":\"manual\",\"info\":\"na\"}},\"storage\":{\"donuts-json\":{\"type\":\"classpath\"},\"queue\":{\"type\":\"queue\"}},\"query\":" +
                "[{\"op\":\"sequence\",\"do\":[{\"op\":\"scan\",\"memo\":\"initial_scan\",\"ref\":\"_MAP\",\"storageengine\":\"donuts-json\"," +
                "\"selection\":{\"path\":\"/donuts.json\",\"type\":\"JSON\"}},{\"op\":\"project\",\"projections\":[{\"expr\":\"_MAP\",\"ref\":" +
                "\"output._MAP\"}]},{\"op\":\"store\",\"storageengine\":\"queue\",\"memo\":\"output sink\",\"target\":{\"number\":0}}]}]}", java.util.Arrays.asList(new String[] {
                "_MAP"}), java.lang.Object.class);
    }


    public java.lang.reflect.Type getElementType() {
        return java.util.Map.class;
    }

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(Driver.class.getName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        OptiqConnection connection =
                (OptiqConnection) DriverManager.getConnection("jdbc:optiq:catalog=file:common/target/test-classes/donuts-model.json");
        new Foo().execute((DataContext) connection.getRootSchema());
    }
}
