/**
 * Java-based client for submitting queries and accepting result sets from a Drill server.
 *
 * Data returned to this client is stored in the default columnar Value Vector data structures used
 * by Drill internally. For users of Drill requiring a more traditional database interface to consume
 * from an application, a JDBC driver is available as well, see the {@see org.apache.drill.jdbc} package.
 */
package org.apache.drill.exec.client;