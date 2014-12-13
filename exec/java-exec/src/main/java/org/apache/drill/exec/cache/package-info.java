/**
 * Distributed cache for syncing state and data between Drillbits.
 *
 * The distributed cache defined in this package can be used to make data
 * available to all of the nodes in a Drill cluster. This is useful in cases where
 * most of the work of a given operation can be separated into independent tasks, but
 * some common information must be available to and mutable by all of the nodes
 * currently involved in the operation.
 */
package org.apache.drill.exec.cache;