/**
 * Logical (execution engine independent) element definitions.
 *
 * Drill has several representations of a query including SQL, logical plans and
 * physical plans. All of the logical constructs of Drill are separated from their
 * physical implementations in the Java execution engine. The components of this
 * package can be used to share common logical constructs between the current engine
 * and any other future alternative physical implementations of the same logical
 * constructs. This is the same for the logical expression constructs defined within
 * this package.
 *
 */
package org.apache.drill.common;