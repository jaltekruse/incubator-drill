/**
 * Drill expression materialization and evaluation facilities.
 *
 * Drill exposes an interface for defining custom scalar and aggregate functions.
 * These functions are found by scanning the classpath at runtime and allow users
 * to add their own functions without rebuilding Drill or changing cluster
 * configuration.
 *
 * The classes that define these functions are actually decomposed at the source
 * level, copied into generated code blocks to evaluate an entire expression
 * tree. This generated source is built at run-time as schema is discovered.
 *
 * This package contains the {@see DrillSimpleFunc} and {@see DrillAggFunc}
 * interfaces that can be implemented by users to define their own aggregate
 * and scalar functions.
 */
package org.apache.drill.exec.expr;