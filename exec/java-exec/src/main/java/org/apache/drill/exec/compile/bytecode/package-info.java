/**
 * Bytecode manipulation utilities for stitching together runtime generated code with prebuilt templates.
 *
 * Also includes a number of performance optimizations, including scalar replacement for small data only objects
 * to avoid object churn. The object definitions are convenient for defining user facing APIs, but end up
 * slowing down code execution significantly.
 */
package org.apache.drill.exec.compile.bytecode;