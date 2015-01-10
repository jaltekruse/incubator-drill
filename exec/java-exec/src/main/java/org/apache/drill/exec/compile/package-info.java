/**
 * Runtime code-generation, compilation and bytecode-manipulation utilities.
 *
 * To achieve optimal performance in a dynamic schema environment, Drill includes a
 * complete code generation system for generating type-specific Java code for
 * most of its physical operators. This generated code is used for scalar expression
 * evaluation as well as value transfers between record batches. As a new schema arrives
 * at an operator, the schema change will be detected and prompt runtime code generation.
 * This runtime-generated code will evaluate the expressions needed for the operator to
 * process the incoming data. As the same code will often be run on a number of nodes
 * in the cluster, compiled classes are stored in the distributed cache to avoid the need
 * to re-compile and JIT-optimize the same code on every machine.
 */
package org.apache.drill.exec.compile;