/**
 * Interface for drill's interaction with the local disk to persist data temporarily during an operation.
 *
 * This interface is used by blocking operators that support 'spilling to disk'. These operators can go
 * beyond the limits of a single machines memory when performing an operation like a sort on a large
 * dataset.
 */
package org.apache.drill.exec.disk;