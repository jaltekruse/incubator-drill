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
package org.apache.drill.optiq;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.vector.ValueVector;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.RSERegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;

import io.netty.buffer.ByteBuf;
import org.apache.drill.jdbc.DrillInstance;

/**
 * Runtime helper that executes a Drill query and converts it into an
 * {@link Enumerable}.
 */
public class EnumerableDrillFullEngine<E>
        extends AbstractEnumerable<E>
        implements Enumerable<E> {
    private final String plan;
    final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(100);
    final DrillConfig config;
    private final String holder;
    private final List<String> fields;

    private static final ObjectMapper mapper = createMapper();

    /**
     * Creates a DrillEnumerable.
     *
     * @param plan   Logical plan
     * @param clazz  Type of elements returned from enumerable
     * @param fields Names of fields, or null to return the whole blob
     */
    public EnumerableDrillFullEngine(DrillConfig config, String plan, Class<E> clazz,
                                     List<String> fields) {
        this.plan = plan;
        this.config = config;
        this.holder = null;
        this.fields = fields;
        config.setSinkQueues(0, queue);
    }

    /**
     * Creates a DrillEnumerable from a plan represented as a string. Each record
     * returned is a {@link JsonNode}.
     */
    public static <E> EnumerableDrillFullEngine<E> of(String plan,
                                                      final List<String> fieldNames, Class<E> clazz, net.hydromatic.optiq.DataContext context) {
        DrillConfig config = DrillConfig.create();
        return new EnumerableDrillFullEngine<>(config, plan, clazz, fieldNames);
    }

    /**
     * Runs the plan as a background task.
     */
    Future<List<QueryResultBatch>> runPlan(
            CompletionService<List<QueryResultBatch>> service) throws Exception {
        IteratorRegistry ir = new IteratorRegistry();


            config.setSinkQueues(0, queue);

            return service.submit(
                new Callable<List<QueryResultBatch>>() {
                    @Override
                    public List<QueryResultBatch> call() throws Exception {
                        List<QueryResultBatch> results = DrillInstance.getClient().runQuery(UserProtos.QueryType.LOGICAL, plan);

                        RecordBatchLoader batchLoader = new RecordBatchLoader(DrillInstance.getBit().getContext().getAllocator());
                        int recordCount = 0;
                        for (QueryResultBatch batch : results) {
                            if (!batch.hasData()) continue;
                            boolean schemaChanged = batchLoader.load(batch.getHeader().getDef(), batch.getData());
                            boolean firstColumn = true;

                            // print headers.
                            if (schemaChanged) {
                            } // do not believe any change is needed for when the schema changes

                            for (int i = 0; i < batchLoader.getRecordCount(); i++) {
                                recordCount++;
                                Map<String, Object> map = new HashMap();

                                for (IntObjectCursor<ValueVector<?>> v : batchLoader) {
                                    map.put(v.value.getField().getName(), v.value.getObject(i));
                                }
                                queue.add(map);
                            }
                        }
                        return null;
                    }
                }
            );
       }


    @Override
    public Enumerator<E> enumerator() {
        // TODO: use a completion service from the container
        final ExecutorCompletionService<List<QueryResultBatch>> service =
                new ExecutorCompletionService<List<QueryResultBatch>>(
                        new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS,
                                new LinkedBlockingDeque<Runnable>(10)));

        // Run the plan using an executor. It runs in a different thread, writing
        // results to our queue.
        //
        // TODO: use the result of task, and check for exceptions

        try {
            runPlan(service);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new ResultEnumerator(queue, fields);
    }

    private static ObjectMapper createMapper() {
        return new ObjectMapper();
    }

    /**
     * Converts a JSON document, represented as an array of bytes, into a Java
     * object (consisting of Map, List, String, Integer, Double, Boolean).
     */
    static Object parseJson(byte[] bytes) {
        try {
            return wrapper(mapper.readTree(bytes));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts a JSON node to Java objects ({@link List}, {@link Map},
     * {@link String}, {@link Integer}, {@link Double}, {@link Boolean}.
     */
    static Object wrapper(JsonNode node) {
        switch (node.asToken()) {
            case START_OBJECT:
                return map((ObjectNode) node);
            case START_ARRAY:
                return array((ArrayNode) node);
            case VALUE_STRING:
                return node.asText();
            case VALUE_NUMBER_INT:
                return node.asInt();
            case VALUE_NUMBER_FLOAT:
                return node.asDouble();
            case VALUE_TRUE:
                return Boolean.TRUE;
            case VALUE_FALSE:
                return Boolean.FALSE;
            case VALUE_NULL:
                return null;
            default:
                throw new AssertionError("unexpected: " + node + ": " + node.asToken());
        }
    }

    private static List<Object> array(ArrayNode node) {
        final List<Object> list = new ArrayList<>();
        for (JsonNode jsonNode : node) {
            list.add(wrapper(jsonNode));
        }
        return Collections.unmodifiableList(list);
    }

    private static SortedMap<String, Object> map(ObjectNode node) {
        // TreeMap makes the results deterministic.
        final TreeMap<String, Object> map = new TreeMap<>();
        final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> next = fields.next();
            map.put(next.getKey(), wrapper(next.getValue()));
        }
        return Collections.unmodifiableSortedMap(map);
    }

    private static class ResultEnumerator implements Enumerator {
        private final BlockingQueue<Object> queue;
        private final String holder;
        private final List<String> fields;
        private Object current;

        public ResultEnumerator(BlockingQueue<Object> queue, List<String> fields) {
            this.queue = queue;
            this.holder = null;
            this.fields = fields;
        }

        public Object current() {
            return current;
        }

        public boolean moveNext() {
            try {
                // not sure how julian's code worked without this check

                // looks like the RI was adding a RunOutcome to the end of the results that was not being added by the full engine, is this somehting
                // we want to add? Is this success reporting being handled differently by the full execution enginge?
                if (queue.isEmpty()) return false;
                Object o = queue.take();
                if (o instanceof RunOutcome.OutcomeType) {
                    switch ((RunOutcome.OutcomeType) o) {
                        case SUCCESS:
                            return false; // end of data
                        case CANCELED:
                            throw new RuntimeException("canceled");
                        case FAILED:
                        default:
                            throw new RuntimeException("failed");
                    }
                } else {
                    if (holder != null) {
                        o = ((Map<String, Object>) o).get(holder);
                    }
                    if (fields == null) {
                        current = o;
                    } else {
                        final Map<String, Object> map = (Map<String, Object>) o;
                        if (fields.size() == 1) {
                            current = map.get(fields.get(0));
                        } else {
                            Object[] os = new Object[fields.size()];
                            for (int i = 0; i < os.length; i++) {
                                os[i] = map.get(fields.get(i));
                            }
                            current = os;
                        }
                    }
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new RuntimeException(e);
            }
        }

        public void reset() {
            throw new UnsupportedOperationException();
        }
    }
}

// End EnumerableDrill.java
