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
package org.apache.drill;

import com.google.common.base.Joiner;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;

import java.io.IOException;
import java.util.Map;

public class TestBuilder {

  // test query to run
  private String query;
  // the type of query for the test
  private UserBitShared.QueryType queryType;
  // should the validation enforce ordering
  private Boolean ordered;
  private boolean approximateEquality;
  private BufferAllocator allocator;
  // Used to pass the type information associated with particular column names rather than relying on the
  // ordering of the columns in the CSV file, or the default type inferences when reading JSON, this is used for the
  // case where results of the test query are adding type casts to the baseline queries, this saves a little bit of
  // setup in cases where strict type enforcement is not necessary for a given test
  protected Map<SchemaPath, TypeProtos.MinorType> baselineTypeMap;
  // queries to run before the baseline or test queries, can be used to set options
  private String baselineOptionSettingQueries;
  private String testOptionSettingQueries;
  // two different methods are available for comparing ordered results, the default reads all of the records
  // into giant lists of objects, like one giant on-heap batch of 'vectors'
  // this flag enables the other approach which iterates through a hyper batch for the test query results and baseline
  // while this does work faster and use less memory, it can be harder to debug as all of the elements are not in a
  // single list
  private boolean highPerformanceComparison;

  public TestBuilder(BufferAllocator allocator) {
    this.allocator = allocator;
    reset();
  }

  public TestBuilder(BufferAllocator allocator, String query, UserBitShared.QueryType queryType, Boolean ordered,
                     boolean approximateEquality, Map<SchemaPath, TypeProtos.MinorType> baselineTypeMap,
                     String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison) {
    this(allocator);
    if (ordered == null) {
      throw new RuntimeException("Ordering not set, you must explicitly call the ordered() or unOrdered() method on the " + this.getClass().getSimpleName());
    }
    this.query = query;
    this.queryType = queryType;
    this.ordered = ordered;
    this.approximateEquality = approximateEquality;
    this.baselineTypeMap = baselineTypeMap;
    this.baselineOptionSettingQueries = baselineOptionSettingQueries;
    this.testOptionSettingQueries = testOptionSettingQueries;
    this.highPerformanceComparison = highPerformanceComparison;
  }

  protected TestBuilder reset() {
    query = "";
    ordered = null;
    approximateEquality = false;
    highPerformanceComparison = false;
    testOptionSettingQueries = "";
    baselineOptionSettingQueries = "";
    return this;
  }

  public DrillTestWrapper build() throws Exception {
    if ( ! ordered && highPerformanceComparison ) {
      throw new Exception("High performance comparison only available for ordered checks, to enforce this restriction, ordered() must be called first.");
    }
    String validationQuery = null;
    if (typeInfoSet()) {
      validationQuery = getValidationQuery();
    }
    return new DrillTestWrapper(this, allocator, query, queryType, baselineOptionSettingQueries, testOptionSettingQueries,
        validationQuery, getValidationQueryType(), ordered, approximateEquality, highPerformanceComparison);
  }

  public TestBuilder sqlQuery(String query) {
    this.query = query;
    this.queryType = UserBitShared.QueryType.SQL;
    return this;
  }

  public TestBuilder physicalPlan(String query) {
    this.query = query;
    this.queryType = UserBitShared.QueryType.PHYSICAL;
    return this;
  }

  public TestBuilder ordered() {
    this.ordered = true;
    return this;
  }

  public TestBuilder unOrdered() {
    this.ordered = false;
    return this;
  }

  // this can only be used with ordered verifications, it does run faster and use less memory but may be
  // a little harder to debug as it iterates over a hyper batch rather than reading all of the values into
  // large on-heap lists
  public TestBuilder highPerformanceComparison() throws Exception {
    this.highPerformanceComparison = true;
    return this;
  }

  // list of queries to run before the baseline query, can be used to set several options
  // list takes the form of a semi-colon separated list
  public TestBuilder optionSettingQueriesForBaseline(String queries) {
    this.baselineOptionSettingQueries = queries;
    return this;
  }

  // list of queries to run before the test query, can be used to set several options
  // list takes the form of a semi-colon separated list
  public TestBuilder optionSettingQueriesForTestQuery(String queries) {
    this.testOptionSettingQueries = queries;
    return this;
  }
  public TestBuilder approximateEquality() {
    this.approximateEquality = true;
    return this;
  }

  public static SchemaPath parsePath(String path) {
    try {
      // logger.debug("Parsing expression string '{}'", expr);
      ExprLexer lexer = new ExprLexer(new ANTLRStringStream(path));
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      ExprParser parser = new ExprParser(tokens);

      //TODO: move functionregistry and error collector to injectables.
      //ctxt.findInjectableValue(valueId, forProperty, beanInstance)
      ExprParser.parse_return ret = parser.parse();

      // ret.e.resolveAndValidate(expr, errorCollector);
      if (ret.e instanceof SchemaPath) {
        return (SchemaPath) ret.e;
      } else {
        throw new IllegalStateException("Schema path is not a valid format.");
      }
    } catch (RecognitionException e) {
      throw new RuntimeException(e);
    }
  }

  String getValidationQuery() throws Exception {
    throw new RuntimeException("Must provide some kind of baseline, either a baseline file or another query");
  }

  protected UserBitShared.QueryType getValidationQueryType() throws Exception {
    throw new RuntimeException("Must provide some kind of baseline, either a baseline file or another query");
  }

  public JSONTestBuilder jsonBaselineFile(String filePath) {
    return new JSONTestBuilder(filePath, allocator, query, queryType, ordered, approximateEquality,
        baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison);
  }

  public CSVTestBuilder csvBaselineFile(String filePath) {
    return new CSVTestBuilder(filePath, allocator, query, queryType, ordered, approximateEquality,
        baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison);
  }

  public TestBuilder baselineTypes(Map<SchemaPath, TypeProtos.MinorType> baselineTypeMap) {
    this.baselineTypeMap = baselineTypeMap;
    return this;
  }

  boolean typeInfoSet() {
    if (baselineTypeMap != null) {
      return true;
    } else {
      return false;
    }
  }

  // provide a SQL query to validate against
  public BaselineQueryTestBuilder sqlBaselineQuery(String baselineQuery) {
    return new BaselineQueryTestBuilder(baselineQuery, UserBitShared.QueryType.SQL, allocator, query, queryType, ordered, approximateEquality,
        baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison);
  }

  // provide a path to a file containing a SQL query to use as a baseline
  public BaselineQueryTestBuilder sqlBaselineQueryFromFile(String baselineQueryFilename) throws IOException {
    String baselineQuery = BaseTestQuery.getFile(baselineQueryFilename);
    return new BaselineQueryTestBuilder(baselineQuery, UserBitShared.QueryType.SQL, allocator, query, queryType, ordered, approximateEquality,
        baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison);
  }

  // as physical plans are verbose, this is the only option provided for specifying them, we should enforce
  // that physical plans, or any large JSON strings do not live in the Java source as literals
  public BaselineQueryTestBuilder physicalPlanBaselineQueryFromFile(String baselinePhysicalPlanPath) throws IOException {
    String baselineQuery = BaseTestQuery.getFile(baselinePhysicalPlanPath);
    return new BaselineQueryTestBuilder(baselineQuery, UserBitShared.QueryType.PHYSICAL, allocator, query, queryType, ordered, approximateEquality,
        baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison);
  }

  public class CSVTestBuilder extends TestBuilder {

    // path to the baseline file that will be inserted into the validation query
    private String baselineFilePath;
    // aliases for the baseline columns, only used in CSV where column names are not known
    private String[] baselineColumnNames;
    // use to cast the baseline file columns, if not set the types
    // that come out of the test query drive interpretation of baseline
    private TypeProtos.MinorType[] baselineTypes;

    CSVTestBuilder(String baselineFile, BufferAllocator allocator, String query, UserBitShared.QueryType queryType, Boolean ordered,
                     boolean approximateEquality, Map<SchemaPath, TypeProtos.MinorType> baselineTypeMap,
                     String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison) {
      super(allocator, query, queryType, ordered, approximateEquality, baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries,
          highPerformanceComparison);
      this.baselineFilePath = baselineFile;
    }

    public CSVTestBuilder baselineColumnNames(String... baselineColumnNames) {
      this.baselineColumnNames = baselineColumnNames;
      return this;
    }

    public CSVTestBuilder baselineTypes(TypeProtos.MinorType... baselineTypes) {
      this.baselineTypes = baselineTypes;
      this.baselineTypeMap = null;
      return this;
    }

    public CSVTestBuilder baselineTypes(Map<SchemaPath, TypeProtos.MinorType> baselineTypeMap) {
      super.baselineTypes(baselineTypeMap);
      this.baselineTypes = null;
      return this;
    }

    protected TestBuilder reset() {
      super.reset();
      baselineColumnNames = null;
      baselineTypeMap = null;
      baselineTypes = null;
      baselineFilePath = null;
      return this;
    }

    boolean typeInfoSet() {
      if (super.typeInfoSet() || baselineTypes != null) {
        return true;
      } else {
        return false;
      }
    }

    String getValidationQuery() throws Exception {
      if (baselineColumnNames.length == 0) {
        throw new Exception("Baseline CSV files require passing column names, please call the baselineColumnNames() method on the test builder.");
      }

      String[] aliasedExpectedColumns = new String[baselineColumnNames.length];
      for (int i = 0; i < baselineColumnNames.length; i++) {
        aliasedExpectedColumns[i] = "columns[" + i + "] ";
        if (baselineTypes != null) {
          aliasedExpectedColumns[i] = "cast(" + aliasedExpectedColumns[i] + " as " + Types.getNameOfMajorType(baselineTypes[i]) + " ) ";
        } else if (baselineTypeMap != null) {
          aliasedExpectedColumns[i] = "cast(" + aliasedExpectedColumns[i] + " as " + Types.getNameOfMajorType(baselineTypeMap.get(parsePath(baselineColumnNames[i]))) + " ) ";
        }
        aliasedExpectedColumns[i] += baselineColumnNames[i];
      }
      String query = "select " + Joiner.on(", ").join(aliasedExpectedColumns) + " from cp.`" + baselineFilePath + "`";
//    System.out.println(query);
      return query;
    }

    protected UserBitShared.QueryType getValidationQueryType() throws Exception {
      return UserBitShared.QueryType.SQL;
    }

  }

  public class JSONTestBuilder extends TestBuilder {

    // path to the baseline file that will be inserted into the validation query
    private String baselineFilePath;

    JSONTestBuilder(String baselineFile, BufferAllocator allocator, String query, UserBitShared.QueryType queryType, Boolean ordered,
                   boolean approximateEquality, Map<SchemaPath, TypeProtos.MinorType> baselineTypeMap,
                   String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison) {
      super(allocator, query, queryType, ordered, approximateEquality, baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries,
          highPerformanceComparison);
      this.baselineFilePath = baselineFile;
    }

    String getValidationQuery() {
      return "select * from cp.`" + baselineFilePath + "`";
    }

    protected UserBitShared.QueryType getValidationQueryType() throws Exception {
      return UserBitShared.QueryType.SQL;
    }

  }

  public class BaselineQueryTestBuilder extends TestBuilder {

    private String baselineQuery;
    private UserBitShared.QueryType baselineQueryType;

    BaselineQueryTestBuilder(String baselineQuery, UserBitShared.QueryType baselineQueryType, BufferAllocator allocator,
                             String query, UserBitShared.QueryType queryType, Boolean ordered,
                             boolean approximateEquality, Map<SchemaPath, TypeProtos.MinorType> baselineTypeMap,
                             String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison) {
      super(allocator, query, queryType, ordered, approximateEquality, baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries,
          highPerformanceComparison);
      this.baselineQuery = baselineQuery;
      this.baselineQueryType = baselineQueryType;
    }

    String getValidationQuery() {
      return baselineQuery;
    }

    protected UserBitShared.QueryType getValidationQueryType() throws Exception {
      return baselineQueryType;
    }

    // This currently assumes that all explicit baseline queries will have fully qualified type information
    // if this changes, the baseline query can be run in a sub query with the implicit or explicit type passing
    // added on top of it, as is currently when done when reading a baseline file
    boolean typeInfoSet() {
      return true;
    }

  }
}
