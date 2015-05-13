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
package org.apache.drill.exec.server;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;

public class ExceptionInWriterTest {
  Class classWithInjector;
  String siteName;
  Class<? extends Throwable> exceptionClass;
  HashMap<String, String> options = new HashMap<>();

  public ExceptionInWriterTest() {};

  public ExceptionInWriterTest(
      Class classWithInjector,
      String siteName,
      Class<? extends Throwable> exceptionClass,
      HashMap<String, String> options
  ) {
    this.classWithInjector = classWithInjector;
    this.siteName = siteName;
    this.exceptionClass = exceptionClass;
    this.options = options;
  }

  public ExceptionInWriterTest clone() {
    return new ExceptionInWriterTest(
        classWithInjector,
        siteName,
        exceptionClass,
        options);
  }

  public static WriterTestBuilder getBuilder() {
    return new WriterTestBuilder();
  }

  public static class WriterTestBuilder {
    List<ExceptionInWriterTest> tests = Lists.newArrayList();
    ExceptionInWriterTest testBeingBuilt = new ExceptionInWriterTest();

    public WriterTestBuilder setClassWithInjector(Class clazz) {
      testBeingBuilt.classWithInjector = clazz; return this;
    }

    public WriterTestBuilder setInjectionSite(String site) {
      testBeingBuilt.siteName = site; return this;
    }

    public WriterTestBuilder setExceptionClass(Class<? extends Throwable> clazz) {
      testBeingBuilt.exceptionClass = clazz; return this;
    }

    public WriterTestBuilder setOption(String name, String val) {
      testBeingBuilt.options.put(name, val); return this;
    }

    public WriterTestBuilder clearOptions() {
      testBeingBuilt.options.clear(); return this;
    }

    public WriterTestBuilder saveCurrentAsNewTest() {
      tests.add(testBeingBuilt.clone());
      return this;
    }

    public List<ExceptionInWriterTest> getTests() {
      return tests;
    }
  }
}
