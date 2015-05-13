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
package org.apache.drill.exec;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.testing.ControlsInjectionUtil;
import org.junit.Ignore;
import org.junit.internal.runners.model.ReflectiveCallable;
import org.junit.internal.runners.statements.Fail;
import org.junit.rules.RunRules;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.List;

public class DrillMemoryLeakFinderTestRunner extends BlockJUnit4ClassRunner {

  BaseTestQuery testClass;

  /**
   * Creates a BlockJUnit4ClassRunner to run {@code klass}
   *
   * @throws org.junit.runners.model.InitializationError
   *          if the test class is malformed.
   */
  public DrillMemoryLeakFinderTestRunner(Class<?> klass) throws InitializationError {
    super(klass);
  }

  @Override
  protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
    Description description = describeChild(method);
    if (method.getAnnotation(Ignore.class) != null) {
      notifier.fireTestIgnored(description);
    } else {
      Statement statement = methodBlock(method);

      CoordinationProtos.DrillbitEndpoint endpoint = testClass.bits[0].getContext().getEndpoint();

      List<Integer> numSkips = new ArrayList();
      // add 1-20
      int i = 0;
      for (; i < 20; i++ ) {
        numSkips.add(i);
      }
      // for 20-40, add every 4th
      for (; i < 50; i += 4 ) {
        numSkips.add(i);
      }
      // for 50-150, add every 10th
      for (; i < 150; i+= 10 ) {
        numSkips.add(i);
      }
      // for 150-400, add every 30th
      for (; i < 400; i += 30 ) {
        numSkips.add(i);
      }
      for (int currNumSkips : numSkips) {
        String controlsString = "{\"injections\":[{"
            + "\"address\":\"" + endpoint.getAddress() + "\","
            + "\"port\":\"" + endpoint.getUserPort() + "\","
            + "\"type\":\"exception\","
            + "\"siteClass\":\"" + TopLevelAllocator.class.getName() + "\","
            + "\"desc\":\"" + TopLevelAllocator.CHILD_ALLOCATOR_INJECTION_SITE + "\","
            + "\"nSkip\":" + currNumSkips + ","
            + "\"nFire\":1,"
            + "\"exceptionClass\":\"" + OutOfMemoryRuntimeException.class.getName() + "\""
            + "}]}";
        ControlsInjectionUtil.setControls(testClass.client, controlsString);
        // run the junit test with this current setting for the number of skips before an injected OOM
        runLeaf(statement, description, notifier);
      }
    }
  }


  /**
   * Copied from superclass, modifed to store the test class in this runner
   */
  protected Statement methodBlock(FrameworkMethod method) {
    Object test;
    try {
      test = new ReflectiveCallable() {
        @Override
        protected Object runReflectiveCall() throws Throwable {
          return createTest();
        }
      }.run();
    } catch (Throwable e) {
      return new Fail(e);
    }

    if (test instanceof BaseTestQuery) {
      System.out.println("Running a Drill test");
      testClass = (BaseTestQuery) test;
    }
    Statement statement = methodInvoker(method, test);
    statement = possiblyExpectingExceptions(method, test, statement);
    statement = withPotentialTimeout(method, test, statement);
    statement = withBefores(method, test, statement);
    statement = withAfters(method, test, statement);
    statement = withRules(method, test, statement);
    return statement;
  }

  // copied from parent because private
  private Statement withRules(FrameworkMethod method, Object target,
                              Statement statement) {
    List<TestRule> testRules = getTestRules(target);
    Statement result = statement;
    result = withMethodRules(method, testRules, target, result);
    result = withTestRules(method, testRules, result);

    return result;
  }

  // copied from parent because private
  private Statement withMethodRules(FrameworkMethod method, List<TestRule> testRules,
                                    Object target, Statement result) {
    for (org.junit.rules.MethodRule each : getMethodRules(target)) {
      if (!testRules.contains(each)) {
        result = each.apply(result, method, target);
      }
    }
    return result;
  }

  private List<org.junit.rules.MethodRule> getMethodRules(Object target) {
    return rules(target);
  }

  // copied from parent because private
  private Statement withTestRules(FrameworkMethod method, List<TestRule> testRules,
                                  Statement statement) {
    return testRules.isEmpty() ? statement :
        new RunRules(statement, testRules, describeChild(method));
  }
}
