/**
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
 */
package org.apache.drill.exec.compile;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassTransformer.ClassNames;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.server.options.OptionManager;
import org.codehaus.commons.compiler.CompileException;

import com.google.common.collect.MapMaker;

public class QueryClassLoader extends URLClassLoader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryClassLoader.class);

  private ClassCompilerSelector compilerSelector;

  private AtomicLong index = new AtomicLong(0);

  private ConcurrentMap<String, byte[]> customClasses = new MapMaker().concurrencyLevel(4).makeMap();

  public QueryClassLoader(DrillConfig config, OptionManager sessionOptions) {
    super(new URL[0]);
    compilerSelector = new ClassCompilerSelector(config, sessionOptions);
  }

  public long getNextClassIndex() {
    return index.getAndIncrement();
  }

  public void injectByteCode(String className, byte[] classBytes) throws IOException {
    if (customClasses.containsKey(className)) {
      throw new IOException(String.format("The class defined %s has already been loaded.", className));
    }
    customClasses.put(className, classBytes);
  }

  @Override
  protected Class<?> findClass(String className) throws ClassNotFoundException {
    byte[] ba = customClasses.get(className);
    if (ba != null) {
      return this.defineClass(className, ba, 0, ba.length);
    }else{
      return super.findClass(className);
    }
  }

  public byte[][] getClassByteCode(final ClassNames className, final String sourceCode)
      throws CompileException, IOException, ClassNotFoundException, ClassTransformationException {
    return compilerSelector.getClassByteCode(className, sourceCode);
  }

  public enum CompilerPolicy {
    DEFAULT, JDK, JANINO;

  }

  private class ClassCompilerSelector {
    private final CompilerPolicy policy;
    private final long janinoThreshold;

    private final AbstractClassCompiler jdkClassCompiler;
    private final AbstractClassCompiler janinoClassCompiler;


    ClassCompilerSelector(DrillConfig config, OptionManager sessionOptions) {
      this.policy = CompilerPolicy.valueOf(sessionOptions.getOption(ExecConstants.JAVA_COMPILER_OPTION).toUpperCase());
      this.janinoThreshold = sessionOptions.getOption(ExecConstants.JAVA_COMPILER_JANINO_MAXSIZE);
      boolean debug = sessionOptions.getOption(ExecConstants.JAVA_COMPILER_DEBUG);

      this.janinoClassCompiler = (policy == CompilerPolicy.JANINO || policy == CompilerPolicy.DEFAULT) ? new JaninoClassCompiler(QueryClassLoader.this, debug) : null;
      this.jdkClassCompiler = (policy == CompilerPolicy.JDK || policy == CompilerPolicy.DEFAULT) ? JDKClassCompiler.newInstance(QueryClassLoader.this, debug) : null;
    }

    private byte[][] getClassByteCode(ClassNames className, String sourceCode)
        throws CompileException, ClassNotFoundException, ClassTransformationException, IOException {
      AbstractClassCompiler classCompiler;
      if (jdkClassCompiler != null &&
          (policy == CompilerPolicy.JDK || (policy == CompilerPolicy.DEFAULT && sourceCode.length() > janinoThreshold))) {
        classCompiler = jdkClassCompiler;
      } else {
        classCompiler = janinoClassCompiler;
      }

      byte[][] bc = classCompiler.getClassByteCode(className, sourceCode);
      /*
       * final String baseDir = System.getProperty("java.io.tmpdir") + File.separator + classCompiler.getClass().getSimpleName();
       * File classFile = new File(baseDir + className.clazz);
       * classFile.getParentFile().mkdirs();
       * BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(classFile));
       * out.write(bc[0]);
       * out.close();
       */
      return bc;
    }

  }

}
