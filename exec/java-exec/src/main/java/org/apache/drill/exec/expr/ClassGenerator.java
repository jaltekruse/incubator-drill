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
package org.apache.drill.exec.expr;

import java.io.IOException;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;

import com.google.common.base.Preconditions;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;


public class ClassGenerator<T>{
  
  private static final String PACKAGE_NAME = "org.apache.drill.exec.test.generated";
  
  private final TemplateClassDefinition<T> definition;
  private final String className;
  private final String fqcn;
  private final JCodeModel model;
  private final CodeGenerator<T> rootGenerator;
  
  ClassGenerator(TemplateClassDefinition<T> definition, FunctionImplementationRegistry funcRegistry) {
    this(CodeGenerator.getDefaultMapping(), definition, funcRegistry);
  }
  
  ClassGenerator(MappingSet mappingSet, TemplateClassDefinition<T> definition, FunctionImplementationRegistry funcRegistry) {
    Preconditions.checkNotNull(definition.getSignature(), "The signature for defintion %s was incorrectly initialized.", definition);
    this.definition = definition;
    this.className = definition.getExternalInterface().getSimpleName() + "Gen" + definition.getNextClassNumber();
    this.fqcn = PACKAGE_NAME + "." + className;
    try{
      this.model = new JCodeModel();
      JDefinedClass clazz = model._package(PACKAGE_NAME)._class(className);
      rootGenerator = new CodeGenerator<>(this, mappingSet, definition.getSignature(), new EvaluationVisitor(funcRegistry), clazz, model);
    } catch (JClassAlreadyExistsException e) {
      throw new IllegalStateException(e);
    }
  }
  
  public CodeGenerator<T> getRoot(){
    return rootGenerator;
  }
  
  public String generate() throws IOException{
    rootGenerator.flushCode();
    
    SingleClassStringWriter w = new SingleClassStringWriter();
    model.build(w);
    return w.getCode().toString();
  }
  
  public TemplateClassDefinition<T> getDefinition(){
    return definition;
  }
  
  public String getMaterializedClassName(){
    return fqcn;
  }
  
  public static <T> ClassGenerator<T> get(TemplateClassDefinition<T> definition, FunctionImplementationRegistry funcRegistry){
    return new ClassGenerator<T>(definition, funcRegistry);
  }
  
  public static <T> ClassGenerator<T> get(MappingSet mappingSet, TemplateClassDefinition<T> definition, FunctionImplementationRegistry funcRegistry){
    return new ClassGenerator<T>(mappingSet, definition, funcRegistry);
  }
  
  
}
