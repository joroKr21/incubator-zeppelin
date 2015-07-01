/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.emma;

import java.io.*;
import java.util.*;
import java.util.logging.*;

import org.apache.zeppelin.interpreter.*;

/**
 * An interactive Zeppelin interpreter for the Emma language.
 */
public class EmmaInterpreter extends Interpreter {

  private static final Logger LOG = Logger.getLogger(className());

  static {
    Map<String, InterpreterProperty> settings =
      new InterpreterPropertyBuilder()
        .add(EmmaRepl.codeGenDirKey(),  EmmaRepl.codeGenDirDef(),
            "Temporary directory for Emma-generated classes")
        .add(EmmaRepl.execBackendKey(), EmmaRepl.execBackendDef(),
            "Runtime backend (native|flink|spark)")
        .add(EmmaRepl.execModeKey(),    EmmaRepl.execModeDef(),
            "Mode of execution (local|remote)").build();

    Interpreter.register("emma", "emma", className(), settings);
  }

  private EmmaRepl repl;

  public EmmaInterpreter(Properties properties) {
    super(properties);
    Properties pom = new Properties();
    try (InputStream is = getClass().getClassLoader().getResourceAsStream("pom.properties")) {
      pom.load(is);
      String flinkPath = pom.getProperty(EmmaRepl.flinkPathKey(), EmmaRepl.flinkPathDef());
      String sparkPath = pom.getProperty(EmmaRepl.sparkPathKey(), EmmaRepl.sparkPathDef());
      properties.setProperty(EmmaRepl.flinkPathKey(), flinkPath);
      properties.setProperty(EmmaRepl.sparkPathKey(), sparkPath);
    } catch (IOException e) {
      LOG.warning(String.format("Couldn't load pom.properties due to %s", e.getMessage()));
    }

    repl = new EmmaRepl(properties);
  }

  @Override public void open() {
    repl.init();
  }

  @Override public void close() {
    repl.close();
  }

  @Override public InterpreterResult interpret(String str, InterpreterContext context) {
    try {
      return repl.interpret(str);
    } catch (Exception e) {
      String message = InterpreterUtils.getMostRelevantMessage(e);
      return new InterpreterResult(InterpreterResult.Code.ERROR, message);
    }
  }

  @Override public void cancel(InterpreterContext context) { }

  @Override public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override public List<String> completion(String buffer, int cursor) {
    return null;
  }

  private static String className() {
    return EmmaInterpreter.class.getName();
  }
}
