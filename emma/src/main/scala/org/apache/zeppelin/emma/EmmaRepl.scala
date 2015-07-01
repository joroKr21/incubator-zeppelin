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
package org.apache.zeppelin.emma

import java.io._
import java.util.Properties

import org.apache.zeppelin.interpreter.InterpreterResult

import scala.Console.{withErr, withOut}
import scala.language.implicitConversions
import scala.tools.nsc._
import scala.tools.nsc.interpreter._

class EmmaRepl(properties: Properties) {
  import EmmaRepl._

  private val genDir = properties.getProperty(codeGenDirKey, codeGenDirDef)
  private val buffer = new ByteArrayOutputStream
  private val writer = new PrintWriter(buffer)

  private val scalaRepl = {
    val settings = new GenericRunnerSettings(writer.println)
    val flinkPath = properties.getProperty(flinkPathKey, flinkPathDef)
    val sparkPath = properties.getProperty(sparkPathKey, sparkPathDef)
    settings.usejavacp.value = true
    settings.nc.value = true
    settings.classpath.append(genDir)
    properties getProperty execBackendKey match {
      case "flink" => settings.classpath.append(flinkPath)
      case "spark" => settings.classpath.append(sparkPath)
      case _       =>
    }

    new IMain(settings, writer)
  }

  def init() {
    val backend = properties.getProperty(execBackendKey, execBackendDef)
    val mode    = properties.getProperty(execModeKey,    execModeDef)
    interpret(s"""System.setProperty("$codeGenDirKey",  "$genDir")""")
    interpret(s"""System.setProperty("$execBackendKey", "$backend")""")
    interpret(s"""System.setProperty("$execModeKey",    "$mode")""")
    interpret("import eu.stratosphere.emma.api._")
    interpret("import eu.stratosphere.emma.runtime._")
    interpret("val rt = default()")
  }

  def close(): Unit = {
    interpret("rt.closeSession()")
    scalaRepl.close()
  }

  def interpret(str: String): InterpreterResult =
    withOut(buffer) {
      withErr(buffer) {
        val code = scalaRepl.interpret(s"$str\n")
        writer.flush()
        val message = buffer.toString
        buffer.reset()
        new InterpreterResult(code, message)
      }
    }

  private implicit def irToCode(ir: IR.Result): InterpreterResult.Code = {
    import InterpreterResult.Code

    ir match {
      case IR.Success    => Code.SUCCESS
      case IR.Incomplete => Code.INCOMPLETE
      case IR.Error      => Code.ERROR
    }
  }
}

object EmmaRepl {
  val codeGenDirKey  = "emma.codegen.dir"
  val codeGenDirDef  = s"${System getProperty "java.io.tmpdir"}/emma/codegen"
  val execBackendKey = "emma.execution.backend"
  val execBackendDef = "native"
  val execModeKey    = "emma.execution.mode"
  val execModeDef    = "local"
  val flinkPathKey   = "emma.flink.path"
  val flinkPathDef   = ""
  val sparkPathKey   = "emma.spark.path"
  val sparkPathDef   = ""
}
