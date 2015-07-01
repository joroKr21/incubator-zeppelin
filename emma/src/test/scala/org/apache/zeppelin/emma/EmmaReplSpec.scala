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

import java.util.Properties

import org.apache.zeppelin.interpreter.InterpreterResult.Code
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks

@RunWith(classOf[JUnitRunner])
class EmmaReplSpec extends FlatSpec with Matchers with PropertyChecks {

  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(minSuccessful = 10)

  "the Emma REPL" should "detect incomplete code" in {
    withRepl() { repl =>
      val result = repl interpret "val x = "
      result.code should be (Code.INCOMPLETE)
    }
  }

  it should "detect compilation errors" in {
    withRepl() { repl =>
      val result = repl interpret "val x: Int = \"42\""
      result.code    should be      (Code.ERROR)
      result.message should include ("error: type mismatch")
    }
  }

  it should "handle correct code without errors" in {
    withRepl() { repl =>
      val result = repl interpret "val x = 42"
      result.code         should be (Code.SUCCESS)
      result.message.trim should be ("x: Int = 42")
    }
  }

  it should "have a predefined runtime object" in {
    withRepl() { repl =>
      val result = repl interpret "rt"
      result.code should be (Code.SUCCESS)
    }
  }

  it should "run a simple sum algorithm" in {
    forAll { xs: Seq[Int] =>
      withRepl() { repl =>
        val result = repl.interpret(s"""
          emma.parallelize {
            DataBag($xs: Seq[Int]).sum()
          }.run(rt)""")

        result.code    should be      (Code.SUCCESS)
        result.message should include (xs.sum.toString)
      }
    }
  }

  def withRepl(codeGenDir: String = EmmaRepl.codeGenDirDef,
               backend:    String = EmmaRepl.execBackendDef,
               mode:       String = EmmaRepl.execModeDef)
              (f: EmmaRepl => Unit): Unit = {
    val props = new Properties()
    props.setProperty(EmmaRepl.codeGenDirKey,  codeGenDir)
    props.setProperty(EmmaRepl.execBackendKey, backend)
    props.setProperty(EmmaRepl.execModeKey,    mode)
    val repl  = new EmmaRepl(props)
    repl.init()
    f(repl)
    repl.close()
  }
}
