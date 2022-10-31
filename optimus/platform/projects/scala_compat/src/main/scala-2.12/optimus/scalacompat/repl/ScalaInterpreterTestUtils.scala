/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.scalacompat.repl

import optimus.scalacompat.repl.ScalaInterpreter.Message

import scala.collection.mutable
import scala.reflect.internal.Reporter.ERROR
import scala.reflect.internal.Reporter.WARNING

object ScalaInterpreterTestUtils {
  implicit def ReplTestOps(repl: ScalaRepl): InterpreterTestOps = new InterpreterTestOps(repl.intp)

  implicit class InterpreterTestOps(val intp: ScalaInterpreter) extends AnyVal {
    private def resName = "__replTestResult"

    def value[T](expr: String): T = {
      intp.interpret(s"val $resName = { $expr }")
      intp.iMain.valueOfTerm(resName).get.asInstanceOf[T]
    }

    def str(expr: String): String = {
      intp.interpret(s"val $resName = scala.runtime.ScalaRunTime.stringOf($expr)")
      intp.iMain.valueOfTerm(resName).get.asInstanceOf[String]
    }

    def messages: List[Message] = intp.reporterState.messages

    def warnings: List[Message] = messages.filter(_.severity == WARNING)
    def warningCount: Int = warnings.length
    def hasWarnings: Boolean = warnings.lengthCompare(0) > 0

    /**
     * Check if the reporter has the exepected warnings. `expected` should contain a substring of each reported warning.
     * Typical usage: assertEquals("", repl.checkWarnings(Set("pure expression does nothing")))
     * @return
     *   the empty string if the tests succeeds, a diagnostic string otherwise
     */
    def checkWarnings(expected: Set[String]): String = checkMessages(expected, warnings)

    def errors: List[Message] = messages.filter(_.severity == ERROR)
    def errorCount: Int = errors.length
    def hasErrors: Boolean = errors.lengthCompare(0) > 0

    /**
     * Check if the reporter has the exepected errors. `expected` should contain a substring of each reported error.
     * Typical usage: assertEquals("", repl.checkErrors(Set("not found: type A")))
     * @return
     *   the empty string if the tests succeeds, a diagnostic string otherwise
     */
    def checkErrors(expected: Set[String]): String = checkMessages(expected, errors)

    def resetMessages(): Unit = intp.reporterState.reset()

    def checkMessages(expected: Set[String], found: List[Message]): String = {
      def lst(msgs: Iterable[String]) = msgs.mkString("- ", "- \n", "- ")
      val count = found.length
      if (count != expected.size)
        s"expected ${expected.size}, found: $count\n${lst(found.map(_.msg))}"
      else {
        val missing = mutable.Set.empty[String]
        val unmatched = found.to[mutable.Set]
        for (e <- expected) {
          unmatched.find(m => m.msg.contains(e)).fold[Unit](missing += e)(unmatched.remove)
        }
        if (unmatched.isEmpty) ""
        else s"unexpected messges:\n${lst(unmatched.map(_.msg))}\nsubstrings not found:\n${lst(missing)}"
      }
    }
  }
}
