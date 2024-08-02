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
package optimus.buildtool.runconf

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger

import java.lang.management.ManagementFactory
import scala.jdk.CollectionConverters._

object DiagnosticApp extends App {
  protected val log: Logger = getLogger(this)

  private val NewLineWithIndent = "\n  "

  private val runtimeMXBean = ManagementFactory.getRuntimeMXBean
  private val jvmArgs = runtimeMXBean.getInputArguments.asScala
  log.info(s"JVM Options: ${jvmArgs.mkString(NewLineWithIndent, NewLineWithIndent, "\n")}")

  private def kvToStr(kv: (String, Any)): String =
    kv match {
      case (key, value) => s"$key = '$value'"
    }

  private def key(kv: (String, Any)): String = kv match {
    case (key, _) => key
  }

  private val envVars = sys.env.toSeq.sortBy(key).map(kvToStr)
  log.info(s"Environment Variables: ${envVars.mkString(NewLineWithIndent, NewLineWithIndent, "\n")}")
}
