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
package optimus.utils.app

import optimus.platform.util.Log
import optimus.utils.ExitCode
import org.kohsuke.args4j
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.ParserProperties

import java.io.StringWriter

/**
 * Base trait to extend your params with.
 *
 * Extends [[Args4jOptionHandlers]] so that you do not need to import all the handlers manually.
 */
trait Args4jAppParams extends Args4jOptionHandlers {
  @args4j.Option(name = "--help", aliases = Array("-h"), usage = "Prints help for this command.")
  var help: Boolean = false
}

/**
 * Base class for non-Optimus args4j apps.
 *
 * Handles argument parsing, printing help and error codes.
 *
 * @tparam Params
 *   params class, needs to extend [[Args4jAppParams]]
 */
trait Args4jApp[Params <: Args4jAppParams] extends ExitCode with Log {

  /**
   * @return
   *   Whether to exit the app after the run method finishes.
   */
  def exitAfterRun: Boolean = true

  def usageWidth: Int = 120

  /**
   * @return
   *   An empty instance of [[Params]] to be filled in by args4j.
   */
  def emptyParams(): Params

  def run(params: Params): ExitCode

  final def main(args: Array[String]): Unit =
    if (exitAfterRun) sys.exit(run(args)) else run(args)

  final def run(args: Array[String]): ExitCode = {
    val props = ParserProperties.defaults().withUsageWidth(usageWidth)
    val params: Params = emptyParams()
    val parser = new CmdLineParser(params, props)

    def usageString: String = {
      val writer = new StringWriter()
      parser.printUsage(writer, null)
      writer.toString
    }

    try {
      parser.parseArgument(args: _*)

      if (params.help) {
        log.info(usageString)
        ExitCode.Success
      } else {
        run(params)
      }
    } catch {
      case cle: CmdLineException =>
        log.error(cle.getMessage)
        log.error(usageString)
        ExitCode.Failure
    }
  }
}
