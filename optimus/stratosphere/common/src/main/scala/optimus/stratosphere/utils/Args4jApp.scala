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
package optimus.stratosphere.utils

import optimus.utils.ExitCode
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

abstract class Args4jApp[AppArgs](appArgs: AppArgs) {

  final def main(args: Array[String]): Unit = {
    val parser: CmdLineParser = new CmdLineParser(appArgs)
    try {
      parser.parseArgument(args: _*)
      run(appArgs)
    } catch {
      case e: CmdLineException =>
        System.err.println(e.getMessage)
        parser.printUsage(System.err)
        System.exit(ExitCode.Failure)
    }
  }

  def run(args: AppArgs): Unit
}
