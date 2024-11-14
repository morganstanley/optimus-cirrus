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
package optimus.examples.platform

import optimus.platform._

import org.kohsuke.args4j._

class CustomCommandLineCmdLine extends OptimusAppCmdLine {
  @Option(name = "-n", aliases = Array("--nope"), usage = "Don't do anything")
  var nope = false

  @Option(name = "-c", aliases = Array("--count"), usage = "Do a countdown")
  var countDown = 0
}

object CustomCommandLine extends LegacyOptimusApp[CustomCommandLineCmdLine] {
  if (cmdLine.nope) {
    println("Not doing anything as per user request.")
    System.exit(0)
  }

  if (cmdLine.countDown > 0) {
    (cmdLine.countDown to 1 by -1) foreach { println(_) }
    println("Blastoff!")
  }

  println("Goodbye")
}
