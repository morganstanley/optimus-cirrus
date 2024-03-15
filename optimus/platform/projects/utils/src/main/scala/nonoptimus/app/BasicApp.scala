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
package nonoptimus.app

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

import scala.reflect.ClassTag
import scala.reflect.classTag

abstract class BasicApp[T <: BasicAppCmdLine: ClassTag] {
  private def parseCmdline(args: Array[String]): T = {
    val cmdLine = classTag[T].runtimeClass.getDeclaredConstructor().newInstance().asInstanceOf[T]
    val parser = new CmdLineParser(cmdLine)
    try {
      parser.parseArgument(args.toArray: _*)
    } catch {
      case x: CmdLineException =>
        System.err.println(x.getMessage)
        parser.printUsage(System.err)
        System.exit(1)
    }
    cmdLine
  }

  final def main(args: Array[String]): Unit = {
    run(parseCmdline(args))
  }

  protected def run(cmdLine: T): Unit
}

class BasicAppCmdLine
