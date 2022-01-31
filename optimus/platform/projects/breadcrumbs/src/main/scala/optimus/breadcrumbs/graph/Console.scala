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
package optimus.breadcrumbs.graph

import optimus.platform.utils.ClassPathUtils

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.ILoop

object Console extends App {
  val cl = new ConsoleLoop()
  val settings = new Settings
  settings.usejavacp.value = true
  settings.classpath.value = ClassPathUtils.expandedApplicationClasspathString
  settings.deprecation.value = true
  cl.process(settings)
}

class ConsoleLoop extends ILoop {

  override def prompt = "==> "
  val initialCommands = """
  import optimus.breadcrumbs.crumbs.{Events => Ev}
  import optimus.breadcrumbs.crumbs.{EdgeType => Ed}
  import optimus.breadcrumbs.crumbs.{Properties=>P}
  import optimus.breadcrumbs.graph._
  import spray.json._
  import DefaultJsonProtocol._
  import optimus.breadcrumbs.crumbs.Properties.JsonImplicits._
  import Vertex._
  """

  override def createInterpreter(): Unit = {
    super.createInterpreter()
    intp.interpret(initialCommands)
  }

  override def printWelcome(): Unit = {
    echo("Breadcrumbs REPL")
  }

}
