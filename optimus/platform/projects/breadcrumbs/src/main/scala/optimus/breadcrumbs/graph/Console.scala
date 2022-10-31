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
import optimus.scalacompat.repl.ScalaRepl
import optimus.scalacompat.repl.ScalaReplSettings

object Console extends App {
  val autoRun = """
  import optimus.breadcrumbs.crumbs.{Events => Ev}
  import optimus.breadcrumbs.crumbs.{EdgeType => Ed}
  import optimus.breadcrumbs.crumbs.{Properties=>P}
  import optimus.breadcrumbs.graph._
  import spray.json._
  import DefaultJsonProtocol._
  import optimus.breadcrumbs.crumbs.Properties.JsonImplicits._
  import Vertex._
  """
  ScalaRepl.start(new ScalaReplSettings {
    override def prompt = Some("==> ")
    override def welcomeMessage = Some("Breadcrumbs REPL")
    override def classpath = ClassPathUtils.expandedApplicationClasspathString
    override def useJavaClasspath = true
    override def seedScript = autoRun
  })
}
