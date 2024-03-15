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
package optimus.buildtool.format

object Names {

  // bundles files
  val ModulesRoot = "modulesRoot"

  val ForbiddenDependencies = "forbiddenDependencies"
  val Name = "name"
  val Configurations = "configurations"
  val AllowedIn = "allowedIn"
  val AllowedPatterns = "allowedPatterns"
  val InternalOnly = "internalOnly"
  val ExternalOnly = "externalOnly"

  // conf.obt
  val ScalaHomePath = "scalaHomePath"
  val JavaHomePath = "javaHomePath"

  // stratosphere.conf
  val Workspace = "workspace"
  val JavaProject = "javaProject"
  val JavaVersion = "javaVersion"
  val StratosphereVersion = "stratosphereVersion"
  val ObtVersion = "obt-version"
  val ScalaSourceCompatibility = "scalaSourceCompatibility"
  val ScalaVersion = "scalaVersion"
  val JavaOptionFiltering = "javaOptionFiltering"

}
