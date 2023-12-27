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
  val modulesRoot = "modulesRoot"

  val forbiddenDependencies = "forbiddenDependencies"
  val name = "name"
  val configurations = "configurations"
  val allowedIn = "allowedIn"
  val allowedPatterns = "allowedPatterns"
  val internalOnly = "internalOnly"
  val externalOnly = "externalOnly"

  // conf.obt
  val scalaHomePath = "scalaHomePath"
  val javaHomePath = "javaHomePath"

  // stratosphere.conf
  val workspace = "workspace"
  val javaProject = "javaProject"
  val javaVersion = "javaVersion"
  val stratosphereVersion = "stratosphereVersion"
  val obtVersion = "obt-version"
  val scalaSourceCompatibility = "scalaSourceCompatibility"
  val scalaVersion = "scalaVersion"
  val javaOptionFiltering = "javaOptionFiltering"

}
