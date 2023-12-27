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
package optimus.buildtool.config

import optimus.buildtool.files.Directory
import optimus.buildtool.format.WarningsConfiguration
import optimus.buildtool.utils.JavaOptionFiltering
import optimus.buildtool.utils.JavaOpts
import spray.json._

import scala.collection.immutable.Seq

final case class JavacConfiguration(
    options: Seq[String],
    release: Int,
    jdkHome: Directory,
    warnings: WarningsConfiguration) {

  def resolvedOptions: Seq[String] = {
    val filteredOptions = JavaOpts.filterJavacOptions(release, options, _ => ())
    filteredOptions ++ List("--release", release.toString)
  }

  def asJson =
    // jdkHome is not included since it's derived from release and is not valid to include in the actual scope .obt file
    JsObject(
      "warnings" -> warnings.asJson,
      "options" -> JsArray(options.map(JsString.apply): _*),
      "release" -> JsNumber(release))
}

object JavacConfiguration {
  def filterJavaRuntimeOptions(javaRuntimeVersion: String, opts: Seq[String], log: String => Unit): Seq[String] =
    JavaOptionFiltering.filterJavaRuntimeOptions(javaRuntimeVersion, opts, log)
}

