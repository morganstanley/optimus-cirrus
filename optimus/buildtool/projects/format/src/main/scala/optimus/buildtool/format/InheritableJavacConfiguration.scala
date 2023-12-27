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

import optimus.buildtool.config.JavacConfiguration
import optimus.buildtool.config.ScopeId
import optimus.buildtool.dependencies.JdkDependencies

import scala.collection.immutable.Seq

final case class InheritableJavacConfiguration(
    options: Seq[String],
    release: Option[Int],
    warnings: WarningsConfiguration) {

  def withParent(parent: InheritableJavacConfiguration): InheritableJavacConfiguration =
    InheritableJavacConfiguration(
      parent.options ++ options,
      release.orElse(parent.release),
      warnings.withParent(parent.warnings))

  def resolve(scopeId: ScopeId, obtFile: ObtFile, jdkDependencies: JdkDependencies): Result[JavacConfiguration] = {
    release match {
      case Some(rel) =>
        jdkDependencies.featureVersionToJdkHome.get(rel) match {
          case Some(home) =>
            Success(JavacConfiguration(options, rel, home, warnings))
          case None => Failure(s"JDK Home for Java Release $rel not found in ${JdkDependenciesConfig.path}", obtFile)
        }
      case None =>
        Failure(s"javac.release not set for $scopeId", obtFile)
    }
  }
}

object InheritableJavacConfiguration {
  val empty = InheritableJavacConfiguration(options = Nil, None, WarningsConfiguration.empty)
}
