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

import optimus.buildtool.utils.Hashing

import scala.util.matching.Regex
import scala.collection.compat._

final case class ForbiddenDependencyConfiguration(
    dependencyId: Option[String],
    _dependencyRegex: Option[String],
    configurations: Seq[String],
    transitive: Boolean,
    isExternal: Boolean,
    allowedInIds: Seq[PartialScopeId],
    _allowedInPatterns: Seq[String]
) extends ForbiddenDependencyConfiguration.Fields

object ForbiddenDependencyConfiguration {
  def fingerprint(fd: ForbiddenDependencyConfiguration): String = {
    // Either .get on dependency or dependencyRegex should be Some(_) as it is a config requirement to have one
    "[ForbiddenDependency]" + fd.dependencyId.getOrElse("") + fd._dependencyRegex.getOrElse("") + "@" + Hashing
      .hashStrings {
        import fd._
        dependencyId.to(Seq) ++ _dependencyRegex ++ configurations ++ allowedInIds.map(_.toString) ++ _allowedInPatterns
      }
  }

  sealed abstract class Fields {
    this: ForbiddenDependencyConfiguration => // hack for spray-json which can't deal with extra non-ctor fields
    val fingerprint: String = ForbiddenDependencyConfiguration.fingerprint(this)
    val dependencyRegex: Option[Regex] = _dependencyRegex.map(_.r)
    val allowedInPatterns: Seq[Regex] = _allowedInPatterns.map(_.r)
  }
}
