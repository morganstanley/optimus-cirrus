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
package optimus.buildtool.builders.reporter

import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.DependencyLookup
import optimus.buildtool.artifacts.ExternalId
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.config.ObtConfig
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeId
import optimus.platform._

import scala.collection.immutable.Seq

object LookupReporter {
  type Discrepancies = Map[ScopeId, Seq[LookupDiscrepancy]]
}

sealed trait LookupDiscrepancy {
  def where: String
}
object LookupDiscrepancy {
  sealed trait Kind
  final case object Unused extends Kind
  final case object Required extends Kind

  final case class External(id: ExternalId, kind: Kind) extends LookupDiscrepancy {
    override def where: String = id.toString
  }
  final case class Internal(id: ScopeId, kind: Kind) extends LookupDiscrepancy {
    override def where: String = id.toString
  }
}

class LookupReporter(config: ObtConfig) {
  import LookupReporter.Discrepancies

  /**
   * Return any lookup discrepancies found in a compilation. If no lookups were made, returns None.
   */
  @node def discrepancies(msgs: Seq[MessagesArtifact]): Option[Discrepancies] = {
    val containsLookups = msgs
      .collect { case msg: CompilerMessagesArtifact => msg }
      .filter(msg => msg.externalDeps.nonEmpty || msg.internalDeps.nonEmpty)
    val hasLookups = containsLookups.nonEmpty

    val lookupsByScopes: Map[ScopeId, (Seq[DependencyLookup.Internal], Seq[DependencyLookup.External])] =
      containsLookups
        .groupBy(_.id.scopeId)
        .collect { case (scope, artifacts) =>
          (scope, (artifacts.flatMap(_.internalDeps), artifacts.flatMap(_.externalDeps)))
        }

    // Note: at this point, we will have repeated lookups in any scope that uses java. For example, if the scala code of
    // scope A calls into scope B for class C1 and the java code for scope A calls into scope B for class C2, the
    // internalDeps will look like
    //
    // Seq(DependencyLookup(A, Seq("C1")), DependencyLookup(A, Seq("C2"))
    //
    // This is ok now but if we ever want to display the classes C1 and C2 we'll have to be more careful in
    // diffReportForScope to properly merge the lookups.

    if (hasLookups) Some(lookupsByScopes.apar.map { case (scope, (internals, externals)) =>
      scope -> diffReportForScope(scope, config.scopeConfiguration(scope), internals, externals)
    })
    else None
  }

  @node private def diffReportForScope(
      id: ScopeId,
      config: ScopeConfiguration,
      internals: Seq[DependencyLookup.Internal],
      externals: Seq[DependencyLookup.External]): Seq[LookupDiscrepancy] = {
    val (scopeNotUsed, scopeNotSet) = {
      val asConfig = (config.compileDependencies.internal ++ config.compileOnlyDependencies.internal).toSet
      val actually = internals.map(_.into).toSet - id // we don't care if we have to use the current scope
      ((asConfig -- actually).toList, (actually -- asConfig).toList)
    }

    val (externalNotUsed, externalNotSet) = {
      val asConfig = (config.compileDependencies.external ++ config.compileOnlyDependencies.external).map { defn =>
        ExternalId(defn.group, defn.name, defn.version)
      }.toSet
      val actually = externals.map(_.into).toSet
      ((asConfig -- actually).toList, (actually -- asConfig).toList)
    }

    import LookupDiscrepancy._
    scopeNotUsed.map { scope => Internal(scope, Unused) } ++
      scopeNotSet.map { scope => Internal(scope, Required) } ++
      externalNotUsed.map { ext => External(ext, Unused) } ++
      externalNotSet.map { ext => External(ext, Required) }
  }
}
