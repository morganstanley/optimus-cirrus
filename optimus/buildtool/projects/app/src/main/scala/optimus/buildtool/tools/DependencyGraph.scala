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
package optimus.buildtool.tools

import optimus.buildtool.format.ScopeDefinition
import optimus.buildtool.config.Dependencies
import optimus.buildtool.config.ScopeId

import scala.collection.immutable.Seq

object DependencyGraph { // TODO (OPTIMUS-47584): remove in favor of dependicon
  def structure(scopes: Map[ScopeId, ScopeDefinition], from: Seq[ScopeId], until: Seq[ScopeId]): DependencyStructure = {
    def validateIds(ids: Seq[ScopeId]): Unit = {
      val bad = ids.filterNot(scopes.contains)
      if (bad.nonEmpty) sys.error(s"bad scope${if (bad.length > 1) "s" else ""}: ${bad.mkString(", ")}")
    }
    validateIds(from ++ until)

    // here's a bogus but simple and functional algorithm (and it's fast enough)
    // - start at "from" and create the entire dependency graph from there
    // - then start at "until" and create it, but follow the edges backwards
    // - all of the edges in both sets are between "from" and "until"
    // "forward" means dependent->dependency (e.g. platform->core); "reverse" means dependency->dependent (e.g. core->platform)

    def toForward(get: ScopeDefinition => Dependencies): Map[ScopeId, Set[ScopeId]] =
      scopes
        .map { case (id, mdef) =>
          id -> (get(mdef).internal.toSet - id)
        }
        .withDefaultValue(Set.empty)

    new DependencyStructure(
      toForward(_.configuration.compileDependencies),
      toForward(_.configuration.runtimeDependencies))
  }

  final class DependencyStructure(val compile: Map[ScopeId, Set[ScopeId]], val runtime: Map[ScopeId, Set[ScopeId]]) {
    def flip: DependencyStructure = {
      def flip1(deps: Map[ScopeId, Set[ScopeId]]): Map[ScopeId, Set[ScopeId]] =
        deps.toSeq
          .flatMap { case (k, vs) =>
            vs.map(_ -> k)
          }
          .groupBy(_._1)
          .map({ case (nk, nvs) => nk -> nvs.map(_._2).toSet })
          .withDefaultValue(Set.empty)
      new DependencyStructure(flip1(compile), flip1(runtime))
    }
    @annotation.tailrec
    def fill(edges: Set[Edge], seen: Set[ScopeId], horizon: Set[ScopeId]): Set[Edge] = {
      if (horizon.isEmpty) edges
      else {
        val next = horizon.flatMap { thisId =>
          def toEdges(deps: Map[ScopeId, Set[ScopeId]], runtime: Boolean): Set[Edge] = {
            deps(thisId).iterator.map(thatId => Edge(thisId, thatId, runtime)).toSet
          }
          toEdges(compile, runtime = false) | toEdges(runtime, runtime = true)
        }
        fill(edges = edges | next, seen = seen | horizon, horizon = next.map(_.to) &~ horizon &~ seen)
      }
    }

    def dependencies(id: ScopeId): Set[ScopeId] = compile.getOrElse(id, Set.empty) ++ runtime.getOrElse(id, Set.empty)
  }

  final case class Edge(from: ScopeId, to: ScopeId, runtime: Boolean) {
    def flip: Edge = Edge(to, from, runtime)
    override def toString: String =
      s""""$from" -> "$to" ${if (runtime) " [style = dotted]" else ""};"""
  }
}
