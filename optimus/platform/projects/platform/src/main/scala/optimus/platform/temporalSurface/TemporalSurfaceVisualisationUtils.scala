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
package optimus.platform.temporalSurface

import java.time.Instant
import optimus.platform.annotations.deprecating
import optimus.platform.dal.QueryTemporality
import optimus.platform.temporalSurface.impl.LeafTemporalSurfaceImpl

@deprecating( // not really deprecated; just restricting usage
  suggestion = "This util is used to extract info of TemporalSurface - contact Optimus Graph")
object TemporalSurfaceVisualisationUtils {

  def currentMatcherLog(matcher: TemporalSurfaceMatcher): String = matcher match {
    case classMatcher: ClassTemporalSurfaceMatcher =>
      s"${classMatcher.toString}"
    case queryBasedMatcher: QueryBasedTemporalSurfaceMatchers =>
      val queryPairs = queryBasedMatcher.asQueries
        .zip(queryBasedMatcher.reactiveQueryProcessors.map(_.allFilters))
        .map(c => s"${c._1},${c._2}")
      s"QueryBasedTemporalSurfaceMatcher[queryPairs=$queryPairs]"
    case dflt: DisjunctiveTemporalSurfaceMatcher =>
      val classes = dflt.classically.classes
      val namespaces = dflt.classically.namespaces
      val queries = dflt.querulously.asQueries zip dflt.querulously.reactiveQueryProcessors.map(_.allFilters)
      val classesStr = classes.mkString("  - ", "\n  - ", "\n")
      val namespacesStr = namespaces.mkString("  - ", "\n  - ", "\n")
      val queriesStr = queries.map(c => s"${c._1},${c._2}").mkString("  - ", "\n  - ", "\n")
      s"""DisjunctiveTemporalSurfaceMatcher:
         |${if (classes.nonEmpty) s"- Classes:\n$classesStr"}
         |${if (namespaces.nonEmpty) s"- Namespaces:\n$namespacesStr"}
         |${if (queries.nonEmpty) s"- Queries:\n$queriesStr"}
       """.stripMargin
    case x => x.toString
  }

  def currentTemporality(leaf: LeafTemporalSurface): QueryTemporality.At = leaf match {
    case tsi: LeafTemporalSurfaceImpl => tsi.currentTemporality
    case _ => throw new IllegalArgumentException(s"Cannot convert ${leaf.getClass.getName} to LeafTemporalSurfaceImpl")
  }

  def getTemporality(ts: TemporalSurface): Option[(Instant, Instant)] = {
    ts match {
      case leaf: LeafTemporalSurface =>
        val current = currentTemporality(leaf)
        Some((current.validTime, current.txTime))
      case _ => None
    }
  }

  def getTag(ts: TemporalSurface): String = {
    ts.tag.getOrElse("")
  }

  def getMatcher(ts: TemporalSurface): String = {
    ts match {
      case leaf: LeafTemporalSurface =>
        currentMatcherLog(leaf.matcher)
      case branch: BranchTemporalSurface =>
        branch.scope.toString
      case _ => throw new IllegalStateException("code error")
    }
  }
}
