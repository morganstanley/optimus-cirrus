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

import optimus.dsi.partitioning.Partition
import optimus.platform._
import optimus.platform.dal.DALImpl
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery
import optimus.platform.temporalSurface.operations.{CompleteMatchAssignment, MatchAssignment}
import optimus.platform.temporalSurface.operations.{SimpleMatchResult, MatchSourceQuery, MatchScope, MatchItemScope}
import optimus.platform.temporalSurface.operations.{AlwaysMatch, NeverMatch}

/**
 * internal object - user-facing API is in TemporalSurfaceMatchers - this is just here due to project dependencies
 */
private[optimus] object DataFreeTemporalSurfaceMatchers {
  val all: TemporalSurfaceMatcher = _all
  val allScope: TemporalSurfaceScopeMatcher = _allScope
  val none: TemporalSurfaceMatcher = _none
  val noneScope: TemporalSurfaceScopeMatcher = _noneScope

  private case object _all extends ConstMatcher(AlwaysMatch) {
    override def ||(other: TemporalSurfaceMatcher): TemporalSurfaceMatcher = this
  }
  private case object _none extends ConstMatcher(NeverMatch) {
    override def ||(other: TemporalSurfaceMatcher): TemporalSurfaceMatcher = other
  }
  private case object _allScope extends ConstScopeMatcher(AlwaysMatch)
  private case object _noneScope extends ConstScopeMatcher(NeverMatch)

  private[this] def allPartitions = DALImpl.partitionMapForNotification.allPartitions

  private[temporalSurface] sealed abstract class ConstMatcher(val result: SimpleMatchResult with MatchSourceQuery)
      extends TemporalSurfaceMatcher {

    private[optimus] override def matchesNamespace(namespace: String) = result == AlwaysMatch

    private[optimus] def partitions: Set[Partition] = allPartitions

    val itemResult: Option[(MatchAssignment, Option[Nothing])] = result match {
      case r: CompleteMatchAssignment => Some(r, None)
      case NeverMatch                 => None
    }

    @node override def matchQuery(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface) = result
    @node override def matchItem(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface)(
        key: operation.ItemKey) = itemResult
    @node override def matchSourceQuery(
        operation: TemporalSurfaceQuery,
        temporalSurface: TemporalSurface): MatchSourceQuery = result

    override def toString = s"ConstMatcher[$result]"
  }

  private[this] sealed abstract class ConstScopeMatcher(result: MatchScope with MatchItemScope)
      extends TemporalSurfaceScopeMatcher {
    private[optimus] def partitions: Set[Partition] = allPartitions

    @node def matchScope(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface) = result
    @node def matchItemScope(operation: TemporalSurfaceQuery, temporalSurface: TemporalSurface)(
        key: operation.ItemKey) = result

    override def toString = s"ConstScopeMatcher[$result]"
  }

}
