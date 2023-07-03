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
package optimus.platform.relational.data.tree

import optimus.platform.relational.tree.QueryMethod
import optimus.platform.relational.RelationalUnsupportedException

trait JoinType

object JoinType {
  case object InnerJoin extends JoinType
  case object LeftOuter extends JoinType
  case object RightOuter extends JoinType
  case object FullOuter extends JoinType
  case object CrossJoin extends JoinType
  case object CrossApply extends JoinType
  case object OuterApply extends JoinType

  def from(qm: QueryMethod): JoinType = {
    import QueryMethod._

    qm match {
      case INNER_JOIN       => InnerJoin
      case LEFT_OUTER_JOIN  => LeftOuter
      case RIGHT_OUTER_JOIN => RightOuter
      case FULL_OUTER_JOIN  => FullOuter
      case _                => throw new RelationalUnsupportedException(s"Invalid query method: $qm for JoinType")
    }
  }
}
