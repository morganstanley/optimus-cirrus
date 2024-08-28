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
package optimus.platform.relational.tree

trait QueryMethod

object QueryMethod {

  case object PERMIT_TABLE_SCAN extends QueryMethod

  case object WHERE extends QueryMethod
  case object OUTPUT extends QueryMethod
  case object INNER_JOIN extends QueryMethod
  case object SORT extends QueryMethod
  case object GROUP_BY extends QueryMethod

  case object LEFT_OUTER_JOIN extends QueryMethod
  case object RIGHT_OUTER_JOIN extends QueryMethod
  case object FULL_OUTER_JOIN extends QueryMethod

  /** a user function to invoked for every row */
  case object DO_FUNC_METHOD extends QueryMethod

  /** tells that entire table to be treated as a single group */
  case object GROUP_ALL extends QueryMethod

  /** instructs to take certain range of rows from result */
  case object TAKE extends QueryMethod

  /** take distinct elements only */
  case object TAKE_DISTINCT extends QueryMethod
  case object TAKE_DISTINCT_BYKEY extends QueryMethod

  // Optimus Relation Operators
  case object UNION extends QueryMethod
  case object MERGE extends QueryMethod
  case object EXTEND extends QueryMethod
  case object EXTEND_TYPED extends QueryMethod
  case object DIFFERENCE extends QueryMethod
  case object DIVISION extends QueryMethod
  case object EXTRACT extends QueryMethod

  case object GROUP_BY_TYPED extends QueryMethod
  case object GROUP_MAP_VALUES extends QueryMethod

  case object NATURAL_INNER_JOIN extends QueryMethod
  case object NATURAL_LEFT_OUTER_JOIN extends QueryMethod
  case object NATURAL_RIGHT_OUTER_JOIN extends QueryMethod
  case object NATURAL_FULL_OUTER_JOIN extends QueryMethod

  case object REPLACE extends QueryMethod
  case object AGGREGATE_BY extends QueryMethod
  case object AGGREGATE_BY_UNTYPED extends QueryMethod
  case object AGGREGATE_BY_IMPLICIT extends QueryMethod

  case object UNTYPE extends QueryMethod
  case object CAST extends QueryMethod
  case object SHAPE extends QueryMethod
  case object FLATMAP extends QueryMethod
  case object ARRANGE extends QueryMethod

  def isJoin(v: QueryMethod) = v match {
    case INNER_JOIN | LEFT_OUTER_JOIN | RIGHT_OUTER_JOIN | FULL_OUTER_JOIN | NATURAL_INNER_JOIN |
        NATURAL_LEFT_OUTER_JOIN | NATURAL_RIGHT_OUTER_JOIN | NATURAL_FULL_OUTER_JOIN =>
      true
    case _ => false
  }
}
