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
package optimus.platform.relational.persistence.protocol

import optimus.platform.relational.tree.QueryMethod
import optimus.platform.relational.tree.QueryMethod._
import optimus.platform.relational.RelationalException

object QueryMethodNumberMap {

  /**
   * this method is used in RelTreeEncoder to get enum MethodType according to the number
   */
  def getNumberByQueryMethod(qm: QueryMethod): Int = qm match {
    case WHERE                    => 1
    case OUTPUT                   => 2
    case INNER_JOIN               => 3
    case SORT                     => 4
    case GROUP_BY                 => 5
    case LEFT_OUTER_JOIN          => 6
    case RIGHT_OUTER_JOIN         => 7
    case FULL_OUTER_JOIN          => 8
    case DO_FUNC_METHOD           => 9
    case GROUP_ALL                => 10
    case TAKE                     => 11
    case TAKE_DISTINCT            => 12
    case UNION                    => 13
    case EXTEND                   => 14
    case EXTEND_TYPED             => 15
    case DIFFERENCE               => 16
    case DIVISION                 => 17
    case EXTRACT                  => 18
    case GROUP_BY_TYPED           => 19
    case GROUP_MAP_VALUES         => 20
    case NATURAL_INNER_JOIN       => 21
    case NATURAL_LEFT_OUTER_JOIN  => 22
    case NATURAL_RIGHT_OUTER_JOIN => 23
    case NATURAL_FULL_OUTER_JOIN  => 24
    case REPLACE                  => 25
    case AGGREGATE_BY             => 26
    case MERGE                    => 27
    case UNTYPE                   => 90
    case CAST                     => 91
    case SHAPE                    => 92
    case FLATMAP                  => 93
    case _ => throw new RelationalException("priql will not encode and decode this QueryMethod " + qm)
  }

  /**
   * this method is used in RelTreeDecoder to get QueryMethod according to enum MethodType.value (int)
   */
  def getQueryMethodByNumber(id: Int): QueryMethod = id match {
    case 1  => WHERE
    case 2  => OUTPUT
    case 3  => INNER_JOIN
    case 4  => SORT
    case 5  => GROUP_BY
    case 6  => LEFT_OUTER_JOIN
    case 7  => RIGHT_OUTER_JOIN
    case 8  => FULL_OUTER_JOIN
    case 9  => DO_FUNC_METHOD
    case 10 => GROUP_ALL
    case 11 => TAKE
    case 12 => TAKE_DISTINCT
    case 13 => UNION
    case 14 => EXTEND
    case 15 => EXTEND_TYPED
    case 16 => DIFFERENCE
    case 17 => DIVISION
    case 18 => EXTRACT
    case 19 => GROUP_BY_TYPED
    case 20 => GROUP_MAP_VALUES
    case 21 => NATURAL_INNER_JOIN
    case 22 => NATURAL_LEFT_OUTER_JOIN
    case 23 => NATURAL_RIGHT_OUTER_JOIN
    case 24 => NATURAL_FULL_OUTER_JOIN
    case 25 => REPLACE
    case 26 => AGGREGATE_BY
    case 27 => MERGE
    case 90 => UNTYPE
    case 91 => CAST
    case 92 => SHAPE
    case 93 => FLATMAP
    case _  => throw new RelationalException("priql will not encode and decode this QueryMethod whose id is " + id)
  }
}
