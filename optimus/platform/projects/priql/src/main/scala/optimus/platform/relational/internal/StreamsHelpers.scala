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
package optimus.platform.relational.internal

import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.MethodArg
import optimus.platform.relational.tree.MethodArgConstants._
import optimus.platform.relational.tree.QueryMethod
import optimus.platform.relational.tree.QueryMethod.INNER_JOIN
import optimus.platform.relational.tree.QueryMethod.LEFT_OUTER_JOIN
import optimus.platform.relational.tree.RelationElement
import optimus.utils.CollectionUtils._

private[platform] object StreamsHelpers {

  def sourceArgRequiredFor(code: QueryMethod): String =
    code match {
      case INNER_JOIN | LEFT_OUTER_JOIN => left
      case _                            => source
    }

  def maybeRelationElement(arguments: List[MethodArg[RelationElement]], name: String): Option[RelationElement] =
    arguments.collectFirst { case ma: MethodArg[_] if ma.name == name => ma.arg }

  def findAllRelationElements(arguments: List[MethodArg[RelationElement]], name: String): List[RelationElement] =
    arguments.filter { case ma: MethodArg[_] => ma.name == name }.map(_.arg)

  def findRelationElement(arguments: List[MethodArg[RelationElement]], name: String): RelationElement =
    maybeRelationElement(arguments, name).getOrThrow(s"Could not find $name in provided priql MethodArguments")

  def findConstValueElement(arguments: List[MethodArg[RelationElement]], name: String): Any = {
    findRelationElement(arguments, name) match {
      case constElem: ConstValueElement => constElem.value
      case element =>
        throw new IllegalStateException(s"RelationElement of type ConstValueElement expected but found $element")
    }
  }
}
