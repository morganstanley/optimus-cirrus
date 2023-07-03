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
package optimus.platform.internal

import optimus.platform.relational.{RelationColumn, RelationColumnView, RelationalException}
import optimus.platform.relational.execution.LambdaExecNode
import optimus.platform.relational.tree.{ConstValueElement, ExpressionListElement, MemberElement, RelationElement}

object ValueFetch {
  def getValue(item: Any, ele: RelationElement): Any = ele match {
    case member: MemberElement =>
      getMemberValue(item, member)
    case ConstValueElement(value, _) =>
      getConstValue(item, value)
    case ExpressionListElement(list) =>
      list.map(e => getValue(item, e))
    case _ =>
      throw new RelationalException(s"cannot fetch the value for type ${ele.getClass}")
  }

  /**
   * this will return an single value or a temp Column
   */
  def getColumnBasedValue(srcColumns: RelationColumnView, ele: RelationElement): Any = ele match {
    case member: MemberElement =>
      getColumnMemberValue(srcColumns, member)
    case ConstValueElement(value, _) =>
      getConstValue(srcColumns, value)
    case _ =>
      throw new RelationalException(s"cannot fetch the value for type ${ele.getClass}")
  }

  def getColumnMemberValue(srcColumns: RelationColumnView, member: MemberElement): AnyRef = {
    // return one of the columns according to the member name
    LambdaExecNode(member, member.rowTypeInfo).evaluateColumns(srcColumns).asInstanceOf[RelationColumn[_]]
  }

  def getMemberValue(item: Any, member: MemberElement): AnyRef = {
    LambdaExecNode(member, member.rowTypeInfo).evaluate(item)
  }

  def getConstValue(item: Any, value: Any): Any = value
}
