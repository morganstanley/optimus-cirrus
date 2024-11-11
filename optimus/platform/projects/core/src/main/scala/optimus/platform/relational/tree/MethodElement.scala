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

import optimus.platform.RelationKey

/**
 * MethodElement represents each Relation operation, such as where, output. The methodCode property indicates what the
 * PriQL operations are. The methodArg property is a list which contains all the arguments used by the operations.
 */
class MethodElement(
    val methodCode: QueryMethod,
    val methodArgs: List[MethodArg[RelationElement]],
    rowTypeInfo: TypeInfo[_],
    key: RelationKey[_],
    pos: MethodPosition = MethodPosition.unknown)
    extends MultiRelationElement(ElementType.Method, rowTypeInfo, key, pos) {

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= serial + " Method " + methodCode + "\n"
    methodArgs.foreach(a => if (a.param != null) a.param.prettyPrint(indent + 1, out))
  }

  def replaceArgs(newArgs: List[MethodArg[RelationElement]]): MethodElement = {
    new MethodElement(methodCode, newArgs, rowTypeInfo, key, pos)
  }

  def replaceKey(newKey: RelationKey[_]): MethodElement = {
    new MethodElement(methodCode, methodArgs, rowTypeInfo, newKey, pos)
  }
}

object MethodElement {
  def unapply(m: MethodElement) = Some((m.methodCode, m.methodArgs, m.rowTypeInfo, m.key, m.pos))
}
