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

/**
 * MemberElement represents a value of the field/member in the Relation[T] Get member from instance
 */
class MemberElement(val instanceProvider: RelationElement, val memberName: String, val member: MemberDescriptor)
    extends RelationElement(
      ElementType.MemberRef,
      if (member == null) null
      else
        member match {
          case fieldDescriptor: FieldDescriptor => fieldDescriptor.fieldType
          case _                                => null
        }) {

  def this(instanceProvider: RelationElement, memberName: String) =
    this(
      instanceProvider,
      memberName,
      if ((instanceProvider ne null) && instanceProvider.rowTypeInfo != null)
        TypeDescriptor.getField(instanceProvider.rowTypeInfo, memberName)
      else null
    )

  def prettyName: String = "Member ref"
  def name: String = "MemberRef"

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= serial + " " + prettyName + ": '" + memberName + "' of\n"
    if (instanceProvider.asInstanceOf[AnyRef] ne null) {
      instanceProvider.prettyPrint(indent + 1, out)
    } else {
      indentAndStar(indent, out)
      out ++= " <NULL> instance\n"
    }
  }

  override def toString: String =
    " " + name + "[ " + (if (instanceProvider != null) instanceProvider.toString() else "") + " . " + memberName + " ] "
}

object MemberElement {
  def unapply(m: MemberElement) = Some(m.instanceProvider, m.memberName)
}
