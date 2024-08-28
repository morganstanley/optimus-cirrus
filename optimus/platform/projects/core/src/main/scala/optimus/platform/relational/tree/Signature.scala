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

import java.lang.reflect.{Type => JType}

import org.objectweb.asm.Type

class Signature(val name: String, val desc: String, val richReturnType: JType) {
  require(name.indexOf('(') < 0)

  def getName = name
  def getReturnType: Type = Type.getReturnType(desc)
  def getArgumentTypes: Array[Type] = Type.getArgumentTypes(desc)

  override def toString: String = s"$name$desc"
  override def hashCode: Int = name.## ^ desc.##
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case s: Signature => s.name == name && s.desc == desc
      case _            => false
    }
  }
}

object Signature {
  def apply(name: String, desc: String, richReturnType: JType): Signature = {
    new Signature(name, desc, richReturnType)
  }
  def apply(
      name: String,
      returnClass: Class[_],
      argumentClasses: collection.Seq[Class[_]],
      richReturnType: JType): Signature = {
    apply(
      name,
      Type.getMethodDescriptor(Type.getType(returnClass), argumentClasses.map(Type.getType).toSeq: _*),
      richReturnType)
  }
  def apply(name: String, desc: String): Signature = {
    apply(name, desc, classOf[AnyRef])
  }
}
