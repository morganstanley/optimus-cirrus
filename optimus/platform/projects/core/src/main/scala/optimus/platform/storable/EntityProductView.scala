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
package optimus.platform.storable

import java.lang.reflect.Modifier

import optimus.graph.NodeKey
import optimus.graph.PropertyInfo0
import optimus.graph.PropertyNode
import optimus.graph.ReallyNontweakablePropertyInfo

/**
 * We use implicit here as the Entity class is already overloaded. It should be replaced with extension methods when
 * upgrading to Scala3
 */
object EntityProductView {
  implicit def toProduct(e: Entity): EntityProductView = {
    new EntityProductView(e)
  }
}

/**
 * This class intended to provide a Product trait for Entity similar to that of a case class - since entities can not be
 * case classes, sometimes we miss the convenience of iterating through field values and names. Note: fields declared in
 * super classes are not included in the Product.
 */
class EntityProductView(val e: Entity) extends Product213 {
  private def $info = e.$info
  private def asyncLookupAndGet[T](node: NodeKey[T]): T = {
    node.asInstanceOf[PropertyNode[T]].lookupAndGet
  }

  private val propertyMap = {
    $info.properties.map(p => (p.name, p)).toMap[String, Any]
  }

  private val allFieldNames = {
    val publicMethodNames = e.getClass.getDeclaredMethods
      .filter(m =>
        m.getReturnType != classOf[Unit]
          && m.getParameterCount == 0
          && Modifier.isPublic(m.getModifiers))
      .map(_.getName)
    val publicGetterNames = e.getClass.getDeclaredFields.map(_.getName).filter(publicMethodNames contains _)

    propertyMap.keys ++ publicGetterNames
  }

  private def maybePropertyValue(name: String): Option[Any] = {
    propertyMap.get(name) map {
      case dpi: PropertyInfo0[_, _] =>
        asyncLookupAndGet(dpi.asInstanceOf[PropertyInfo0[Entity, _]].createNodeKey(e))
      case nt: ReallyNontweakablePropertyInfo[_, _] =>
        nt.asInstanceOf[ReallyNontweakablePropertyInfo[Entity, _]].getValue(e)
      case _ =>
    }
  }

  override def productElement(n: Int): Any = {
    val name = productElementName(n)
    maybePropertyValue(name).getOrElse {
      e.getClass.getDeclaredMethod(name).invoke(e)
    }
  }

  override def productArity: Int = allFieldNames.size
  override def canEqual(that: Any): Boolean = false
  override def productElementNames: Iterator[String] = allFieldNames.iterator
  override def productElementName(n: Int): String = allFieldNames.slice(n, n + 1).toList.head
}
