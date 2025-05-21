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
package optimus.platform

import scala.reflect.runtime.universe._
import optimus.platform.storable.Entity
import optimus.platform.annotations.nodeSync
import optimus.graph.NodeFuture
import optimus.platform.relational.tree.TypeInfo

object RelationKey {

  def compareTwoValues(l: Any, r: Any): Int = {
    ((l, r): @unchecked) match {
      case (null, null)                                               => 0
      case (_, null)                                                  => 1
      case (null, _)                                                  => -1
      case (lCompare: java.lang.Comparable[Any] @unchecked, rCompare) => lCompare.compareTo(rCompare)
      case (lOption: Option[_], rOption: Option[_]) =>
        compareTwoValues(lOption.getOrElse(null), rOption.getOrElse(null))
    }
  }

  private[optimus /*platform*/ ] def isTypeComparable(fieldType: TypeInfo[_]): Boolean = {
    import TypeInfo._
    fieldType match {
      case BYTE | BOOLEAN | INT | CHAR | SHORT | LONG | DOUBLE | FLOAT | STRING              => true
      case i if (i.runtimeClass == classOf[BigInt] || i.runtimeClass == classOf[BigDecimal]) => true
      case i if (i.runtimeClass == classOf[Option[_]]) => isTypeComparable(i.typeParams.head)
      case _ => fieldType.classes.exists(c => classOf[java.lang.Comparable[_]].isAssignableFrom(c))
    }
  }

  def isFieldsComparable(elementType: TypeInfo[_], fields: Seq[String]): Boolean = {
    if (fields == null || fields.isEmpty) false
    val keyPropertyMap = elementType.propertyMap.filter(p => fields.contains(p._1))
    fields.forall(f => {
      val keyType = keyPropertyMap(f)
      RelationKey.isTypeComparable(keyType)
    })
  }

  def compareArrayData(left: Array[Byte], right: Array[Byte]): Int = {
    if (left.length != right.length) {
      left.length - right.length
    } else {
      var i = 0
      var result = 0
      while (result == 0 && i < left.length) {
        val l = left(i)
        val r = right(i)
        if (l != r)
          result = l - r
        i += 1
      }
      result
    }
  }
}
// sealed because we need to control the implementations to ensure that they are uniquely defined by their type
trait RelationKey[-T] {
  // N.B. this code is in Core, which isn't compiled with the plugin, so we need to stub out
  // the generated methods ourself otherwise the asyncgraph transform won't actually treat
  // this as a node
  @nodeSync def of(t: T): AnyRef // gets the key of t
  def of$queued(t: T): NodeFuture[AnyRef]

  def fields: Seq[String] // gets the names of the fields which make up the key

  override def toString = getClass.getSimpleName() + "[" + fields.mkString(", ") + "]"

  // optimization: if the key "isSyncSafe", you can call "ofSync" instead of "of" to avoid node overhead
  def isSyncSafe = false
  def ofSync(t: T): Any = throw new RuntimeException("you can only call ofSync on keys where isSyncSafe == true")

  def compareKeys(leftKey: Any, rightKey: Any): Int = ???

  val isKeyComparable: Boolean
}

object RelationKeys {
  private[optimus /*platform*/ ] val unwantedMethodSyms = typeOf[Entity].members.collect { case t: TermSymbol =>
    t.name.toString
  }.toSet + "$init$"
}
