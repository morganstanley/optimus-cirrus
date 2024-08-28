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
package optimus.entity

import optimus.platform.pickling.PickledOutputStream
import optimus.platform.pickling.PropertyMapOutputStream
import optimus.platform.storable._
import optimus.scalacompat.collection.AbstractImmutableMap

import scala.collection.mutable

abstract class IndexInfo[A <: Storable, K](
    val name: String,
    // NB: These may be synthetic/fake property names when the index constituent is a member of an embeddable.
    val propertyNames: collection.Seq[String],
    val unique: Boolean,
    val indexed: Boolean,
    val default: Boolean,
    val isCollection: Boolean) {
  type PropType = K

  def queryable: Boolean = false
  def queryByEref: Boolean = false

  def pickle(entity: A, out: PickledOutputStream): Unit

  private[optimus] def entityToSerializedKey(
      entity: A,
      referenceMap: collection.Map[Entity, EntityReference]): collection.Seq[SerializedKey]
  def entityToKeyImpl(entity: A): Key[A]
  // get key value from entity
  def getRaw(a: A): Any
  // get Key from key value
  def makeKey(p: K): Key[A] = KeyImpl(p)
  protected def KeyImpl(p: K): Key[A]

  def storableClass: Class[A]
  def isEntityIndex: Boolean = classOf[Entity] isAssignableFrom storableClass

  override def toString = s"IndexInfo(${storableClass.getSimpleName}:${name},U:${unique},D:${default})"

  final def entityToString(e: A): String = {
    val bldr = new PropertyMapOutputStream {

      override protected def newMapBuilder: mutable.Builder[(String, Any), Map[String, Any]] =
        SeqMap.newBuilder

      override def writeEntity(e: Entity): Unit =
        if (e.dal$entityRef eq null)
          writeRawObject("<heap entity>")
        else
          writeRawObject(e.dal$entityRef)
    }

    bldr.writeStartArray()
    pickle(e, bldr)
    bldr.writeEndArray()
    val values = bldr.value.asInstanceOf[Seq[Any]]
    propertyNames.iterator.zip(values.iterator).map { case (k, v) => s"$k -> $v" }.mkString(", ")
  }

  private final class SeqMap[K, V](entries: Seq[(K, V)]) extends AbstractImmutableMap[K, V] {
    override def removed(elem: K): Map[K, V] = throw new UnsupportedOperationException()
    override def updated[V1 >: V](k: K, v: V1): Map[K, V1] = throw new UnsupportedOperationException()
    override def get(key: K): Option[V] = entries.find(_._1 == key).map(_._2)
    override def iterator: Iterator[(K, V)] = entries.iterator
  }

  private object SeqMap {
    def newBuilder[K, V]: mutable.Builder[(K, V), SeqMap[K, V]] = Vector.newBuilder[(K, V)].mapResult(new SeqMap(_))
  }
}
