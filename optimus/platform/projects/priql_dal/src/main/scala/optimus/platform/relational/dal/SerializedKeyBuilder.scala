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
package optimus.platform.relational.dal

import optimus.entity.IndexInfo
import optimus.platform.pickling.PickledOutputStream
import optimus.platform.pickling.Pickler
import optimus.platform.pickling.PropertyMapOutputStream
import optimus.platform.storable.SerializedKey
import optimus.platform.dsi.bitemporal.proto.ProtoSerialization
import optimus.platform.pickling.EntityPickler
import optimus.platform.pickling.OptionPickler
import optimus.platform.relational.dal.accelerated.PicklerSelector
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.utils.CollectionUtils._

trait SerializedKeyBuilder {
  def toSerializedKey(value: Any): SerializedKey
}

object SerializedKeyBuilder {
  def apply(ii: IndexInfo[_, _]): SerializedKeyBuilder = {
    if (ii.propertyNames.isEmpty) new SKBuilder0(ii)
    else if (ii.propertyNames.length == 1) new SKBuilder1(ii)
    else new SKBuilderN(ii)
  }

  abstract class SKBuilderBase(val indexInfo: IndexInfo[_, _]) extends SerializedKeyBuilder {
    def ns = indexInfo.propertyNames
    def unique = indexInfo.unique
    def indexed = indexInfo.indexed
    def queryByEref = indexInfo.queryByEref
    def storableClass = indexInfo.storableClass
    def isExpandedIndex: Boolean = indexInfo.isCollection && indexed && !unique
    // to support expression like following with "@indexed val position"
    // e => e.position.dal$entityRef == <constant_ref>
    val picklerSeq = {
      indexInfo.underlyingPicklerSeq.map {
        case EntityPickler                => EntityOrEntityReferencePickler
        case OptionPickler(EntityPickler) => OptionPickler(EntityOrEntityReferencePickler)
        case p                            => p
      }
    }

    def toSerializedKey(value: Any): SerializedKey = {
      serialize(value).single
    }

    protected def pickleRep(res: Any, out: PickledOutputStream): Unit

    protected def serialize(res: Any): Seq[SerializedKey] = {
      val decoratedRes =
        if (isExpandedIndex && !res.isInstanceOf[Iterable[_]] && !res.isInstanceOf[Array[_]])
          Seq(res)
        else res
      val out = new PropertyMapOutputStream(stableOrdering = true)
      out.writeStartArray()
      pickleRep(decoratedRes, out)
      out.writeEndArray()
      val props = out.value.asInstanceOf[Iterable[_]]
      require(props.size == picklerSeq.length)
      makeSerializedKeys(ns zip props)
    }

    private def makeSerializedKeys(fieldValues: Seq[(String, Any)]): Seq[SerializedKey] = {
      if (isExpandedIndex) {
        val (fieldName, fieldValue) = fieldValues.single
        ProtoSerialization
          .distinctBy(fieldValue.asInstanceOf[Iterable[Any]].iterator, identity[Any])
          .map(e => SerializedKey(storableClass.getName, Seq(fieldName -> e), unique, indexed, queryByEref))
          .toIndexedSeq
      } else
        Seq(SerializedKey(storableClass.getName, fieldValues, unique, indexed, queryByEref))
    }
  }

  class SKBuilder0(ii: IndexInfo[_, _]) extends SKBuilderBase(ii) {
    require(ii.propertyNames.isEmpty)

    def pickleRep(res: Any, out: PickledOutputStream): Unit = {}
  }

  class SKBuilder1(ii: IndexInfo[_, _]) extends SKBuilderBase(ii) {
    val pickler = picklerSeq.single.asInstanceOf[Pickler[Any]]

    def pickleRep(res: Any, out: PickledOutputStream): Unit = {
      out.write(res, pickler)
    }
  }

  class SKBuilderN(ii: IndexInfo[_, _]) extends SKBuilderBase(ii) {
    def pickleRep(res: Any, out: PickledOutputStream): Unit = {
      val product = res.asInstanceOf[Product]
      require(picklerSeq.length == product.productArity)
      val it = product.productIterator
      picklerSeq foreach { p =>
        out.write(it.next(), p.asInstanceOf[Pickler[Any]])
      }
    }
  }

  private object EntityOrEntityReferencePickler extends Pickler[Any] {
    override def pickle(t: Any, visitor: PickledOutputStream): Unit = {
      t match {
        case e: Entity          => EntityPickler.pickle(e, visitor)
        case e: EntityReference => PicklerSelector.IdentityPickler.pickle(e, visitor)
      }
    }
  }
}
