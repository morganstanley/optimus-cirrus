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
package optimus.dsi.serialization.json

import java.{util => ju}

import net.iharder.Base64
import optimus.dsi.serialization.AbstractTypeTransformerOps
import optimus.dsi.serialization.JavaCollectionUtils
import optimus.dsi.serialization.TypeTransformException
import optimus.platform.pickling.ImmutableByteArray
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.EntityReference
import optimus.platform.storable.FinalReference
import optimus.platform.storable.TemporaryReference
import optimus.platform.storable.VersionedReference

import scala.jdk.CollectionConverters._
import scala.reflect.classTag

trait StringTypeTransformerOps extends AbstractTypeTransformerOps[String] {

  override protected val tTag = classTag[String]
  val TypeMarkers = StringMarkers

  protected def isMarker(expected: String, actual: Any): Boolean = actual match {
    case b: String => expected.sameElements(b.getBytes)
    case _         => false
  }

  protected def toByteArray(s: Iterator[Any]): ImmutableByteArray =
    ImmutableByteArray(Base64.decode(s.next().toString, Base64.DONT_GUNZIP))
  protected def fromByteArray(b: Array[Byte]) =
    JavaCollectionUtils.mkArrayList(TypeMarkers.ByteArray, Base64.encodeBytes(b))

  protected def toLong(a: Any): Long = if (a.isInstanceOf[Int]) a.asInstanceOf[Int].toLong else a.asInstanceOf[Long]
  protected def fromLong(l: Long) = l.longValue(): java.lang.Long

  private[optimus] def toFinalEntityReference(r: String, i: Option[Int]): FinalReference =
    EntityReference.fromString(r, i).asInstanceOf[FinalReference]
  private[optimus] def toBusinessEventReference(r: String, i: Option[Int]) =
    i.map(i => BusinessEventReference.typedFromString(r, i)).getOrElse(BusinessEventReference.fromString(r))
  private[optimus] def toVersionedReference(o: String) = VersionedReference.fromString(o)

  // these TemporaryReference defs should ONLY be used for self-contained events (@event(contained=true))
  private[optimus] def fromTemporaryEntityReference(e: TemporaryReference): ju.ArrayList[Any] =
    JavaCollectionUtils.mkArrayList(StringMarkers.TempRef, e.data)

  private[optimus] def toTemporaryEntityRef(s: Iterator[Any]): TemporaryReference =
    s.toList match {
      case (data: String) :: Nil => EntityReference.temporaryFromString(data)
      case other => throw new TypeTransformException(s"Cannot get EntityReference from ${other.mkString(",")}")
    }

  override def serialize(o: Any): AnyRef = o match {
    case eref: TemporaryReference => fromTemporaryEntityReference(eref)
    case _                        => super.serialize(o)
  }

  override def deserialize(o: Any): Any = {

    def fromSeqOrElse(s: Iterator[Any]): Any = {
      if (s.isEmpty) Nil
      else {
        val marker = s.next()
        if (isMarker(StringMarkers.TempRef, marker)) toTemporaryEntityRef(s)
        else super.deserialize(o)
      }
    }

    o match {
      case jl: ju.List[_] => fromSeqOrElse(jl.iterator.asScala)
      case l: List[_]     => fromSeqOrElse(l.iterator)
      case _              => super.deserialize(o)
    }
  }
}
