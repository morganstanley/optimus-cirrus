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
package optimus.dsi.serialization.bson

import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.util.{Map => JMap}
import java.util.{TreeMap => JTreeMap}
import java.util.{TreeSet => JTreeSet}

import optimus.dsi.serialization.AbstractTypeTransformerOps
import optimus.dsi.serialization.JavaCollectionUtils
import optimus.dsi.serialization.ReferenceTransformer
import optimus.dsi.serialization.TypedReferenceAwareTransformer
import optimus.platform.pickling.ImmutableByteArray
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.EntityReference
import optimus.platform.storable.FinalReference
import optimus.platform.storable.VersionedReference
import optimus.platform.dsi.bitemporal.DateTimeSerialization

import scala.jdk.CollectionConverters._
import scala.reflect.classTag

trait BsonTypeTransformerOps extends AbstractTypeTransformerOps[Array[Byte]] {
  override protected val tTag = classTag[Array[Byte]]
  val TypeMarkers = BinaryMarkers
  protected def isMarker(expected: Array[Byte], actual: Any): Boolean = actual match {
    case a: Array[Byte] => expected.sameElements(a)
    case _              => false
  }

  protected def toByteArray(s: Iterator[Any]): ImmutableByteArray = ???
  protected def fromByteArray(b: Array[Byte]) = b

  protected def toLong(a: Any): Long = a.asInstanceOf[Long]
  protected def fromLong(l: Long) = l.asInstanceOf[AnyRef]

  private[optimus] def toFinalEntityReference(r: Array[Byte], i: Option[Int]) = EntityReference(r, i)
  private[optimus] def toBusinessEventReference(r: Array[Byte], i: Option[Int]) = BusinessEventReference(r, i)
  private[optimus] def toVersionedReference(o: Array[Byte]) = VersionedReference(o)

  override def deserialize(o: Any): Any = o match {
    case b: Array[Byte] => ImmutableByteArray(b)
    case _              => super.deserialize(o)
  }

  final def toBson(o: Any): AnyRef = serialize(o)
  final def fromBson(o: Any): Any = deserialize(o)
}

/**
 * Transforms pickled types to BSON-supported types and back.
 */
object TypedRefAwareBsonTypeTransformer extends BsonTypeTransformerOps with TypedReferenceAwareTransformer[Array[Byte]]

object OrderedBsonTypeTransformer extends BsonTypeTransformerOps with ReferenceTransformer[Array[Byte]] with Ordered

object BsonTypeTransformer extends BsonTypeTransformerOps with ReferenceTransformer[Array[Byte]]

object RegisteredIndexBsonTransformer
    extends BsonTypeTransformerOps
    with TypedReferenceAwareTransformer[Array[Byte]]
    with RegisteredIndexTransformerBase[Array[Byte]]

object OrderedTypedRefAwareBsonTypeTransformer
    extends BsonTypeTransformerOps
    with TypedReferenceAwareTransformer[Array[Byte]]
    with Ordered

trait Ordered {
  self: BsonTypeTransformerOps =>
  override protected def toJavaMap(m: Map[String, Any]): JMap[String, Any] = {
    val output = new JTreeMap[String, Any]()
    val it = m.iterator
    while (it.hasNext) {
      val ent = it.next()
      output.put(ent._1, toBson(ent._2))
    }
    output
  }

  override protected def toJavaSet[T](s: Set[T]) =
    new JTreeSet[T](s.asJava)
}

class BSONTransformException(error: String) extends Exception(error)

object RegisteredIndexTransformerBase {
  val NANOS_PER_SECOND = 1000000000L
}

// Serializer for "@indexed" values into format suitable for backend-level (either Mongo or Postgres) index creation.
trait RegisteredIndexTransformerBase[T] extends AbstractTypeTransformerOps[T] {
  import JavaCollectionUtils._

  override def serialize(o: Any): AnyRef = o match {
    case _: Boolean | _: Byte | _: Char | _: Double | _: Float | _: Int | _: Short | _: String =>
      o.asInstanceOf[AnyRef]
    case l: Long       => fromLong(l)
    case d: LocalDate  => fromLocalDate(d).asInstanceOf[AnyRef]
    case t: LocalTime  => fromLocalTime(t).asInstanceOf[AnyRef]
    case t: OffsetTime =>
      // Following is simply copied from OffsetTime.toEpochNano(), which is unfortunately private.
      val nod = t.toLocalTime.toNanoOfDay
      val offsetNanos = t.getOffset.getTotalSeconds * RegisteredIndexTransformerBase.NANOS_PER_SECOND
      (nod - offsetNanos).asInstanceOf[AnyRef]
    case z: ZonedDateTime        => DateTimeSerialization.fromInstant(z.toInstant).asInstanceOf[AnyRef]
    case iba: ImmutableByteArray => serialize(iba.data)
    case ab: Array[Byte]         => fromByteArray(ab)
    case x: collection.Seq[_]    => mapToArrayList(serialize _, x)
    case x: Set[_]               => mapToArrayList(serialize _, x)
    case v => throw new IllegalArgumentException(s"Cannot serialize value: $v of type: ${v.getClass.getName}")
  }

  override def deserialize(o: Any): Any = throw new IllegalStateException("Didn't expect this method to get invoked.")
}
