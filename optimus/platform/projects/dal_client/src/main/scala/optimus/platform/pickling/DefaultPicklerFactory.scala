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
package optimus.platform.pickling

import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetTime
import java.time.Period
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.Year
import java.time.YearMonth
import msjava.base.util.uuid.MSUuid
import optimus.platform.BusinessEvent
import optimus.platform.Compressed
import optimus.platform.CovariantSet
import optimus.platform.ImmutableArray
import optimus.platform.MSUnique
import optimus.platform.cm.Knowable
import optimus.platform.storable.Entity

import scala.collection.immutable
import scala.collection.SortedSet
import scala.reflect.runtime.universe._
import optimus.breadcrumbs.ChainedID
import optimus.datatype.FullName
import optimus.platform.dal.session.RolesetMode
import optimus.platform.storable
import optimus.platform.storable.ReferenceHolder

class DefaultPicklerFactory extends PicklerFactory with DefaultFactory[Pickler] {
  import DefaultPicklers._

  override protected lazy val values: Map[ClassKey, List[Type] => Option[Pickler[_]]] = Map(
    /* no type arguments */
    classKeyOf[Int] -> typeParams0(intPickler),
    classKeyOf[Long] -> typeParams0(longPickler),
    classKeyOf[Float] -> typeParams0(floatPickler),
    classKeyOf[Double] -> typeParams0(doublePickler),
    classKeyOf[Boolean] -> typeParams0(booleanPickler),
    classKeyOf[String] -> typeParams0(stringPickler),
    classKeyOf[Unit] -> typeParams0(unitPickler),
    classKeyOf[ChainedID] -> typeParams0(chainedIdPickler),
    classKeyOf[ZonedDateTime] -> typeParams0(zdtPickler),
    classKeyOf[LocalDate] -> typeParams0(localDatePickler),
    classKeyOf[LocalTime] -> typeParams0(localTimePickler),
    classKeyOf[OffsetTime] -> typeParams0(offsetTimePickler),
    classKeyOf[RolesetMode] -> typeParams0(rolesetModePickler),
    classKeyOf[ZoneId] -> typeParams0(zoneIdPickler),
    classKeyOf[Period] -> typeParams0(periodPickler),
    classKeyOf[Instant] -> typeParams0(instantPickler),
    classKeyOf[Duration] -> typeParams0(durationPickler),
    classKeyOf[YearMonth] -> typeParams0(yearMonthPickler),
    classKeyOf[Year] -> typeParams0(yearPickler),
    classKeyOf[MSUuid] -> typeParams0(uuidPickler),
    classKeyOf[Short] -> typeParams0(shortPickler),
    classKeyOf[Byte] -> typeParams0(bytePickler),
    classKeyOf[BigDecimal] -> typeParams0(bdPickler),
    classKeyOf[Char] -> typeParams0(charPickler),
    classKeyOf[FullName[_]] -> typeParams1(t => fullNamePickler),

    /* one type argument */
    classKeyOf[Compressed[_]] -> {
      case List(t) if typesMatch(t, typeOf[String]) =>
        Some(compressedPickler(DefaultPicklers.stringPickler, Compression.stringCompressible))
      case _ => None
    },
    classKeyOf[Knowable[_]] -> typeParams1(t => knowablePickler(t.pickler)),
    classKeyOf[Option[_]] -> typeParams1(t => optionPickler(t.pickler)),
    classKeyOf[Iterable[_]] -> typeParams1(t => iterablePickler(t.pickler)),
    classKeyOf[Seq[_]] -> typeParams1(t => seqPickler(t.pickler)),
    classKeyOf[immutable.Seq[_]] -> typeParams1(t => iseqPickler(t.pickler)),
    classKeyOf[Vector[_]] -> typeParams1(t => vectorPickler(t.pickler)),
    classKeyOf[IndexedSeq[_]] -> typeParams1(t => indexedSeqPickler(t.pickler)),
    classKeyOf[List[_]] -> typeParams1(t => listPickler(t.pickler)),
    classKeyOf[scala.collection.Set[_]] -> typeParams1(t => setPickler(t.pickler)),
    classKeyOf[Set[_]] -> typeParams1(t => setPickler(t.pickler)),
    classKeyOf[immutable.Set[_]] -> typeParams1(t => isetPickler(t.pickler)),
    classKeyOf[SortedSet[_]] -> typeParams1(t => sortedSetPickler(t.pickler)),
    classKeyOf[immutable.SortedSet[_]] -> typeParams1(t => isortedSetPickler(t.pickler)),
    classKeyOf[CovariantSet[_]] -> typeParams1(t => covariantSetPickler(t.pickler)),
    classKeyOf[ImmutableArray[_]] -> {
      case List(t) if typesMatch(t, typeOf[Byte]) => Some(ibytearrayPickler)
      case typeArgs                               => typeParams1(t => iarrayPickler(t.pickler))(typeArgs)
    },
    classKeyOf[ReferenceHolder[_]] -> typeParams1(_ => referenceHolderPickler),
    /* two type arguments*/
    classKeyOf[immutable.Map[_, _]] -> typeParams2((a, b) => inonstringMapPickler(a.pickler, b.pickler)),
    classKeyOf[immutable.SortedMap[_, _]] -> typeParams2((a, b) => sortedMapPickler(a.pickler, b.pickler)),
    classKeyOf[immutable.ListMap[_, _]] -> typeParams2((a, b) => listMapPickler(a.pickler, b.pickler)),
    classKeyOf[immutable.TreeMap[_, _]] -> typeParams2((a, b) => treeMapPickler(a.pickler, b.pickler)),
    classKeyOf[(_, _)] -> typeParams2((a, b) => tuple2pickler(a.pickler, b.pickler)),
    /* three or more type arguments*/
    classKeyOf[(_, _, _)] -> typeParams3((a, b, c) => tuple3pickler(a.pickler, b.pickler, c.pickler)),
    classKeyOf[(_, _, _, _)] -> typeParams4((a, b, c, d) => tuple4pickler(a.pickler, b.pickler, c.pickler, d.pickler)),
    classKeyOf[(_, _, _, _, _)] -> typeParams5((a, b, c, d, e) =>
      tuple5pickler(a.pickler, b.pickler, c.pickler, d.pickler, e.pickler)),
    classKeyOf[(_, _, _, _, _, _)] -> typeParams6((a, b, c, d, e, f) =>
      tuple6pickler(a.pickler, b.pickler, c.pickler, d.pickler, e.pickler, f.pickler)),
    classKeyOf[(_, _, _, _, _, _, _)] -> typeParams7((a, b, c, d, e, f, g) =>
      tuple7pickler(a.pickler, b.pickler, c.pickler, d.pickler, e.pickler, f.pickler, g.pickler)),
    classKeyOf[(_, _, _, _, _, _, _, _)] -> typeParams8((a, b, c, d, e, f, g, h) =>
      tuple8pickler(a.pickler, b.pickler, c.pickler, d.pickler, e.pickler, f.pickler, g.pickler, h.pickler)),
    classKeyOf[(_, _, _, _, _, _, _, _, _)] -> typeParams9((a, b, c, d, e, f, g, h, i) =>
      tuple9pickler(a.pickler, b.pickler, c.pickler, d.pickler, e.pickler, f.pickler, g.pickler, h.pickler, i.pickler)),
    classKeyOf[(_, _, _, _, _, _, _, _, _, _)] -> typeParams10((a, b, c, d, e, f, g, h, i, j) =>
      tuple10pickler(
        a.pickler,
        b.pickler,
        c.pickler,
        d.pickler,
        e.pickler,
        f.pickler,
        g.pickler,
        h.pickler,
        i.pickler,
        j.pickler))
  )

  override protected lazy val superclassValues: Map[ClassKey, Type => Option[Pickler[_]]] = Map(
    classKeyOf[Entity] -> (_ => Some(EntityPickler)),
    classKeyOf[Enumeration#Value] -> (_ => Some(enumPickler)),
    classKeyOf[Enum[_]] -> (_ => Some(javaEnumPickler)),
    classKeyOf[BusinessEvent] -> (_ => Some(BusinessEvent.eventpickler)),
    classKeyOf[MSUnique] -> (_ => Some(MSUnique.MSUniquePickler)),
    classKeyOf[storable.Embeddable] -> (tpe => Some(EmbeddablePicklers.picklerForType(tpe)))
  )

  def pickleableClasses: Set[String] = values.keySet ++ superclassValues.keySet
}
