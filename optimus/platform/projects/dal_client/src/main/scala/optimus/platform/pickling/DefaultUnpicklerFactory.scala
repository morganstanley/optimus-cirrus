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

import msjava.base.util.uuid.MSUuid
import optimus.breadcrumbs.ChainedID
import optimus.datatype.FullName
import optimus.platform.BusinessEvent
import optimus.platform.Compressed
import optimus.platform.CovariantSet
import optimus.platform.ImmutableArray
import optimus.platform.MSUnique
import optimus.platform.cm.Knowable
import optimus.platform.dal.session.RolesetMode
import optimus.platform.pickling.PicklingReflectionUtils._
import optimus.platform.storable
import optimus.platform.storable.Entity
import optimus.platform.storable.ReferenceHolder

import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetTime
import java.time.Period
import java.time.Year
import java.time.YearMonth
import java.time.ZoneId
import java.time.ZonedDateTime
import scala.collection.immutable
import scala.reflect.runtime.universe._

class DefaultUnpicklerFactory extends UnpicklerFactory with DefaultFactory[Unpickler] {
  import DefaultUnpicklers._

  override protected lazy val values: Map[ClassKey, List[Type] => Option[Unpickler[_]]] = Map(
    /* no type argument */
    classKeyOf[Int] -> typeParams0(intUnpickler),
    classKeyOf[Long] -> typeParams0(longUnpickler),
    classKeyOf[Float] -> typeParams0(floatUnpickler),
    classKeyOf[Double] -> typeParams0(doubleUnpickler),
    classKeyOf[Boolean] -> typeParams0(booleanUnpickler),
    classKeyOf[Short] -> typeParams0(shortUnpickler),
    classKeyOf[Byte] -> typeParams0(byteUnpickler),
    classKeyOf[Char] -> typeParams0(charUnpickler),
    classKeyOf[String] -> typeParams0(stringUnpickler),
    classKeyOf[BigDecimal] -> typeParams0(bigdecimalUnpickler),
    classKeyOf[Unit] -> typeParams0(unitUnpickler),
    classKeyOf[ChainedID] -> typeParams0(chainedIdUnpickler),
    classKeyOf[ZonedDateTime] -> typeParams0(zdtUnpickler),
    classKeyOf[LocalDate] -> typeParams0(localDateUnpickler),
    classKeyOf[LocalTime] -> typeParams0(localTimeUnpickler),
    classKeyOf[OffsetTime] -> typeParams0(offsetTimeUnpickler),
    classKeyOf[RolesetMode] -> typeParams0(rolesetModeUnpickler),
    classKeyOf[ZoneId] -> typeParams0(zoneIdUnpickler),
    classKeyOf[Period] -> typeParams0(periodUnpickler),
    classKeyOf[Instant] -> typeParams0(instantUnpickler),
    classKeyOf[Duration] -> typeParams0(durationUnpickler),
    classKeyOf[YearMonth] -> typeParams0(yearMonthUnpickler),
    classKeyOf[Year] -> typeParams0(yearUnpickler),
    classKeyOf[MSUuid] -> typeParams0(msuuidUnpickler),
    classKeyOf[FullName[_]] -> typeParams1(t => fullNameUnpickler),

    /* one type argument*/
    classKeyOf[Compressed[_]] -> {
      case List(t) if typesMatch(t, typeOf[String]) =>
        Some(
          compressedUnpickler(
            DefaultUnpicklers.stringUnpickler,
            DefaultPicklers.stringPickler,
            Compression.stringCompressible))
      case _ => None
    },
    classKeyOf[Knowable[_]] -> typeParams1(t => knowableUnpickler(t.unpickler, t.manifest)),
    classKeyOf[Option[_]] -> typeParams1(t => optionUnpickler(t.unpickler, t.manifest)),
    classKeyOf[Seq[_]] -> typeParams1(t => seqUnpickler(t.unpickler)(t.manifest)),
    classKeyOf[immutable.Seq[_]] -> typeParams1(t => iseqUnpickler(t.unpickler)(t.manifest)),
    classKeyOf[IndexedSeq[_]] -> typeParams1(t => indexedSeqUnpickler(t.unpickler)(t.manifest)),
    classKeyOf[immutable.LinearSeq[_]] -> typeParams1(t => linearSeqUnpickler(t.unpickler)(t.manifest)),
    classKeyOf[Vector[_]] -> typeParams1(t => vectorUnpickler(t.unpickler)(t.manifest)),
    classKeyOf[List[_]] -> typeParams1(t => listUnpickler(t.unpickler)(t.manifest)),
    classKeyOf[scala.collection.Set[_]] -> typeParams1(t => setUnpickler(t.unpickler)(t.manifest)),
    classKeyOf[Set[_]] -> typeParams1(t => setUnpickler(t.unpickler)(t.manifest)),
    classKeyOf[Iterable[_]] -> typeParams1(t => iterableUnpickler(t.unpickler)(t.manifest)),
    classKeyOf[ImmutableArray[_]] -> {
      case List(t) if typesMatch(t, typeOf[Byte]) => Some(ibytearrayUnpickler)
      case typeArgs => typeParams1(t => iarrayUnpickler(t.unpickler)(t.manifest))(typeArgs)
    },
    classKeyOf[CovariantSet[_]] -> typeParams1(t => covariantSetUnpickler(t.unpickler)(t.manifest)),
    classKeyOf[immutable.SortedSet[_]] -> typeParams1(t =>
      sortedSetUnpickler(t.unpickler)(t.manifest, orderingOf(t.tpe))),
    classKeyOf[ReferenceHolder[_]] -> typeParams1(_ => referenceHolderUnpickler),
    /* two type arguments */
    classKeyOf[immutable.ListMap[_, _]] -> typeParams2((a, b) =>
      listMapUnpickler(a.unpickler, b.unpickler)(a.manifest, b.manifest)),
    classKeyOf[Map[_, _]] -> typeParams2((a, b) =>
      nonstringMapUnpickler(a.unpickler, b.unpickler)(a.manifest, b.manifest)),
    classKeyOf[immutable.SortedMap[_, _]] -> typeParams2((a, b) =>
      sortedMapUnpickler(a.unpickler, b.unpickler)(a.manifest, orderingOf(a.tpe), b.manifest)),
    classKeyOf[immutable.TreeMap[_, _]] -> typeParams2((a, b) =>
      treeMapUnpickler(a.unpickler, b.unpickler)(a.manifest, orderingOf(a.tpe), b.manifest)),
    classKeyOf[(_, _)] -> typeParams2((a, b) => tuple2Unpickler(a.unpickler, b.unpickler)(a.manifest, b.manifest)),
    /* three or more type arguments*/
    classKeyOf[(_, _, _)] -> typeParams3((a, b, c) =>
      tuple3Unpickler(a.unpickler, b.unpickler, c.unpickler)(a.manifest, b.manifest, c.manifest)),
    classKeyOf[(_, _, _, _)] -> typeParams4((a, b, c, d) =>
      tuple4Unpickler(a.unpickler, b.unpickler, c.unpickler, d.unpickler)(
        a.manifest,
        b.manifest,
        c.manifest,
        d.manifest)),
    classKeyOf[(_, _, _, _, _)] -> typeParams5((a, b, c, d, e) =>
      tuple5Unpickler(a.unpickler, b.unpickler, c.unpickler, d.unpickler, e.unpickler)(
        a.manifest,
        b.manifest,
        c.manifest,
        d.manifest,
        e.manifest)),
    classKeyOf[(_, _, _, _, _, _)] -> typeParams6((a, b, c, d, e, f) =>
      tuple6Unpickler(a.unpickler, b.unpickler, c.unpickler, d.unpickler, e.unpickler, f.unpickler)(
        a.manifest,
        b.manifest,
        c.manifest,
        d.manifest,
        e.manifest,
        f.manifest)),
    classKeyOf[(_, _, _, _, _, _, _)] -> typeParams7((a, b, c, d, e, f, g) =>
      tuple7Unpickler(a.unpickler, b.unpickler, c.unpickler, d.unpickler, e.unpickler, f.unpickler, g.unpickler)(
        a.manifest,
        b.manifest,
        c.manifest,
        d.manifest,
        e.manifest,
        f.manifest,
        g.manifest)),
    classKeyOf[(_, _, _, _, _, _, _, _)] -> typeParams8((a, b, c, d, e, f, g, h) =>
      tuple8Unpickler(
        a.unpickler,
        b.unpickler,
        c.unpickler,
        d.unpickler,
        e.unpickler,
        f.unpickler,
        g.unpickler,
        h.unpickler)(a.manifest, b.manifest, c.manifest, d.manifest, e.manifest, f.manifest, g.manifest, h.manifest)),
    classKeyOf[(_, _, _, _, _, _, _, _, _)] -> typeParams9((a, b, c, d, e, f, g, h, i) =>
      tuple9Unpickler(
        a.unpickler,
        b.unpickler,
        c.unpickler,
        d.unpickler,
        e.unpickler,
        f.unpickler,
        g.unpickler,
        h.unpickler,
        i.unpickler)(
        a.manifest,
        b.manifest,
        c.manifest,
        d.manifest,
        e.manifest,
        f.manifest,
        g.manifest,
        h.manifest,
        i.manifest)),
    classKeyOf[(_, _, _, _, _, _, _, _, _, _)] -> typeParams10((a, b, c, d, e, f, g, h, i, j) =>
      tuple10Unpickler(
        a.unpickler,
        b.unpickler,
        c.unpickler,
        d.unpickler,
        e.unpickler,
        f.unpickler,
        g.unpickler,
        h.unpickler,
        i.unpickler,
        j.unpickler)(
        a.manifest,
        b.manifest,
        c.manifest,
        d.manifest,
        e.manifest,
        f.manifest,
        g.manifest,
        h.manifest,
        i.manifest,
        j.manifest))
  )

  override protected lazy val superclassValues: Map[ClassKey, Type => Option[Unpickler[_]]] = Map(
    classKeyOf[Entity] -> (tpe => Some(entityUnpickler(manifestOf(tpe)))),
    classKeyOf[Enumeration#Value] -> (tpe => Some(enumUnpickler(manifestOf(tpe)))),
    classKeyOf[Enum[_]] -> (tpe => Some(javaEnumUnpickler(manifestOf(tpe)))),
    classKeyOf[BusinessEvent] -> (_ => Some(BusinessEvent.eventunpickler)),
    classKeyOf[MSUnique] -> (_ => Some(MSUnique.MSUniqueUnpickler)),
    classKeyOf[storable.Embeddable] -> (tpe => Some(EmbeddablePicklers.unpicklerForType(tpe)))
  )
}
