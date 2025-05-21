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
package optimus.platform.relational.dal.accelerated

import optimus.entity.EntityInfoRegistry
import optimus.entity.IndexInfo
import optimus.graph.DiagnosticSettings
import optimus.platform._
import optimus.platform.dal.SessionFetcher
import optimus.platform.dsi.bitemporal.proto.ProtoSerialization.distinctBy
import optimus.platform.dsi.Feature.SerializedKeyBasedFilterForAccelerator
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.DALMappingEntity
import optimus.platform.relational.dal.core.IndexColumnInfo
import optimus.platform.relational.data.mapping.QueryMapping
import optimus.platform.relational.data.translation.ColumnCollector
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.platform.storable.Storable

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * This optimizer will try to utilize @indexed/@key to improve the index search performance for acc queries.
 * DAL index stored in mongodb is usually more efficient for equal/contains conditions.
 */
class SerializedKeyBasedIndexOptimizer(mapping: QueryMapping) extends DbQueryTreeVisitor {
  import BinaryExpressionType._
  import SerializedKeyBasedIndexOptimizer._

  // we do not visit into SubqueryElement
  protected override def handleSubquery(subquery: SubqueryElement): RelationElement = subquery

  protected override def handleSelect(select: SelectElement): RelationElement = {
    select.from match {
      // we only handle the conditions applied to TableElement directly
      case t: TableElement if isOptimizableEntitySource(t, select) =>
        val entityInfo = EntityInfoRegistry.getClassInfo(t.entity.projectedType.runtimeClass)
        val queryableIndexInfoIt = entityInfo.indexes.iterator.filter(i => i.queryable)
        val indexInfoSeq = distinctBy[IndexInfo[_ <: Storable, _], String](queryableIndexInfoIt, _.name).toIndexedSeq
        if (indexInfoSeq.isEmpty) select
        else {
          val condBuf = new mutable.ArrayBuffer[RelationElement]
          val propToBufIndexForEqualCond = new util.HashMap[String, Int]
          val propsToBufIndexForInCond = new util.HashMap[Seq[String], Int]

          // flatten BOOLAND conditions, rewrite TupleElement equals conditions and prepare condBuf
          Query.flattenBOOLANDConditions(select.where) foreach {
            case b: BinaryExpressionElement if b.op == EQ =>
              (b.left, b.right) match {
                case (TupleElement(elems), ConstValueElement(value: Product, _))
                    if elems.length == value.productArity =>
                  elems.zip(value.productIterator.toList) foreach { case (e, v) =>
                    condBuf += ElementFactory.equal(e, ElementFactory.constant(v, e.rowTypeInfo))
                  }
                case (ConstValueElement(value: Product, _), TupleElement(elems))
                    if elems.length == value.productArity =>
                  elems.zip(value.productIterator.toList) foreach { case (e, v) =>
                    condBuf += ElementFactory.equal(e, ElementFactory.constant(v, e.rowTypeInfo))
                  }
                case _ => condBuf += b
              }
            case binary @ BinaryExpressionElement(
                  BOOLOR,
                  contains: ContainsElement,
                  BinaryExpressionElement(EQ, OptionElement(col1: ColumnElement), none @ ConstValueElement(None, _), _),
                  _) =>
              (contains.element, contains.values) match {
                case (OptionElement(col2: ColumnElement), Right(values)) if col2 == col1 =>
                  condBuf += new ContainsElement(contains.element, Right(none :: values))
                case _ => condBuf += binary
              }
            case e => condBuf += e
          }

          val mappingEntity = mapping.getEntity(t.entity.projectedType).asInstanceOf[DALMappingEntity]
          var condBufChanged = false
          def isOption(col: ColumnElement): Boolean =
            TypeInfo.isOption(mappingEntity.getOriginalType(col.name))

          // collect conditions that can be executed by PriQL DefaultPlan
          var idx = 0
          while (idx < condBuf.length) {
            condBuf(idx) match {
              // t => t.property == [constant]
              case b: BinaryExpressionElement if b.op == BinaryExpressionType.EQ =>
                (b.left, b.right) match {
                  case (OptionElement(col: ColumnElement), _: ConstValueElement) if isOption(col) =>
                    propToBufIndexForEqualCond.putIfAbsent(col.name, idx)
                  case (_: ConstValueElement, OptionElement(col: ColumnElement)) if isOption(col) =>
                    propToBufIndexForEqualCond.putIfAbsent(col.name, idx)
                    condBuf(idx) = ElementFactory.equal(b.right, b.left)
                  case (col: ColumnElement, c: ConstValueElement) =>
                    propToBufIndexForEqualCond.putIfAbsent(col.name, idx)
                    if (isOption(col)) {
                      val optType = TypeInfo(classOf[Option[_]], c.rowTypeInfo)
                      val newConst = ElementFactory.constant(Option(c.value), optType)
                      condBuf(idx) = ElementFactory.equal(OptionElement(col), newConst)
                    }
                  case (c: ConstValueElement, col: ColumnElement) =>
                    propToBufIndexForEqualCond.putIfAbsent(col.name, idx)
                    if (isOption(col)) {
                      val optType = TypeInfo(classOf[Option[_]], c.rowTypeInfo)
                      val newConst = ElementFactory.constant(Option(c.value), optType)
                      condBuf(idx) = ElementFactory.equal(OptionElement(col), newConst)
                    } else {
                      condBuf(idx) = ElementFactory.equal(b.right, b.left)
                    }
                  case _ =>
                }
              // t => [constant collection].contains(t.property)
              case contains: ContainsElement =>
                (contains.element, contains.values) match {
                  case (TupleElement(elems), Right(_)) =>
                    val names = elems.collect {
                      case col: ColumnElement if !isOption(col)               => col.name
                      case OptionElement(col: ColumnElement) if isOption(col) => col.name
                    }
                    if (names.length == elems.length)
                      propsToBufIndexForInCond.putIfAbsent(names, idx)
                  case (OptionElement(col: ColumnElement), Right(_)) if isOption(col) =>
                    propsToBufIndexForInCond.putIfAbsent(Seq(col.name), idx)
                  case (col: ColumnElement, Right(values)) =>
                    if (isOption(col)) {
                      val optType = TypeInfo(classOf[Option[_]], col.rowTypeInfo)
                      val newValues = values collect { case ConstValueElement(v, _) =>
                        ElementFactory.constant(Option(v), optType)
                      }
                      if (newValues.length == values.length) {
                        condBuf(idx) = new ContainsElement(OptionElement(col), Right(newValues))
                        propsToBufIndexForInCond.putIfAbsent(Seq(col.name), idx)
                      }
                    } else {
                      propsToBufIndexForInCond.putIfAbsent(Seq(col.name), idx)
                    }
                  case _ =>
                }
              // t => t.collectionProperty.contains([constant])
              case FuncElement(m: MethodCallee, List(ConstValueElement(v, _)), col: ColumnElement)
                  if m.name == "contains" && col.rowTypeInfo <:< classOf[Iterable[_]] =>
                // try to rewrite here, we cannot combine such conditions with other conditions
                indexInfoSeq find { i =>
                  i.isCollection &&
                  (i.name == col.name || mappingEntity.getCompoundMembers(i.name).exists(_.exists(_.name == col.name)))
                } foreach { i =>
                  val ci = IndexColumnInfo(i)
                  val newValue = ElementFactory.constant(ci.toSerializedKey(v))
                  // we still use col.name here to reserve the path into embeddable (e.g. "embeddable.prop1")
                  val newCol = new ColumnElement(TypeInfo.ANY, t.alias, col.name, ci)
                  // reserve the contains FuncElement for entitlement check
                  condBuf(idx) = ElementFactory.call(newCol, m.method, newValue :: Nil)
                  condBufChanged = true
                }
              case _ =>
            }
            idx += 1
          }

          // begin to construct compound indexes if possible
          indexInfoSeq.sortBy(ii => (!ii.unique, ii.indexed, -ii.propertyNames.length)) foreach { ii =>
            val indexPropertyNames =
              if (ii.propertyNames.length > 1) ii.propertyNames
              else mappingEntity.getCompoundMembers(ii.name).map(_.map(_.name)).getOrElse(ii.propertyNames)

            if (indexPropertyNames.forall(propToBufIndexForEqualCond.containsKey)) {
              // combine multiple "==" condition into one compound condition
              var minIndex = condBuf.length // we will put this back to the smallest index in condBuf
              val valueSeq = indexPropertyNames map { p =>
                val idx = propToBufIndexForEqualCond.remove(p)
                val BinaryExpressionElement(_, _, ConstValueElement(value, _), _) = condBuf(idx)
                minIndex = math.min(minIndex, idx)
                condBuf(idx) = null
                value
              }
              val ci = IndexColumnInfo(ii)
              val newValue = ElementFactory.constant(ci.toSerializedKey(seqToTuple(valueSeq)))
              val newColName = if (indexPropertyNames.length == 1) indexPropertyNames.head else ii.name
              val newCol = new ColumnElement(TypeInfo.ANY, t.alias, newColName, ci)
              condBuf(minIndex) = ElementFactory.equal(newCol, newValue)
              condBufChanged = true
            } else if (propsToBufIndexForInCond.containsKey(indexPropertyNames)) {
              // rewrite Contains condition that can be directly translated to compound Contains condition
              val idx = propsToBufIndexForInCond.remove(indexPropertyNames)
              condBuf(idx) match {
                case contains: ContainsElement =>
                  val Right(elemList) = contains.values
                  val ci = IndexColumnInfo(ii)
                  val skList = elemList.collect { case ConstValueElement(v, _) =>
                    ElementFactory.constant(ci.toSerializedKey(v))
                  }
                  if (skList.length == elemList.length) {
                    val newColName = if (indexPropertyNames.length == 1) indexPropertyNames.head else ii.name
                    val newCol = new ColumnElement(TypeInfo.ANY, t.alias, newColName, ci)
                    condBuf(idx) = new ContainsElement(newCol, Right(skList))
                    condBufChanged = true
                  }
              }
            } else if (indexPropertyNames.length > 1) {
              // combine Contains condition with "==" conditions into a new compound Contains condition
              propsToBufIndexForInCond.keySet().iterator.asScala.filter(_.length <= indexPropertyNames.length) find {
                propSeq =>
                  var found = false
                  val (inCondProps, otherProps) = indexPropertyNames.toSet.partition(pn => propSeq.contains(pn))
                  val inCondMatches = inCondProps.size == propSeq.length
                  if (inCondMatches && otherProps.forall(propToBufIndexForEqualCond.containsKey)) {
                    val inCondIdx = propsToBufIndexForInCond.get(propSeq)
                    condBuf(inCondIdx) match {
                      case contains: ContainsElement =>
                        val Right(elemList) = contains.values
                        val productList = if (propSeq.length == 1) {
                          elemList.collect { case ConstValueElement(v: Any, _) => Tuple1(v) }
                        } else {
                          elemList.collect { case ConstValueElement(v: Product, _) => v }
                        }
                        if (productList.length == elemList.length) {
                          val ci = IndexColumnInfo(ii)
                          val otherValues = otherProps.map { p =>
                            val idx = propToBufIndexForEqualCond.remove(p)
                            val BinaryExpressionElement(_, _, ConstValueElement(value, _), _) = condBuf(idx)
                            condBuf(idx) = null
                            p -> value
                          } toMap
                          val inCondPosition = propSeq.zipWithIndex.toMap
                          val skList = productList.map { p =>
                            val seq = indexPropertyNames.map { pn =>
                              otherValues.getOrElse(pn, p.productElement(inCondPosition(pn)))
                            }
                            ElementFactory.constant(ci.toSerializedKey(seqToTuple(seq)))
                          }

                          val newCol = new ColumnElement(TypeInfo.ANY, t.alias, ii.name, ci)
                          condBuf(inCondIdx) = new ContainsElement(newCol, Right(skList))
                          condBufChanged = true
                          found = true
                        }
                    }
                  }
                  found
              } foreach { propsToBufIndexForInCond.remove _ }
            }
          }

          if (condBufChanged) {
            val newWhere = BinaryExpressionElement.balancedAnd(condBuf.iterator.filter(_ != null).map {
              case contains: ContainsElement =>
                (contains.element, contains.values) match {
                  // write back
                  case (OptionElement(_: ColumnElement), Right((c @ ConstValueElement(None, _)) :: tail)) =>
                    val l = new ContainsElement(contains.element, Right(tail))
                    val r = ElementFactory.equal(contains.element, c)
                    ElementFactory.orElse(l, r)
                  case _ => contains
                }
              case e => e
            }.to(ArraySeq))
            updateSelect(
              select,
              select.from,
              newWhere,
              select.orderBy,
              select.groupBy,
              select.skip,
              select.take,
              select.columns)
          } else select
        }
      case _ => super.handleSelect(select)
    }
  }
}

object SerializedKeyBasedIndexOptimizer {
  private val enableSerializedKeyBasedFilterForTestProp =
    "optimus.priql.projected.enableSerializedKeyBasedFilterForTest"
  private val defaultEnableSerializedKeyBasedFilterForTest: Boolean =
    DiagnosticSettings.getBoolProperty(enableSerializedKeyBasedFilterForTestProp, false)
  private val _enableSerializedKeyBasedFilterForTest = new AtomicBoolean(defaultEnableSerializedKeyBasedFilterForTest)
  private def enableSerializedKeyBasedFilterForTest: Boolean = _enableSerializedKeyBasedFilterForTest.get()
  private[optimus] def setEnableSerializedKeyBasedFilterForTest(value: Boolean): Unit = {
    _enableSerializedKeyBasedFilterForTest.set(value)
  }
  private[optimus] def resetEnableSerializedKeyBasedFilterForTest(): Unit = {
    _enableSerializedKeyBasedFilterForTest.set(defaultEnableSerializedKeyBasedFilterForTest)
  }

  private def serverSupportSerializedKeyBasedFilter: Boolean = {
    Option(EvaluationContext.env).flatMap(e => Option(e.entityResolver)) collect { case sf: SessionFetcher =>
      sf.dsi.serverFeatures().supports(SerializedKeyBasedFilterForAccelerator)
    } getOrElse false
  }

  def optimize(e: RelationElement, mapping: QueryMapping): RelationElement = {
    if (enableSerializedKeyBasedFilterForTest || serverSupportSerializedKeyBasedFilter) {
      val select = PredicatePushdownOptimizer.optimize(e)
      new SerializedKeyBasedIndexOptimizer(mapping).visitElement(select)

    } else e
  }

  private def seqToTuple(s: Seq[Any]): Any = {
    s match {
      case Seq(a1)                                      => a1 // intentional to serve a special case
      case Seq(a1, a2)                                  => (a1, a2)
      case Seq(a1, a2, a3)                              => (a1, a2, a3)
      case Seq(a1, a2, a3, a4)                          => (a1, a2, a3, a4)
      case Seq(a1, a2, a3, a4, a5)                      => (a1, a2, a3, a4, a5)
      case Seq(a1, a2, a3, a4, a5, a6)                  => (a1, a2, a3, a4, a5, a6)
      case Seq(a1, a2, a3, a4, a5, a6, a7)              => (a1, a2, a3, a4, a5, a6, a7)
      case Seq(a1, a2, a3, a4, a5, a6, a7, a8)          => (a1, a2, a3, a4, a5, a6, a7, a8)
      case Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9)      => (a1, a2, a3, a4, a5, a6, a7, a8, a9)
      case Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
      case _                                            => ???
    }
  }

  private def isOptimizableEntitySource(t: TableElement, s: SelectElement): Boolean = {
    t.entity.projectedType <:< classOf[Entity] &&
    s.where != null &&
    !ColumnCollector.collect(s).exists {
      case col: ColumnElement => col.name == DALProvider.InitiatingEvent
      case _                  => false
    }
  }
}
