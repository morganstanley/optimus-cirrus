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

import optimus.graph.DiagnosticSettings
import optimus.platform.Query
import optimus.platform.relational.data.translation.AliasReplacer
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree.BinaryExpressionElement.balancedAnd
import optimus.platform.relational.tree.BinaryExpressionType._
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable.ArraySeq
import scala.collection.mutable

class PushableSourceColumnFinder(via: ColumnElement) extends DbQueryTreeVisitor {
  private var source: Option[TableElement] = None
  private var column: ColumnElement = via

  def find(e: RelationElement): Option[ColumnElement] = {
    visitElement(e)
    source.map(_ => column)
  }

  // we do not visit into SubqueryElement like in SerializedKeyBasedIndexOptimizer
  override protected def handleSubquery(subquery: SubqueryElement): RelationElement = subquery

  protected override def handleSelect(select: SelectElement): RelationElement = {
    if (select.alias == column.alias && select.groupBy == null && select.skip == null && select.take == null) {
      select.columns.find(d => d.name == column.name) match {
        case Some(ColumnDeclaration(_, c: ColumnElement)) =>
          column = c
          visitElement(select.from)
        case _ =>
      }
    }
    select
  }

  override protected def handleJoin(join: JoinElement): RelationElement = {
    def processSide(side: RelationElement, joinTypes: Set[JoinType]): RelationElement = side match {
      // top-down check for pushable tables
      case other if joinTypes.contains(join.joinType) => visitSource(other)
      case other                                      => other
    }
    processSide(join.left, PushableSourceColumnFinder.leftTableJoinTypes)
    processSide(join.right, PushableSourceColumnFinder.rightTableJoinTypes)
    join
  }

  protected override def handleTable(table: TableElement): RelationElement = {
    if (table.alias == column.alias && table.entity.projectedType <:< classOf[Entity])
      source = Some(table)
    table
  }
}

object PushableSourceColumnFinder {
  private val leftTableJoinTypes =
    Set[JoinType](JoinType.InnerJoin, JoinType.LeftOuter, JoinType.CrossJoin, JoinType.CrossApply)
  private val rightTableJoinTypes =
    Set[JoinType](JoinType.InnerJoin, JoinType.RightOuter, JoinType.CrossJoin, JoinType.CrossApply)

  def findPushableSourceColumn(from: RelationElement, c: ColumnElement): Option[ColumnElement] = {
    from match {
      case _: TableElement => None
      case _               => new PushableSourceColumnFinder(c).find(from)
    }
  }
}

/**
 * collects columns from all parts of the query except WHERE clause to form the select columns for the TableElement wrapped within a SelectElement
 */
class SelectiveColumnCollector extends DbQueryTreeVisitor {
  val elements = mutable.Map.empty[ColumnElement, Int]

  override protected def handleColumn(column: ColumnElement): RelationElement = {
    val newCount = elements.getOrElse(column, 0) + 1
    elements.update(column, newCount)
    column
  }
}

object SelectiveColumnCollector {
  def collect(element: RelationElement): mutable.Map[ColumnElement, Int] = {
    val c = new SelectiveColumnCollector()
    c.visitElement(element)
    c.elements
  }
}

/**
 * Optimize query performance by applying filters at the table level before joins.
 * This reduces the number of records retrieved from MongoDB and PostgresDB.
 */
class PredicatePushdownOptimizer(e: RelationElement) extends DbQueryTreeVisitor {
  private val selectConditions = mutable.Map.empty[TableAlias, mutable.ArrayBuffer[RelationElement]]
  private val newSelectColumns = SelectiveColumnCollector.collect(e).groupBy(_._1.alias)

  override protected def handleSelect(select: SelectElement): RelationElement = {
    val cachedSrcColMapping = mutable.Map.empty[(TableAlias, String), Option[ColumnElement]]

    def findInnerMostPushableAlias(from: RelationElement, col: ColumnElement): Option[TableAlias] = {
      cachedSrcColMapping
        .getOrElseUpdate((col.alias, col.name), PushableSourceColumnFinder.findPushableSourceColumn(from, col))
        .map(_.alias)
    }

    if (select.where != null) {
      val condBuf = Query.flattenBOOLANDConditions(select.where)
      // TODO (OPTIMUS-72490): rewrite TupleElement equals conditions in flattened BOOLAND conditions to better pushdown

      // group the conditions by their inner most pushable select/table
      condBuf.foreach {
        // t => t.property == [constant]
        case b: BinaryExpressionElement if b.op == EQ =>
          (b.left, b.right) match {
            case (OptionElement(col: ColumnElement), _: ConstValueElement) =>
              val (newAlias, newCond) = findInnerMostPushableAlias(select.from, col) match {
                case Some(tableAlias) =>
                  newSelectColumns(col.alias)(col) -= 1
                  if (tableAlias != col.alias) {
                    (
                      tableAlias,
                      updateBinary(
                        b,
                        new OptionElement(new ColumnElement(col.rowTypeInfo, tableAlias, col.name, col.columnInfo)),
                        b.right))
                  } else (tableAlias, b)
                case None => (select.alias, b)
              }
              selectConditions.getOrElseUpdate(newAlias, mutable.ArrayBuffer.empty).append(newCond)
            case (_: ConstValueElement, OptionElement(col: ColumnElement)) =>
              val (newAlias, newCond) = findInnerMostPushableAlias(select.from, col) match {
                case Some(tableAlias) =>
                  newSelectColumns(col.alias)(col) -= 1
                  if (tableAlias != col.alias) {
                    (
                      tableAlias,
                      updateBinary(
                        b,
                        b.left,
                        new OptionElement(new ColumnElement(col.rowTypeInfo, tableAlias, col.name, col.columnInfo))))
                  } else (tableAlias, b)
                case None => (select.alias, b)
              }
              selectConditions.getOrElseUpdate(newAlias, mutable.ArrayBuffer.empty).append(newCond)
            case (col: ColumnElement, _: ConstValueElement) =>
              val (newAlias, newCond) = findInnerMostPushableAlias(select.from, col) match {
                case Some(tableAlias) =>
                  newSelectColumns(col.alias)(col) -= 1
                  if (tableAlias != col.alias) {
                    (
                      tableAlias,
                      updateBinary(
                        b,
                        new ColumnElement(col.rowTypeInfo, tableAlias, col.name, col.columnInfo),
                        b.right))
                  } else (tableAlias, b)
                case None => (select.alias, b)
              }
              selectConditions.getOrElseUpdate(newAlias, mutable.ArrayBuffer.empty).append(newCond)
            case (_: ConstValueElement, col: ColumnElement) =>
              val (newAlias, newCond) = findInnerMostPushableAlias(select.from, col) match {
                case Some(tableAlias) =>
                  newSelectColumns(col.alias)(col) -= 1
                  if (tableAlias != col.alias) {
                    (
                      tableAlias,
                      updateBinary(
                        b,
                        b.left,
                        new ColumnElement(col.rowTypeInfo, tableAlias, col.name, col.columnInfo)
                      ))
                  } else (tableAlias, b)
                case None => (select.alias, b)
              }
              selectConditions.getOrElseUpdate(newAlias, mutable.ArrayBuffer.empty).append(newCond)
            case _ =>
              selectConditions.getOrElseUpdate(select.alias, mutable.ArrayBuffer.empty).append(b)
          }
        // t => [constant collection].contains(t.property)
        case contains: ContainsElement =>
          (contains.element, contains.values) match {
            case (TupleElement(elems), Right(_)) =>
              val columnWithNewAliasSeq = elems.flatMap {
                case col: ColumnElement =>
                  findInnerMostPushableAlias(select.from, col).map(a => (col, a))
                case OptionElement(col: ColumnElement) =>
                  findInnerMostPushableAlias(select.from, col).map(a => (col, a))
                case _ => None
              }

              val aliasSet = columnWithNewAliasSeq.map(_._2).toSet
              // push the condition down if all columns are pushable and from the same table,
              if (columnWithNewAliasSeq.size == elems.size && aliasSet.size == 1) {
                val map = new ScopedHashMap[TableAlias, TableAlias](null)
                var isAliasDiff = false
                columnWithNewAliasSeq
                  .foreach({
                    case (c, newAlias) =>
                      if (c.alias != newAlias) {
                        isAliasDiff = true
                        map.put(c.alias, newAlias)
                      }
                      // remove the pushable column from the counter to avoid unnecessary column in the select columns
                      newSelectColumns(c.alias)(c) -= 1
                    case _ =>
                  })
                val newCond = if (isAliasDiff) AliasReplacer.replace(contains, map) else contains
                selectConditions.getOrElseUpdate(aliasSet.head, mutable.ArrayBuffer.empty).append(newCond)
              } else selectConditions.getOrElseUpdate(select.alias, mutable.ArrayBuffer.empty).append(contains)
            case (OptionElement(col: ColumnElement), Right(_)) =>
              val (newAlias, newCond) = findInnerMostPushableAlias(select.from, col) match {
                case Some(tableAlias) =>
                  newSelectColumns(col.alias)(col) -= 1
                  if (tableAlias != col.alias) {
                    (
                      tableAlias,
                      updateContains(
                        contains,
                        new OptionElement(new ColumnElement(col.rowTypeInfo, tableAlias, col.name, col.columnInfo)),
                        contains.values
                      ))
                  } else (tableAlias, contains)
                case None => (select.alias, contains)
              }
              selectConditions.getOrElseUpdate(newAlias, mutable.ArrayBuffer.empty).append(newCond)
            case (col: ColumnElement, Right(_)) =>
              val (newAlias, newCond) = findInnerMostPushableAlias(select.from, col) match {
                case Some(tableAlias) =>
                  newSelectColumns(col.alias)(col) -= 1
                  if (tableAlias != col.alias) {
                    (
                      tableAlias,
                      updateContains(
                        contains,
                        new ColumnElement(col.rowTypeInfo, tableAlias, col.name, col.columnInfo),
                        contains.values
                      ))
                  } else (tableAlias, contains)
                case None => (select.alias, contains)
              }
              selectConditions.getOrElseUpdate(newAlias, mutable.ArrayBuffer.empty).append(newCond)
            case _ =>
              selectConditions.getOrElseUpdate(select.alias, mutable.ArrayBuffer.empty).append(contains)
          }
        // t => t.collectionProperty.contains([constant])
        case f @ FuncElement(m: MethodCallee, List(ConstValueElement(_, _)), col: ColumnElement)
            if m.name == "contains" && col.rowTypeInfo <:< classOf[Iterable[_]] =>
          val (newAlias, newCond) = findInnerMostPushableAlias(select.from, col) match {
            case Some(tableAlias) =>
              newSelectColumns(col.alias)(col) -= 1
              if (tableAlias != col.alias) {
                (
                  tableAlias,
                  updateFuncCall(
                    f,
                    new ColumnElement(col.rowTypeInfo, tableAlias, col.name, col.columnInfo),
                    f.arguments
                  ))
              } else (tableAlias, f)
            case None => (select.alias, f)
          }
          selectConditions.getOrElseUpdate(newAlias, mutable.ArrayBuffer.empty).append(newCond)
        case other =>
          selectConditions.getOrElseUpdate(select.alias, mutable.ArrayBuffer.empty).append(other)
      }
    }

    select.from match {
      // to avoid creating additional inner SelectElement
      case t: TableElement if selectConditions.contains(t.alias) =>
        val newWhereCond = ArraySeq
          .newBuilder[RelationElement]
          .addAll(selectConditions.getOrElse(select.alias, Nil))
          .addAll(selectConditions.getOrElse(t.alias, Nil))
          .result()
        updateSelect(
          select,
          select.from,
          if (newWhereCond.isEmpty) null else balancedAnd(newWhereCond),
          select.orderBy,
          select.groupBy,
          select.skip,
          select.take,
          select.columns
        )
      case _ =>
        val newWhereCond = ArraySeq.from(selectConditions.getOrElse(select.alias, Nil))
        updateSelect(
          select,
          visitSource(select.from),
          if (newWhereCond.isEmpty) null else balancedAnd(newWhereCond),
          select.orderBy,
          select.groupBy,
          select.skip,
          select.take,
          select.columns
        )
    }
  }

  override protected def handleTable(table: TableElement): RelationElement = {
    if (selectConditions.contains(table.alias)) {
      // create a new alias to avoid conflict with the outer alias
      val newTable = new TableElement(new TableAlias(), table.entity, table.name)
      val select = new SelectElement(
        table.alias,
        newSelectColumns
          .getOrElse(table.alias, mutable.Map.empty)
          .filter(_._2 > 0)
          .keys
          .map(c => ColumnDeclaration(c.name, c))
          .toList
          .sortBy(_.name), // to display the select columns in a deterministic order
        newTable,
        balancedAnd(selectConditions(table.alias).to(ArraySeq)),
        null,
        null,
        null,
        null,
        false,
        false
      )
      AliasReplacer.replace(select, table.alias, newTable.alias)
    } else table
  }
}

object PredicatePushdownOptimizer {
  private val enablePredicatePushdownProp =
    "optimus.priql.projected.enablePredicatePushdown"
  private val defaultEnablePredicatePushdownProp: Boolean =
    DiagnosticSettings.getBoolProperty(enablePredicatePushdownProp, true)
  private val _enablePredicatePushdown = new AtomicBoolean(defaultEnablePredicatePushdownProp)
  private def enablePredicatePushdown: Boolean = _enablePredicatePushdown.get()

  private[optimus] def setEnablePredicatePushdown(value: Boolean): Unit = {
    _enablePredicatePushdown.set(value)
  }
  private[optimus] def resetEnablePredicatePushdown(): Unit = {
    _enablePredicatePushdown.set(defaultEnablePredicatePushdownProp)
  }

  def optimize(e: RelationElement): RelationElement = {
    if (enablePredicatePushdown) {
      new PredicatePushdownOptimizer(e).visitElement(e)
    } else e
  }
}
