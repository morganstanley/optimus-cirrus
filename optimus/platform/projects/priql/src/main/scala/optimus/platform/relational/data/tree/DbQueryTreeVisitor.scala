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
package optimus.platform.relational.data.tree

import optimus.platform.relational.tree.LambdaElement
import optimus.platform.relational.tree.QueryTreeVisitor
import optimus.platform.relational.tree.RelationElement

import scala.collection.mutable.ListBuffer

class DbQueryTreeVisitor extends QueryTreeVisitor {
  override def visitElement(element: RelationElement): RelationElement = {
    element match {
      case e: TableElement                => handleTable(e)
      case e: ColumnElement               => handleColumn(e)
      case e: SelectElement               => handleSelect(e)
      case e: JoinElement                 => handleJoin(e)
      case e: ProjectionElement           => handleProjection(e)
      case e: DbEntityElement             => handleDbEntity(e)
      case e: ContainsElement             => handleContains(e)
      case e: NamedValueElement           => handleNamedValue(e)
      case e: SubqueryElement             => handleSubquery(e)
      case e: EmbeddableCollectionElement => handleEmbeddableCollection(e)
      case e: EmbeddableCaseClassElement  => handleEmbeddableCaseClass(e)
      case e: DALHeapEntityElement        => handleDALHeapEntity(e)
      case e: TupleElement                => handleTupleElement(e)
      case e: OptionElement               => handleOptionElement(e)
      case e: AggregateElement            => handleAggregate(e)
      case e: AggregateSubqueryElement    => handleAggregateSubquery(e)
      case e: OuterJoinedElement          => handleOuterJoined(e)
      case e: DynamicObjectElement        => handleDynamicObject(e)
      case _                              => super.visitElement(element)
    }
  }

  protected def handleDynamicObject(d: DynamicObjectElement): RelationElement = {
    val src = visitElement(d.source)
    updateDynamicObject(d, src)
  }

  final protected def updateDynamicObject(d: DynamicObjectElement, source: RelationElement): RelationElement = {
    if (d.source eq source) d
    else new DynamicObjectElement(source)
  }

  protected def handleOuterJoined(outer: OuterJoinedElement): RelationElement = {
    val test = visitElement(outer.test)
    val element = visitElement(outer.element)
    val defaultValue = visitElement(outer.defaultValue)
    updateOuterJoined(outer, test, element, defaultValue)
  }

  final protected def updateOuterJoined(
      outer: OuterJoinedElement,
      test: RelationElement,
      element: RelationElement,
      defaultValue: RelationElement): RelationElement = {
    if ((test ne outer.test) || (element ne outer.element) || (defaultValue ne outer.defaultValue))
      new OuterJoinedElement(test, element, defaultValue)
    else outer
  }

  protected def handleAggregateSubquery(aggregate: AggregateSubqueryElement): RelationElement = {
    val subquery = visitElement(aggregate.aggregateAsSubquery).asInstanceOf[ScalarElement]
    if (subquery ne aggregate.aggregateAsSubquery)
      new AggregateSubqueryElement(aggregate.groupAlias, aggregate.aggregateInGroupSelect, subquery)
    else
      aggregate
  }

  protected def handleSubquery(subquery: SubqueryElement): RelationElement = {
    subquery match {
      case e: ScalarElement => handleScalar(e)
      case e: ExistsElement => handleExists(e)
      case _                => subquery
    }
  }

  protected def handleExists(exists: ExistsElement): RelationElement = {
    val s = visitElement(exists.select).asInstanceOf[SelectElement]
    if (s ne exists.select)
      new ExistsElement(s)
    else
      exists
  }

  protected def handleScalar(scalar: ScalarElement): RelationElement = {
    val s = visitElement(scalar.select).asInstanceOf[SelectElement]
    if (s ne scalar.select)
      new ScalarElement(scalar.rowTypeInfo, s)
    else
      scalar
  }

  protected def handleAggregate(aggregate: AggregateElement): RelationElement = {
    val args = visitElementList(aggregate.arguments)
    if (args ne aggregate.arguments)
      new AggregateElement(aggregate.rowTypeInfo, aggregate.aggregateName, args, aggregate.isDistinct)
    else
      aggregate
  }

  protected def handleNamedValue(value: NamedValueElement): RelationElement = {
    value
  }

  protected def handleContains(contains: ContainsElement): RelationElement = {
    val elem = visitElement(contains.element)
    val values = contains.values match {
      case Left(v) =>
        val newValue = visitElement(v).asInstanceOf[ScalarElement]
        if (v eq newValue) contains.values else Left(newValue)
      case Right(v) =>
        val newValue = visitElementList(v)
        if (v eq newValue) contains.values else Right(newValue)
    }
    updateContains(contains, elem, values)
  }

  final protected def updateContains(
      contains: ContainsElement,
      elem: RelationElement,
      values: Either[ScalarElement, List[RelationElement]]): ContainsElement = {
    if ((elem ne contains.element) || (values ne contains.values))
      new ContainsElement(elem, values)
    else
      contains
  }

  protected def handleTupleElement(tuple: TupleElement): RelationElement = {
    val elements = visitElementList(tuple.elements)
    updateTupleElement(tuple, elements)
  }

  final protected def updateTupleElement(tuple: TupleElement, elements: List[RelationElement]): RelationElement = {
    if (elements ne tuple.elements)
      new TupleElement(elements)
    else
      tuple
  }

  protected def handleOptionElement(option: OptionElement): RelationElement = {
    val e = visitElement(option.element)
    updateOption(option, e)
  }

  final protected def updateOption(option: OptionElement, element: RelationElement): RelationElement = {
    if (element ne option.element)
      OptionElement(element)
    else
      option
  }

  protected def handleEmbeddableCollection(collection: EmbeddableCollectionElement): RelationElement = {
    val element = visitElement(collection.element)
    val foreignKey = visitElement(collection.foreignKey)
    updateEmbeddableCollection(collection, element, foreignKey)
  }

  final protected def updateEmbeddableCollection(
      collection: EmbeddableCollectionElement,
      element: RelationElement,
      foreignKey: RelationElement): RelationElement = {
    if ((element ne collection.element) || (foreignKey ne collection.foreignKey))
      EmbeddableCollectionElement(collection.info, element, foreignKey)
    else
      collection
  }

  protected def handleEmbeddableCaseClass(caseClass: EmbeddableCaseClassElement): RelationElement = {
    val members = this.visitElementList(caseClass.members)
    updateEmbeddableCaseClass(caseClass, members)
  }

  final protected def updateEmbeddableCaseClass(
      caseClass: EmbeddableCaseClassElement,
      members: List[RelationElement]): RelationElement = {
    if (members ne caseClass.members)
      new EmbeddableCaseClassElement(
        caseClass.owner,
        caseClass.ownerProperty,
        caseClass.projectedType(),
        members,
        caseClass.memberNames)
    else
      caseClass
  }

  protected def handleDALHeapEntity(entity: DALHeapEntityElement): RelationElement = {
    val members = this.visitElementList(entity.members)
    updateDALHeapEntity(entity, members)
  }

  final protected def updateDALHeapEntity(
      entity: DALHeapEntityElement,
      members: List[RelationElement]): RelationElement = {
    if (members ne entity.members)
      new DALHeapEntityElement(entity.companion, entity.projectedType(), members, entity.memberNames)
    else
      entity
  }

  protected def handleDbEntity(entity: DbEntityElement): RelationElement = {
    val e = visitElement(entity.element)
    updateDbEntity(entity, e)
  }

  final protected def updateDbEntity(entity: DbEntityElement, e: RelationElement): DbEntityElement = {
    if (e ne entity.element)
      new DbEntityElement(entity.entity, e)
    else
      entity
  }

  protected def handleTable(table: TableElement): RelationElement = table

  protected def handleColumn(column: ColumnElement): RelationElement = column

  protected def handleSelect(select: SelectElement): RelationElement = {
    val from = visitSource(select.from)
    val where = visitElement(select.where)
    val columns = visitColumnDeclarations(select.columns)
    val orderBy = visitOrderBy(select.orderBy)
    val groupBy = visitElementList(select.groupBy)
    val skip = visitElement(select.skip)
    val take = visitElement(select.take)
    updateSelect(select, from, where, orderBy, groupBy, skip, take, columns)
  }

  final protected def updateSelect(
      select: SelectElement,
      from: RelationElement,
      where: RelationElement,
      orderBy: List[OrderDeclaration],
      groupBy: List[RelationElement],
      skip: RelationElement,
      take: RelationElement,
      columns: List[ColumnDeclaration]): SelectElement = {
    if (
      (from ne select.from) ||
      (where ne select.where) ||
      (take ne select.take) ||
      (skip ne select.skip) ||
      (orderBy ne select.orderBy) ||
      (groupBy ne select.groupBy) ||
      (columns ne select.columns)
    )
      new SelectElement(
        select.alias,
        columns,
        from,
        where,
        orderBy,
        groupBy,
        skip,
        take,
        select.isDistinct,
        select.reverse)
    else
      select
  }

  protected def visitOrderBy(elements: List[OrderDeclaration]): List[OrderDeclaration] = {
    if (elements eq null) null
    else {
      val elementList = elements.map(_.element)
      val list = visitElementList(elementList)
      if (list eq elementList) elements
      else {
        elements.zip(elementList.zip(list)).map { case (oldOrder, (oldElem, newElem)) =>
          if (oldElem eq newElem) oldOrder
          else new OrderDeclaration(oldOrder.direction, newElem)
        }
      }
    }
  }

  protected def handleJoin(join: JoinElement): RelationElement = {
    val left = visitSource(join.left)
    val right = visitSource(join.right)
    val condition = visitElement(join.condition)
    updateJoin(join, join.joinType, left, right, condition)
  }

  final protected def updateJoin(
      join: JoinElement,
      joinType: JoinType,
      left: RelationElement,
      right: RelationElement,
      condition: RelationElement): JoinElement = {
    if (
      joinType != join.joinType ||
      (left ne join.left) ||
      (right ne join.right) ||
      (condition ne join.condition)
    )
      new JoinElement(joinType, left, right, condition)
    else
      join
  }

  protected def handleProjection(proj: ProjectionElement): RelationElement = {
    val select = visitElement(proj.select).asInstanceOf[SelectElement]
    val projector = visitElement(proj.projector)
    updateProjection(proj, select, projector, proj.aggregator)
  }

  final protected def updateProjection(
      proj: ProjectionElement,
      select: SelectElement,
      projector: RelationElement,
      aggregator: LambdaElement): ProjectionElement = {
    if (
      (select ne proj.select) ||
      (projector ne proj.projector) ||
      (aggregator ne proj.aggregator)
    )
      new ProjectionElement(
        select,
        projector,
        proj.key,
        proj.keyPolicy,
        aggregator,
        proj.viaCollection,
        entitledOnly = proj.entitledOnly)
    else
      proj
  }

  protected def visitSource(source: RelationElement) = visitElement(source)

  final protected def visitColumnDeclarations(columns: List[ColumnDeclaration]): List[ColumnDeclaration] = {
    var alternate: ListBuffer[ColumnDeclaration] = null
    var index = 0
    for (column <- columns) {
      val e = visitElement(column.element)
      if ((alternate eq null) && (e ne column.element)) {
        alternate = new ListBuffer[ColumnDeclaration]()
        alternate ++= columns.take(index)
      }
      if (alternate ne null)
        alternate += new ColumnDeclaration(column.name, e)
      index += 1
    }
    if (alternate ne null) alternate.toList else columns
  }
}
