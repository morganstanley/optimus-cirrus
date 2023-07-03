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

import optimus.platform.relational.data.translation.DeclaredAliasGatherer
import optimus.platform.relational.tree._

class DbQueryTreeComparer protected (
    paramScope: ScopedHashMap[ParameterElement, ParameterElement],
    fnCompare: (Any, Any) => Boolean,
    protected var aliasScope: ScopedHashMap[TableAlias, TableAlias])
    extends QueryTreeComparer(paramScope, fnCompare) {

  override protected def compare(a: RelationElement, b: RelationElement): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else if (a.elementType != b.elementType) false
    else if (a.rowTypeInfo != b.rowTypeInfo) false
    else
      (a, b) match {
        case (ta: TableElement, tb: TableElement)                               => compareTable(ta, tb)
        case (ca: ColumnElement, cb: ColumnElement)                             => compareColumn(ca, cb)
        case (sa: SelectElement, sb: SelectElement)                             => compareSelect(sa, sb)
        case (ja: JoinElement, jb: JoinElement)                                 => compareJoin(ja, jb)
        case (pa: ProjectionElement, pb: ProjectionElement)                     => compareProjection(pa, pb)
        case (ea: DbEntityElement, eb: DbEntityElement)                         => compareDbEntity(ea, eb)
        case (ca: ContainsElement, cb: ContainsElement)                         => compareContains(ca, cb)
        case (na: NamedValueElement, nb: NamedValueElement)                     => compareNamedValue(na, nb)
        case (ea: EmbeddableCollectionElement, eb: EmbeddableCollectionElement) => compareEmbeddableCollection(ea, eb)
        case (ea: EmbeddableCaseClassElement, eb: EmbeddableCaseClassElement)   => compareEmbeddableCaseClass(ea, eb)
        case (da: DALHeapEntityElement, db: DALHeapEntityElement)               => compareDALHeapEntity(da, db)
        case (ta: TupleElement, tb: TupleElement)                               => compareTuple(ta, tb)
        case (oa: OptionElement, ob: OptionElement)                             => compareOption(oa, ob)
        case (aa: AggregateElement, ab: AggregateElement)                       => compareAggregate(aa, ab)
        case (aa: AggregateSubqueryElement, ab: AggregateSubqueryElement)       => compareAggregateSubquery(aa, ab)
        case (sa: SubqueryElement, sb: SubqueryElement)                         => compareSubquery(sa, sb)
        case (oa: OuterJoinedElement, ob: OuterJoinedElement)                   => compareOuterJoined(oa, ob)
        case (da: DynamicObjectElement, db: DynamicObjectElement)               => compareDynamicObject(da, db)
        case _                                                                  => super.compare(a, b)
      }
  }

  protected def compareOuterJoined(a: OuterJoinedElement, b: OuterJoinedElement): Boolean = {
    compare(a.test, b.test) && compare(a.element, b.element) && compare(a.defaultValue, b.defaultValue)
  }

  protected def compareDynamicObject(a: DynamicObjectElement, b: DynamicObjectElement): Boolean = {
    compare(a.source, b.source)
  }

  protected def compareTable(a: TableElement, b: TableElement): Boolean = {
    a.name == b.name
  }

  protected def compareColumn(a: ColumnElement, b: ColumnElement): Boolean = {
    compareAlias(a.alias, b.alias) && a.name == b.name
  }

  protected def compareAlias(a: TableAlias, b: TableAlias): Boolean = {
    if (aliasScope ne null) {
      val mapped = aliasScope.getOrElse(a, null)
      if (mapped ne null) mapped == b else a == b
    } else a == b
  }

  protected def compareSelect(a: SelectElement, b: SelectElement): Boolean = {
    val save = aliasScope
    try {
      compare(a.from, b.from) && {
        aliasScope = new ScopedHashMap(save)
        mapAliases(a.from, b.from)

        compare(a.where, b.where) &&
        compareOrderBy(a.orderBy, b.orderBy) &&
        compareElementList(a.groupBy, b.groupBy) &&
        compare(a.skip, b.skip) &&
        compare(a.take, b.take) &&
        a.isDistinct == b.isDistinct &&
        a.reverse == b.reverse &&
        compareColumnDeclarations(a.columns, b.columns)
      }
    } finally {
      aliasScope = save
    }
  }

  private def mapAliases(a: RelationElement, b: RelationElement): Unit = {
    val prodA = DeclaredAliasGatherer.gather(a)
    val prodB = DeclaredAliasGatherer.gather(b)
    for ((a1, a2) <- prodA.zip(prodB)) {
      aliasScope.put(a1, a2)
    }
  }

  protected def compareOrderBy(a: List[OrderDeclaration], b: List[OrderDeclaration]): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else if (a.size != b.size) false
    else {
      a.zip(b).forall { case (oa, ob) =>
        oa.direction == ob.direction && compare(oa.element, ob.element)
      }
    }
  }

  protected def compareColumnDeclarations(a: List[ColumnDeclaration], b: List[ColumnDeclaration]): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else if (a.size != b.size) false
    else {
      a.zip(b).forall(t => compareColumnDeclaration(t._1, t._2))
    }
  }

  protected def compareColumnDeclaration(a: ColumnDeclaration, b: ColumnDeclaration): Boolean = {
    a.name == b.name && compare(a.element, b.element)
  }

  protected def compareJoin(a: JoinElement, b: JoinElement): Boolean = {
    if (a.joinType != b.joinType || !compare(a.left, b.left)) false
    else if (a.joinType == JoinType.CrossApply || a.joinType == JoinType.OuterApply) {
      val save = aliasScope
      try {
        aliasScope = new ScopedHashMap(save)
        mapAliases(a.left, b.left)
        compare(a.right, b.right) && {
          mapAliases(a.right, b.right)
          compare(a.condition, b.condition)
        }
      } finally {
        aliasScope = save
      }
    } else {
      compare(a.right, b.right) && {
        val save = aliasScope
        try {
          aliasScope = new ScopedHashMap(save)
          mapAliases(a.left, b.left)
          mapAliases(a.right, b.right)
          compare(a.condition, b.condition)
        } finally {
          aliasScope = save
        }
      }
    }
  }

  protected def compareProjection(a: ProjectionElement, b: ProjectionElement): Boolean = {
    if (!compare(a.select, b.select)) false
    else {
      val save = aliasScope
      try {
        aliasScope = new ScopedHashMap(save)
        aliasScope.put(a.select.alias, b.select.alias)
        compare(a.projector, b.projector) &&
        compare(a.aggregator, b.aggregator) &&
        a.isSingleton == b.isSingleton
      } finally {
        aliasScope = save
      }
    }
  }

  protected def compareDbEntity(a: DbEntityElement, b: DbEntityElement): Boolean = {
    a.entity == b.entity && compare(a.element, b.element)
  }

  protected def compareContains(a: ContainsElement, b: ContainsElement): Boolean = {
    compare(a.element, b.element) && {
      (a.values, b.values) match {
        case (Left(v1), Left(v2))   => compare(v1, v2)
        case (Right(v1), Right(v2)) => compareElementList(v1, v2)
        case _                      => false
      }
    }
  }

  protected def compareNamedValue(a: NamedValueElement, b: NamedValueElement): Boolean = {
    a.name == b.name && compare(a.value, b.value)
  }

  protected def compareEmbeddableCollection(a: EmbeddableCollectionElement, b: EmbeddableCollectionElement): Boolean = {
    a.info == b.info && compare(a.element, b.element) && compare(a.foreignKey, b.foreignKey)
  }

  protected def compareEmbeddableCaseClass(a: EmbeddableCaseClassElement, b: EmbeddableCaseClassElement): Boolean = {
    a.owner == b.owner && a.ownerProperty == b.ownerProperty
  }

  protected def compareDALHeapEntity(a: DALHeapEntityElement, b: DALHeapEntityElement): Boolean = {
    a.companion == b.companion
  }

  protected def compareTuple(a: TupleElement, b: TupleElement): Boolean = {
    compareElementList(a.elements, b.elements)
  }

  protected def compareOption(oa: OptionElement, ob: OptionElement): Boolean = {
    compare(oa.element, ob.element)
  }

  protected def compareAggregate(a: AggregateElement, b: AggregateElement): Boolean = {
    a.aggregateName == b.aggregateName && compareElementList(a.arguments, b.arguments) && a.isDistinct == b.isDistinct
  }

  protected def compareAggregateSubquery(a: AggregateSubqueryElement, b: AggregateSubqueryElement): Boolean = {
    compare(a.aggregateAsSubquery, b.aggregateAsSubquery) &&
    compare(a.aggregateInGroupSelect, b.aggregateInGroupSelect) &&
    a.groupAlias == b.groupAlias
  }

  protected def compareSubquery(a: SubqueryElement, b: SubqueryElement): Boolean = {
    (a, b) match {
      case (sa: ScalarElement, sb: ScalarElement) => compareScalar(sa, sb)
      case (ea: ExistsElement, eb: ExistsElement) => compareExists(ea, eb)
      case _                                      => false
    }
  }

  protected def compareExists(a: ExistsElement, b: ExistsElement): Boolean = {
    compare(a.select, b.select)
  }

  protected def compareScalar(a: ScalarElement, b: ScalarElement): Boolean = {
    compare(a.select, b.select)
  }
}

object DbQueryTreeComparer {
  def areEqual(a: RelationElement, b: RelationElement): Boolean = {
    areEqual(null, null, a, b)
  }

  def areEqual(
      paramScope: ScopedHashMap[ParameterElement, ParameterElement],
      aliasScope: ScopedHashMap[TableAlias, TableAlias],
      a: RelationElement,
      b: RelationElement): Boolean = {
    new DbQueryTreeComparer(paramScope, null, aliasScope).compare(a, b)
  }

  def areStructureEqual(a: RelationElement, b: RelationElement): Boolean = {
    new StructureComparer(null).compare(a, b)
  }

  class StructureComparer(fnCompare: (Any, Any) => Boolean)
      extends DbQueryTreeComparer(null, fnCompare, new ScopedHashMap(null)) {
    override protected def compareAlias(a: TableAlias, b: TableAlias): Boolean = {
      val mapped = aliasScope.getOrElse(a, null)
      if (mapped ne null) mapped == b
      else {
        aliasScope.put(a, b)
        true
      }
    }
  }
}
