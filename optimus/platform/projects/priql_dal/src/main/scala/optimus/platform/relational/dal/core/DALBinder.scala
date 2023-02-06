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
package optimus.platform.relational.dal.core

import java.util

import optimus.platform.DynamicObject
import optimus.platform.NoKey
import optimus.platform.Query
import optimus.platform.pickling.DefaultUnpicklers
import optimus.platform.relational.PriqlSettings
import optimus.platform.relational.RelationalException
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.DALQueryMethod
import optimus.platform.relational.dal.internal.RawReferenceKey
import optimus.platform.relational.data.Aggregator
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryBinder
import optimus.platform.relational.data.mapping.QueryMapper
import optimus.platform.relational.data.translation.AliasReplacer
import optimus.platform.relational.data.translation.DefaultBinder
import optimus.platform.relational.data.translation.ElementReplacer
import optimus.platform.relational.data.tree._
import optimus.platform.relational.inmemory.ScalaTypeMultiRelation
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity

import scala.collection.mutable

class DALBinder protected (mapper: QueryMapper, root: RelationElement) extends DefaultBinder(mapper, root) {
  import DALBinder._
  import BinaryExpressionType.EQ

  private[this] val permitTableScanFlags = new mutable.HashMap[TableElement, Boolean]
  private[this] val conditionToFuncElement = new mutable.HashMap[RelationElement, FuncElement]

  def bind(e: RelationElement): RelationElement = {
    val elem = visitElement(e)
    maybeDoPermitTableScanCheck(elem)
    new ConditionOptimizer().optimize(elem)
  }

  protected def maybeDoPermitTableScanCheck(elem: RelationElement): Unit = {
    if (PriqlSettings.enableTableScanCheck) {
      new ProjectionLocator()
        .locate(elem)
        .foreach(p => {
          if ((p.select.take eq null) && (p.aggregator eq null)) {
            // 'from' must be a TableElement
            val table = p.select.from.asInstanceOf[TableElement]
            val permitTableScan = permitTableScanFlags.getOrElse(table, false)
            if (!permitTableScan && !hasFilterCondition(p.select))
              throw new RelationalException(
                "Table scan detected in the query! To skip the check, please call 'permitTableScan' explicitly.")
          }
        })
    }
  }

  protected def hasFilterCondition(s: SelectElement): Boolean = {
    if (s.where ne null) true
    else
      s.from match {
        case sel: SelectElement => hasFilterCondition(sel)
        case _                  => false
      }
  }

  override def handleMethod(method: MethodElement): RelationElement = {
    import QueryMethod._

    method.methodCode match {
      case WHERE                   => bindWhere(method)
      case UNTYPE                  => bindUntype(method)
      case PERMIT_TABLE_SCAN       => bindPermitTableScan(method)
      case TAKE                    => bindTake(method)
      case DALQueryMethod.SortByTT => bindSortByTT(method)
      case _                       => visitIntoMethod(method)
    }
  }

  protected def bindSortByTT(method: MethodElement) = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val projection = convertToSequence(source)
    if ((projection eq null) || ((projection.select.orderBy ne null) && projection.select.orderBy.nonEmpty)) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val s = projection.select
      val alias = s.from.asInstanceOf[AliasedElement].alias
      val ConstValueElement(isAsc: Boolean, _) = others.head.param
      val columnInfo = ColumnInfo(ColumnType.Default, Some(DefaultUnpicklers.instantUnpickler))
      val column = new ColumnElement(DALProvider.StorageTxTimeType, alias, DALProvider.StorageTxTime, columnInfo)
      val orderBy = OrderDeclaration(if (isAsc) SortDirection.Ascending else SortDirection.Descending, column)
      val newSel = new SelectElement(
        s.alias,
        s.columns,
        s.from,
        s.where,
        List(orderBy),
        s.groupBy,
        s.skip,
        s.take,
        s.isDistinct,
        s.reverse)
      new ProjectionElement(
        newSel,
        projection.projector,
        projection.key,
        projection.keyPolicy,
        entitledOnly = projection.entitledOnly)
    }
  }

  override protected def bindAggregate(func: FuncElement, mc: MethodCallee): RelationElement = {
    def canBeAggregate(c: ColumnElement, shapeType: TypeInfo[_]): Boolean = {
      c.columnInfo match {
        case i: IndexColumnInfo if i.columnType == ColumnType.Index =>
          i.index.storableClass == shapeType.clazz
        case _ =>
          false
      }
    }
    // it must be count
    val src = func.instance
    val source = super.visitElement(src)
    val projection = convertToSequence(source)
    if ((projection eq null) || (projection.select.take ne null) || (func ne root)) {
      updateFuncCall(func, source, func.arguments)
    } else {
      val shouldContinue = projection.select.where match {
        case null => true
        case BinaryExpressionElement(EQ, c: ColumnElement, _, _) =>
          canBeAggregate(c, projection.rowTypeInfo)
        case BinaryExpressionElement(EQ, _, c: ColumnElement, _) =>
          canBeAggregate(c, projection.rowTypeInfo)
        case _ => false
      }
      if (shouldContinue) {
        val s = projection.select
        val aggElem = new AggregateElement(mc.resType, mc.method.name, Nil, false)
        val columns = List(ColumnDeclaration("", aggElem))
        val newSel = new SelectElement(s.alias, columns, s.from, s.where, null, null, null, null, false, false)
        val aggregator = Aggregator.getHeadAggregator(mc.resType)
        new ProjectionElement(
          newSel,
          new ColumnElement(mc.resType, s.alias, "", ColumnInfo.Calculated),
          NoKey,
          projection.keyPolicy,
          aggregator,
          entitledOnly = projection.entitledOnly)
      } else {
        updateFuncCall(func, source, func.arguments)
      }
    }
  }

  override protected def bindTake(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val projection = convertToSequence(source)
    if (
      (projection eq null) || (projection.select.orderBy eq null) || projection.select.orderBy.isEmpty || (projection.select.take ne null)
    ) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val (skipArg :: limitArg :: _) = others
      (skipArg.arg, limitArg.arg) match {
        case (ConstValueElement(0, _), take @ ConstValueElement(limit: Int, _)) =>
          val s = projection.select
          val newSel = new SelectElement(
            s.alias,
            s.columns,
            s.from,
            s.where,
            s.orderBy,
            s.groupBy,
            s.skip,
            take,
            s.isDistinct,
            s.reverse)
          new ProjectionElement(
            newSel,
            projection.projector,
            projection.key,
            projection.keyPolicy,
            entitledOnly = projection.entitledOnly)

        case (ConstValueElement(-1, _), take @ ConstValueElement(limit: Int, _)) =>
          val s = projection.select
          val newSel = new SelectElement(
            s.alias,
            s.columns,
            s.from,
            s.where,
            s.orderBy,
            s.groupBy,
            s.skip,
            take,
            s.isDistinct,
            !s.reverse)
          new ProjectionElement(
            newSel,
            projection.projector,
            projection.key,
            projection.keyPolicy,
            entitledOnly = projection.entitledOnly)

        case _ => replaceArgs(method, others, source)
      }
    }
  }

  private def onWhereFullyBound(
      serverWhere: RelationElement,
      funcOpt: Option[ScalaLambdaCallee[_, _]],
      paramType: TypeInfo[_]): Unit = {
    val argList = List(new Argument(paramType))
    funcOpt
      .map(f =>
        new FuncElement(
          f.asInstanceOf[ScalaLambdaCallee[Any, Any]].copy(lambdaFunc = None, arguments = argList),
          Nil,
          null))
      .foreach(f => {
        Query.flattenBOOLANDConditions(serverWhere).foreach(c => conditionToFuncElement.put(c, f))
      })
  }

  override protected def bindWhere(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val (projection, viaUntype) = convertToSequenceForWhere(source)
    val result = if ((projection eq null) || (projection.select.take ne null)) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val (lambdaOpt, funcOpt) = others.head.arg match {
        case FuncElement(c: ScalaLambdaCallee[_, _], _, _) => (c.lambdaElement, Some(c))
        case l: LambdaElement                              => (Some(l), None)
        case _                                             => (None, None)
      }
      val proj = lambdaOpt.flatMap { case LambdaElement(_, body, List(param)) =>
        val s = projection.select
        val alias = s.from.asInstanceOf[AliasedElement].alias
        val projector = if (viaUntype) new DynamicObjectElement(projection.projector) else projection.projector
        var serverWhere: RelationElement = null
        var clientWhere: RelationElement = null
        for (cond <- Query.flattenBOOLANDConditions(body)) {
          map.put(param, projector)
          val where = AliasReplacer.replace(visitElement(cond), s.alias, alias)
          if (!language.canBeWhere(where))
            clientWhere = if (clientWhere eq null) cond else ElementFactory.andAlso(clientWhere, cond)
          else serverWhere = if (serverWhere eq null) where else ElementFactory.andAlso(serverWhere, where)
        }
        if (serverWhere eq null) None
        else {
          val newWhere = if (s.where eq null) serverWhere else ElementFactory.andAlso(s.where, serverWhere)
          val newSel = new SelectElement(
            s.alias,
            s.columns,
            s.from,
            newWhere,
            s.orderBy,
            s.groupBy,
            s.skip,
            s.take,
            s.isDistinct,
            s.reverse)
          val pe = new ProjectionElement(
            newSel,
            projection.projector,
            projection.key,
            projection.keyPolicy,
            entitledOnly = projection.entitledOnly)
          val result = ElementReplacer.replace(source, List(projection), List(pe))
          if (clientWhere eq null) {
            onWhereFullyBound(serverWhere, funcOpt, param.rowTypeInfo)
            Some(result)
          } else {
            val predicate = funcOpt match {
              case Some(c) => new FuncElement(c.copy(lambdaFunc = None), Nil, null)
              case _       => ElementFactory.lambda(clientWhere, List(param))
            }
            Some(replaceArgs(method, MethodArg("p", predicate) :: others.tail, result))
          }
        }
      }
      proj.getOrElse(if (source eq s.param) method else replaceArgs(method, others, source))
    }
    if (PriqlSettings.enableTableScanCheck)
      setPermitTableScanFlag(result, false)
    result
  }

  override protected def bindPermitTableScan(method: MethodElement): RelationElement = {
    val result = visitSource(method.methodArgs.head.param)
    if (PriqlSettings.enableTableScanCheck)
      setPermitTableScanFlag(result, true)
    result
  }

  override protected def bindContains(binary: BinaryExpressionElement): RelationElement = {
    binary.right match {
      case ConstValueElement(i: Iterable[_], _) =>
        if (i.isEmpty) ElementFactory.constant(false)
        else {
          val l = visitElement(binary.left)
          new ContainsElement(l, Right(i.iterator.map(v => ElementFactory.constant(v, l.rowTypeInfo)).toList))
        }
      case _ =>
        // we should rewrite it back to contains call, so it could be filtered
        val desc = new RuntimeMethodDescriptor(binary.right.rowTypeInfo, "contains", TypeInfo.BOOLEAN)
        visitElement(ElementFactory.call(binary.right, desc, binary.left :: Nil))
    }
  }

  private def visitIntoMethod(method: MethodElement): RelationElement = {
    val newArgs = visitArgList(method.methodArgs)
    if (newArgs ne method.methodArgs) method.replaceArgs(newArgs) else method
  }

  private def setPermitTableScanFlag(e: RelationElement, value: Boolean): Unit = {
    new PermitTableScanSetter(permitTableScanFlags, value).visitElement(e)
  }

  /**
   * Optimize DAL conditions:
   *   1. when there are unique/key conditions, only execute them on the server side 2. otherwise run index conditions
   *      on the server side 3. when sortByTT...take... is used, run all conditions on the server side
   */
  protected class ConditionOptimizer extends DbQueryTreeVisitor {
    private[this] var dynamicConditions: List[FuncElement] = Nil

    def optimize(e: RelationElement) = {
      visitElement(e)
    }

    private def rewriteCondition(cond: RelationElement): RelationElement = {
      cond match {
        case BinaryExpressionElement(EQ, c: ColumnElement, v: ConstValueElement, _) =>
          cond
        case BinaryExpressionElement(EQ, v: ConstValueElement, c: ColumnElement, _) =>
          ElementFactory.equal(c, v)
        case c: ContainsElement if c.element.isInstanceOf[ColumnElement] =>
          cond
        case c: ColumnElement =>
          ElementFactory.equal(c, ElementFactory.constant(true))
        case FuncElement(_, List(v: ConstValueElement), c: ColumnElement) =>
          ElementFactory.equal(c, v)
        case _ =>
          throw new RelationalUnsupportedException(s"Unsupported condition pattern: $cond")
      }
    }

    override def handleMethod(method: MethodElement): RelationElement = {
      val result = super.handleMethod(method)
      if (method.methodCode == QueryMethod.UNTYPE && dynamicConditions.nonEmpty) {
        val conditions = dynamicConditions
        dynamicConditions = Nil
        conditions.foldLeft(result) { (m, f) =>
          new MethodElement(QueryMethod.WHERE, List(MethodArg("src", m), MethodArg("p", f)), m.rowTypeInfo, NoKey)
        }
      } else result
    }

    override def handleProjection(proj: ProjectionElement): RelationElement = {
      proj.projector match {
        case DynamicObjectElement(projector) if proj.aggregator eq null =>
          val newProj: RelationElement =
            new ProjectionElement(proj.select, projector, NoKey, proj.keyPolicy, entitledOnly = proj.entitledOnly)
          visitElement(
            new MethodElement(QueryMethod.UNTYPE, MethodArg("src", newProj) :: Nil, proj.rowTypeInfo, proj.key))
        case _ =>
          optimizeProjection(proj)
      }
    }

    private def optimizeProjection(proj: ProjectionElement): RelationElement = {
      // we only optimize the top projection
      val map = new util.HashMap[ColumnType, mutable.ListBuffer[RelationElement]]
      map.put(ColumnType.Key, mutable.ListBuffer.empty)
      map.put(ColumnType.UniqueIndex, mutable.ListBuffer.empty)
      map.put(ColumnType.Index, mutable.ListBuffer.empty)
      var alwaysFalse = false

      // "canBeWhere" guarantee that the conditions here are valid
      Query
        .flattenBOOLANDConditions(proj.select.where)
        .foreach(ex =>
          ex match {
            case BinaryExpressionElement(EQ, c: ColumnElement, _: ConstValueElement, _) =>
              map.get(c.columnInfo.columnType).append(ex)
            case BinaryExpressionElement(EQ, _: ConstValueElement, c: ColumnElement, _) =>
              map.get(c.columnInfo.columnType).append(ex)
            case c: ContainsElement if c.element.isInstanceOf[ColumnElement] =>
              map.get(c.element.asInstanceOf[ColumnElement].columnInfo.columnType).append(ex)
            case c: ColumnElement =>
              map.get(c.columnInfo.columnType).append(ex)
            case ConstValueElement(v: Boolean, _) =>
              alwaysFalse |= !v
            case FuncElement(_, List(_: ConstValueElement), c: ColumnElement) =>
              map.get(c.columnInfo.columnType).append(ex)
            case _ =>
              throw new RelationalUnsupportedException(s"Unsupported condition pattern: $ex")
          })

      if (alwaysFalse) {
        ScalaTypeMultiRelation(Vector.empty, proj.key, MethodPosition.unknown, proj.rowTypeInfo, false, proj.keyPolicy)
      } else if ((proj.select.take ne null) || (proj.aggregator ne null)) {
        val serverConds =
          map.get(ColumnType.Key).result() ++ map.get(ColumnType.UniqueIndex) ++ map.get(ColumnType.Index)
        val newWhere =
          serverConds.map(rewriteCondition _).reduceOption((e1, e2) => ElementFactory.andAlso(e1, e2)).getOrElse(null)
        val s = proj.select
        val newSel = new SelectElement(
          s.alias,
          s.columns,
          s.from,
          newWhere,
          s.orderBy,
          s.groupBy,
          s.skip,
          s.take,
          s.isDistinct,
          s.reverse)
        val newProj = new ProjectionElement(
          newSel,
          proj.projector,
          proj.key,
          proj.keyPolicy,
          proj.aggregator,
          entitledOnly = proj.entitledOnly)
        if (!newProj.select.reverse) newProj
        else {
          new MethodElement(DALQueryMethod.Reverse, List(MethodArg("src", newProj)), newProj.rowTypeInfo, NoKey)
        }
      } else {
        val uniqueConds = map.get(ColumnType.Key).result() ++ map.get(ColumnType.UniqueIndex)
        val indexConds = map.get(ColumnType.Index).result()
        val (serverConds, clientConds) = if (uniqueConds.nonEmpty) (uniqueConds, indexConds) else (indexConds, Nil)
        val newWhere =
          serverConds.map(rewriteCondition _).reduceOption((e1, e2) => ElementFactory.andAlso(e1, e2)).getOrElse(null)
        val s = proj.select
        val newSel = new SelectElement(
          s.alias,
          s.columns,
          s.from,
          newWhere,
          Nil,
          s.groupBy,
          s.skip,
          s.take,
          s.isDistinct,
          s.reverse)
        val newProj: RelationElement =
          new ProjectionElement(newSel, proj.projector, proj.key, proj.keyPolicy, entitledOnly = proj.entitledOnly)

        val withWhere =
          if (clientConds.isEmpty) newProj
          else {
            val (dynamicConds, otherConds) = clientConds
              .flatMap(c => conditionToFuncElement.get(c))
              .distinct
              .partition(f => f.callee.arguments(0).argType <:< classOf[DynamicObject])
            dynamicConditions = dynamicConds
            otherConds.foldLeft(newProj) { (m, f) =>
              new MethodElement(QueryMethod.WHERE, List(MethodArg("src", m), MethodArg("p", f)), m.rowTypeInfo, NoKey)
            }
          }
        // attach arrange
        val arranged =
          new MethodElement(QueryMethod.ARRANGE, List(MethodArg("src", withWhere)), withWhere.rowTypeInfo, arrangeKey)

        proj.select.orderBy match {
          case null | Nil =>
            arranged
          case List(OrderDeclaration(d, c: ColumnElement)) if c.name == DALProvider.StorageTxTime =>
            val srcArg = MethodArg[RelationElement]("src", arranged)
            val orderingArg =
              MethodArg[RelationElement]("ordering", ElementFactory.constant(d == SortDirection.Ascending))
            new MethodElement(DALQueryMethod.SortByTT, List(srcArg, orderingArg), arranged.rowTypeInfo, NoKey)
          case _ =>
            throw new RelationalUnsupportedException(s"Unsupported sortBy list: ${proj.select.orderBy}")
        }
      }
    }
  }
}

object DALBinder extends QueryBinder {
  def bind(mapper: QueryMapper, e: RelationElement): RelationElement = {
    new DALBinder(mapper, e).bind(e)
  }

  override def bindMember(source: RelationElement, member: MemberInfo): RelationElement = {
    source match {
      case e: DALHeapEntityElement =>
        e.memberNames
          .zip(e.members)
          .collectFirst {
            case (name, e) if name == member.name => e
          }
          .getOrElse(makeMemberAccess(source, member))
      case _ => super.bindMember(source, member)
    }
  }

  private val arrangeKey = RawReferenceKey[Entity]((t: Entity) => t.dal$entityRef)

  private class PermitTableScanSetter(flags: mutable.HashMap[TableElement, Boolean], value: Boolean)
      extends DbQueryTreeVisitor {
    override protected def handleTable(table: TableElement): RelationElement = {
      flags.put(table, value)
      table
    }

    override protected def handleMethod(m: MethodElement): RelationElement = {
      import QueryMethod._

      if (value) super.handleMethod(m)
      else
        m.methodCode match {
          case INNER_JOIN | LEFT_OUTER_JOIN | RIGHT_OUTER_JOIN | FULL_OUTER_JOIN | NATURAL_INNER_JOIN |
              NATURAL_FULL_OUTER_JOIN | NATURAL_LEFT_OUTER_JOIN | NATURAL_RIGHT_OUTER_JOIN | UNION | MERGE =>
            m
          case _ => super.handleMethod(m)
        }
    }
  }

  private[dal] class ProjectionLocator extends DbQueryTreeVisitor {
    private[this] var projs: List[ProjectionElement] = Nil

    def locate(e: RelationElement): List[ProjectionElement] = {
      visitElement(e)
      projs
    }

    override def handleProjection(proj: ProjectionElement): RelationElement = {
      projs = proj :: projs
      proj
    }
  }

}
