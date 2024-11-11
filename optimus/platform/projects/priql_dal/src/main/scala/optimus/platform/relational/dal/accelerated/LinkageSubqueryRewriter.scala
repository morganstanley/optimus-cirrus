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

import optimus.platform.dsi.expressions.ConvertOps
import optimus.platform.relational.RelationalException
import optimus.platform.relational.dal.core.SpecialElementRewriter
import optimus.platform.relational.data.DbQueryTreeReducerBase._
import optimus.platform.relational.data.FieldReader
import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.data.mapping.QueryMapping
import optimus.platform.relational.data.translation.ColumnProjector
import optimus.platform.relational.data.translation.SingletonProjectionRewriter.ColumnMapper
import optimus.platform.relational.data.translation.UnusedColumnRemover
import optimus.platform.relational.data.tree.ColumnInfo.ColumnInfoImpl
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._

// This class is to rewrite Projection with linkage collection type in projector to Scalar subquery in Select.
class LinkageSubqueryRewriter private (mapping: QueryMapping, language: QueryLanguage) extends DbQueryTreeVisitor {
  // This is to mark if the projection is in top level.
  private var isTopLevel = true
  // The currentSelect may be rewritten to select aggregation of c2p (child-to-parent) field.
  private var currentSelect: SelectElement = null

  protected override def handleProjection(proj: ProjectionElement): RelationElement = {
    if (isTopLevel) {
      isTopLevel = false
      currentSelect = proj.select
      val projector = visitElement(proj.projector)
      updateProjection(proj, currentSelect, projector, proj.aggregator)
    } else {
      proj.viaCollection match {
        case Some(linkageCls) =>
          require(proj.aggregator == null)
          val newProj =
            UnusedColumnRemover.remove(mapping, SpecialElementRewriter.rewrite(proj)).asInstanceOf[ProjectionElement]
          // Rewrite currentSelect to select tuple of selected columns.
          val newAlias = new TableAlias
          currentSelect = addRedundantSelect(currentSelect, newAlias)
          val source = ColumnMapper.map(newProj.select, newAlias, currentSelect.alias).asInstanceOf[SelectElement]
          val (aggregatedColumn, columnName) = getAggregatedColumn(source.columns)
          val aggSource = aggregateSelect(source, aggregatedColumn, columnName)
          val pe = new ScalarElement(TypeInfo(linkageCls, aggregatedColumn.rowTypeInfo), aggSource)
          val pc = ColumnProjector.projectColumns(
            language,
            pe,
            currentSelect.columns,
            currentSelect.alias,
            newAlias,
            newProj.select.alias)
          currentSelect = new SelectElement(
            currentSelect.alias,
            pc.columns,
            currentSelect.from,
            currentSelect.where,
            currentSelect.orderBy,
            currentSelect.groupBy,
            currentSelect.skip,
            currentSelect.take,
            currentSelect.isDistinct,
            currentSelect.reverse
          )

          (newProj.projector, pc.projector) match {
            case (_: ColumnElement, _) => pc.projector
            case (de: DALHeapEntityElement, c: ColumnElement) =>
              new ColumnElement(TypeInfo(linkageCls, de.rowTypeInfo), c.alias, c.name, c.columnInfo)
            case (_, aggColumn: ColumnElement) =>
              // Rewrite projector to 'c.map(t => f(t(0), t(1)))' or 'c.apar.map(t => nf(t(0), t(1)))'
              val aggColumnUnpickler =
                PicklerSelector.getSeqUnpickler(
                  new AggColumnUnpickler(source.columns.length),
                  aggColumn.rowTypeInfo.clazz)
              val colInfo = ColumnInfoImpl(ColumnType.Calculated, Option(aggColumnUnpickler))
              val newAggColumn = new ColumnElement(aggColumn.rowTypeInfo, aggColumn.alias, aggColumn.name, colInfo)

              val param = ElementFactory.parameter("t", FieldReader.TYPE)
              val body = new ProjectorRewriter(param, source.columns).visitElement(newProj.projector)
              val lambda = ElementFactory.lambda(body, Seq(param))

              generateMapFunc(lambda, newAggColumn, TypeInfo(linkageCls, newProj.rowTypeInfo))

            case (_, e) => throw new RelationalException(s"Unexpected aggregated column: $e")
          }

        case _ =>
          val saveTop = isTopLevel
          val saveSelect = currentSelect
          isTopLevel = true
          currentSelect = null
          val result = handleProjection(proj)
          isTopLevel = saveTop
          currentSelect = saveSelect
          result
      }
    }
  }

  private def addRedundantSelect(sel: SelectElement, newAlias: TableAlias): SelectElement = {
    val newColumns = sel.columns.map { cd =>
      new ColumnDeclaration(
        cd.name,
        new ColumnElement(cd.element.rowTypeInfo, newAlias, cd.name, ColumnInfo.from(cd.element)))
    }
    val newFrom = new SelectElement(
      newAlias,
      sel.columns,
      sel.from,
      sel.where,
      sel.orderBy,
      sel.groupBy,
      sel.skip,
      sel.take,
      sel.isDistinct,
      sel.reverse)
    new SelectElement(sel.alias, newColumns, newFrom, null, null, null, null, null, false, false)
  }

  private def getAggregatedColumn(columns: List[ColumnDeclaration]) = {
    if (columns.length == 1) {
      val head = columns.head
      (head.element, head.name)
    } else {
      val desc = new RuntimeMethodDescriptor(TypeInfo.UNIT, ConvertOps.ToJsonbArray.Name, FieldReader.TYPE)
      val arguments = columns.map { c =>
        if (c.element.rowTypeInfo.clazz == classOf[BigDecimal]) {
          val toTextDesc = new RuntimeMethodDescriptor(TypeInfo.UNIT, ConvertOps.ToText.Name, TypeInfo.STRING)
          ElementFactory.call(null, toTextDesc, c.element :: Nil)
        } else c.element
      }
      val jsonbArray = ElementFactory.call(null, desc, arguments)
      (jsonbArray, "agg")
    }
  }

  // There should only be one aggregatedColumn(default/jsonb column) and we aggregate it as array type.
  private def aggregateSelect(
      select: SelectElement,
      aggregatedColumn: RelationElement,
      columnName: String): SelectElement = {
    val columnAgg = new AggregateElement(
      TypeInfo(classOf[Seq[_]], aggregatedColumn.rowTypeInfo),
      "array_agg",
      List(aggregatedColumn),
      false)

    val columns = List(ColumnDeclaration(columnName, columnAgg))
    new SelectElement(
      select.alias,
      columns,
      select.from,
      select.where,
      select.orderBy,
      select.groupBy,
      select.skip,
      select.take,
      select.isDistinct,
      select.reverse)
  }

  private def generateMapFunc(lambda: LambdaElement, projector: RelationElement, returnType: TypeInfo[_]) = {
    val liftedLambda = liftLambda1(lambda)
    val map = new RuntimeMethodDescriptor(TypeInfos.ProviderCompanion, "mapNodeorFunc", TypeInfo.ITERABLE)
    val func = ElementFactory.call(TypeInfos.Provider, map, List(projector, liftedLambda))

    // convert to set/covariantSet
    ElementFactory.convert(func, returnType)
  }
}

object LinkageSubqueryRewriter {
  def rewrite(m: QueryMapping, l: QueryLanguage, e: RelationElement): RelationElement = {
    new LinkageSubqueryRewriter(m, l).visitElement(e)
  }
}

private class ProjectorRewriter(reader: ParameterElement, aggColumns: List[ColumnDeclaration])
    extends DbQueryTreeVisitor {

  private val nameToIndex: Map[String, Int] = aggColumns.zipWithIndex.iterator.map { case (decl, index) =>
    decl.name -> index
  }.toMap

  protected override def handleColumn(column: ColumnElement): RelationElement = {
    DALAccReducer.getFieldReaderFunction(reader, column.rowTypeInfo, nameToIndex(column.name), column)
  }

  protected override def handleDALHeapEntity(de: DALHeapEntityElement): RelationElement = {
    de.members.head match {
      case c: ColumnElement => DALAccReducer.getFieldReaderFunction(reader, de.rowTypeInfo, nameToIndex(c.name), c)
    }
  }

  protected override def handleEmbeddableCaseClass(caseClass: EmbeddableCaseClassElement): RelationElement = {
    caseClass.members.head match {
      case c: ColumnElement =>
        DALAccReducer.getFieldReaderFunction(reader, caseClass.rowTypeInfo, nameToIndex(c.name), c)
    }
  }

  protected override def handleOptionElement(option: OptionElement): RelationElement = {
    option.element match {
      case de: DALHeapEntityElement =>
        de.members.head match {
          case c: ColumnElement =>
            DALAccReducer.getFieldReaderFunction(reader, option.rowTypeInfo, nameToIndex(c.name), c)
        }
      case c: ColumnElement if !TypeInfo.isOption(c.rowTypeInfo) =>
        DALAccReducer.getFieldReaderFunction(reader, option.rowTypeInfo, nameToIndex(c.name), c)
      case _ => super.handleOptionElement(option)
    }
  }
}
