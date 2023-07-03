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
package optimus.platform.relational.inmemory

import optimus.platform._
import optimus.platform.relational.tree._
import optimus.platform.relational.RelationalException
import optimus.platform.relational.aggregation.Aggregator

import scala.collection.mutable.ListBuffer

class QueryExplainGenerator extends QueryTreeVisitor {
  var levelId = 0
  val queryExplainList = new ListBuffer[QueryExplainItem]

  protected override def handleQuerySrc(element: ProviderRelation): RelationElement = {
    element.fillQueryExplainItem(levelId + 1, queryExplainList)
    element
  }

  protected override def handleMethod(method: MethodElement): RelationElement = {
    levelId = levelId + 1

    method.methodCode match {
      case QueryMethod.WHERE            => explainWhere(method)
      case QueryMethod.OUTPUT           => explainTransformMethod(method)
      case QueryMethod.FLATMAP          => explainTransformMethod(method)
      case QueryMethod.GROUP_BY         => explainGroupBy(method)
      case QueryMethod.GROUP_BY_TYPED   => explainGroupBy(method)
      case QueryMethod.GROUP_MAP_VALUES => explainGroupBy(method)
      case QueryMethod.EXTEND_TYPED     => explainExtendTyped(method)
      case QueryMethod.REPLACE          => explainTransformMethod(method)
      case QueryMethod.SORT             => explainSort(method)
      case QueryMethod.ARRANGE          => explainArrange(method)
      case QueryMethod.UNTYPE           => explainTransformMethod(method)
      case QueryMethod.SHAPE            => explainTransformMethod(method)
      case QueryMethod.EXTEND           => explainExtend(method)
      case QueryMethod.MERGE            => explainMerge(method)
      case QueryMethod.UNION            => explainUnion(method)
      case QueryMethod.DIFFERENCE       => explainDifference(method)
      case QueryMethod.TAKE             => explainTake(method)
      case QueryMethod.INNER_JOIN | QueryMethod.LEFT_OUTER_JOIN | QueryMethod.RIGHT_OUTER_JOIN |
          QueryMethod.FULL_OUTER_JOIN | QueryMethod.NATURAL_FULL_OUTER_JOIN | QueryMethod.NATURAL_INNER_JOIN |
          QueryMethod.NATURAL_LEFT_OUTER_JOIN | QueryMethod.NATURAL_RIGHT_OUTER_JOIN =>
        explainJoin(method)
      case QueryMethod.AGGREGATE_BY          => explainAggregateBy(method)
      case QueryMethod.AGGREGATE_BY_IMPLICIT => explainAggregateBy(method)
      case QueryMethod.PERMIT_TABLE_SCAN     => explainPermitTableScan(method)
      case _                                 => explainMethod(method)
    }
    method
  }

  protected def explainMethod(m: MethodElement) = {
    queryExplainList += new QueryExplainItem(levelId, s"MethodElement(${m.methodCode})", "-", "custom operator", 0)
    val curLevel = levelId
    m.methodArgs.foreach(_.param match {
      case m: MultiRelationElement =>
        visitElement(m)
        levelId = curLevel
      case _ =>
    })
  }

  protected def explainPermitTableScan(m: MethodElement) = {
    val (src :: _) = m.methodArgs
    queryExplainList += new QueryExplainItem(levelId, s"MethodElement(${m.methodCode})", "-", "permit table scan", 0)
    visitElement(src.param)
  }

  protected def explainWhere(m: MethodElement) = {
    val (src :: _) = m.methodArgs
    queryExplainList += new QueryExplainItem(levelId, s"MethodElement(${m.methodCode})", "-", "filter using lambda", 0)
    visitElement(src.param)
  }

  protected def explainTransformMethod(m: MethodElement) = {
    val (src :: _) = m.methodArgs
    queryExplainList += new QueryExplainItem(
      levelId,
      s"MethodElement(${m.methodCode})",
      s"${m.projectedType().name}",
      s"Key: ${m.key}",
      0)
    visitElement(src.param)
  }

  protected def explainGroupBy(m: MethodElement) = {
    val (src :: _) = m.methodArgs
    queryExplainList += new QueryExplainItem(
      levelId,
      s"MethodElement(${m.methodCode})",
      s"${m.projectedType().name}",
      s"${m.methodCode}",
      0)
    visitElement(src.param)
  }

  protected def explainSort(m: MethodElement) = {
    val (src :: f :: order :: _) = m.methodArgs
    (f.param, order.param) match {
      case (FuncElement(c: ScalaLambdaCallee[_, _], _, _), ConstValueElement(ord: Ordering[_], _)) =>
        queryExplainList += new QueryExplainItem(
          levelId,
          s"MethodElement(${m.methodCode})",
          "-",
          s"Sort ordering: $ord",
          0)
      case x => throw new IllegalArgumentException(s"unexpected pair $x")
    }
    visitElement(src.param)
  }

  protected def explainArrange(m: MethodElement): RelationElement = {
    val (src :: _) = m.methodArgs
    queryExplainList += new QueryExplainItem(
      levelId,
      s"MethodElement(${m.methodCode})",
      "-",
      "Arrange to guarantee the absolute order",
      0)
    visitElement(src.param)
  }

  protected def explainAggregateBy(m: MethodElement): RelationElement = {
    val (src :: others) = m.methodArgs
    val (ConstValueElement(keyType: TypeInfo[_], _) :: ConstValueElement(
      valueType: TypeInfo[_],
      _) :: ConstValueElement(aggregateType: TypeInfo[_], _) :: _) = others.map(_.arg)

    val objectName = s"keyType: $keyType; valueType: $valueType; aggregateType: $aggregateType"
    queryExplainList += new QueryExplainItem(
      levelId,
      s"MethodElement(${m.methodCode})",
      objectName,
      s"Key: ${m.key}",
      0)
    visitElement(src.param)
  }

  protected def explainTake(m: MethodElement): RelationElement = {
    val (src :: o :: n :: _) = m.methodArgs
    val (offset, numRows) = (o.param, n.param) match {
      case (ConstValueElement(o: Int, _), ConstValueElement(r: Int, _)) => (o, r)
      case _ => throw new RelationalException("TAKE should have two parameter: offset and value which are both Int")
    }
    val detail = s"offset: $offset; numRows: $numRows"
    queryExplainList += new QueryExplainItem(levelId, s"MethodElement(${m.methodCode})", "-", detail, 0)
    visitElement(src.param)
  }

  protected def explainExtend(m: MethodElement): RelationElement = {
    val (src :: field :: _) = m.methodArgs
    field.param match {
      case ConstValueElement(fieldName: String, _) =>
        queryExplainList += new QueryExplainItem(
          levelId,
          s"MethodElement(${m.methodCode})",
          s"${m.projectedType().name} extends fieldName($fieldName)",
          "Create a new (java/scala) dynamic object",
          0)
    }
    visitElement(src.param)
  }

  protected def explainExtendTyped(m: MethodElement): RelationElement = {
    val (src :: _) = m.methodArgs
    queryExplainList += new QueryExplainItem(
      levelId,
      s"MethodElement(${m.methodCode})",
      s"${m.projectedType().name}",
      "proxy base and extension classes",
      0)
    visitElement(src.param)
  }

  protected def explainMerge(m: MethodElement) = {
    val src :: ConstValueElement(ag: Aggregator[_], _) :: others = m.methodArgs.map(_.param)
    queryExplainList += new QueryExplainItem(
      levelId,
      s"MethodElement(${m.methodCode})",
      "-",
      s"Key: ${m.key}, ValueType: ${ag.resultType.name}",
      0)
    val curLevel = levelId
    (src :: others).foreach(arg => {
      visitElement(arg)
      levelId = curLevel
    })
  }

  protected def explainUnion(m: MethodElement) = {
    queryExplainList += new QueryExplainItem(levelId, s"MethodElement(${m.methodCode})", "-", s"Key: ${m.key}", 0)
    val curLevel = levelId
    m.methodArgs.foreach(arg => {
      visitElement(arg.param)
      levelId = curLevel
    })
  }

  protected def explainJoin(m: MethodElement) = {
    val (src1 :: src2 :: _) = m.methodArgs
    queryExplainList += new QueryExplainItem(
      levelId,
      s"MethodElement(${m.methodCode})",
      s"${m.projectedType().name}}",
      s"Nested Loop Join (m*n). Key: ${m.key}",
      0)
    val curLevel = levelId
    visitElement(src1.param)
    levelId = curLevel
    visitElement(src2.param)
  }

  protected def explainDifference(m: MethodElement) = {
    val (src :: other :: _) = m.methodArgs
    queryExplainList += new QueryExplainItem(levelId, s"MethodElement(${m.methodCode})", "-", s"Key: ${m.key}", 0)
    val curLevel = levelId
    visitElement(src.param)
    levelId = curLevel
    visitElement(other.param)
  }
}

object QueryExplainGenerator {
  def generate(e: RelationElement): String = {
    val generator = new QueryExplainGenerator()
    generator.visitElement(e)
    e.generateQueryExplain(generator.queryExplainList)
  }
}
