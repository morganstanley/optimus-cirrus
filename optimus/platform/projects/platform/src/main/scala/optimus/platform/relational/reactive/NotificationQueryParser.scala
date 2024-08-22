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
package optimus.platform.relational.reactive

import optimus.entity.ClassEntityInfo
import optimus.platform.relational.dal.{DALProvider, EntityMultiRelation}
import optimus.platform.relational.tree.BinaryExpressionType._
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity

import scala.collection.mutable.{HashMap, ListBuffer}

/**
 * Assume the parser will visit the priql elements and collection information for
 *   1. Server side query (init snap shot query - by executeReference) 2. CPS filter query (to subscribe live data) 3.
 *      Range query (get the gap for live data - by rangeQuery) 4. Client operations, which isn't supported by broker
 *      (extract, sortBy, take, ect.)
 */
private class NotificationQueryParser extends QueryTreeVisitor {
  private[this] var _provider: ProviderRelation = _
  private[this] var _entityInfo: ClassEntityInfo = _
  def entityInfo = _entityInfo
  private[this] val _allFilters: ListBuffer[RelationElement] = ListBuffer.empty

  def allFilters(flat: Boolean = true) = {
    def flatten(element: RelationElement): List[RelationElement] = {
      element match {
        case binary: BinaryExpressionElement if binary.op == BOOLAND =>
          flatten(binary.left) ::: flatten(binary.right)
        case _ =>
          element :: Nil
      }
    }

    if (flat)
      _allFilters flatMap flatten
    else
      _allFilters
  }

  def visit(element: RelationElement): RelationElement = {
    require(_entityInfo eq null)
    require(_allFilters.isEmpty)
    val result = visitElement(element)
    result
  }

  override protected def handleMethod(m: MethodElement): RelationElement = {
    m.methodCode match {
      case QueryMethod.WHERE             => parseWhere(m)
      case QueryMethod.OUTPUT            => parseProject(m)
      case QueryMethod.PERMIT_TABLE_SCAN => visitElement(m.methodArgs(0).param)
      case _ => throw new UnsupportedOperation(s"Method is not supported in reactive: ${m.methodCode}")
    }
    m
  }

  override protected def handleQuerySrc(p: ProviderRelation): RelationElement = {
    p match {
      case s: EntityMultiRelation[_] =>
        require(entityInfo eq null, "Only one source is allowed")
        _entityInfo = s.classEntityInfo
        _provider = s
      case d: DALProvider =>
        require(entityInfo eq null, "Only one source is allowed")
        _entityInfo = d.classEntityInfo
        _provider = EntityMultiRelation[Entity](_entityInfo, d.key, d.pos)(d.rowTypeInfo.cast[Entity], d.dalApi)
    }
    p
  }

  protected def parseWhere(m: MethodElement): Unit = {

    // originally NotificationQueryPaser will use its own NotificationRelationElementBuilder to parse expr tree, and "a.name < "abc"" expr tree is
    // "constLiftPriql.argumentString(a.name).<("abc")", so its own NotificationRelationElementBuilder will throws UnsupportedOperation("augmentString")
    // But now we already convert expr tree into LambdaElement during macro, "a.name < "abc"" will be translated to BinaryExpressionElement and will
    // not throw any exception. But TemporalSurfaceMatcher still need to prevent string comparation go to CPS filter as well as "contains"
    // so we use this visitor to recognize this pattern "a.name < "abc"" and still throw exception
    object BackwardCompatibilityChecker extends QueryTreeVisitor {
      def check(element: RelationElement): Unit = visitElement(element)

      override def handleBinaryExpression(element: BinaryExpressionElement) = {
        element match {
          case BinaryExpressionElement(LE | GE | LT | GT, m: MemberElement, _, _)
              if (m.rowTypeInfo == typeInfo[String]) =>
            throw new UnsupportedOperation("augmentString")
          case _ => super.handleBinaryExpression(element)
        }
      }
    }

    val (s :: others) = m.methodArgs
    val source = super.visitElement(s.param)

    val lambdaOpt = others.head.arg match {
      case FuncElement(callee: ScalaLambdaCallee[_, _], _, _) =>
        callee.lambdaElementEvaluated
      case _ =>
        throw new UnsupportedOperation(
          s"second element in Where MethodElement methodArgs is not ExpressionListElement or FuncElement but $others.head.arg")
    }

    lambdaOpt.map { case lambda @ LambdaElement(_, body, _) =>
      BackwardCompatibilityChecker.check(body)
      val out = new ColumnReferenceReplacer(_provider).visitElement(lambda)
      _allFilters += out.asInstanceOf[LambdaElement].body
      body
    } getOrElse (throw new UnsupportedOperation(s"Cannot parse function"))
  }

  protected def parseProject(m: MethodElement): Unit = {
    val (s :: others) = m.methodArgs
    val source = super.visitElement(s.param)
    val p = others.head.arg match {
      case FuncElement(c: ScalaLambdaCallee[_, _], _, _) =>
        c.lambdaElementEvaluated.flatMap { case LambdaElement(_, body, _) =>
          if (body.elementType == ElementType.Parameter) Some(body) else None
        }
      case _ => None
    }
    p.getOrElse(throw new UnsupportedOperation("Projection"))
  }

  class ColumnReferenceReplacer(resolvers: ProviderRelation*) extends QueryTreeVisitor {
    override protected def handleLambda(lambda: LambdaElement): RelationElement = {
      if (lambda.parameters.size != resolvers.size)
        throw new RuntimeException("Size mismatch!")
      for ((p, rp) <- lambda.parameters.zip(resolvers))
        parameter.put(p, rp)
      val newBody = super.visitElement(lambda.body)
      ElementFactory.lambda(newBody, lambda.parameters)
    }

    override protected def handleParameter(p: ParameterElement): RelationElement = {
      parameter.getOrElse(p, super.handleParameter(p))
    }

    override protected def handleBinaryExpression(b: BinaryExpressionElement): RelationElement = {
      b match {
        case BinaryExpressionElement(ITEM_IS_IN, l, r, _) if r.rowTypeInfo <:< classOf[Iterable[_]] =>
          val left = visitElement(l)
          val right = visitElement(r)
          val md = new RuntimeMethodDescriptor(right.rowTypeInfo, FunctionNames.IN, TypeInfo.BOOLEAN)
          ElementFactory.call(right, md, List(left))
        case _ =>
          super.handleBinaryExpression(b)
      }
    }

    private val parameter = new HashMap[ParameterElement, ProviderRelation]
  }
}

object NotificationQueryParser {
  def parse(element: RelationElement, flat: Boolean = true) = {
    val parser = new NotificationQueryParser
    parser.visit(element)
    QueryParseResult(parser.entityInfo, parser.allFilters(flat).result())
  }
}

final case class QueryParseResult(val entityInfo: ClassEntityInfo, val allFilters: List[RelationElement])
