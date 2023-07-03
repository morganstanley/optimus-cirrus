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

import optimus.graph.Node
import optimus.platform.Query._
import optimus.platform._
import optimus.platform.relational.AsyncValueHolder
import optimus.platform.relational.QueryOps
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.asm.Func
import optimus.platform.relational.tree._

/**
 * Used to construct runtime dynamic Query.
 */
class MultiRelationElementRemover protected () extends QueryTreeVisitor {
  import MultiRelationElementRemover._

  override protected def handleQuerySrc(element: ProviderRelation): RelationElement = {
    // we use NoKey policy here
    val p = ElementFactory.constant(QueryProvider.NoKey, TypeInfos.QueryProvider)
    val shapeType = findShapeType(element)
    val createQuery =
      new RuntimeMethodDescriptor(TypeInfos.QueryProvider, "createQuery", TypeInfos.query(shapeType), List(shapeType))
    val elem = ElementFactory.constant(element, TypeInfos.MultiRelationElement)
    ElementFactory.call(p, createQuery, List(elem))
  }

  override protected def handleConstValue(const: ConstValueElement): RelationElement = {
    const match {
      case ConstValueElement(v: AsyncValueHolder[_], typeInfo) =>
        // rewrite to v.evaluate
        val newConstType = TypeInfo.javaTypeInfo(classOf[AsyncValueHolder[_]]).copy(typeParams = Seq(typeInfo))
        val newConst = ElementFactory.constant(v, newConstType)
        val method = new RuntimeMethodDescriptor(newConstType, "evaluate", typeInfo)
        ElementFactory.call(newConst, method, Nil)
      case _ => super.handleConstValue(const)
    }
  }

  override protected def handleFuncCall(func: FuncElement): RelationElement = {
    func match {
      case FuncElement(m: MethodCallee, args, inst) if isQueryOps(m.method) =>
        m.name match {
          case "sum" | "max" | "min" | "average" | "count" | "variance" | "stddev" | "corr" =>
            handleAggregate(m, args, inst)
          case n if (args.isEmpty) => toMemberRef(m, inst)
          case _                   => super.handleFuncCall(func)
        }
      case _ => super.handleFuncCall(func)
    }
  }

  protected def toMemberRef(m: MethodCallee, inst: RelationElement): RelationElement = {
    val source = visitElement(inst)
    val sourceType = findShapeType(source)
    val queryOps = wrapAsQueryOps(source, sourceType)
    val aggMember = new RuntimeFieldDescriptor(TypeInfos.queryOps(sourceType), m.name, m.resType)
    ElementFactory.makeMemberAccess(visitElement(queryOps), aggMember)
  }

  protected def handleAggregate(
      m: MethodCallee,
      args: List[RelationElement],
      inst: RelationElement): RelationElement = {
    val source = visitElement(inst)
    val sourceType = findShapeType(source)
    val queryOps = wrapAsQueryOps(source, sourceType)

    visitElementList(args) match {
      case lam1 :: lam2 :: agg :: Nil => // for 'corr' only currently
        val lambda1 = toLambda1(lam1, sourceType, m.resType)
        val lambda2 = toLambda1(lam2, sourceType, m.resType)
        val argList = new ExpressionListElement(List(lambda1, lambda2), TypeInfo.ANY)
        val implicitArgList = new ExpressionListElement(List(agg), TypeInfo.ANY)
        val aggMethod = new RuntimeMethodDescriptor(TypeInfos.queryOps(sourceType), m.name, m.resType)
        ElementFactory.call(queryOps, aggMethod, List(argList, implicitArgList))
      case lam :: agg :: Nil =>
        val lambda1 = toLambda1(lam, sourceType, m.resType)
        if (m.name == "count") {
          val count = new RuntimeMethodDescriptor(TypeInfos.queryOps(sourceType), m.name, m.resType)
          ElementFactory.call(queryOps, count, List(lambda1))
        } else {
          val argList = new ExpressionListElement(List(lambda1), TypeInfo.ANY)
          val implicitArgList = new ExpressionListElement(List(agg), TypeInfo.ANY)
          val genericTypeArgs = m.name match {
            case "average" | "variance" | "stddev" => List(sourceType, m.resType)
            case _                                 => List(m.resType)
          }
          val aggMethod =
            new RuntimeMethodDescriptor(TypeInfos.queryOps(sourceType), m.name, m.resType, genericTypeArgs)
          ElementFactory.call(queryOps, aggMethod, List(argList, implicitArgList))
        }
      case argList =>
        if (m.name == "count") {
          val count = new RuntimeFieldDescriptor(TypeInfos.queryOps(sourceType), m.name, m.resType)
          ElementFactory.makeMemberAccess(queryOps, count)
        } else {
          val genericTypeArgs = m.name match {
            case "average" | "variance" | "stddev" => List(sourceType, m.resType)
            case _                                 => Nil
          }
          val aggMethod =
            new RuntimeMethodDescriptor(TypeInfos.queryOps(sourceType), m.name, m.resType, genericTypeArgs)
          ElementFactory.call(queryOps, aggMethod, argList)
        }
    }
  }

  override protected def handleMethod(method: MethodElement): RelationElement = {
    import QueryMethod._

    method.methodCode match {
      case OUTPUT              => handleMap(method)
      case WHERE               => handleWhere(method)
      case FLATMAP             => handleFlatMap(method)
      case GROUP_BY            => handleGroupBy(method)
      case SORT                => handleSortBy(method)
      case TAKE_DISTINCT_BYKEY => handleDistinct(method)
      case x                   => throw new RelationalUnsupportedException(s"Unsupported method code: $x")
    }
  }

  protected def handleMap(method: MethodElement): RelationElement = {
    val (s :: f :: _) = method.methodArgs
    val source = visitElement(s.param)
    val sourceType = findShapeType(source)
    val resultType = findShapeType(method)

    val queryOps = wrapAsQueryOps(source, sourceType)
    val lambda1 = toLambda1(f.param, sourceType, resultType)
    val argList = new ExpressionListElement(List(lambda1), TypeInfo.ANY)
    val uType = ElementFactory.constant(resultType, TypeInfos.typeInfo(resultType))
    val pos = ElementFactory.constant(method.pos, TypeInfos.MethodPosition)
    val implicitArgList = new ExpressionListElement(List(uType, pos))
    val map =
      new RuntimeMethodDescriptor(TypeInfos.queryOps(sourceType), "map", TypeInfos.query(resultType), List(resultType))
    ElementFactory.call(queryOps, map, List(argList, implicitArgList))
  }

  protected def handleWhere(method: MethodElement): RelationElement = {
    val (s :: f :: _) = method.methodArgs
    val source = visitElement(s.param)
    val sourceType = findShapeType(source)

    val queryOps = wrapAsQueryOps(source, sourceType)
    val lambda1 = toLambda1(f.param, sourceType, TypeInfo.BOOLEAN)
    val argList = new ExpressionListElement(List(lambda1), TypeInfo.ANY)
    val pos = ElementFactory.constant(method.pos, TypeInfos.MethodPosition)
    val implicitArgList = new ExpressionListElement(List(pos))
    val filter = new RuntimeMethodDescriptor(TypeInfos.queryOps(sourceType), "filter", source.rowTypeInfo)
    ElementFactory.call(queryOps, filter, List(argList, implicitArgList))
  }

  protected def handleFlatMap(method: MethodElement): RelationElement = {
    val (s :: f :: conv :: Nil) = method.methodArgs
    val source = visitElement(s.param)
    val sourceType = findShapeType(source)
    val resultType = findShapeType(method)

    val queryOps = wrapAsQueryOps(source, sourceType)
    val lambda1 = toLambda1(f.param, sourceType, resultType)
    val argList = new ExpressionListElement(List(lambda1), TypeInfo.ANY)
    val uType = ElementFactory.constant(resultType, TypeInfos.typeInfo(resultType))
    val pos = ElementFactory.constant(method.pos, TypeInfos.MethodPosition)
    val implicitArgList = new ExpressionListElement(List(conv.param, uType, pos))
    val flatMap = new RuntimeMethodDescriptor(TypeInfos.queryOps(sourceType), "flatMap", TypeInfos.query(resultType))
    ElementFactory.call(queryOps, flatMap, List(argList, implicitArgList))
  }

  protected def handleGroupBy(method: MethodElement): RelationElement = {
    val (s :: f :: _) = method.methodArgs
    val source = visitElement(s.param)
    val sourceType = findShapeType(source)
    val resultType = findShapeType(method)
    val keyType = resultType.typeParams(0)

    val queryOps = wrapAsQueryOps(source, sourceType)
    val lambda1 = toLambda1(f.param, sourceType, keyType)
    val argList = new ExpressionListElement(List(lambda1), TypeInfo.ANY)
    val uType = ElementFactory.constant(keyType, TypeInfos.typeInfo(keyType))
    val pos = ElementFactory.constant(method.pos, TypeInfos.MethodPosition)
    val implicitArgList = new ExpressionListElement(List(uType, pos))
    val groupBy = new RuntimeMethodDescriptor(TypeInfos.queryOps(sourceType), "groupBy", TypeInfos.query(resultType))
    ElementFactory.call(queryOps, groupBy, List(argList, implicitArgList))
  }

  protected def handleSortBy(method: MethodElement): RelationElement = {
    val (s :: f :: ordering :: Nil) = method.methodArgs
    val source = visitElement(s.param)
    val sourceType = findShapeType(source)

    val queryOps = wrapAsQueryOps(source, sourceType)
    val lambda1 = toLambda1(f.param, sourceType, TypeInfo.ANY)
    val argList = new ExpressionListElement(List(lambda1), TypeInfo.ANY)
    val uType = ElementFactory.constant(TypeInfo.ANY, TypeInfos.typeInfo(TypeInfo.ANY))
    val pos = ElementFactory.constant(method.pos, TypeInfos.MethodPosition)
    val implicitArgList = new ExpressionListElement(List(ordering.param, uType, pos))
    val sortBy = new RuntimeMethodDescriptor(TypeInfos.queryOps(sourceType), "sortBy", source.rowTypeInfo)
    ElementFactory.call(queryOps, sortBy, List(argList, implicitArgList))
  }

  protected def handleDistinct(method: MethodElement): RelationElement = {
    val (s :: Nil) = method.methodArgs
    val source = visitElement(s.param)
    val sourceType = findShapeType(source)
    val queryOps = wrapAsQueryOps(source, sourceType)
    val pos = ElementFactory.constant(method.pos, TypeInfos.MethodPosition)
    val distinct = new RuntimeMethodDescriptor(TypeInfos.queryOps(sourceType), "distinct", source.rowTypeInfo)
    ElementFactory.call(queryOps, distinct, List(pos))
  }

  private def wrapAsQueryOps(source: RelationElement, sourceType: TypeInfo[_]): RelationElement = {
    val apply = new RuntimeMethodDescriptor(
      TypeInfos.QueryOpsCompanion,
      "apply",
      TypeInfos.queryOps(sourceType),
      List(sourceType))
    ElementFactory.call(null, apply, List(source))
  }

  private def toLambda1(e: RelationElement, sourceType: TypeInfo[_], resultType: TypeInfo[_]): RelationElement = {
    e match {
      case FuncElement(c: ScalaLambdaCallee[_, _], _, _) => toLambda1(c.lambda, sourceType, resultType)
      case l: LambdaElement                              => toLambda1(l, sourceType, resultType)
      case _ => throw new RelationalUnsupportedException(s"Unsupported argument f: ${e}")
    }
  }

  private def toLambda1[T, U](
      f: Either[T => Node[U], T => U],
      sourceType: TypeInfo[_],
      resultType: TypeInfo[_]): RelationElement = {
    val l = f match {
      case Left(nf) => Lambda1(None, Some(nf), None)
      case Right(f) => Lambda1(Some(f), None, None)
    }
    ElementFactory.constant(l, TypeInfos.lambda1(sourceType, resultType))
  }

  private def toLambda1(l: LambdaElement, sourceType: TypeInfo[_], resultType: TypeInfo[_]): RelationElement = {
    val lam = visitElement(l)
    val toLambda1 = new RuntimeMethodDescriptor(
      TypeInfo(Func.getClass),
      "asLambda1",
      TypeInfos.lambda1(sourceType, resultType),
      List(sourceType, resultType))
    ElementFactory.call(null, toLambda1, List(lam))
  }
}

object MultiRelationElementRemover {
  def remove(e: RelationElement): RelationElement = {
    new MultiRelationElementRemover().visitElement(e)
  }

  def isQueryOps(member: MemberDescriptor): Boolean = {
    if (member eq null) false
    else if (member.declaringType <:< classOf[Query[_]]) true
    else false
  }

  object TypeInfos {
    val MultiRelationElement = TypeInfo(classOf[MultiRelationElement])
    val QueryProvider = TypeInfo(classOf[QueryProvider])
    val MethodPosition = TypeInfo(classOf[MethodPosition])
    val QueryOpsCompanion = TypeInfo(QueryOps.getClass)
    def typeInfo(t: TypeInfo[_]): TypeInfo[_] = TypeInfo(classOf[TypeInfo[_]], t)
    def query(t: TypeInfo[_]): TypeInfo[_] = TypeInfo(classOf[Query[_]], t)
    def queryOps(t: TypeInfo[_]): TypeInfo[_] = TypeInfo(classOf[QueryOps[_]], t)
    def lambda1(t1: TypeInfo[_], t2: TypeInfo[_]): TypeInfo[_] = TypeInfo(classOf[Lambda1[_, _]], t1, t2)
  }
}
