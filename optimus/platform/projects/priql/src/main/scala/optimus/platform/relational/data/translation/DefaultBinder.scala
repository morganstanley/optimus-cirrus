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
package optimus.platform.relational.data.translation

import java.util.HashMap
import optimus.platform.DynamicObject
import optimus.platform.NoKey
import optimus.platform.Query
import optimus.platform.relational.EntitledOnlySupport
import optimus.platform.cm.Knowable
import optimus.platform.cm.Known
import optimus.platform.cm.NotApplicable
import optimus.platform.cm.Unknown
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.aggregation.PopulationStandardDeviation
import optimus.platform.relational.aggregation.PopulationVariance
import optimus.platform.relational.data.Aggregator
import optimus.platform.relational.data.DataProvider
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryBinder
import optimus.platform.relational.data.mapping.QueryMapper
import optimus.platform.relational.data.tree._
import optimus.platform.relational.internal.RelationalUtils
import optimus.platform.relational.tree.MethodArgConstants._
import optimus.platform.relational.tree._

/*
 * Converts PriQL query operators into custom DbRelationElements
 */
class DefaultBinder protected (mapper: QueryMapper, root: RelationElement) extends DbQueryTreeVisitor {
  import BinaryExpressionType._
  import DefaultBinder._
  import Query._

  protected val map = new HashMap[ParameterElement, RelationElement]
  protected val groupByMap = new HashMap[RelationElement, GroupByInfo]
  protected val language = mapper.translator.language
  protected val lookup = mapper.translator.entityLookup
  private var innerQueryLevel = 0
  // This flag is to mark if there is a 'distinct/distinctByKey' after 'filter/map/flatMap/untype/sortBy/extendTyped'
  private var hasDistinctByKeyAfter = false

  protected override def handleQuerySrc(provider: ProviderRelation): RelationElement = {
    provider match {
      case d: DataProvider if mapper.mapping.isProviderSupported(d) =>
        val mappingEntity = lookup.getEntity(d.rowTypeInfo)
        val pe = visitSequence(mapper.getQueryElement(mappingEntity, d.key, d.keyPolicy))
        d match {
          case e: EntitledOnlySupport if e.entitledOnly =>
            new ProjectionElement(pe.select, pe.projector, pe.key, pe.keyPolicy, pe.aggregator, pe.viaCollection, true)
          case _ => pe
        }
      case _ => provider
    }
  }

  protected override def handleUnaryExpression(unary: UnaryExpressionElement): RelationElement = {
    val operand = visitElement(unary.element)
    if (unary.op == UnaryExpressionType.CONVERT && TypeInfo.typeEquals(unary.rowTypeInfo, operand.rowTypeInfo)) operand
    else updateUnary(unary, operand)
  }

  protected override def handleFuncCall(func: FuncElement): RelationElement = {
    func.callee match {
      case mc: MethodCallee
          if mc.method.name == "apply" && mc.method.declaringType.name == s"scala.Tuple${func.arguments.size}$$" =>
        new TupleElement(visitElementList(func.arguments))
      case mc: MethodCallee if mc.method.name == "apply" && mc.method.declaringType.clazz == Option.getClass =>
        new OptionElement(visitElement(func.arguments.head))
      case mc: MethodCallee if language.isAggregate(mc.method) =>
        bindAggregate(func, mc)
      case mc: MethodCallee if language.isHeadLast(mc.method) =>
        bindHeadLast(func, mc)
      case mc: MethodCallee
          if mc.method.declaringType <:< classOf[
            DynamicObject] && (mc.method.name == "get" || mc.method.name == "apply") =>
        val inst = visitElement(func.instance)
        val args = visitElementList(func.arguments)
        val resultOpt = (inst, args) match {
          case (d @ DynamicObjectElement(_), ConstValueElement(v: String, _) :: Nil) if v.indexOf('.') < 0 =>
            mapper.binder.bindUntypedMember(d, v, lookup)
          case _ => None
        }
        resultOpt.getOrElse(updateFuncCall(func, inst, args))
      case mc: MethodCallee if mc.method.declaringType <:< classOf[Option[_]] =>
        val inst = visitElement(func.instance)
        val resultOpt = (inst, func.arguments) match {
          case (OptionElement(e), List(l: LambdaElement)) =>
            mc.method.name match {
              case "map" =>
                val cond = optionToCondition(e)
                this.map.put(l.parameters.head, cond.ifFalse)
                val b = visitElement(l.body)
                Some(OptionElement(ElementFactory.condition(cond.test, cond.ifTrue, b, b.rowTypeInfo)))
              case "flatMap" =>
                val cond = optionToCondition(e)
                this.map.put(l.parameters.head, cond.ifFalse)
                val b = visitElement(l.body)
                Some(ElementFactory.condition(cond.test, OptionElement(cond.ifTrue), b, b.rowTypeInfo))
              case "filter" | "withFilter" =>
                val cond = optionToCondition(e)
                this.map.put(l.parameters.head, cond.ifFalse)
                val b = visitElement(l.body)
                val newTest = ElementFactory.orElse(cond.test, invertTest(b))
                Some(OptionElement(ElementFactory.condition(newTest, cond.ifTrue, cond.ifFalse, cond.rowTypeInfo)))
              case "filterNot" =>
                val cond = optionToCondition(e)
                this.map.put(l.parameters.head, cond.ifFalse)
                val b = visitElement(l.body)
                val newTest = ElementFactory.orElse(cond.test, b)
                Some(OptionElement(ElementFactory.condition(newTest, cond.ifTrue, cond.ifFalse, cond.rowTypeInfo)))
              case "forall" =>
                val cond = optionToCondition(e)
                this.map.put(l.parameters.head, cond.ifFalse)
                val b = visitElement(l.body)
                val ifTrue = ElementFactory.constant(true, TypeInfo.BOOLEAN)
                Some(ElementFactory.condition(cond.test, ifTrue, b, TypeInfo.BOOLEAN))
              case "exists" =>
                val cond = optionToCondition(e)
                this.map.put(l.parameters.head, cond.ifFalse)
                val b = visitElement(l.body)
                val ifTrue = ElementFactory.constant(false, TypeInfo.BOOLEAN)
                Some(ElementFactory.condition(cond.test, ifTrue, b, TypeInfo.BOOLEAN))
              case "getOrElse" =>
                val cond = optionToCondition(e)
                Some(ElementFactory.condition(cond.test, visitElement(l.body), cond.ifFalse, cond.rowTypeInfo))
              case _ => None
            }
          case (OptionElement(e), List(c: RelationElement)) =>
            mc.method.name match {
              case "contains" =>
                val cond = optionToCondition(e)
                val ifTrue = ElementFactory.constant(false, TypeInfo.BOOLEAN)
                val ifFalse = ElementFactory.equal(cond.ifFalse, c)
                Some(ElementFactory.condition(cond.test, ifTrue, ifFalse, TypeInfo.BOOLEAN))
              case _ => None
            }
          case _ => None
        }
        resultOpt.getOrElse(updateFuncCall(func, inst, visitElementList(func.arguments)))

      case mc: MethodCallee
          if mc.method.declaringType <:< classOf[Iterable[_]] && !RelationalUtils.isImmutableArrayOfByte(
            mc.method.declaringType) =>
        val inst = visitElement(func.instance)
        val name = mc.method.name
        val resultOpt = (convertToSequence(inst), func.arguments) match {
          case (projection: ProjectionElement, args @ List(ExpressionListElementUnwrap(l: LambdaElement), _*))
              if projection.viaCollection.isDefined =>
            name match {
              case "map" =>
                val ele = new MethodElement(
                  QueryMethod.OUTPUT,
                  List(MethodArg("src", projection), MethodArg("f", l)),
                  l.rowTypeInfo,
                  NoKey,
                  MethodPosition.unknown)
                convertToLinkageProj(visitElement(ele), projection.viaCollection).orElse {
                  Some(updateFuncCall(func, projection, visitElementList(args)))
                }
              case "flatMap" =>
                val ele = new MethodElement(
                  QueryMethod.FLATMAP,
                  List(MethodArg("src", projection), MethodArg("f", l)),
                  l.rowTypeInfo,
                  NoKey,
                  MethodPosition.unknown)
                convertToLinkageProj(visitElement(ele), projection.viaCollection).orElse {
                  Some(updateFuncCall(func, projection, visitElementList(args)))
                }
              case "filter" | "withFilter" =>
                val elem = new MethodElement(
                  QueryMethod.WHERE,
                  List(MethodArg("src", projection), MethodArg("p", l)),
                  l.rowTypeInfo,
                  NoKey,
                  MethodPosition.unknown)
                convertToLinkageProj(visitElement(elem), projection.viaCollection).orElse {
                  Some(updateFuncCall(func, projection, visitElementList(args)))
                }
              case "filterNot" =>
                val p = ElementFactory.lambda(invertTest(l.body), l.parameters)
                val elem = new MethodElement(
                  QueryMethod.WHERE,
                  List(MethodArg("src", projection), MethodArg("p", p)),
                  l.rowTypeInfo,
                  NoKey,
                  MethodPosition.unknown)
                convertToLinkageProj(visitElement(elem), projection.viaCollection).orElse {
                  Some(updateFuncCall(func, projection, visitElementList(args)))
                }
              case "exists" => // expand to filter(...).nonEmpty
                val desc1 = new RuntimeMethodDescriptor(mc.method.declaringType, "filter", mc.method.declaringType)
                val desc2 = new RuntimeMethodDescriptor(mc.method.declaringType, "nonEmpty", TypeInfo.BOOLEAN)
                val rewritten = ElementFactory.call(ElementFactory.call(projection, desc1, args), desc2, Nil)
                val visited = visitElement(rewritten)
                if (visited eq rewritten) None else Some(visited)
              case "forall" => // expand to filterNot(..).isEmpty
                val desc1 = new RuntimeMethodDescriptor(mc.method.declaringType, "filterNot", mc.method.declaringType)
                val desc2 = new RuntimeMethodDescriptor(mc.method.declaringType, "isEmpty", TypeInfo.BOOLEAN)
                val rewritten = ElementFactory.call(ElementFactory.call(projection, desc1, args), desc2, Nil)
                val visited = visitElement(rewritten)
                if (visited eq rewritten) None else Some(visited)
              case _ => None
            }

          case (p: ProjectionElement, List(arg))
              if p.viaCollection.isDefined && (name == "contains") && isSimpleProjector(p.projector) =>
            visitElement(arg) match {
              case e @ (_: ColumnElement | _: DbEntityElement | _: ConstValueElement | _: DALHeapEntityElement) =>
                Some(new ContainsElement(e, Left(new ScalarElement(p.rowTypeInfo, p.select))))
              case _ => None
            }

          case (p: ProjectionElement, _)
              if p.viaCollection.isDefined && (name == "sum" || (name == "min" && !(mc.method.returnType <:< classOf[
                Option[_]])) || name == "max" || name == "size") && isSimpleProjector(p.projector) =>
            val projection = if (name == "min" || name == "max") p else addDistinctToProjection(p, None)
            val newFunc = new FuncElement(func.callee, func.arguments, projection)
            val newCallee = if (name == "size") {
              val method = new RuntimeMethodDescriptor(mc.method.declaringType, "count", mc.method.returnType)
              new MethodCallee(method)
            } else mc
            convertToLinkageProj(bindAggregate(newFunc, newCallee), None)

          case (p: ProjectionElement, Nil) if p.viaCollection.isDefined && (name == "isEmpty" || name == "nonEmpty") =>
            val s = p.select
            // change to "select 1 from ..."
            val decls = new ColumnDeclaration("", ElementFactory.constant(1, TypeInfo.INT)) :: Nil
            val sel =
              new SelectElement(s.alias, decls, s.from, s.where, s.orderBy, s.groupBy, s.skip, s.take, false, false)
            val ex = new ExistsElement(sel)
            Some(
              if (name == "nonEmpty") ex
              else ElementFactory.equal(ex, ElementFactory.constant(false, TypeInfo.BOOLEAN)))

          case _ => None
        }
        resultOpt.getOrElse(updateFuncCall(func, inst, visitElementList(func.arguments)))

      case mc: MethodCallee if mc.method.declaringType <:< classOf[Knowable[_]] =>
        val inst = visitElement(func.instance)
        mc.method.name match {
          case "toOption" =>
            OptionElement(KnowableValueElement(inst))
          case "nonEmpty" | "isDefined" =>
            val kv = KnowableValueElement(inst)
            ElementFactory.notEqual(kv, ElementFactory.constant(null, kv.rowTypeInfo))
          case "isEmpty" =>
            val kv = KnowableValueElement(inst)
            ElementFactory.equal(kv, ElementFactory.constant(null, kv.rowTypeInfo))
          case _ =>
            updateFuncCall(func, inst, visitElementList(func.arguments))
        }

      case _ => super.handleFuncCall(func)
    }
  }

  // Add distinct to p.select and rewrite p.viaLinkageCollection if needed.
  private def addDistinctToProjection(p: ProjectionElement, newViaCollection: Option[Class[_]]) = {
    if (language.shouldBeDistinct(p.viaCollection.get)) {
      val alias = nextAlias()
      val pc = projectColumns(p.projector, alias, p.select.alias)
      val select = new SelectElement(alias, pc.columns, p.select, null, null, null, null, null, true, false)
      val linkage = newViaCollection.orElse(p.viaCollection)
      new ProjectionElement(select, pc.projector, NoKey, p.keyPolicy, null, linkage, p.entitledOnly)
    } else {
      newViaCollection
        .map(c =>
          new ProjectionElement(p.select, p.projector, p.key, p.keyPolicy, p.aggregator, Some(c), p.entitledOnly))
        .getOrElse(p)
    }
  }

  private def invertTest(test: RelationElement): RelationElement = test match {
    case BinaryExpressionElement(EQ, left, ConstValueElement(false, _), _) =>
      left
    case b @ BinaryExpressionElement(EQ | NE, left, right, _) =>
      ElementFactory.makeBinary(BinaryExpressionElement.invert(b.op), left, right)
    case b @ BinaryExpressionElement(LT | GT | LE | GE, left, right, _) =>
      if (
        left.rowTypeInfo == TypeInfo.DOUBLE || left.rowTypeInfo == TypeInfo.FLOAT
        || right.rowTypeInfo == TypeInfo.DOUBLE || right.rowTypeInfo == TypeInfo.FLOAT
      )
        ElementFactory.equal(test, ElementFactory.constant(false)) // deal with NaN
      else
        ElementFactory.makeBinary(BinaryExpressionElement.invert(b.op), left, right)
    case b @ BinaryExpressionElement(BOOLAND | BOOLOR, left, right, _) =>
      ElementFactory.makeBinary(BinaryExpressionElement.invert(b.op), invertTest(left), invertTest(right))
    case _ =>
      ElementFactory.equal(test, ElementFactory.constant(false))
  }

  private def optionToCondition(e: RelationElement): ConditionalElement = {
    // merge the null branch
    def flatten(cond: ConditionalElement) = ConditionalFlattener.flatten(cond).asInstanceOf[ConditionalElement]

    e match {
      case c @ ConditionalElement(_, ConstValueElement(null, _), _, _) => flatten(c)
      case ConditionalElement(test, ifTrue, ifFalse @ ConstValueElement(null, _), returnType) =>
        flatten(ElementFactory.condition(invertTest(test), ifFalse, ifTrue, returnType))
      case OptionElement(x) => // since 'Some(None)' makes no sense
        val cond = optionToCondition(x)
        ElementFactory.condition(cond.test, cond.ifTrue, OptionElement(cond.ifFalse), e.rowTypeInfo)
      case _ =>
        val constNull = ElementFactory.constant(if (e.rowTypeInfo <:< classOf[Option[_]]) None else null, e.rowTypeInfo)
        val test = ElementFactory.equal(e, constNull)
        flatten(ElementFactory.condition(test, ElementFactory.constant(null, TypeInfo.ANY), e, e.rowTypeInfo))
    }
  }

  protected override def handleBinaryExpression(binary: BinaryExpressionElement): RelationElement = {
    import BinaryExpressionType._
    binary.op match {
      case ITEM_IS_IN => bindContains(binary)
      case EQ =>
        val (l, r) = (visitElement(binary.left), visitElement(binary.right)) match {
          case (OptionElement(e1), FuncElement(mc: MethodCallee, List(e2), null))
              if mc.method.name == "apply" && mc.method.declaringType <:< Some.getClass =>
            (e1, e2)
          case (FuncElement(mc: MethodCallee, List(e1), null), OptionElement(e2))
              if mc.method.name == "apply" && mc.method.declaringType <:< Some.getClass =>
            (e1, e2)
          case (c: ColumnElement, FuncElement(mc: MethodCallee, List(e), null))
              if mc.method.name == "apply" && mc.method.declaringType <:< Known.getClass =>
            (KnowableValueElement(c), e)
          case (FuncElement(mc: MethodCallee, List(e), null), c: ColumnElement)
              if mc.method.name == "apply" && mc.method.declaringType <:< Known.getClass =>
            (KnowableValueElement(c), e)
          case (e1, e2) => (e1, e2)
        }
        updateBinary(binary, l, r)
      case _ => super.handleBinaryExpression(binary)
    }
  }

  protected def bindContains(binary: BinaryExpressionElement): RelationElement = {
    val leftType = binary.left.rowTypeInfo
    binary.right match {
      case ConstValueElement(i: Iterable[_], _)
          if !TypeInfo.isCollection(leftType) || RelationalUtils.isImmutableArrayOfByte(leftType) =>
        val values: List[RelationElement] = i.iterator.collect {
          case v if (validateValue(v, leftType)) => ElementFactory.constant(v, leftType)
        }.toList
        // Add Binary(eq, left, null) to result if has none value.
        val isOption = leftType <:< classOf[Option[_]]
        val isKnowable = leftType <:< classOf[Knowable[_]]
        val hasNoneValue = isOption && i.exists(v => v == None)
        val hasUnknown = isKnowable && i.exists(v => v.isInstanceOf[Unknown[_]])
        val hasNotApplicable = isKnowable && i.exists(v => v == NotApplicable)
        if (values.isEmpty) {
          if (hasNoneValue) {
            val left = visitElement(binary.left)
            ElementFactory.equal(left, ElementFactory.constant(None, leftType))
          } else if (isKnowable) {
            val left = visitElement(binary.left)
            if (hasUnknown && hasNotApplicable) {
              val kv = KnowableValueElement(left)
              ElementFactory.equal(kv, ElementFactory.constant(null, kv.rowTypeInfo))
            } else if (hasUnknown)
              ElementFactory.equal(left, ElementFactory.constant(Unknown(), leftType))
            else if (hasNotApplicable)
              ElementFactory.equal(left, ElementFactory.constant(NotApplicable, leftType))
            else
              ElementFactory.constant(false)
          } else
            ElementFactory.constant(false)
        } else {
          val left = visitElement(binary.left)
          if (isOption) {
            val optContains = new ContainsElement(left, Right(values))
            if (hasNoneValue) {
              val isNone = ElementFactory.equal(left, ElementFactory.constant(None, leftType))
              ElementFactory.orElse(optContains, isNone)
            } else optContains
          } else if (isKnowable) {
            val kv = KnowableValueElement(left)
            val knValues = values.collect { case ConstValueElement(k: Known[_], _) =>
              ElementFactory.constant(k.get, kv.rowTypeInfo)
            }
            val knContains = new ContainsElement(kv, Right(knValues))
            val eOpt =
              if (hasUnknown && hasNotApplicable)
                Some(ElementFactory.equal(kv, ElementFactory.constant(null, kv.rowTypeInfo)))
              else if (hasUnknown)
                Some(ElementFactory.equal(left, ElementFactory.constant(Unknown(), leftType)))
              else if (hasNotApplicable)
                Some(ElementFactory.equal(left, ElementFactory.constant(NotApplicable, leftType)))
              else
                None
            eOpt.map(x => ElementFactory.orElse(knContains, x)).getOrElse(knContains)
          } else new ContainsElement(left, Right(values))
        }

      case proj: ProjectionElement
          if proj.viaCollection.isDefined && isSimpleProjector(proj.projector) && proj.aggregator == null =>
        new ContainsElement(visitElement(binary.left), Left(new ScalarElement(proj.rowTypeInfo, proj.select)))

      case _ =>
        // we should rewrite it back to contains call, so it could be filtered
        val desc = new RuntimeMethodDescriptor(binary.right.rowTypeInfo, "contains", TypeInfo.BOOLEAN)
        visitElement(ElementFactory.call(binary.right, desc, binary.left :: Nil))
    }
  }

  private def validateValue(value: Any, typeInfo: TypeInfo[_]): Boolean = {
    val isOption = typeInfo <:< classOf[Option[_]]
    val isKnownable = typeInfo <:< classOf[Knowable[_]]
    value match {
      case None                      => false
      case Some(v)                   => if (isOption) validateValue(v, typeInfo.typeParams.head) else false
      case NotApplicable | Unknown() => false
      case Known(v)                  => isKnownable
      case _                         => if (isOption || isKnownable) false else true
    }
  }

  protected override def handleMethod(method: MethodElement): RelationElement = {
    import QueryMethod._

    method.methodCode match {
      case TAKE_DISTINCT | TAKE_DISTINCT_BYKEY =>
        atInnerDistinctScope(true) {
          bindMethod(method)
        }
      case OUTPUT | WHERE | SORT | FLATMAP | UNTYPE | PERMIT_TABLE_SCAN => bindMethod(method)
      case _ =>
        atInnerDistinctScope(false) {
          bindMethod(method)
        }
    }
  }

  private def bindMethod(method: MethodElement): RelationElement = {
    import QueryMethod._

    method.methodCode match {
      case OUTPUT                                                            => bindSelect(method)
      case WHERE                                                             => bindWhere(method)
      case GROUP_BY                                                          => bindGroupBy(method)
      case INNER_JOIN | FULL_OUTER_JOIN | LEFT_OUTER_JOIN | RIGHT_OUTER_JOIN => bindJoin(method)
      case TAKE                                                              => bindTake(method)
      case SORT                                                              => bindSort(method)
      case FLATMAP                                                           => bindFlatMap(method)
      case GROUP_MAP_VALUES                                                  => bindMapValues(method)
      case TAKE_DISTINCT | TAKE_DISTINCT_BYKEY                               => bindDistinct(method)
      case UNTYPE                                                            => bindUntype(method)
      case PERMIT_TABLE_SCAN                                                 => bindPermitTableScan(method)
      case _                                                                 => super.handleMethod(method)
    }
  }

  private def atInnerDistinctScope(isDistinct: Boolean)(f: => RelationElement): RelationElement = {
    val savedHasDistinct = hasDistinctByKeyAfter
    hasDistinctByKeyAfter = isDistinct
    val e = f
    hasDistinctByKeyAfter = savedHasDistinct
    e
  }

  protected override def handleParameter(param: ParameterElement): RelationElement = {
    val e = map.get(param)
    if (e ne null) e else param
  }

  protected override def handleMemberRef(member: MemberElement): RelationElement = {
    val instance = visitElement(member.instanceProvider)
    mapper.binder
      .bindMember(instance, MemberInfo(member.member.declaringType, member.memberName, member.projectedType())) match {
      case MemberElement(inst, name) if (inst eq member.instanceProvider) && name == member.memberName =>
        member
      case x => x
    }
  }

  protected def replaceArgs(
      method: MethodElement,
      otherArgs: List[MethodArg[RelationElement]],
      e: RelationElement): MethodElement = {
    method.replaceArgs(MethodArg("src", e) :: otherArgs)
  }

  protected def replaceArgs(
      method: MethodElement,
      otherArgs: List[MethodArg[RelationElement]],
      left: RelationElement,
      right: RelationElement): MethodElement = {
    method.replaceArgs(MethodArg("left", left) :: MethodArg("right", right) :: otherArgs)
  }

  protected def bindPermitTableScan(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = visitElement(s.param)
    val projection = convertToSequence(source)
    if (projection eq null) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      // we do not do this check for db yet, we by-pass it here
      projection
    }
  }

  protected def bindSelect(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val projection = convertToSequence(source)
    val newKey =
      if ((projection ne null) && (innerQueryLevel > 0) && (method.key == NoKey))
        projection.keyPolicy.mkKey(method.rowTypeInfo)
      else method.key

    // For inner query, we stop reducing if newKey is not NoKey
    if ((projection eq null) || ((innerQueryLevel > 0) && (newKey != NoKey))) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val lambdaOpt = others.head.arg match {
        case FuncElement(c: ScalaLambdaCallee[_, _], _, _) => c.lambdaElement
        case l: LambdaElement                              => Some(l)
        case _                                             => None
      }
      val proj = lambdaOpt.flatMap { case LambdaElement(_, body, List(param)) =>
        val unboundBody = ParameterReplacer.replace(body, param, projection.projector)
        val (select, e) = bindAndVisitLambdaBody(projection, unboundBody)
        if (!language.canBeProjector(e)) None
        else {
          val alias = nextAlias()
          val pc = projectColumns(e, alias, select.alias)
          Some(
            new ProjectionElement(
              new SelectElement(alias, pc.columns, select, null, null, null, null, null, false, false),
              pc.projector,
              newKey,
              projection.keyPolicy,
              entitledOnly = projection.entitledOnly
            ))
        }
      }
      proj.getOrElse(replaceArgs(method, others, projection))
    }
  }

  protected def bindWhere(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val (projection, viaUntype) = convertToSequenceForWhere(source)
    if (projection eq null) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val (lambdaOpt, funcOpt) = others.head.arg match {
        case FuncElement(c: ScalaLambdaCallee[_, _], _, _) => (c.lambdaElement, Some(c))
        case l: LambdaElement                              => (Some(l), None)
        case _                                             => (None, None)
      }
      val proj = lambdaOpt.flatMap { case LambdaElement(_, body, List(param)) =>
        var select = projection.select
        val projector = if (viaUntype) new DynamicObjectElement(projection.projector) else projection.projector
        var serverWhere: RelationElement = null
        var clientWhere: RelationElement = null
        for (cond <- Query.flattenBOOLANDConditions(body)) {
          val unboundCond = ParameterReplacer.replace(cond, param, projector)
          val (sel, where) = bindAndVisitLambdaBody(select, unboundCond)
          if (!language.canBeWhere(where))
            clientWhere = if (clientWhere eq null) cond else ElementFactory.andAlso(clientWhere, cond)
          else {
            select = sel
            serverWhere = if (serverWhere eq null) where else ElementFactory.andAlso(serverWhere, where)
          }
        }
        if (serverWhere eq null) None
        else {
          val alias = nextAlias()
          val pc = projectColumns(projection.projector, alias, select.alias)
          val pe = new ProjectionElement(
            new SelectElement(alias, pc.columns, select, serverWhere, null, null, null, null, false, false),
            pc.projector,
            method.key,
            projection.keyPolicy,
            entitledOnly = projection.entitledOnly
          )
          val result = ElementReplacer.replace(source, List(projection), List(pe))
          if (clientWhere eq null) Some(result)
          else {
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
  }

  protected def bindFlatMap(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val projection = convertToSequence(source)
    val newKey =
      if ((projection ne null) && (innerQueryLevel > 0) && (method.key == NoKey))
        projection.keyPolicy.mkKey(method.rowTypeInfo)
      else method.key

    // For inner query, we stop reducing if newKey is not NoKey
    if ((projection eq null) || ((innerQueryLevel > 0) && (newKey != NoKey))) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val lambdaOpt = others.head.arg match {
        case FuncElement(c: ScalaLambdaCallee[_, _], _, _) => c.lambdaElement
        case l: LambdaElement                              => Some(l)
        case _                                             => None
      }
      val proj = lambdaOpt.flatMap { case LambdaElement(_, body, List(param)) =>
        val unboundBody = ParameterReplacer.replace(body, param, projection.projector)
        val (leftSel, boundedBody) = bindAndVisitLambdaBody(projection, unboundBody)
        val collProj = convertToSequence(boundedBody)
        if (collProj eq null) stripOption2Iterable(boundedBody) match {
          case OptionElement(e) if language.canBeProjector(e) =>
            val cond = optionToCondition(e)
            val where = e match {
              case _: ConditionalElement =>
                ElementFactory.andAlso(
                  invertTest(cond.test),
                  ElementFactory.notEqual(
                    cond.ifFalse,
                    ElementFactory.constant(
                      if (cond.ifFalse.rowTypeInfo <:< classOf[Option[_]]) None else null,
                      cond.ifFalse.rowTypeInfo))
                )
              case _ => invertTest(cond.test)
            }
            if (!language.canBeWhere(where)) None
            else {
              val alias = nextAlias()
              val pc = projectColumns(cond.ifFalse, alias, leftSel.alias)
              Some(
                new ProjectionElement(
                  new SelectElement(alias, pc.columns, leftSel, where, null, null, null, null, false, false),
                  pc.projector,
                  newKey,
                  projection.keyPolicy,
                  entitledOnly = projection.entitledOnly
                ))
            }
          case _ => None
        }
        else {
          val isTable = collProj.select.from.isInstanceOf[TableElement]
          val joinType = if (isTable) JoinType.CrossJoin else JoinType.CrossApply
          // Set select.distinct = true if right is c2p field and there is no distinct after the flatMap
          val rightSel =
            if (
              (hasDistinctByKeyAfter && (innerQueryLevel == 0)) ||
              collProj.viaCollection.map(c => !language.shouldBeDistinct(c)).getOrElse(true)
            ) {
              collProj.select
            } else {
              val s = collProj.select
              new SelectElement(
                s.alias,
                s.columns,
                s.from,
                s.where,
                s.orderBy,
                s.groupBy,
                s.skip,
                s.take,
                true,
                s.reverse)
            }
          val join = new JoinElement(joinType, leftSel, rightSel, null)
          val alias = nextAlias()
          val pc = projectColumns(collProj.projector, alias, leftSel.alias, collProj.select.alias)
          Some(
            new ProjectionElement(
              new SelectElement(alias, pc.columns, join, null, null, null, null, null, false, false),
              pc.projector,
              method.key,
              projection.keyPolicy,
              entitledOnly = projection.entitledOnly
            ))
        }
      }
      proj.getOrElse(replaceArgs(method, others, projection))
    }
  }

  private def stripOption2Iterable(e: RelationElement): RelationElement = e match {
    case FuncElement(callee: MethodCallee, List(arg), _)
        if (callee.name == "option2Iterable") &&
          callee.method.declaringType <:< Option.getClass =>
      arg
    case _ => e
  }

  protected def bindGroupBy(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val projection = convertToSequence(source)
    if (projection eq null) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val lambdaOpt = others.head.arg match {
        case FuncElement(c: ScalaLambdaCallee[_, _], _, _) => c.lambdaElement
        case l: LambdaElement                              => Some(l)
        case _                                             => None
      }
      val proj = lambdaOpt.flatMap { case LambdaElement(_, body, List(param)) =>
        val unboundBody = ParameterReplacer.replace(body, param, projection.projector)
        val (select, boundedBody) = bindAndVisitLambdaBody(projection, unboundBody)
        val keyElem = getGroupKeyElement(select, boundedBody)

        if (!language.canBeGroupBy(keyElem)) None
        else {
          // use projectColumns to get group-by expressions from key expression
          val groupElems = getGroupKeyColumns(keyElem, select.alias)

          // make duplicate of source query as basis of element subquery
          val subqueryBasis = DbQueryTreeDuplicator.duplicate(projection).asInstanceOf[ProjectionElement]
          val subqueryUnboundBody = ParameterReplacer.replace(body, param, subqueryBasis.projector)
          val (subquerySelect, subQueryBoundedBody) = bindAndVisitLambdaBody(subqueryBasis, subqueryUnboundBody)
          val subqueryKey = getGroupKeyElement(subquerySelect, subQueryBoundedBody)

          // use same projection trick to get group-by expressions based on subquery
          val subqueryGroupElems = getGroupKeyColumns(subqueryKey, subquerySelect.alias)
          val subqueryCorrelation = buildPredicateWithNullsEqual(subqueryGroupElems, groupElems)

          // build subquery that projects the desired element
          val rightAlias = nextAlias()
          val rightPC = projectColumns(subqueryBasis.projector, rightAlias, subquerySelect.alias)
          val rightSubquery = new ProjectionElement(
            new SelectElement(
              rightAlias,
              rightPC.columns,
              subquerySelect,
              subqueryCorrelation,
              null,
              null,
              null,
              null,
              false,
              false),
            rightPC.projector,
            projection.key,
            projection.keyPolicy,
            entitledOnly = projection.entitledOnly
          )

          val alias = nextAlias()
          val resultElem = TupleElement(List(keyElem, rightSubquery))
          val pc = projectColumns(resultElem, alias, select.alias)
          pc.projector match {
            case TupleElement(List(_, q)) =>
              // make it possible to tie aggregates back to this group-by
              val info = new GroupByInfo(alias, projection.projector)
              groupByMap.put(q, info)
            case _ =>
          }
          Some(
            new ProjectionElement(
              new SelectElement(alias, pc.columns, select, null, null, groupElems, null, null, false, false),
              pc.projector,
              method.key,
              projection.keyPolicy,
              entitledOnly = projection.entitledOnly
            ))
        }
      }
      proj.getOrElse(replaceArgs(method, others, projection))
    }
  }

  protected def getGroupKeyColumns(keyElem: RelationElement, alias: TableAlias): List[RelationElement] = {
    val keyProj = projectColumns(keyElem, alias, alias)
    keyProj.columns.map(_.element)
  }

  protected def getGroupKeyElement(select: SelectElement, projector: RelationElement): RelationElement = {
    projector
  }

  protected def bindMapValues(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val projection = convertToSequence(source)
    if (projection eq null) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val lambdaOpt = others.head.arg match {
        case FuncElement(c: ScalaLambdaCallee[_, _], _, _) => c.lambdaElement
        case l: LambdaElement                              => Some(l)
        case _                                             => None
      }
      val proj = lambdaOpt.flatMap { case LambdaElement(_, body, List(param)) =>
        val TupleElement(List(keyElem, valueElem)) = projection.projector
        val unboundBody = ParameterReplacer.replace(body, param, valueElem)
        val (select, e) = bindAndVisitLambdaBody(projection, unboundBody)

        if (!language.canBeProjector(e)) None
        else {
          val alias = nextAlias()

          val resultElem = TupleElement(List(keyElem, e))
          val pc = projectColumns(resultElem, alias, select.alias)
          Some(
            new ProjectionElement(
              new SelectElement(alias, pc.columns, select, null, null, null, null, null, false, false),
              pc.projector,
              method.key,
              projection.keyPolicy,
              entitledOnly = projection.entitledOnly
            ))
        }
      }
      proj.getOrElse(replaceArgs(method, others, projection))
    }
  }

  protected def bindAggregate(func: FuncElement, mc: MethodCallee): RelationElement = {
    if (func.arguments.size < 3) bindAggregate1(func, mc) else bindAggregate2(func, mc)
  }

  /**
   * bind aggregate function that takes less than 3 arguments (e.g. count, max, sum, etc)
   */
  protected def bindAggregate1(func: FuncElement, mc: MethodCallee): RelationElement = {
    import QueryMethod.WHERE
    require(func.arguments.size < 3)

    val hasPredicateArg = language.aggregateArgumentIsPredicate(mc.method.name)
    val (lambda, shouldContinue) = func.arguments match {
      case FuncElement(c: ScalaLambdaCallee[_, _], _, _) :: _ =>
        val lOpt = c.lambdaElement
        (lOpt, lOpt.isDefined)
      case (l: LambdaElement) :: _ => (Some(l), true)
      case _                       => (None, true)
    }

    if (!shouldContinue) super.handleFuncCall(func)
    else {
      val (lambdaOpt, src, argumentWasPredicate) = lambda map { l =>
        if (hasPredicateArg) {
          val shapeType = findShapeType(func.instance)
          val where = new MethodElement(
            WHERE,
            List(MethodArg("src", func.instance), MethodArg("p", l)),
            shapeType,
            NoKey,
            MethodPosition.unknown)
          (None, where, true)
        } else {
          (Some(l), func.instance, false)
        }
      } getOrElse { (None, func.instance, false) }

      val source = super.visitElement(src)
      val projection = convertToSequence(source)
      if (projection eq null) {
        super.handleFuncCall(func)
      } else {
        val (sel, arg) = lambdaOpt map { l =>
          val unboundBody = ParameterReplacer.replace(l.body, l.parameters(0), projection.projector)
          bindAndVisitLambdaBody(projection, unboundBody)
        } getOrElse {
          if (!hasPredicateArg) (projection.select, projection.projector) else (projection.select, null)
        }

        if (!language.canBeAggregate(arg)) {
          updateFuncCall(func, projection, func.arguments)
        } else {
          val aggName = mc.method.name match {
            case n @ ("variance" | "stddev") =>
              func.arguments.last match {
                case ConstValueElement(_: PopulationVariance[_] | _: PopulationStandardDeviation[_], _) => n + "_pop"
                case _                                                                                  => n
              }
            case "count" if projection.projector.rowTypeInfo <:< classOf[Option[_]] => "countOption"
            case n                                                                  => n
          }
          val alias = nextAlias()
          val aggElem = new AggregateElement(mc.resType, aggName, Option(arg).toList, false)
          // Since the aggEle is the only column in select.columns, this colName won't have conflict.
          val colName = "aggr"
          val columns = List(ColumnDeclaration(colName, aggElem))
          val select = new SelectElement(alias, columns, sel, null, null, null, null, null, false, false)

          if (func eq root) {
            val aggregator = Aggregator.getHeadAggregator(mc.resType)
            new ProjectionElement(
              select,
              new ColumnElement(mc.resType, alias, colName, ColumnInfo.Calculated),
              NoKey,
              projection.keyPolicy,
              aggregator,
              entitledOnly = projection.entitledOnly)
          } else {
            val subquery = new ScalarElement(mc.resType, select)
            val info = groupByMap.get(projection)
            if (!argumentWasPredicate && (info ne null)) {
              val arg1 = lambdaOpt.map { l =>
                map.put(l.parameters(0), info.element)
                visitLambdaBody(l.body)
              } getOrElse {
                if (!hasPredicateArg) info.element else arg
              }
              if (language.canBeAggregate(arg1)) {
                val aggElem1 = new AggregateElement(mc.resType, aggName, Option(arg1).toList, false)
                new AggregateSubqueryElement(info.alias, aggElem1, subquery)
              } else subquery
            } else {
              subquery
            }
          }
        }
      }
    }
  }

  /**
   * bind aggregate function that takes at least 3 arguments (e.g. corr, covariance)
   */
  protected def bindAggregate2(func: FuncElement, mc: MethodCallee): RelationElement = {
    require(func.arguments.size > 2)

    val argumentList = func.arguments.dropRight(1)
    val lambdaList = argumentList.flatMap(_ match {
      case FuncElement(c: ScalaLambdaCallee[_, _], _, _) => c.lambdaElement
      case l: LambdaElement                              => Some(l)
      case _                                             => None
    })
    if (lambdaList.size != argumentList.size) super.handleFuncCall(func)
    else {
      val source = super.visitElement(func.instance)
      val projection = convertToSequence(source)
      if (projection eq null) {
        super.handleFuncCall(func)
      } else {
        var shouldContinue = true
        var sel = projection.select
        val argList = lambdaList.flatMap { l =>
          if (!shouldContinue) Nil
          else {
            val unboundBody = ParameterReplacer.replace(l.body, l.parameters(0), projection.projector)
            val (s, arg) = bindAndVisitLambdaBody(sel, unboundBody)
            if (language.canBeAggregate(arg)) {
              sel = s
              List(arg)
            } else {
              shouldContinue = false
              Nil
            }
          }
        }
        if (!shouldContinue) updateFuncCall(func, projection, func.arguments)
        else {
          val alias = nextAlias()
          val pc = projectColumns(projection.projector, alias, sel.alias)
          val aggElem = new AggregateElement(mc.resType, mc.method.name, argList, false)
          // Since the aggEle is the only column in select.columns, this colName won't have conflict.
          val colName = "aggr"
          val columns = List(ColumnDeclaration(colName, aggElem))
          val select = new SelectElement(alias, columns, sel, null, null, null, null, null, false, false)

          if (func eq root) {
            val aggregator = Aggregator.getHeadAggregator(mc.resType)
            new ProjectionElement(
              select,
              new ColumnElement(mc.resType, alias, colName, ColumnInfo.Calculated),
              NoKey,
              projection.keyPolicy,
              aggregator,
              entitledOnly = projection.entitledOnly)
          } else {
            val subquery = new ScalarElement(mc.resType, select)
            val info = groupByMap.get(projection)
            if (info ne null) {
              val argList1 = lambdaList.map { l =>
                map.put(l.parameters(0), info.element)
                visitLambdaBody(l.body)
              }
              if (argList1.forall(language.canBeAggregate(_))) {
                val aggElem1 = new AggregateElement(mc.resType, mc.method.name, argList1, false)
                new AggregateSubqueryElement(info.alias, aggElem1, subquery)
              } else subquery
            } else {
              subquery
            }
          }
        }
      }
    }
  }

  protected def bindHeadLast(func: FuncElement, mc: MethodCallee): RelationElement = {
    val source = visitElement(func.instance)
    val projection = convertToSequence(source)
    if (projection eq null) {
      super.handleFuncCall(func)
    } else {
      val isLast = mc.name.startsWith("last")
      val take = ElementFactory.constant(1, TypeInfo.INT)
      val alias = nextAlias()
      val pc = projectColumns(projection.projector, alias, projection.select.alias)
      val aggregator =
        if (mc.name.endsWith("Option")) Aggregator.getHeadOptionAggregator(mc.resType)
        else Aggregator.getHeadAggregator(mc.resType)
      new ProjectionElement(
        new SelectElement(alias, pc.columns, projection.select, null, null, null, null, take, false, isLast),
        pc.projector,
        NoKey,
        projection.keyPolicy,
        aggregator,
        entitledOnly = projection.entitledOnly
      )
    }
  }

  private def buildPredicateWithNullsEqual(
      list1: List[RelationElement],
      list2: List[RelationElement]): RelationElement = {
    // we do not add IsNullElement here, since ComparisonRewriter will do this later if the column is not lifted to NamedValue
    list1 zip list2 map { t =>
      ElementFactory.equal(t._1, t._2)
    } reduce { (l, r) =>
      ElementFactory.andAlso(l, r)
    }
  }

  protected def bindJoin(method: MethodElement): RelationElement = {
    val (l :: r :: others) = method.methodArgs
    val left = super.visitElement(l.param)
    val right = super.visitElement(r.param)
    val leftProj = convertToSequence(left)
    val rightProj = convertToSequence(right)

    (leftProj, rightProj) match {
      case (null, null) =>
        if ((left eq l.param) && (right eq r.param)) method else replaceArgs(method, others, left, right)
      case (_, null)                                  => replaceArgs(method, others, leftProj, right)
      case (null, _)                                  => replaceArgs(method, others, left, rightProj)
      case (l, r) if l.entitledOnly != r.entitledOnly => replaceArgs(method, others, leftProj, rightProj)
      case _ =>
        val onElem = others
          .collectFirst { case x if x.name == "on" => x.param }
          .getOrElse(if (others.size >= 3) others(2).param else null)
        val lambdaOpt = onElem match {
          case FuncElement(c: ScalaLambdaCallee2[_, _, _], _, _) => c.lambdaElement
          case l: LambdaElement                                  => Some(l)
          case _                                                 => None
        }

        val leftDefaultEle = others.collectFirst { case x if x.name == "leftDefault" => x.param }.getOrElse(null)
        val leftDefaultLambdaOpt = leftDefaultEle match {
          case FuncElement(c: ScalaLambdaCallee[_, _], _, _) => c.lambdaElement
          case l: LambdaElement                              => Some(l)
          case _                                             => None
        }

        val rightDefaultEle = others.collectFirst { case x if x.name == rightDefault => x.param }.getOrElse(null)
        val rightDefaultLambdaOpt = rightDefaultEle match {
          case FuncElement(c: ScalaLambdaCallee[_, _], _, _) => c.lambdaElement
          case l: LambdaElement                              => Some(l)
          case _                                             => None
        }

        val proj = lambdaOpt.flatMap { case LambdaElement(_, body, List(param1, param2)) =>
          // replace the left param with left projector
          val leftBody = ParameterReplacer.replace(body, param1, leftProj.projector)
          // bind the relationship and attach the related join to left select
          val (leftSelect, lb) = bindAndVisitLambdaBody(leftProj, leftBody)
          // replace the right param with right projector
          val rightBody = ParameterReplacer.replace(lb, param2, rightProj.projector)
          // bind the relationship and attach the related join to the right select
          val (rightSelect, on) = bindAndVisitLambdaBody(rightProj, rightBody)

          if (!language.canBeOn(on, method.methodCode))
            None
          else {
            val resultExpr = new TupleElement(List(leftProj.projector, rightProj.projector))
            val join = new JoinElement(JoinType.from(method.methodCode), leftSelect, rightSelect, on)
            val alias = nextAlias()
            val pc = projectColumns(resultExpr, alias, leftProj.select.alias, rightProj.select.alias)
            val joinProj = new ProjectionElement(
              new SelectElement(alias, pc.columns, join, null, null, null, null, null, false, false),
              pc.projector,
              method.key,
              leftProj.keyPolicy,
              entitledOnly = leftProj.entitledOnly
            )

            val TupleElement(leftValue :: rightValue :: Nil) = pc.projector

            val (select, withDefaultLeft, withDefaultRight) = method.methodCode match {
              case QueryMethod.INNER_JOIN => (joinProj.select, leftValue, rightValue)
              case QueryMethod.LEFT_OUTER_JOIN =>
                val (sel, rightDefault) =
                  getDefaultValue(rightDefaultLambdaOpt, leftValue, joinProj.select, rightValue, false)
                (sel, leftValue, rightDefault)
              case QueryMethod.RIGHT_OUTER_JOIN =>
                val (sel, leftDefault) =
                  getDefaultValue(leftDefaultLambdaOpt, rightValue, joinProj.select, leftValue, true)
                (sel, leftDefault, rightValue)
              case QueryMethod.FULL_OUTER_JOIN =>
                val (sel1, rightDefault) =
                  getDefaultValue(rightDefaultLambdaOpt, leftValue, joinProj.select, rightValue, false)
                val (sel2, leftDefault) = getDefaultValue(leftDefaultLambdaOpt, rightValue, sel1, leftValue, true)
                (sel2, leftDefault, rightDefault)
            }
            if ((select ne joinProj.select) || (withDefaultLeft ne leftValue) || (withDefaultRight ne rightValue)) {
              val projector = new TupleElement(List(withDefaultLeft, withDefaultRight))
              val proj =
                new ProjectionElement(
                  select,
                  projector,
                  joinProj.key,
                  joinProj.keyPolicy,
                  joinProj.aggregator,
                  entitledOnly = joinProj.entitledOnly)
              Some(proj)
            } else Some(joinProj)
          }
        }

        proj.getOrElse({
          if ((leftProj eq l.param) && (rightProj eq r.param)) method
          else replaceArgs(method, others, leftProj, rightProj)
        })
    }
  }

  // Use defaultValue and originValue to build OuterJoinedElement
  private def getDefaultValue(
      defaultLambdaOpt: Option[RelationElement],
      lambdaParam: RelationElement,
      select: SelectElement,
      originValue: RelationElement,
      isLeft: Boolean): (SelectElement, RelationElement) = {
    val defaultValue = defaultLambdaOpt.map { case LambdaElement(_, body, List(param)) =>
      this.map.put(param, lambdaParam)
      visitLambdaBody(body)
    } getOrElse (null)

    language.addOuterJoinTest(select, originValue, defaultValue, isLeft)
  }

  protected def bindTake(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val projection = convertToSequence(source)
    if (projection eq null) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val ((offset @ ConstValueElement(skip: Int, _)) :: (limit @ ConstValueElement(take: Int, _)) :: Nil) =
        others.map(_.param)
      if (skip < 0) // db cannot support takeRight
        replaceArgs(method, others, projection)
      else {
        val alias = nextAlias()
        val pc = projectColumns(projection.projector, alias, projection.select.alias)
        new ProjectionElement(
          new SelectElement(
            alias,
            pc.columns,
            projection.select,
            null,
            null,
            null,
            if (skip == 0) null else offset,
            if (take == 0) null else limit,
            false,
            false),
          pc.projector,
          method.key,
          projection.keyPolicy,
          entitledOnly = projection.entitledOnly
        )
      }
    }
  }

  protected def bindSort(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val projection = convertToSequence(source)
    if (projection eq null) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val otherElements = others.map(_.arg)
      val lambdaOpt = otherElements match {
        case FuncElement(c: ScalaLambdaCallee[_, _], _, _) :: _ => c.lambdaElement
        case (l: LambdaElement) :: _                            => Some(l)
        case _                                                  => None
      }
      val proj = lambdaOpt.flatMap { case LambdaElement(_, body, List(param)) =>
        val unboundBody = ParameterReplacer.replace(body, param, projection.projector)
        val (select, orderSel) = bindAndVisitLambdaBody(projection, unboundBody)

        if (!language.canBeOrderBy(orderSel)) None
        else {
          val orderBy = OrderDeclarationCollector.collect(orderSel)
          val alias = nextAlias()
          val pc = projectColumns(projection.projector, alias, select.alias)
          Some(
            new ProjectionElement(
              new SelectElement(alias, pc.columns, select, null, orderBy, null, null, null, false, false),
              pc.projector,
              method.key,
              projection.keyPolicy,
              entitledOnly = projection.entitledOnly
            ))
        }
      }
      proj.getOrElse(replaceArgs(method, others, projection))
    }
  }

  protected def bindDistinct(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val projection = convertToSequence(source)
    if (projection eq null) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val alias = nextAlias()
      val pc = projectColumns(projection.projector, alias, projection.select.alias)
      new ProjectionElement(
        new SelectElement(alias, pc.columns, projection.select, null, null, null, null, null, true, false),
        pc.projector,
        method.key,
        projection.keyPolicy,
        entitledOnly = projection.entitledOnly
      )
    }
  }

  protected def bindUntype(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val projection = convertToSequence(source)
    if (projection eq null) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val newProjector = new DynamicObjectElement(projection.projector)
      new ProjectionElement(
        projection.select,
        newProjector,
        method.key,
        projection.keyPolicy,
        entitledOnly = projection.entitledOnly)
    }
  }

  protected def nextAlias() = new TableAlias

  protected def projectColumns(
      e: RelationElement,
      newAlias: TableAlias,
      existingAliases: TableAlias*): ProjectedColumns = {
    ColumnProjector.projectColumns(language, e, null, newAlias, existingAliases)
  }

  protected def visitSequence(source: RelationElement): ProjectionElement = {
    convertToSequence(super.visitElement(source))
  }

  protected def convertToSequenceForWhere(e: RelationElement): (ProjectionElement, Boolean) = {
    e match {
      case p: ProjectionElement =>
        (p, false)
      case m: MethodElement if m.methodCode == QueryMethod.WHERE =>
        convertToSequenceForWhere(m.methodArgs(0).arg)
      case m: MethodElement if m.methodCode == QueryMethod.UNTYPE =>
        convertToSequenceForWhere(m.methodArgs(0).arg).copy(_2 = true)
      case _ =>
        (null, false)
    }
  }

  protected def convertToSequence(e: RelationElement): ProjectionElement = {
    e match {
      case p: ProjectionElement => p
      case MemberElement(p: ProjectionElement, "toSeq" | "toList")
          if p.viaCollection.isDefined && isSimpleProjector(p.projector) =>
        addDistinctToProjection(p, Some(e.rowTypeInfo.clazz))
      case _ => null
    }
  }

  protected def convertToLinkageProj(e: RelationElement, viaLinkage: Option[Class[_]]): Option[RelationElement] = {
    e match {
      case p: ProjectionElement if language.canBeSubqueryProjector(p.projector) =>
        Some(
          new ProjectionElement(
            p.select,
            p.projector,
            p.key,
            p.keyPolicy,
            p.aggregator,
            viaLinkage,
            entitledOnly = p.entitledOnly))
      case s: ScalarElement => Some(s)
      case _                => None
    }
  }

  private def visitLambdaBody(e: RelationElement): RelationElement = {
    innerQueryLevel += 1
    val visited = visitElement(e)
    innerQueryLevel -= 1
    visited
  }

  final protected def bindAndVisitLambdaBody(
      outer: ProjectionElement,
      body: RelationElement): (SelectElement, RelationElement) = {
    bindAndVisitLambdaBody(outer.select, body)
  }

  final protected def bindAndVisitLambdaBody(
      outer: SelectElement,
      body: RelationElement): (SelectElement, RelationElement) = {
    var select = outer
    var unboundBody = body
    var boundedBody = body
    do {
      unboundBody = boundedBody
      val ProjectionElement(sel, b, _) =
        RelationshipBinder.bind(
          mapper,
          new ProjectionElement(select, unboundBody, NoKey, KeyPropagationPolicy.NoKey, entitledOnly = false))
      select = sel
      boundedBody = visitLambdaBody(b)
    } while (unboundBody ne boundedBody)
    (select, boundedBody)
  }

  private def isSimpleProjector(projector: RelationElement): Boolean = {
    projector.elementType match {
      case DbElementType.Column | DbElementType.DbEntity | DbElementType.DALHeapEntity |
          DbElementType.EmbeddableCaseClass =>
        true
      case _ => false
    }
  }
}

object DefaultBinder extends QueryBinder {
  def bind(mapper: QueryMapper, e: RelationElement): RelationElement = {
    new DefaultBinder(mapper, e).visitElement(e)
  }

  class ParameterReplacer private (searchFor: ParameterElement, replaceWith: RelationElement)
      extends DbQueryTreeVisitor {
    protected override def handleParameter(p: ParameterElement): RelationElement = {
      if (p eq searchFor) replaceWith else p
    }
  }

  object ParameterReplacer {
    def replace(e: RelationElement, searchFor: ParameterElement, replaceWith: RelationElement): RelationElement = {
      new ParameterReplacer(searchFor, replaceWith).visitElement(e)
    }
  }
}

class GroupByInfo(val alias: TableAlias, val element: RelationElement)
