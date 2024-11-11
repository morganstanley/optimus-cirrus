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
package optimus.platform.relational.data

import optimus.entity.ClassEntityInfo
import optimus.graph.Node
import optimus.platform._
import optimus.platform._
import optimus.platform.cm.Known
import optimus.platform.pickling.PickledInputStream
import optimus.platform.relational.RelationalException
import optimus.platform.relational.ScalaBasedDynamicObjectFactory
import optimus.platform.relational.asm.Func
import optimus.platform.relational.asm.LambdaCompiler
import optimus.platform.relational.asm.Typer
import optimus.platform.relational.data.language.FormattedQuery
import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.mapping.QueryMapping
import optimus.platform.relational.data.translation.ComparisonRewriter
import optimus.platform.relational.data.translation.ConditionalFlattener
import optimus.platform.relational.data.translation.ElementReplacer
import optimus.platform.relational.data.translation.NamedValueGatherer
import optimus.platform.relational.data.translation.OuterParameterizer
import optimus.platform.relational.data.translation.Scope
import optimus.platform.relational.data.tree._
import optimus.platform.relational.inmemory.IterableProvider
import optimus.platform.relational.tree._
import optimus.platform.util.ReflectUtils

import scala.collection.immutable

abstract class DbQueryTreeReducerBase extends DbQueryTreeVisitor with ReducerVisitor {
  import DbQueryTreeReducerBase._

  protected var isTop = true
  protected var scope: Scope = null
  protected var nReaders = 0
  protected var translator: QueryTranslator = _
  protected var executeOptions: ExecuteOptions = ExecuteOptions.Default

  override def reduce(tree: RelationElement, execOptions: ExecuteOptions): RelationElement = {
    executeOptions = execOptions
    val element = tree match {
      case e: LambdaElement => e.body
      case e                => e
    }
    translator = createTranslator()
    val e = translator.translate(element)
    visitElement(e)
  }

  def provider: DataProvider
  protected def createMapping(): QueryMapping
  protected def createLanguage(lookup: MappingEntityLookup): QueryLanguage

  protected def createTranslator(): QueryTranslator = {
    val mapping = createMapping()
    val language = createLanguage(mapping)
    new QueryTranslator(language, mapping)
  }

  protected override def handleDynamicObject(d: DynamicObjectElement): RelationElement = {
    ScalaBasedDynamicObjectFactory(d.source.rowTypeInfo) match {
      case Left(nf) =>
        val nfType = TypeInfo(Typer.nodeFunctionClass(1), TypeInfo.ANY, DynamicObject.typeTag)
        val inst = ElementFactory.constant(asNode.apply$withNode(nf), nfType)
        val method = new RuntimeMethodDescriptor(nfType, "apply", DynamicObject.typeTag)
        ElementFactory.call(inst, method, List(visitElement(d.source)))
      case Right(f) =>
        val fType = TypeInfo(Typer.functionClass(1), TypeInfo.ANY, DynamicObject.typeTag)
        val inst = ElementFactory.constant(f, fType)
        val method = new RuntimeMethodDescriptor(fType, "apply", DynamicObject.typeTag)
        ElementFactory.call(inst, method, List(visitElement(d.source)))
    }
  }

  protected override def handleDbEntity(entity: DbEntityElement): RelationElement = {
    visitElement(entity.element)
  }

  protected def getReaderFunction(column: ColumnElement, returnType: TypeInfo[_]): Option[RelationElement] = {
    if (scope eq null) None
    else
      scope.getValue(column).map { case (reader, iOrdinal) =>
        FieldReader.getReaderFunction(returnType, iOrdinal, reader)
      }
  }

  protected final def getCheckNoneFunction(invert: Boolean, column: ColumnElement): Option[RelationElement] = {
    if (scope eq null) None
    else
      scope.getValue(column).map { case (reader, iOrdinal) =>
        // generate "reader.isNone(iOrdinal)"
        val check = FieldReader.getCheckNoneFunction(iOrdinal, reader)
        if (!invert) check
        else ElementFactory.equal(check, ElementFactory.constant(false, TypeInfo.BOOLEAN))
      }
  }

  protected override def handleColumn(column: ColumnElement): RelationElement = {
    getReaderFunction(column, column.projectedType()).getOrElse(column)
  }

  protected override def handleOptionElement(option: OptionElement): RelationElement = {
    val readerOpt = option.element match {
      case KnowableValueElement(kn) =>
        val desc = new RuntimeMethodDescriptor(kn.rowTypeInfo, "toOption", option.rowTypeInfo)
        Some(visitElement(ElementFactory.call(kn, desc, Nil)))
      case c: ColumnElement if !TypeInfo.isOption(c.rowTypeInfo) => getReaderFunction(c, option.projectedType())
      case _                                                     => None
    }
    readerOpt.getOrElse {
      val e = visitElement(option.element)
      val method =
        new RuntimeMethodDescriptor(TypeInfos.OptionCompanion, "apply", option.rowTypeInfo, List(e.rowTypeInfo))
      ElementFactory.call(null, method, List(e))
    }
  }

  protected override def handleBinaryExpression(binary: BinaryExpressionElement): RelationElement = {
    import BinaryExpressionType._
    binary match {
      case BinaryExpressionElement(EQ | NE, c: ColumnElement, ConstValueElement(null, _), _) =>
        getCheckNoneFunction(binary.op == NE, c).getOrElse(binary)
      case BinaryExpressionElement(EQ | NE, KnowableValueElement(kn), ConstValueElement(null, _), _) =>
        val method = if (binary.op == EQ) "isEmpty" else "nonEmpty"
        val desc = new RuntimeMethodDescriptor(kn.rowTypeInfo, method, TypeInfo.BOOLEAN)
        visitElement(ElementFactory.call(kn, desc, Nil))
      case BinaryExpressionElement(EQ, KnowableValueElement(kn), e, _) =>
        val desc = new RuntimeMethodDescriptor(typeInfo[Known.type], "apply", kn.rowTypeInfo)
        visitElement(ElementFactory.equal(kn, ElementFactory.call(null, desc, List(e))))
      case _ => super.handleBinaryExpression(binary)
    }
  }

  protected override def handleProjection(proj: ProjectionElement): RelationElement = {
    val wasTop = isTop
    val element = if (isTop) {
      isTop = false
      executeProjection(proj)
    } else {
      buildInner(proj)
    }
    isTop = wasTop
    element
  }

  protected def buildInner(e: RelationElement): RelationElement

  protected override def handleTupleElement(tuple: TupleElement): RelationElement = {
    val elements = visitElementList(tuple.elements)
    mkTuple(elements, tuple.projectedType())
  }

  private def mkTuple(elements: List[RelationElement], resultType: TypeInfo[_] = null): RelationElement = {
    val genericTypes = elements.map(_.projectedType())
    val tupleTypeInfo = if (resultType eq null) TypeInfo.mkTuple(genericTypes) else resultType
    val method = new RuntimeMethodDescriptor(
      TypeInfo.javaTypeInfo(Class.forName(s"scala.Tuple${elements.size}$$")),
      "apply",
      tupleTypeInfo,
      genericTypes)
    ElementFactory.call(null, method, elements)
  }

  protected override def handleOuterJoined(outer: OuterJoinedElement): RelationElement = {
    val ifFalse = visitElement(outer.element)
    val column = outer.test.asInstanceOf[ColumnElement]

    scope.getValue(column).map { case (reader, iOrdinal) =>
      val test = FieldReader.getCheckNoneFunction(iOrdinal, reader)
      val ifTrue = visitElement(outer.defaultValue) match {
        case null => ElementFactory.constant(TypeInfo.defaultValue(outer.rowTypeInfo), outer.rowTypeInfo)
        case e    => e
      }
      ElementFactory.condition(test, ifTrue, ifFalse, outer.rowTypeInfo)
    } getOrElse (ifFalse)
  }

  protected override def handleEmbeddableCollection(collection: EmbeddableCollectionElement): RelationElement = {
    visitElement(collection.element)
  }

  protected override def handleEmbeddableCaseClass(caseClass: EmbeddableCaseClassElement): RelationElement = {
    val mems = visitElementList(caseClass.members)
    val companionType = TypeInfo.javaTypeInfo(ReflectUtils.getCompanion(caseClass.rowTypeInfo.clazz))
    val method = new RuntimeMethodDescriptor(companionType, "apply", caseClass.rowTypeInfo, Nil)
    ElementFactory.call(null, method, mems)
  }

  protected override def handleDALHeapEntity(he: DALHeapEntityElement): RelationElement = {
    val mems = visitElementList(he.members)
    if (he.isStorable) {
      val tuples = he.memberNames.zip(mems).map { case (name, value) =>
        mkTuple(List(ElementFactory.constant(name, TypeInfo.STRING), value))
      }
      val apply = new RuntimeMethodDescriptor(TypeInfos.MapCompanion, "apply", TypeInfos.MapStringAny)
      val propertyMap = ElementFactory.call(null, apply, tuples)
      val inputStreamMethod =
        new RuntimeMethodDescriptor(TypeInfos.DbInputStreamCompanion, "apply", TypeInfos.DbInputStream)
      val stream = ElementFactory.call(null, inputStreamMethod, propertyMap :: Nil)
      val info = ElementFactory.constant(he.companion.info, TypeInfos.ClassEntityInfo)
      val forceUnpickle = ElementFactory.constant(true, TypeInfo.BOOLEAN)
      val method = new RuntimeMethodDescriptor(TypeInfos.ClassEntityInfo, "createUnpickled", he.projectedType())
      ElementFactory.call(info, method, List(stream, forceUnpickle))
    } else {
      val companionType = TypeInfo.javaTypeInfo(he.companion.getClass)
      val method = new RuntimeMethodDescriptor(companionType, "apply", he.rowTypeInfo, Nil)
      ElementFactory.call(null, method, mems)
    }
  }

  protected def executeProjection(proj: ProjectionElement): RelationElement = {
    var projection = proj

    // also convert references to outer alias to named values!  these become SQL parameters too
    if (scope ne null)
      projection = OuterParameterizer.parameterize(scope, projection).asInstanceOf[ProjectionElement]

    val (command, namedValues) = createQueryCommand(projection)
    val values = namedValues.map(v => visitElement(v.value))
    executeProjection(projection, command, values)
  }

  private def executeProjection(
      proj: ProjectionElement,
      command: QueryCommand,
      values: List[RelationElement]): RelationElement = {
    val savedScope = scope
    val reader = ElementFactory.parameter(s"r$nReaders", FieldReader.TYPE)
    nReaders += 1
    scope = new Scope(scope, reader, proj.select.alias, proj.select.columns)
    val body = ReaderClosureRewriter.rewrite(reader, visitElement(proj.projector))
    val projector = ElementFactory.lambda(body, Seq(reader))
    val result = if (savedScope ne null) {
      val instance = new ConstValueElement(provider, TypeInfos.DataProvider)
      val method = new RuntimeMethodDescriptor(TypeInfos.DataProvider, "executeDeferred", TypeInfos.ProviderRelation)
      val arguments = new ConstValueElement(command, typeInfo[QueryCommand]) :: liftLambda1(projector) ::
        new ConstValueElement(proj.key, TypeInfos.RelationKeyAny) :: mkList(values, TypeInfo.ANY) ::
        new ConstValueElement(proj.projectedType(), typeInfo[TypeInfo[Any]]) ::
        new ConstValueElement(executeOptions.asEntitledOnlyIf(proj.entitledOnly), typeInfo[ExecuteOptions]) :: Nil
      val executeDeferred = ElementFactory.call(instance, method, arguments)
      val p = new ConstValueElement(ProviderWithKeyPropagationPolicy(proj.keyPolicy), TypeInfos.QueryProvider)
      val shapeType = Query.findShapeType(proj)
      val createQueryMethod =
        new RuntimeMethodDescriptor(TypeInfos.QueryProvider, "createQuery", TypeInfos.query(shapeType), List(shapeType))
      ElementFactory.call(p, createQueryMethod, List(executeDeferred))
    } else {
      // here we assume the values is empty for the top level
      compileAndExecute(command, projector, proj)
    }
    scope = savedScope
    if (proj.aggregator eq null) result
    else {
      ElementReplacer.replace(proj.aggregator.body, List(proj.aggregator.parameters.head), List(result))
    }
  }

  protected def compileAndExecute(
      command: QueryCommand,
      projector: LambdaElement,
      proj: ProjectionElement): RelationElement = {
    val fnProjector = LambdaCompiler.compileAsNodeOrFunc1[FieldReader, Any](projector)
    provider.execute(
      command,
      fnProjector,
      proj.key,
      proj.rowTypeInfo,
      executeOptions.asEntitledOnlyIf(proj.entitledOnly))
  }

  protected override def handleContains(contains: ContainsElement): RelationElement = {
    contains.values match {
      case Left(_) =>
        throw new RelationalException("Expect ConstValueElement list in ContainsElement but got ScalarElement")
      case Right(values) =>
        val valueSet: Set[Any] = values.iterator.map { case const: ConstValueElement =>
          const.value
        }.toSet
        val (left, constSet) = contains.element match {
          case KnowableValueElement(kn) => (kn, valueSet.map(Known(_)))
          case e                        => (e, valueSet)
        }
        val constEle = ElementFactory.constant(constSet, TypeInfo.SET)
        ElementFactory.makeBinary(BinaryExpressionType.ITEM_IS_IN, visitElement(left), constEle)
    }
  }

  protected def mkList(elems: List[RelationElement], itemType: TypeInfo[_]): RelationElement = {
    val listTypeInfo = TypeInfo(classOf[List[_]], itemType)
    val method = new RuntimeMethodDescriptor(TypeInfos.ListCompanion, "apply", listTypeInfo, List(itemType))
    ElementFactory.call(null, method, elems)
  }

  protected def getFormattedQuery(proj: ProjectionElement): FormattedQuery = {
    var select = ComparisonRewriter.rewrite(proj.select)
    select = ConditionalFlattener.flatten(select)
    translator.dialect.format(select)
  }

  protected def createQueryCommand(projection: ProjectionElement): (QueryCommand, List[NamedValueElement]) = {
    val formattedQuery = getFormattedQuery(projection)
    val namedValues = NamedValueGatherer.gather(projection.select)
    val command = QueryCommand(formattedQuery, namedValues.map(v => QueryParameter(v.name, v.rowTypeInfo)))
    (command, namedValues)
  }
}

object DbQueryTreeReducerBase {
  object TypeInfos {
    val RelationKeyAny = TypeInfo(classOf[RelationKey[_]], TypeInfo.ANY)
    val ListCompanion = TypeInfo(List.getClass)
    val ProviderRelation = TypeInfo(classOf[ProviderRelation])
    val MapCompanion = TypeInfo(immutable.Map.getClass)
    val ClassEntityInfo = TypeInfo(classOf[ClassEntityInfo])
    val QueryProvider = TypeInfo(classOf[QueryProvider])
    val MapStringAny = TypeInfo(classOf[Map[_, _]], TypeInfo.STRING, TypeInfo.ANY)
    val DataProvider = TypeInfo(classOf[DataProvider])
    val OptionCompanion = TypeInfo(Option.getClass)
    val DbInputStreamCompanion = TypeInfo(DbPickledInputStream.getClass)
    val DbInputStream = TypeInfo(classOf[DbPickledInputStream])
    val FieldReader = TypeInfo(classOf[FieldReader])
    val PickledInputStream = TypeInfo(classOf[PickledInputStream])
    val ProviderCompanion = TypeInfo(IterableProvider.getClass)
    val Provider = ElementFactory.constant(IterableProvider, ProviderCompanion)

    def query(t: TypeInfo[_]): TypeInfo[_] = TypeInfo(classOf[Query[_]], t)
    def either(t1: TypeInfo[_], t2: TypeInfo[_]): TypeInfo[_] = TypeInfo(classOf[Either[_, _]], t1, t2)
    def node(t: TypeInfo[_]): TypeInfo[_] = TypeInfo(classOf[Node[_]], t)
    def function1(t: TypeInfo[_], r: TypeInfo[_]): TypeInfo[_] = TypeInfo(classOf[Function1[_, _]], t, r)
  }

  private class ReaderClosureRewriter(reader: ParameterElement) extends DbQueryTreeVisitor {
    import scala.collection.mutable

    private[this] var closureDepth = 0
    private val readOpList = new mutable.ListBuffer[RelationElement]
    private val paramList = new mutable.ListBuffer[ParameterElement]

    override def handleLambda(lambda: LambdaElement): RelationElement = {
      closureDepth += 1
      val l = super.handleLambda(lambda)
      closureDepth -= 1
      l
    }

    override def handleFuncCall(func: FuncElement): RelationElement = {
      // see details at OPTIMUS-16943
      if (closureDepth > 0 && (func.instance eq reader)) {
        val md = func.callee.asInstanceOf[MethodCallee].method
        if (md.name == "isNone" || md.name.startsWith("readOption") || md.name == "pickledInputStream") {
          val p = ElementFactory.parameter(s"${reader.parameterName}_${readOpList.size}", func.rowTypeInfo)
          paramList += p
          readOpList += func
          p
        } else {
          // replace readXXX with readOptionXXX
          val newReturnType = TypeInfo(classOf[Option[_]], func.rowTypeInfo)
          val p = ElementFactory.parameter(s"${reader.parameterName}_${readOpList.size}", newReturnType)
          paramList += p
          val newMethod = new RuntimeMethodDescriptor(
            md.declaringType,
            s"readOption${md.name.substring(4)}",
            newReturnType,
            md.genericArgTypes)
          readOpList += ElementFactory.call(func.instance, newMethod, func.arguments)
          val getMethod = new RuntimeMethodDescriptor(newReturnType, "get", func.rowTypeInfo)
          ElementFactory.call(p, getMethod, Nil)
        }
      } else super.handleFuncCall(func)
    }
  }

  object ReaderClosureRewriter {
    def rewrite(reader: ParameterElement, projector: RelationElement): RelationElement = {
      val rewriter = new ReaderClosureRewriter(reader)
      val proj = rewriter.visitElement(projector)
      val paramList = rewriter.paramList.result()
      val readOpList = rewriter.readOpList.result()
      if (paramList.isEmpty) proj
      else {
        val lam = ElementFactory.lambda(proj, paramList)
        val reflectType = TypeInfo(Typer.functionClass(paramList.size), paramList.map(_.rowTypeInfo): _*)
        val method = new RuntimeMethodDescriptor(reflectType, "apply", proj.rowTypeInfo)
        ElementFactory.call(lam, method, readOpList)
      }
    }
  }

  def liftLambda1(l: LambdaElement): RelationElement = {
    val sourceType = l.parameters(0).rowTypeInfo
    val targetType = l.body.rowTypeInfo
    val leftType = TypeInfos.function1(sourceType, TypeInfos.node(targetType))
    val rightType = TypeInfos.function1(sourceType, targetType)
    val returnType = TypeInfos.either(leftType, rightType)
    val asEither =
      new RuntimeMethodDescriptor(TypeInfo(Func.getClass), "asEither", returnType, List(sourceType, targetType))
    ElementFactory.call(null, asEither, List(l))
  }
}
