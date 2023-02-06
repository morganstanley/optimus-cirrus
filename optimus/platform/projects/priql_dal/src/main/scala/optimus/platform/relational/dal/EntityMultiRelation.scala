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
package optimus.platform.relational.dal

import optimus.utils.datetime.ZoneIds
import java.lang.reflect.Method
import java.time.ZonedDateTime

import msjava.slf4jutils.scalalog
import optimus.entity.ClassEntityInfo
import optimus.entity.EntityInfoRegistry
import optimus.entity.IndexInfo
import optimus.entity.StorableInfo
import optimus.platform._
import optimus.platform.dal._
import optimus.platform.dsi.bitemporal.DateTimeSerialization
import optimus.platform.relational._
import optimus.platform.relational.dal.core.RelationElementQuery
import optimus.platform.relational.execution._
import optimus.platform.relational.internal.OptimusCoreAPI._
import optimus.platform.relational.internal.TxTimeOrdering._
import optimus.platform.relational.internal._
import optimus.platform.relational.persistence.protocol._
import optimus.platform.relational.tree._
import optimus.platform.storable._

import scala.collection.mutable.ListBuffer

trait StorableClassElement {
  def classInfo: StorableInfo
  def rowTypeInfo: TypeInfo[_]
  def key: RelationKey[_]
}

/**
 * EntityDepot Query, it will get all entities of T at current loadContext if there is no query condition. It will throw
 * exception if it cannot support whereElement.
 */
class EntityStorableRelationElement[T](rowTypeInfo: TypeInfo[T], key: RelationKey[_])
    extends ProviderRelation(rowTypeInfo, key)
    with StorableClassElement
    with ProviderPersistenceSupport {

  override def getProviderName: String = "EntityProvider"
  override def serializerForThisProvider: ProviderPersistence = EntityProviderPersistence

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= s"$serial Provider:  '${getProviderName}'\n"
  }

  private[this] val log = scalalog.getLogger[EntityStorableRelationElement[_]]
  def entityClassName: String = rowTypeInfo.name

  override def makeKey(newKey: RelationKey[_]): EntityStorableRelationElement[T] =
    new EntityStorableRelationElement(rowTypeInfo, newKey)

  override def classInfo = EntityInfoRegistry.getClassInfo(rowTypeInfo.runtimeClassName)
}

sealed class TxTimeExtracting(val ordering: TxTimeOrdering, val numRows: ConstValueElement)

class EntityMultiRelation[T <: Entity](
    val classEntityInfo: ClassEntityInfo,
    val whereElement: ExpressionListElement,
    val filterLambdaElement: List[FuncElement],
    val extractInfo: TxTimeExtracting,
    rowTypeInfo: TypeInfo[T],
    key: RelationKey[_],
    pos: MethodPosition,
    val executionReferenceContext: Boolean = false)(implicit val dalApi: DalAPI)
    extends ProviderRelation(rowTypeInfo, key, pos)
    with IterativeExecNodeProvider[T]
    with StorableClassElement
    with ProviderPersistenceSupport {

  val memberGetterMap: Map[String, Method] = classEntityInfo.runtimeClass.getMethods.iterator
    .filter(m => m.getParameterTypes.length == 0)
    .map(m => (m.getName, m))
    .toMap
  // we fetch entity class name from TypeInfo.runtimeClass to avoid RelTreeDecoder fails to decode classes of TypeInfo from byte array.
  val encodedEntityClassName = rowTypeInfo.runtimeClass.getName

  if (whereElement ne null)
    whereElement.exprList.foreach(be =>
      verifyServerSideBinaryExpressionElement(this, be.asInstanceOf[BinaryExpressionElement]))

  def classInfo = classEntityInfo

  def entityClass = classEntityInfo.runtimeClass

  override def getExecutableVersion(): IterativeExecNode[T] = new EntityExecNode[T](this, rowTypeInfo, key, pos)

  override def getProviderName: String = "EntityProvider"

  // Note - Ordering is important - UntypeReducer must be applied before Entity reducer
  override def reducersFor(category: ExecutionCategory): List[ReducerVisitor] =
    List(new UntypeReducer, new EntityTreeReducer)

  override def serializerForThisProvider: ProviderPersistence = EntityProviderPersistence

  override def makeKey(newKey: RelationKey[_]) =
    new EntityMultiRelation(
      classEntityInfo,
      whereElement,
      filterLambdaElement,
      extractInfo,
      rowTypeInfo,
      newKey,
      pos,
      executionReferenceContext)

  /**
   * create a copy EntityMultiRelation with given BinaryExpression
   */
  private[optimus] def copyWithExtraFilters(
      extraWhere: List[ExpressionElement],
      extraFilters: List[FuncElement]): EntityMultiRelation[T] = {
    val originalExprList = if (whereElement == null || whereElement.exprList == null) Nil else whereElement.exprList
    val originalFuncList = if (filterLambdaElement == null) Nil else filterLambdaElement

    val newExprList = new ExpressionListElement(originalExprList ++ extraWhere)
    val newFuncList = originalFuncList ++ extraFilters

    new EntityMultiRelation(
      classEntityInfo,
      newExprList,
      newFuncList,
      extractInfo,
      rowTypeInfo,
      key,
      pos,
      executionReferenceContext)
  }

  private[optimus] def copyWithExtractOrder(isAsc: Boolean, numRows: ConstValueElement): EntityMultiRelation[T] = {
    if (isAsc)
      new EntityMultiRelation(
        classEntityInfo,
        whereElement,
        filterLambdaElement,
        new TxTimeExtracting(TxTimeAscOrdering, numRows),
        rowTypeInfo,
        key,
        pos)
    else
      new EntityMultiRelation(
        classEntityInfo,
        whereElement,
        filterLambdaElement,
        new TxTimeExtracting(TxTimeDescOrdering, numRows),
        rowTypeInfo,
        key,
        pos)

  }

  /**
   * return value means whether we met IndexedMemberElement during parsing, which will be used when we met ||.
   */
  private def verifyServerSideBinaryExpressionElement(
      entityElement: EntityMultiRelation[_],
      expr: BinaryExpressionElement): Boolean = {
    import BinaryExpressionType._
    expr.op match {
      case BOOLAND                     => verifyBoolAndBinaryExpression(entityElement, expr)
      case BOOLOR                      => verifyBoolOrBinaryExpression(entityElement, expr)
      case EQ | NE | LT | LE | GT | GE => verifyArithmeticBinaryExpression(entityElement, expr)
      case ITEM_IS_IN | ITEM_IS_NOT_IN => verifyInClauseExpression(entityElement, expr)
      case _ => throw new UnsupportedOperationException("" + expr.op + " is not supported in Entity query.")
    }
  }

  /**
   * don't allow index filter on either side of ||, e.g. indexFilter || nonIndexFilter , indexFilter && (indexFilter ||
   * nonIndexFilter) only nonIndexFilter is allowed on both sides of || since on server side logic, we get entities
   * based on returned index entries, which is a AND relationship
   *
   * so here it will throw an exception and goes back to super.handleMethod(element)
   */
  private def verifyBoolOrBinaryExpression(
      entityElement: EntityMultiRelation[_],
      expr: BinaryExpressionElement): Boolean = {
    val l = verifyServerSideBinaryExpressionElement(entityElement, expr.left.asInstanceOf[BinaryExpressionElement])
    val r = verifyServerSideBinaryExpressionElement(entityElement, expr.right.asInstanceOf[BinaryExpressionElement])
    if (l == true || r == true)
      throw new RelationalException("only nonIndexFilter is allowed on both sides of ||")
    else false
  }

  private def verifyBoolAndBinaryExpression(
      entityElement: EntityMultiRelation[_],
      expr: BinaryExpressionElement): Boolean = {
    val l = verifyServerSideBinaryExpressionElement(entityElement, expr.left.asInstanceOf[BinaryExpressionElement])
    val r = verifyServerSideBinaryExpressionElement(entityElement, expr.right.asInstanceOf[BinaryExpressionElement])
    l || r
  }

  private def verifyInClauseExpression(entityElement: EntityMultiRelation[_], expr: BinaryExpressionElement): Boolean =
    (expr.left, expr.right) match {
      // Indexed filter queries are run server side if enabled.
      case (member @ MemberElement(_, memberName), ExpressionListElement(exprList))
          if (EntityTreeReducerUtils.isIndexedMetadata(member.member.metadata)) =>
        if (!entityElement.memberGetterMap.contains(memberName))
          throw new IllegalStateException(
            "Cannot find member " + memberName + " in entity " + entityElement.classEntityInfo.runtimeClass)

        exprList foreach {
          case ExpressionListElement(list) => {
            list foreach {
              case c: ConstValueElement => ()
              case _ => throw new RelationalException("Server side queries only support ConstValueElement in In Clause")
            }
          }
          case _ => throw new RelationalException("Does not support Non-ExpressionListElement in In Clause")
        }
        true
      case (MemberElement(_, memberName), ExpressionListElement(exprList)) =>
        if (!entityElement.memberGetterMap.contains(memberName))
          throw new IllegalStateException(
            "Cannot find member " + memberName + " in entity " + entityElement.classEntityInfo.runtimeClass)

        exprList foreach {
          case t: ConstValueElement => ()
          case _                    => throw new RelationalException("Does not support Non-ConstElement in In Clause")
        }
        false
      case _ =>
        throw new RelationalException("Does not support binary expression " + expr)

    }

  private def verifyArithmeticBinaryExpression(
      entityElement: EntityMultiRelation[_],
      expr: BinaryExpressionElement): Boolean = {
    def checkMember(member: MemberElement) = {
      if (!entityElement.memberGetterMap.contains(member.memberName))
        throw new IllegalStateException(
          "Cannot find member " + member.memberName + " in entity " + entityElement.classEntityInfo.runtimeClass)
    }

    (expr.left, expr.right) match {
      case (ExpressionListElement(exprList), right: MemberElement)
          if (EntityTreeReducerUtils.isIndexedMetadata(right.member.metadata)) => {
        checkMember(right)
        true
      }
      case (left: MemberElement, ExpressionListElement(exprList))
          if (EntityTreeReducerUtils.isIndexedMetadata(left.member.metadata)) => {
        checkMember(left)
        true
      }
      case (left: MemberElement, right: ConstValueElement) => {
        checkMember(left)
        false
      }
      case (left: ConstValueElement, right: MemberElement) => {
        checkMember(right)
        false
      }
      case _ => throw new RelationalException("Does not support binary expression " + expr)
    }
  }

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= s"$serial Provider:  '${getProviderName}'\n"
  }
}

object EntityMultiRelation {
  def apply[A <: Entity: TypeInfo](classEntityInfo: ClassEntityInfo, key: RelationKey[_], pos: MethodPosition)(implicit
      dalApi: DalAPI): EntityMultiRelation[A] = {
    new EntityMultiRelation[A](classEntityInfo, null, null, null, implicitly[TypeInfo[A]], key, pos)
  }

  def unapply(emr: EntityMultiRelation[_]) = Some(emr.classEntityInfo, emr.key, emr.pos)
}

//-----------------------------------------------------------------------------------------------------------
//  Define the Entity query's execute node
//-----------------------------------------------------------------------------------------------------------

/**
 * EntityExecNode is the executable query via DAL
 */
object EntityExecNode {
  private val logger = scalalog.getLogger[EntityExecNode[_]]
}

class EntityExecNode[T <: Entity](
    val entityRelation: EntityMultiRelation[T],
    rowTypeInfo: TypeInfo[T],
    key: RelationKey[_],
    pos: MethodPosition)(implicit dalApi: DalAPI)
    extends IterativeExecNode[T](rowTypeInfo, key, pos)
    with DALReferenceExecNodeExperimental[T]
    with DALReferenceExecNode[T] {

  import EntityExecNode.logger

  def normalize(left: RelationElement, right: RelationElement): Option[(MemberElement, RelationElement)] =
    (left, right) match {
      // Index lookup must be on concrete value
      case (_: MemberElement, _: MemberElement) => None
      case (l: MemberElement, _)                => Some(l, right)
      case (_, r: MemberElement)                => Some(r, left)
      case _                                    => None
    }

  override def fillQueryExplainItem(level_id: Integer, table: ListBuffer[QueryExplainItem]): Unit = {
    table += new QueryExplainItem(level_id, "EntityExecNode", "-", "DALScan", 0)
  }

  override protected def shouldExecuteDistinct = false

  @node override protected def doCreateIterable(dependencies: IndexedSeq[Iterable[_]]): Iterable[T] = {
    val loadContext = dalApi.loadContext

    (entityRelation.whereElement, entityRelation.extractInfo) match {
      case (null, null) =>
        logger.warn(
          "No filter condition and extract condition found in the query of Entity {}, Relational will load all entities from DAL to client.",
          entityRelation.classEntityInfo.runtimeClass
        )
        val cl = entityRelation.entityClass.asInstanceOf[Class[T]]
        EvaluationContext.env.entityResolver
          .asInstanceOf[EntityResolverReadImpl]
          .findEntitiesByClassWithContext(cl, loadContext)
      case _ =>
        logger.info(
          "Index filter conditions found in the query of Entity {}, executing on server.",
          entityRelation.classEntityInfo.runtimeClass)
        executeDSIServerQuery(loadContext)
    }
  }

  /**
   * BinaryExpressionElement should be one == expression, so we use reflection to get data from Entity and check it.
   */
  private def checkEquals(entity: T, b: BinaryExpressionElement): Boolean = {
    require(b.op == BinaryExpressionType.EQ, "Not supported operation: " + b.op + ", only support ==")

    val newBin = normalize(b.left, b.right)
    val (member, freevar) =
      if (newBin == None)
        throw new IllegalStateException(
          "Unexpected element: " + b + ", does not find Member in lambda element, e.g. filter( t=> t.m==1)")
      else (newBin.get._1, getConstOrFreevar(newBin.get._2))

    val method = entityRelation.memberGetterMap(member.memberName)
    if (method == null)
      throw new IllegalStateException(
        "Cannot find member " + member.memberName + " in entity " + entityRelation.classEntityInfo.runtimeClass)
    freevar == method.invoke(entity)
  }

  @node protected def executeDSIServerQuery(loadContext: TemporalContext): Iterable[T] =
    withRethrow {
      logger.debug("Run Priql DAL server query {}", this.entityRelation.whereElement)
      executeDALQuery(loadContext)
    } { nr =>
      logger.error("Failed to execute dal server query. error: {}", nr.exception)
    }
  @node def executeDALQuery(loadContext: TemporalContext): Iterable[T] =
    EvaluationContext.env.entityResolver.enumerateQuery[T](
      new RelationElementQuery(serverQueryTree),
      Seq(entityRelation.classEntityInfo.runtimeClass.getName()),
      loadContext)

  @node override def createReferences: Iterable[ReferenceHolder[T]] = executeDALReferenceQuery(dalApi.loadContext)

  @node override def streamProcessResults: AsyncIterator[T] = {
    val references = this.executeDALReferenceQuery(dalApi.loadContext)
    new EntityExecNodeIterator(references)
  }

  private[optimus] class EntityExecNodeIterator(val holders: Iterable[ReferenceHolder[T]])
      extends FlatMapPriqlGrouppedIterator[ReferenceHolder[T], T](
        null,
        Integer.getInteger("optimus.priql.entityExecNode.executeReference.batchsize", 1000)) {
    private val groupedHoldersIter: Iterator[Iterable[ReferenceHolder[T]]] =
      holders.grouped(Integer.getInteger("optimus.priql.entityExecNode.executeReference.batchsize", 1000))
    @node override def hasNextBatch: Boolean = groupedHoldersIter.hasNext
    @node override def nextBatch: Iterable[T] = {
      val res = groupedHoldersIter.next()
      val r = res.apar.map(_.payload)
      r
    }
  }

  def serverQueryTree: MultiRelationElement = {

    (entityRelation.whereElement, entityRelation.extractInfo) match {
      case (null, null) => {
        this.entityRelation
      }
      case (null, _) => {
        val isAsc =
          if (entityRelation.extractInfo.ordering == TxTimeAscOrdering) new ConstValueElement(true)
          else new ConstValueElement(false)
        LegacyElementFactory.extract(
          this.entityRelation,
          "lam_param_name",
          new ExpressionListElement(isAsc :: List(entityRelation.extractInfo.numRows)),
          null,
          this.entityRelation.key,
          null,
          pos)
      }
      case (_, null) => {
        val filterElement = this.entityRelation.whereElement.exprList.tail
          .foldLeft(this.entityRelation.whereElement.exprList.head)((acc, next) =>
            new BinaryExpressionElement(BinaryExpressionType.BOOLAND, acc, next, TypeInfo.BOOLEAN))
          .asInstanceOf[BinaryExpressionElement]
        LegacyElementFactory.where(
          this.entityRelation,
          "lam_param_name",
          new ExpressionListElement(filterElement :: Nil),
          this.entityRelation.key,
          pos)
      }
      case (_, _) => {
        val filterElement = this.entityRelation.whereElement.exprList.tail
          .foldLeft(this.entityRelation.whereElement.exprList.head)((acc, next) =>
            new BinaryExpressionElement(BinaryExpressionType.BOOLAND, acc, next, TypeInfo.BOOLEAN))
          .asInstanceOf[BinaryExpressionElement]
        val isAsc =
          if (entityRelation.extractInfo.ordering == TxTimeAscOrdering) new ConstValueElement(true)
          else new ConstValueElement(false)
        val methodElement = LegacyElementFactory.where(
          this.entityRelation,
          "lam_param_name",
          new ExpressionListElement(filterElement :: Nil),
          this.entityRelation.key,
          pos)
        LegacyElementFactory.extract(
          methodElement,
          "lam_param_name",
          new ExpressionListElement(isAsc :: List(entityRelation.extractInfo.numRows)),
          null,
          this.entityRelation.key,
          null,
          pos)
      }
    }
  }

  @node private[optimus /*platform*/ ] def executeDALReferenceQuery(
      loadContext: TemporalContext): Iterable[ReferenceHolder[T]] = {
    logger.info("Run Priql Mongo server Reference Query {}", serverQueryTree)
    val res = EvaluationContext.env.entityResolver.enumerateReferenceQuery[T](
      new RelationElementQuery(serverQueryTree),
      Seq(this.entityRelation.classEntityInfo.runtimeClass.getName),
      loadContext)
    res
  }

  private def getConstOrFreevar(b: RelationElement): Any = b match {
    case ConstValueElement(value, _) => value
    case _                           => throw new IllegalStateException("Unexpected element: " + b)
  }

  override protected def doCreateColumns(): RelationColumnView =
    RelationalUtils.convertEntityRowDataToColumnData(doCreateIterable(Vector.empty))(rowTypeInfo)

}

object EntityTreeReducerUtils {

  def isIndexedMetadata(metadata: Map[String, Any]): Boolean = {
    metadata.nonEmpty && metadata.contains("unique") && metadata
      .contains("indexed") && metadata.contains("typeName") && metadata.contains("propertyNames")
  }

  // If the constant expression of an index filter is an entity then the entities load context must match the
  private def validateIndexConstValueElement(element: RelationElement)(implicit dalApi: DalAPI): Unit = element match {
    case ConstValueElement(value, _) => {
      value match {
        case entity: Entity if !entity.dal$isTemporary && entity.dal$temporalContext != dalApi.loadContext =>
          throw new InvalidTemporalContextException(
            s"Temporal context of entity in index filter condition must equal the current load context. ${entity}")
        case _ =>
      }
    }
    case _ =>
      throw new RelationalException(
        s"Index filter expression ConstValueElement contains invalid relational element of type ${element.getClass.getName}")
  }

  def validateIndexConstValue(element: RelationElement)(implicit dalApi: DalAPI): Unit = element match {
    case c: ConstValueElement            => validateIndexConstValueElement(c)
    case ExpressionListElement(exprList) => exprList.foreach(validateIndexConstValueElement)
    case _ =>
      throw new RelationalException(
        s"Index filter BinaryExpressionElement contains invalid constant type ${element.getClass.getName}")
  }

  private def visitBinaryExpressionElement(expr: ExpressionElement): List[BinaryExpressionElement] = expr match {
    case binaryExpressionElement @ BinaryExpressionElement(op, left, right, _) =>
      op match {
        case BinaryExpressionType.BOOLOR =>
          throw new UnsupportedOperationException("Bool operator || is not supported in Entity query, only support &&.")
        case BinaryExpressionType.BOOLAND => {
          val leftAndExpList = visitBinaryExpressionElement(left.asInstanceOf[BinaryExpressionElement])
          val rightAndExpList = visitBinaryExpressionElement(right.asInstanceOf[BinaryExpressionElement])
          leftAndExpList ::: rightAndExpList
        }
        case BinaryExpressionType.EQ | BinaryExpressionType.ITEM_IS_IN => {
          require(left.isInstanceOf[MemberElement] || right.isInstanceOf[MemberElement])
          binaryExpressionElement :: Nil
        }
        case _ => throw new UnsupportedOperationException("" + op + " is not supported in Entity query.")
      }
  }

  private def checkAndAddTypeInfo(memberElement: MemberElement, newTypeInfo: TypeInfo[_]) = memberElement match {
    case MemberElement(ParameterElement(parameterName, typeDescriptor), memberName)
        if typeDescriptor <:< classOf[DynamicObject] =>
      Option(new MemberElement(new ParameterElement(parameterName, newTypeInfo), memberName))
    case _ => None
  }

  /**
   * Right stands for MemberElement with index metadata
   */
  def normalize(
      binExpr: BinaryExpressionElement): Either[(MemberElement, RelationElement), (MemberElement, RelationElement)] = {
    binExpr match {
      case BinaryExpressionElement(_, memberElement: MemberElement, constElement: RelationElement, _) =>
        if (isIndexedMetadata(memberElement.member.metadata)) Right((memberElement, constElement))
        else Left((memberElement, constElement))
      case BinaryExpressionElement(_, constElement: RelationElement, memberElement: MemberElement, _) =>
        if (isIndexedMetadata(memberElement.member.metadata)) Right((memberElement, constElement))
        else Left((memberElement, constElement))
      case _ =>
        throw new RelationalException(
          s"left side or right side of BinaryExpressionElement is not either MemberElement but: ${binExpr.left.getClass}")
    }
  }

  def reType(binExpr: BinaryExpressionElement, entityTypeInfo: TypeInfo[_]) = {
    binExpr match {
      case BinaryExpressionElement(op, memberElement: MemberElement, constElement: RelationElement, resTypeInfo) =>
        checkAndAddTypeInfo(memberElement, entityTypeInfo)
          .map(processedMemberElement =>
            new BinaryExpressionElement(op, processedMemberElement, constElement, resTypeInfo))
          .getOrElse(binExpr)
      case BinaryExpressionElement(op, constElement: RelationElement, memberElement: MemberElement, resTypeInfo) =>
        checkAndAddTypeInfo(memberElement, entityTypeInfo)
          .map(processedMemberElement =>
            new BinaryExpressionElement(op, processedMemberElement, constElement, resTypeInfo))
          .getOrElse(binExpr)
      case _ => binExpr
    }
  }

  def flattenAndSplitBinaryExpression(
      expr: ExpressionElement): (List[BinaryExpressionElement], List[BinaryExpressionElement]) =
    visitBinaryExpressionElement(expr).partition { e =>
      normalize(e).isRight
    }
}

//-----------------------------------------------------------------------------------------------------------
//  Define  Entity query's Tree Reducer
//-----------------------------------------------------------------------------------------------------------
trait EntityTreeReducerCommon { self: QueryTreeVisitor with FilterBuilder =>

  def getFilterExpressions(binExpr: BinaryExpressionElement, entityTypeInfo: TypeInfo[_]) =
    EntityTreeReducerUtils.flattenAndSplitBinaryExpression(
      this.visitElement(EntityTreeReducerUtils.reType(binExpr, entityTypeInfo)).asInstanceOf[ExpressionElement])

  def reduceImpl(tree: RelationElement): RelationElement = self.visitElement(tree)

  def processExpressionList(
      expressionListElement: ExpressionListElement): (Option[FuncElement], Option[BinaryExpressionElement]) =
    expressionListElement match {
      case FuncExpressionListElement(Some(List(func)), Some(List(binExpr: BinaryExpressionElement)), _, _) =>
        (Some(func), Some(binExpr))
      case FuncExpressionListElement(None, Some(List(binExpr: BinaryExpressionElement)), _, _) => (None, Some(binExpr))
      case _                                                                                   => (None, None)
    }

  def processOrderingExprList(orderingExprList: ExpressionListElement): (Option[Boolean], Option[Boolean]) = {
    val order = orderingExprList.exprList.head.asInstanceOf[ConstValueElement].value
    order match {
      case TxTimeAscOrdering  => (Some(true), Some(true))
      case TxTimeDescOrdering => (Some(true), Some(false))
      case _                  => (Some(false), None)
    }
  }

  def buildBoolQuery(binExpr: BinaryExpressionElement): BinaryExpressionElement = {
    val left = self.visitElement(binExpr.left)
    val right = self.visitElement(binExpr.right)
    if (left == binExpr.left && right == binExpr.right) {
      binExpr
    } else {
      new BinaryExpressionElement(binExpr.op, left, right, TypeInfo.BOOLEAN)
    }
  }

  /**
   * Format ZonedDateTime to Long
   */
  def formatTimeStamp(zdt: ZonedDateTime): Long = {
    val asUtc = zdt.withZoneSameInstant(ZoneIds.UTC)
    val utcLocal = DateTimeSerialization.fromInstant(asUtc.toInstant)
    utcLocal
  }

  /**
   * convert any member refs which access an @indexed field or @key to indexed member refs. fetch constValueElement and
   * convert to SerializedKey using memberName.
   *
   * We have to do this here since on server side, EntityInfoRegistry may not contain the Entity class information
   */
  def buildMemberRef(binExpr: BinaryExpressionElement): BinaryExpressionElement = {
    EntityTreeReducerUtils.normalize(binExpr) match {
      case Left((memberElement, relationElement)) => {
        if (memberElement.instanceProvider != null) {

          val targetType = memberElement.instanceProvider.rowTypeInfo

          if (targetType != null && targetType <:< classOf[Entity]) {
            val companion = EntityInfoRegistry.getCompanion(targetType.clazz)

            companion.info.indexes.find(t =>
              t.queryable && ((t.propertyNames == Seq(
                memberElement.memberName)) || (t.name == memberElement.memberName))) match {
              case Some(indexOpt) => {
                val (newIndexedElement, newConstValueElement) = relationElement match {
                  case constValueElement: ConstValueElement =>
                    buildArithmeticClause(memberElement, constValueElement, indexOpt)
                  case expressionListElement: ExpressionListElement =>
                    buildInClause(memberElement, expressionListElement, indexOpt)
                  // we may have query a < b + 3 which a and b is field of Entity class
                  case otherMemberElement: MemberElement                => (memberElement, relationElement)
                  case binaryExpressionElement: BinaryExpressionElement => (memberElement, relationElement)
                  case _ =>
                    throw new RelationalException(
                      s"right side of BinaryExpressionElement is not ConstValueElement or ExpressionListElement or BinaryExpressionElement or MemberElement but: ${binExpr.left.getClass}")
                }
                new BinaryExpressionElement(binExpr.op, newIndexedElement, newConstValueElement, binExpr.rowTypeInfo)
              }
              case None => binExpr
            }
          } else {
            binExpr
          }
        } else {
          binExpr
        }
      }
      case Right(_) => binExpr
    }
  }

  def getSerializedKey(indexInfo: IndexInfo[_, _], propValue: Any): SerializedKey = {
    if (indexInfo.propertyNames == null || indexInfo.propertyNames.isEmpty)
      throw new IllegalArgumentException("index can only contain one field or multiple fields")
    val key = indexInfo.asInstanceOf[IndexInfo[_, Any]].makeKey(propValue)
    val r = key.toSerializedKey
    r
  }

  def getMemberElementWithIndexMeta(indexInfo: IndexInfo[_, _], element: MemberElement): MemberElement = {
    val meta = Map[String, Any](
      "unique" -> indexInfo.unique,
      "indexed" -> indexInfo.indexed,
      "refFilter" -> indexInfo.queryByEref,
      "typeName" -> indexInfo.storableClass.getName,
      "propertyNames" -> indexInfo.propertyNames
    )
    val newMemberDescriptor = TypeDescriptor.getMemberDescriptorWithMetadata(element.member, meta.toMap)
    new MemberElement(element.instanceProvider, element.memberName, newMemberDescriptor)
  }

}

trait FilterBuilder {
  def buildArithmeticClause(
      element: MemberElement,
      constElement: ConstValueElement,
      indexInfo: IndexInfo[_, _]): (MemberElement, RelationElement)
  def buildInClause(
      element: MemberElement,
      exprListElement: ExpressionListElement,
      indexInfo: IndexInfo[_, _]): (MemberElement, RelationElement)
}

/**
 * EntityTreeReducer reduces the tree to bind the where element of Entity query.
 */
class EntityTreeReducer(implicit dalApi: DalAPI)
    extends NonRepetitiveMethodTreeVisitor
    with ReducerVisitor
    with FilterBuilder
    with EntityTreeReducerCommon {

  private[this] val log = scalalog.getLogger[EntityTreeReducer]

  override def reduce(tree: RelationElement, executeOptions: ExecuteOptions): RelationElement = reduceImpl(tree)

  override def handleBinaryExpression(binExpr: BinaryExpressionElement): RelationElement = {
    binExpr.op match {
      case BinaryExpressionType.BOOLAND => buildBoolQuery(binExpr)
      case BinaryExpressionType.BOOLOR  => buildBoolQuery(binExpr)
      case BinaryExpressionType.EQ      => buildMemberRef(binExpr)
      case BinaryExpressionType.NE      => buildMemberRef(binExpr)
      case BinaryExpressionType.LT      => buildMemberRef(binExpr)
      case BinaryExpressionType.LE      => buildMemberRef(binExpr)
      case BinaryExpressionType.GT      => buildMemberRef(binExpr)
      case BinaryExpressionType.GE      => buildMemberRef(binExpr)
      case BinaryExpressionType.PLUS | BinaryExpressionType.MINUS | BinaryExpressionType.MUL |
          BinaryExpressionType.DIV | BinaryExpressionType.MODULO | BinaryExpressionType.ASSIGN =>
        super.handleBinaryExpression(binExpr)
      case BinaryExpressionType.ITEM_IS_IN     => buildMemberRef(binExpr)
      case BinaryExpressionType.ITEM_IS_NOT_IN => buildMemberRef(binExpr)
      case _ => throw new RelationalUnsupportedException(s"Do not support query operator: ${binExpr.op}")
    }
  }

  override def handleMethod(method: MethodElement): RelationElement = {
    method.methodCode match {
      case QueryMethod.WHERE   => bindWhere(method)
      case QueryMethod.EXTRACT => bindExtract(method)
      case _                   => super.handleMethod(method)
    }
  }

  protected def bindExtract(extract: MethodElement): RelationElement = {
    try {
      extract match {
        case ExtractMethodElement(origSrc, orderingExprList, lambdaExprList, numRows) => {
          (visitElement(origSrc), processOrderingExprList(orderingExprList)) match {
            case (src: EntityMultiRelation[_], (Some(true), isAsc: Option[Boolean])) =>
              src.copyWithExtractOrder(isAsc.get, numRows)
            case _ => super.handleMethod(extract)
          }
        }
        case _ => super.handleMethod(extract)
      }
    } catch {
      case e: Exception => {
        log.info(
          s"reducer cannot reduce where element: ${extract.dbgPrint()} in EntityTreeReducer bindWhere, so will fall back to in memory execution.\n the reason comes from: ${e
              .getStackTrace()(0)} \n",
          e.getStackTrace()(0)
        )
        super.handleMethod(extract)
      }
    }
  }

  protected def bindWhere(where: MethodElement): RelationElement = {

    try {
      where match {
        // For execution on the server side we push all index filters adjacent to source onto the EntityMultiRelation
        case FilterMethodElement(origSrc, srcName, exprListElement, key, pos) => {
          (visitElement(origSrc), processExpressionList(exprListElement)) match {

            case (
                  UntypeMethodElement(src: EntityMultiRelation[_], dynamicExpr, typeInfo, key, pos),
                  (funcOpt, Some(binExpr))) =>
              val (indexExprList, nonIndexExprList) = getFilterExpressions(binExpr, src.typeInfo)

              (indexExprList, nonIndexExprList) match {
                // No indexed filters.
                case (Nil, _) =>
                  super.handleMethod(where)

                // All indexed filters
                case (_, Nil) =>
                  UntypeMethodElement(src.copyWithExtraFilters(indexExprList, Nil), dynamicExpr, typeInfo, key, pos)

                // Some indexed filters
                case (_, _) =>
                  if (funcOpt.isDefined) {
                    val source =
                      UntypeMethodElement(src.copyWithExtraFilters(indexExprList, Nil), dynamicExpr, typeInfo, key, pos)
                    val filter = new ExpressionListElement(funcOpt.toList)
                    FilterMethodElement(source, src.getProviderName, filter, key, pos)
                  } else {
                    val nonIndexExpr = nonIndexExprList.tail.foldLeft(nonIndexExprList.head)((left, right) =>
                      ElementFactory.andAlso(left, right))
                    val nonIndexLambda = liftNode { (row: Any) =>
                      ExpressionElementHelper.executeExpression(nonIndexExpr, row).asInstanceOf[Boolean]
                    }
                    val nonIndexFunc = new FuncElement(
                      new ScalaLambdaCallee(Left(nonIndexLambda), None, TypeInfo.BOOLEAN, Nil),
                      Nil,
                      null)
                    val filter = new ExpressionListElement(List(nonIndexFunc))
                    FilterMethodElement(
                      src.copyWithExtraFilters(indexExprList, Nil),
                      src.getProviderName,
                      filter,
                      key,
                      pos)
                  }
              }

            case (src: EntityMultiRelation[_], (funcOpt, Some(binExpr))) if src.extractInfo == null => {
              // if it is not null, we cannot continue to do the reduce since the results number may be different

              val (indexExprList, nonIndexExprList) = getFilterExpressions(binExpr, src.typeInfo)

              (indexExprList, nonIndexExprList) match {
                // No indexed filters.
                case (Nil, _) =>
                  super.handleMethod(where)

                // All indexed filters
                case (_, Nil) =>
                  src.copyWithExtraFilters(indexExprList, funcOpt.toList)

                // Some indexed filters
                case (_, _) =>
                  if (funcOpt.isDefined) {
                    val source = src.copyWithExtraFilters(indexExprList, Nil)
                    val filter = new ExpressionListElement(funcOpt.toList)
                    FilterMethodElement(source, src.getProviderName, filter, key, pos)
                  } else {
                    val nonIndexExpr = nonIndexExprList.tail.foldLeft(nonIndexExprList.head)((left, right) =>
                      ElementFactory.andAlso(left, right))
                    val nonIndexLambda = liftNode { (row: Any) =>
                      ExpressionElementHelper.executeExpression(nonIndexExpr, row).asInstanceOf[Boolean]
                    }
                    val nonIndexFunc = new FuncElement(
                      new ScalaLambdaCallee(Left(nonIndexLambda), None, TypeInfo.BOOLEAN, Nil),
                      Nil,
                      null)
                    val filter = new ExpressionListElement(List(nonIndexFunc))
                    FilterMethodElement(
                      src.copyWithExtraFilters(indexExprList, Nil),
                      src.getProviderName,
                      filter,
                      key,
                      pos)
                  }
              }
            }
            case _ => super.handleMethod(where)
          }
        }
        case _ => super.handleMethod(where)
      }
    } catch {
      case e: InvalidTemporalContextException => {
        log.error("Invalid entity filter expression.")
        throw e
      }
      case e: Exception => {
        log.info(
          s"reducer cannot reduce where element: ${where.dbgPrint()} in EntityTreeReducer bindWhere, so will fall back to in memory execution.\n the reason comes from: ${e
              .getStackTrace()(0)} \n",
          e.getStackTrace()(0)
        )
        super.handleMethod(where)
      }
    }
  }

  def buildInClause(
      element: MemberElement,
      exprListElement: ExpressionListElement,
      indexInfo: IndexInfo[_, _]): (MemberElement, RelationElement) = {
    EntityTreeReducerUtils.validateIndexConstValue(exprListElement)

    val serializedKeys: List[ExpressionListElement] = exprListElement.exprList.iterator.map { v =>
      serializeKey(v.asInstanceOf[ConstValueElement], indexInfo)
    }.toList

    val newIndexedMemberElement = getMemberElementWithIndexMeta(indexInfo, element)
    (newIndexedMemberElement, new ExpressionListElement(serializedKeys, TypeInfo.ANY))
  }

  def buildArithmeticClause(
      element: MemberElement,
      constElement: ConstValueElement,
      indexInfo: IndexInfo[_, _]): (MemberElement, RelationElement) = {
    EntityTreeReducerUtils.validateIndexConstValue(constElement)

    val serializedKey = serializeKey(constElement, indexInfo)

    val newIndexedMemberElement = getMemberElementWithIndexMeta(indexInfo, element)
    (newIndexedMemberElement, serializedKey)
  }

  private def serializeKey(elt: ConstValueElement, indexInfo: IndexInfo[_, _]): ExpressionListElement = {
    val serializedKey = getSerializedKey(indexInfo, elt.value)
    val propertyValues: List[ConstValueElement] = indexInfo.propertyNames.iterator.map { propName =>
      val propVal = serializedKey.properties(propName)
      new ConstValueElement(propVal, serializedValue = true)
    }.toList
    new ExpressionListElement(propertyValues, TypeInfo.ANY)
  }
}

/**
 * Moved from core/optimus.platform.relational.tree.UntypeReducer For new Query api, we no longer need to use this, move
 * here for backward compatibility for Relation api
 */
class UntypeReducer extends QueryTreeVisitor with ReducerVisitor {

  def reduce(tree: RelationElement, executeOptions: ExecuteOptions) = visitElement(tree)

  def canExecuteBinaryExpressionUntyped(b: RelationElement): Boolean = b match {
    case BinaryExpressionElement(op, left, right, _) =>
      op match {
        case BinaryExpressionType.BOOLAND | BinaryExpressionType.BOOLOR =>
          canExecuteBinaryExpressionUntyped(left) && canExecuteBinaryExpressionUntyped(right)
        case BinaryExpressionType.EQ | BinaryExpressionType.ITEM_IS_IN | BinaryExpressionType.ITEM_IS_NOT_IN => true
        case _                                                                                               => false
      }
  }

  def canReduceFilter(element: ExpressionListElement) = element.exprList match {
    case List(_, binaryExpression: BinaryExpressionElement) => canExecuteBinaryExpressionUntyped(binaryExpression)
    case _                                                  => false
  }

  override def handleMethod(method: MethodElement): RelationElement = method match {
    // Push untyped node towards provider past any filter nodes.
    case UntypeMethodElement(
          FilterMethodElement(filterSrc, filterSrcName, filterExpr, filterKey, filterPos),
          untypeExpr,
          projectedType,
          untypeKey,
          untypePos) if canReduceFilter(filterExpr) =>
      FilterMethodElement(
        visitElement(UntypeMethodElement(filterSrc, untypeExpr, projectedType, untypeKey, untypePos))
          .asInstanceOf[MultiRelationElement],
        filterSrcName,
        filterExpr,
        untypeKey,
        filterPos
      )
    case _ => super.handleMethod(method)
  }
}
