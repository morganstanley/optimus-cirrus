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

import java.lang.reflect.Modifier
import java.time.Duration
import java.time.Instant
import java.time._
import optimus.platform.CovariantSet
import optimus.platform.annotations.internal._projected
import optimus.platform.cm.Knowable
import optimus.platform.relational.PriqlSettings
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.DALBinder.ProjectionLocator
import optimus.platform.relational.dal.core.DALMappingEntity
import optimus.platform.relational.dal.core.ExpressionQuery
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.language.QueryDialect
import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.translation.ColumnCollector
import optimus.platform.relational.data.translation.QueryTreeChecker
import optimus.platform.relational.data.translation.UnusedColumnRemover
import optimus.platform.relational.data.tree._
import optimus.platform.relational.internal.RelationalUtils
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.util.ZonedDateTimeOps

import scala.collection.mutable

class DALLanguage(val lookup: MappingEntityLookup) extends QueryLanguage {
  import DALLanguage._

  override def createDialect(translator: QueryTranslator): QueryDialect = {
    new DALDialect(this, translator)
  }

  protected def canBeConditionalColumn(e: RelationElement): Boolean = {
    import DbElementType._
    import ElementType._
    // 'if (x) y else z' is also a supported column
    QueryTreeChecker.forall(
      e,
      ele =>
        ele.elementType match {
          case Column | Scalar | Exists | Aggregate | AggregateSubquery | TernaryExpression | BinaryExpression |
              ConstValue =>
            true
          case _ => false
        })
  }

  override def mustBeColumn(e: RelationElement): Boolean = {
    e match {
      case typeIs: TypeIsElement if isProjectedEntityTypeIs(typeIs) => true
      case conditional: ConditionalElement                          => canBeConditionalColumn(conditional)
      case _                                                        => super.mustBeColumn(e)
    }
  }

  override def canBeWhere(e: RelationElement): Boolean = {
    ExpressionChecker.canBeExpression(e, lookup)
  }

  override def canBeGroupBy(e: RelationElement): Boolean = {
    def isValid(elem: RelationElement): Boolean = {
      elem match {
        case c: ColumnElement =>
          PriqlSettings.enableGroupByCollection ||
          !(TypeInfo.isCollection(c.rowTypeInfo) || c.rowTypeInfo.typeParams.exists(TypeInfo.isCollection))
        case _: DALHeapEntityElement => true
        case c: ConditionalElement   => canBeConditionalColumn(c)
        case _                       => false
      }
    }

    def performCheck(elem: RelationElement): Boolean = elem match {
      case o: OptionElement => isValid(o.element) // we only allow OptionElement at top level
      case _                => isValid(elem)
    }

    canBeProjector(e) && {
      e match {
        case TupleElement(elements) => elements.forall(performCheck)
        case _                      => performCheck(e)
      }
    }
  }

  override def canBeAggregate(e: RelationElement): Boolean = {
    if (e eq null) true
    else {
      // Acc Priql only support aggregation on primitive type/ String/ BigDecimal.
      val clz = TypeInfo.underlying(e.rowTypeInfo).clazz
      val canBeAggType = clz match {
        case _ if clz.isPrimitive || clz == classOf[String] || clz == classOf[BigDecimal] => true
        case _                                                                            => false
      }
      canBeAggType && ExpressionChecker.canBeExpression(e, lookup)
    }
  }

  override def canBeOrderBy(e: RelationElement): Boolean = {
    def doCheck(elem: RelationElement): Boolean = {
      elem match {
        case FuncElement(callee: MethodCallee, arguments, null) if callee.name == "apply" =>
          val className = callee.resType.clazz.getName()
          if (className == "optimus.platform.relational.desc" || className == "optimus.platform.relational.asc")
            arguments.forall(e => doCheck(e))
          else false
        case TupleElement(elements) => elements.forall(doCheck)
        case _ => if (isOrderType(elem.rowTypeInfo)) ExpressionChecker.canBeExpression(elem, lookup) else false
      }
    }

    // Acc Priql only support “sortBy” on primitive type/ String / BigDecimal / DateTime.
    // Other type which implements Comparable can’t be executed at server side since the SQL sort result may not be expected.
    def isOrderType(rowTypeInfo: TypeInfo[_]): Boolean = {
      val clz = TypeInfo.underlying(rowTypeInfo).clazz
      clz match {
        case _ if clz.isPrimitive || clz == classOf[String] || clz == classOf[BigDecimal]           => true
        case _ if clz == classOf[LocalDate] || clz == classOf[LocalTime] || clz == classOf[Instant] => true
        case _                                                                                      => false
      }
    }

    doCheck(e)
  }

  override def canBeOn(e: RelationElement, methodCode: QueryMethod): Boolean = {
    if (methodCode == QueryMethod.FULL_OUTER_JOIN)
      canBeFullOuterJoinOn(e) && canBeWhere(e)
    else canBeWhere(e)
  }

  override def canBeSubqueryProjector(e: RelationElement): Boolean = {
    // make sure there is no sub query (ProjectionElement) in projector.
    (ColumnCollector.collect(e).size >= 1) && (new ProjectionLocator().locate(e).isEmpty)
  }

  // For 'FULL OUTER JOIN', 'on' must contain at least one 'equal' condition and no 'or' condition at first level.
  private def canBeFullOuterJoinOn(e: RelationElement): Boolean = {
    import BinaryExpressionType._
    e match {
      case BinaryExpressionElement(EQ, l, r, _) =>
        l.elementType != DbElementType.Option && r.elementType != DbElementType.Option
      case BinaryExpressionElement(BOOLAND, left, right, _) => canBeFullOuterJoinOn(left) || canBeFullOuterJoinOn(right)
      case _                                                => false
    }
  }

  override def shouldBeDistinct(clz: Class[_]): Boolean = {
    clz == classOf[Set[_]] || clz == classOf[CovariantSet[_]]
  }
}

private class DALDialect(lan: QueryLanguage, tran: QueryTranslator) extends QueryDialect(lan, tran) {
  override def translate(e: RelationElement): RelationElement = {
    // The element is visited and rewritten for several times so we define it as var.
    var ele = UnusedColumnRemover.remove(translator.mapping, e)
    ele = LinkageSubqueryRewriter.rewrite(language, ele)
    ele = super.translate(ele)
    ele = new EntityRefConverter().visitElement(ele)
    RedundantLeftJoinRemover.remove(ele)
  }

  override def format(e: RelationElement): ExpressionQuery = {
    DALFormatter.format(e)
  }

  /**
   * This is used to convert Column(dal$entityRef) to Func(toEntity(Column(dal$entityRef))) if this column is used in
   * DALHeapEntity of projector. An additional "dal$entityRef_keep" column will be added if the column is also used
   * other than DALHeapEntity of projector.
   */
  class EntityRefConverter extends DbQueryTreeVisitor {
    val convertedRefColumns = new mutable.HashMap[TableAlias, mutable.HashSet[String]]
    val keepRefColumns = new mutable.HashMap[TableAlias, mutable.HashSet[String]]
    private val outerAliases = mutable.HashSet[TableAlias]()

    protected override def handleProjection(proj: ProjectionElement): RelationElement = {
      outerAliases += proj.select.alias
      val projector = visitElement(proj.projector)
      outerAliases -= proj.select.alias
      val s = rewriteSelect(proj.select)
      val select = visitElement(s).asInstanceOf[SelectElement]
      updateProjection(proj, select, projector, proj.aggregator)
    }

    private def rewriteSelect(s: SelectElement): SelectElement = {
      val convertedNames = convertedRefColumns.getOrElse(s.alias, mutable.HashSet.empty)
      val keepNames = keepRefColumns.getOrElse(s.alias, mutable.HashSet.empty)

      if (convertedNames.isEmpty && keepNames.isEmpty) s
      else {
        val newCols = new mutable.ListBuffer[ColumnDeclaration]

        s.columns.foreach {
          case decl @ ColumnDeclaration(name, c: ColumnElement) if convertedNames.contains(name) =>
            if (keepNames.contains(name)) {
              val keepCol = ColumnDeclaration(name.concat("_keep"), c)
              newCols.append(keepCol)
            }
            val method =
              new RuntimeMethodDescriptor(DALProvider.EntityRefType, "toEntity", DALProvider.PersistentEntityType)
            val converted = ElementFactory.call(null, method, c :: Nil)
            newCols.append(ColumnDeclaration(name, converted))

          case decl @ ColumnDeclaration(name, c: ColumnElement) if keepNames.contains(name) =>
            val keepCol = ColumnDeclaration(name.concat("_keep"), c)
            newCols.append(keepCol)

          case e => newCols.append(e)
        }

        updateSelect(s, s.from, s.where, s.orderBy, s.groupBy, s.skip, s.take, newCols.result())
      }
    }

    protected override def handleDALHeapEntity(he: DALHeapEntityElement): RelationElement = {
      he.members.head match {
        // If the EntityRef column is in DALHeapEntityElement of Projector, we will rewrite it to "toEntity(c)" and change it type to PersistentEntity.
        case c: ColumnElement
            if outerAliases.contains(c.alias) && (c.rowTypeInfo eq DALProvider.EntityRefType) && c.name.startsWith(
              DALProvider.EntityRef) =>
          putAliasColumn(convertedRefColumns, c.alias, c)
          val column = new ColumnElement(DALProvider.PersistentEntityType, c.alias, c.name, c.columnInfo)
          new DALHeapEntityElement(he.companion, he.rowTypeInfo, column :: Nil, he.memberNames)
        case _ => he
      }
    }

    protected override def handleColumn(c: ColumnElement): RelationElement = {
      if (
        outerAliases.contains(c.alias) && (c.rowTypeInfo eq DALProvider.EntityRefType) && c.name.startsWith(
          DALProvider.EntityRef)
      ) {
        // If the EntityRef column is not in DALHeapEntityElement of Projector, we still keep the column in Select.
        putAliasColumn(keepRefColumns, c.alias, c)
        new ColumnElement(c.rowTypeInfo, c.alias, c.name.concat("_keep"), c.columnInfo)
      } else c
    }

    private def putAliasColumn(
        map: mutable.HashMap[TableAlias, mutable.HashSet[String]],
        alias: TableAlias,
        column: ColumnElement) = {
      val names = map.getOrElseUpdate(alias, new mutable.HashSet[String])
      names += column.name
    }
  }
}

object DALLanguage {
  private class ExpressionChecker(lookup: MappingEntityLookup) extends DbQueryTreeVisitor {
    import DbElementType._
    import ElementType._
    import java.time.Instant
    import java.time._

    private var canBeExpression = true

    override def visitElement(element: RelationElement): RelationElement = {
      if (!canBeExpression || (element eq null)) element
      else
        element.elementType match {
          case Column | ConstValue | DbEntity | DALHeapEntity | Scalar | Exists | EmbeddableCaseClass => element
          case BinaryExpression | UnaryExpression | Contains | MemberRef | Tuple | Option | ForteFuncCall |
              TernaryExpression | TypeIs =>
            super.visitElement(element)
          case _ =>
            canBeExpression = false
            element
        }
    }

    override def handleTypeIs(typeIs: TypeIsElement): RelationElement = {
      if (!isProjectedEntityTypeIs(typeIs)) canBeExpression = false
      typeIs
    }

    override def handleUnaryExpression(unary: UnaryExpressionElement): RelationElement = {
      if (unary.op != UnaryExpressionType.CONVERT) canBeExpression = false
      else {
        val t = unary.rowTypeInfo
        if (t.classes.size != 1 || !t.clazz.isPrimitive) canBeExpression = false
      }
      unary
    }

    override def handleBinaryExpression(binary: BinaryExpressionElement): RelationElement = {
      (binary.left, binary.right) match {
        // we don't support filter by collection "=="/"!=" unless compare with None/null/ImmutableArray[Byte].
        case (ele: RelationElement, ConstValueElement(value, _))
            if (value != None) && (value != null) && TypeInfo.isCollection(ele.rowTypeInfo) && !RelationalUtils
              .isImmutableArrayOfByte(ele.rowTypeInfo) =>
          canBeExpression = false
          binary
        case (ConstValueElement(value, _), ele: RelationElement)
            if (value != None) && (value != null) && TypeInfo.isCollection(ele.rowTypeInfo) && !RelationalUtils
              .isImmutableArrayOfByte(ele.rowTypeInfo) =>
          canBeExpression = false
          binary
        case _ => super.handleBinaryExpression(binary)
      }
    }

    override def handleMemberRef(element: MemberElement): RelationElement = {
      val desc = element.member
      val accelerated = desc.declaringType.clazz.getMethod(desc.name).getAnnotation(classOf[_projected]) != null
      if (accelerated) {
        val e = lookup.getEntity(desc.declaringType).asInstanceOf[DALMappingEntity]
        canBeExpression &= e.isMapped(desc.name)
      } else canBeExpression = false
      if (canBeExpression) element.instanceProvider match {
        case m: MemberElement   => visitElement(m)
        case _: DbEntityElement =>
        case _                  => canBeExpression = false
      }
      element
    }

    override def handleFuncCall(func: FuncElement): RelationElement = {
      func.callee match {
        case m: MethodCallee if m.method != null =>
          // must be the supported func call
          val desc = m.method
          val declType = desc.declaringType
          if (declType <:< classOf[String]) desc.name match {
            case "startsWith" | "endsWith" | "contains" | "matches" | "length" =>
            case "toLowerCase" | "toUpperCase" if func.arguments.isEmpty       =>
            case _                                                             => canBeExpression = false
          }
          else if (declType <:< classOf[Knowable[_]]) desc.name match {
            case "value" =>
            case _       => canBeExpression = false
          }
          else if (declType <:< classOf[Number]) desc.name match {
            case "doubleValue" | "intValue" | "longValue" | "shortValue" | "floatValue" | "byteValue"
                if func.arguments.isEmpty =>
            case _ => canBeExpression = false
          }
          else if (declType <:< BigDecimal.getClass) desc.name match {
            case "double2bigDecimal" | "long2bigDecimal" | "int2bigDecimal" =>
            case _                                                          => canBeExpression = false
          }
          else if (declType <:< classOf[ZonedDateTime]) desc.name match {
            case "isAfter" | "isBefore" | "equalInstant" | "equals" =>
            case _                                                  => canBeExpression = false
          }
          else if (declType <:< ZonedDateTimeOps.getClass) desc.name match {
            case "equalInstant" =>
            case _              => canBeExpression = false
          }
          else if (declType <:< classOf[LocalDate]) desc.name match {
            case "isAfter" | "isBefore" | "equals" =>
            case _                                 => canBeExpression = false
          }
          else if (declType <:< classOf[Instant]) desc.name match {
            case "isAfter" | "isBefore" | "equals" =>
            case _                                 => canBeExpression = false
          }
          else if (declType <:< classOf[LocalTime]) desc.name match {
            case "isAfter" | "isBefore" | "equals" =>
            case _                                 => canBeExpression = false
          }
          else if (
            declType <:< classOf[Iterable[_]] && !RelationalUtils.isImmutableArrayOfByte(declType) && (func.instance
              .isInstanceOf[ColumnElement])
          ) desc.name match {
            // Don't support List[List[_]].contains() and Map[_,_].contains to be executed at server side.
            case "contains"
                if !(declType <:< classOf[Map[_, _]]) && declType.typeParams.forall(isSupportedType)
                  && func.arguments.size == 1 && func.arguments.head.elementType == ConstValue =>
            case "isEmpty" | "nonEmpty" =>
            case _                      => canBeExpression = false
          }
          else canBeExpression = false

        case _ => canBeExpression = false
      }
      if (!canBeExpression) func else super.handleFuncCall(func)
    }
  }

  private object ExpressionChecker {
    def canBeExpression(e: RelationElement, lookup: MappingEntityLookup): Boolean = {
      val c = new ExpressionChecker(lookup)
      c.visitElement(e)
      c.canBeExpression
    }
  }

  val supportedType: Set[Class[_]] = Set(
    classOf[String],
    classOf[ZonedDateTime],
    classOf[LocalDate],
    classOf[LocalTime],
    classOf[ZoneId],
    classOf[Period],
    classOf[Instant],
    classOf[Duration],
    classOf[YearMonth],
    classOf[EntityReference]
  )

  def isSupportedType(shape: TypeInfo[_]): Boolean = {
    val target = shape.clazz
    target.isPrimitive || supportedType.contains(target) || shape <:< classOf[Entity]
  }

  def isProjectedEntityTypeIs(typeIs: TypeIsElement): Boolean = {
    val clz = typeIs.targetType.clazz
    typeIs.element match {
      case entity: DbEntityElement
          if ((entity.rowTypeInfo <:< classOf[Entity]) && (typeIs.targetType <:< classOf[Entity]) &&
            (typeIs.targetType.classes.length == 1) && Modifier.isFinal(clz.getModifiers) &&
            DALProvider.isProjectedEntity(clz)) =>
        true
      case _ => false
    }
  }
}
