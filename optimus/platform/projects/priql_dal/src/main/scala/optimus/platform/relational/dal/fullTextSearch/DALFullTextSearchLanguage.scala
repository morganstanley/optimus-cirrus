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
package optimus.platform.relational.dal.fullTextSearch

import optimus.platform.BusinessEvent
import optimus.platform.annotations.internal._
import optimus.platform.cm.Knowable
import optimus.platform.dsi.bitemporal.QueryPlan
import optimus.platform.dsi.expressions.Binary
import optimus.platform.dsi.expressions.{Entity => _, _}
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.core.DALDialect
import optimus.platform.relational.dal.core.DALFormatter
import optimus.platform.relational.dal.core.DALFormatterHelper
import optimus.platform.relational.dal.core.DALLanguage
import optimus.platform.relational.dal.core.DALMappingEntity
import optimus.platform.relational.dal.core.ExpressionQuery
import optimus.platform.relational.dal.core.RichConstant
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.language.QueryDialect
import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.DbElementType
import optimus.platform.relational.data.tree.DbEntityElement
import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.data.tree.ContainsElement
import optimus.platform.relational.internal.RelationalUtils
import optimus.platform.relational.tree.BinaryExpressionElement
import optimus.platform.relational.tree.BinaryExpressionType
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.ElementType
import optimus.platform.relational.tree.FuncElement
import optimus.platform.relational.tree.MemberElement
import optimus.platform.relational.tree.MethodCallee
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.relational.tree.UnaryExpressionElement
import optimus.platform.relational.tree.UnaryExpressionType
import optimus.platform.storable.Entity

import java.time._

class DALFullTextSearchLanguage(lookup: MappingEntityLookup) extends DALLanguage(lookup) {
  import DALFullTextSearchLanguage._

  override def createDialect(translator: QueryTranslator): QueryDialect = {
    new DALFullTextSearchDialect(this, translator)
  }

  override def canBeWhere(e: RelationElement): Boolean = {
    ExpressionChecker.canBeExpression(e, lookup)
  }

}

class DALFullTextSearchDialect(lan: QueryLanguage, tran: QueryTranslator) extends DALDialect(lan, tran) {
  override def format(element: RelationElement): ExpressionQuery = {
    FullTextSearchFormatter.format(element)
  }
}

class FullTextSearchFormatter extends DALFormatter {
  protected override def handleBinaryExpression(binary: BinaryExpressionElement): RelationElement = {
    binary match {
      case BinaryExpressionElement(
            BinaryExpressionType.EQ | BinaryExpressionType.NE,
            c: ColumnElement,
            v: ConstValueElement,
            _) =>
        val l = getExpression(visitElement(c))
        val r = RichConstant(v.value, v.rowTypeInfo, getIndexInfo(c))
        ExpressionElement(Binary(DALFormatterHelper.binaryOperator(binary.op), l, r))
      case BinaryExpressionElement(
            BinaryExpressionType.EQ | BinaryExpressionType.NE,
            v: ConstValueElement,
            c: ColumnElement,
            _) =>
        val l = getExpression(visitElement(c))
        val r = RichConstant(v.value, v.rowTypeInfo, getIndexInfo(c))
        ExpressionElement(Binary(DALFormatterHelper.binaryOperator(binary.op), l, r))
      case _ =>
        super.handleBinaryExpression(binary)
    }
  }

  protected override def handleFuncCall(func: FuncElement): RelationElement = {
    val mc = func.callee.asInstanceOf[MethodCallee]
    val desc = mc.method
    val declType = desc.declaringType
    val inst = getExpression(visitElement(func.instance))
    val args = visitElementList(func.arguments).map(getExpression)
    if (declType <:< classOf[String]) {
      ExpressionElement(Function(s"text.${desc.name}", inst :: args))
    } else if (declType <:< DALProvider.EntityRefType)
      ExpressionElement(Function(s"convert.${desc.name}", args))
    else
      super.handleFuncCall(func)
  }

}

object FullTextSearchFormatter {
  def format(element: RelationElement): ExpressionQuery = {
    val formatter = new FullTextSearchFormatter
    val e = formatter.visitElement(element)
    ExpressionQuery(formatter.getExpression(e), QueryPlan.FullTextSearch)
  }
}

object DALFullTextSearchLanguage {
  private class ExpressionChecker(lookup: MappingEntityLookup) extends DbQueryTreeVisitor {
    import DbElementType._
    import ElementType._
    import java.time._

    private var canBeExpression = true

    override def visitElement(element: RelationElement): RelationElement = {
      if (!canBeExpression || (element eq null)) element
      else
        element.elementType match {
          case Column | ConstValue | DbEntity | DALHeapEntity | Scalar | Exists => element
          case BinaryExpression | UnaryExpression | Contains | MemberRef | Tuple | Option | ForteFuncCall |
              TernaryExpression =>
            super.visitElement(element)
          case _ =>
            canBeExpression = false
            element
        }
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
      val fullTextSearchAnnotated =
        desc.declaringType.clazz.getMethod(desc.name).getAnnotation(classOf[_fullTextSearch]) != null
      if (fullTextSearchAnnotated) {
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
            case "startsWith" | "endsWith" | "contains" =>
            case _                                      => canBeExpression = false
          }
          else if (
            declType <:< classOf[LocalDate] ||
            declType <:< classOf[LocalTime] ||
            declType <:< classOf[OffsetTime] ||
            declType <:< classOf[ZonedDateTime]
          ) desc.name match {
            case "isAfter" | "isBefore" | "equalInstant" | "equals" =>
            case _                                                  => canBeExpression = false
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
    classOf[OffsetTime]
  )

  def isSupportedType(shape: TypeInfo[_]): Boolean = {
    val target = shape.clazz
    supportedType.contains(target) || shape <:< classOf[Entity]
  }

}
