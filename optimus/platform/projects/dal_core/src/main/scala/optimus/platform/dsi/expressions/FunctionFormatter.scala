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
package optimus.platform.dsi.expressions

import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime

import optimus.dsi.serialization.json.StringMarkers
import optimus.dsi.serialization.json.TypedRefAwareJsonTypeTransformer
import optimus.platform.dsi.expressions.BinaryOperator._
import optimus.platform.dsi.expressions.TextOps.Left256
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.EntityReference

trait FunctionFormatter {
  def resultType: TypeCode
  def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit
}

object FunctionFormatter {
  def apply(name: String): Option[FunctionFormatter] = methodFormatterMap.get(name)

  private val methodFormatterMap: Map[String, FunctionFormatter] = Map(
    CollectionOps.Contains.Name -> CollectionOps.Contains,
    TextOps.Concat.Name -> TextOps.Concat,
    TextOps.StartsWith.Name -> TextOps.StartsWith,
    TextOps.EndsWith.Name -> TextOps.EndsWith,
    TextOps.Contains.Name -> TextOps.Contains,
    TextOps.Matches.Name -> TextOps.Matches,
    TextOps.Length.Name -> TextOps.Length,
    TextOps.ToLowerCase.Name -> TextOps.ToLowerCase,
    TextOps.ToUpperCase.Name -> TextOps.ToUpperCase,
    TextOps.Md5.Name -> TextOps.Md5,
    TextOps.Left256.Name -> TextOps.Left256,
    DateOps.IsBefore.Name -> DateOps.IsBefore,
    DateOps.IsAfter.Name -> DateOps.IsAfter,
    DateOps.Equals.Name -> DateOps.Equals,
    DateOps.NotEquals.Name -> DateOps.NotEquals,
    DateOps.In.Name -> DateOps.In,
    TimeOps.IsBefore.Name -> TimeOps.IsBefore,
    TimeOps.IsAfter.Name -> TimeOps.IsAfter,
    TimeOps.Equals.Name -> TimeOps.Equals,
    TimeOps.NotEquals.Name -> TimeOps.NotEquals,
    TimeOps.In.Name -> TimeOps.In,
    DateTimeOps.Format.Name -> DateTimeOps.Format,
    ZonedDateTimeOps.IsBefore.Name -> ZonedDateTimeOps.IsBefore,
    ZonedDateTimeOps.IsAfter.Name -> ZonedDateTimeOps.IsAfter,
    ZonedDateTimeOps.Equals.Name -> ZonedDateTimeOps.Equals,
    ZonedDateTimeOps.NotEquals.Name -> ZonedDateTimeOps.NotEquals,
    ZonedDateTimeOps.In.Name -> ZonedDateTimeOps.In,
    ZonedDateTimeOps.EqualInstant.Name -> ZonedDateTimeOps.EqualInstant,
    InstantOps.IsBefore.Name -> InstantOps.IsBefore,
    InstantOps.IsAfter.Name -> InstantOps.IsAfter,
    InstantOps.Equals.Name -> InstantOps.Equals,
    ConvertOps.ToJsonb.Name -> ConvertOps.ToJsonb,
    ConvertOps.ToText.Name -> ConvertOps.ToText,
    ConvertOps.ToEntity.Name -> ConvertOps.ToEntity,
    ConvertOps.ToJsonbArray.Name -> ConvertOps.ToJsonbArray,
    EntityOps.AsTypedRef.Name -> EntityOps.AsTypedRef,
    EntityOps.AsVersionedRef.Name -> EntityOps.AsVersionedRef,
    EntityOps.Equals.Name -> EntityOps.Equals,
    EntityOps.Format.Name -> EntityOps.Format,
    EntityOps.NotEquals.Name -> EntityOps.NotEquals,
    EntityOps.In.Name -> EntityOps.In,
    EntityOps.TypeIs.Name -> EntityOps.TypeIs,
    EventOps.AsTypedRef.Name -> EventOps.AsTypedRef,
    EventOps.Equals.Name -> EventOps.Equals,
    EventOps.Format.Name -> EventOps.Format,
    EventOps.NotEquals.Name -> EventOps.NotEquals,
    EventOps.In.Name -> EventOps.In,
    KnowableOps.IsNotApplicable.Name -> KnowableOps.IsNotApplicable,
    KnowableOps.IsUnknown.Name -> KnowableOps.IsUnknown,
    KnowableOps.TypeEquals.Name -> KnowableOps.TypeEquals,
    KnowableOps.Value.Name -> KnowableOps.Value,
    JsonbOps.Contains.Name -> JsonbOps.Contains,
    JsonbOps.IsEmpty.Name -> JsonbOps.IsEmpty,
    JsonbOps.NonEmpty.Name -> JsonbOps.NonEmpty,
    HashLikeBtreeIndexOps.Equals.Name -> HashLikeBtreeIndexOps.Equals,
    HashLikeBtreeIndexOps.In.Name -> HashLikeBtreeIndexOps.In
  )
}

object CollectionOps {
  object Contains extends FunctionFormatter {
    val Name = "collection.contains"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit =
      // not implemented, this function should only be used to express clauses in an entitlements filter and should not
      // be directly executed
      throw new IllegalStateException(s"Should not have attempted to sql-format the $Name formatter")
    def resultType: TypeCode = TypeCode.Boolean
  }
}

object TextOps {
  object Concat extends FunctionFormatter {
    val Name = "text.concat"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(" || ")
      sqlFormatter.visit(arguments(1))
    }
    def resultType: TypeCode = TypeCode.String
  }

  object StartsWith extends FunctionFormatter {
    val Name = "text.startsWith"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("(")
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(" LIKE ")
      sqlFormatter.visit(arguments(1))
      sqlFormatter.write(" || '%'")
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object EndsWith extends FunctionFormatter {
    val Name = "text.endsWith"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("(")
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(" LIKE ")
      sqlFormatter.write("'%' || ")
      sqlFormatter.visit(arguments(1))
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object Contains extends FunctionFormatter {
    val Name = "text.contains"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("(")
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(" LIKE ")
      sqlFormatter.write("'%' || ")
      sqlFormatter.visit(arguments(1))
      sqlFormatter.write(" || '%'")
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object Matches extends FunctionFormatter {
    val Name = "text.matches"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("(")
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(" ~ ")
      sqlFormatter.visit(arguments(1))
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object Length extends FunctionFormatter {
    val Name = "text.length"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("length(")
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Int
  }

  object ToLowerCase extends FunctionFormatter {
    val Name = "text.toLowerCase"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("lower(")
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.String
  }

  object ToUpperCase extends FunctionFormatter {
    val Name = "text.toUpperCase"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("upper(")
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.String
  }

  object Md5 extends FunctionFormatter {
    val Name = "text.md5"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("md5(")
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.String
  }

  object Left256 extends FunctionFormatter {
    val Name = "text.left256"

    /**
     * TextLimit should never be changed unless we rebuild all index. We set index text limit as 256.
     *   1. case object embeddable should be < 256 chars. 2. Most small case class embeddable is less than 256
     */
    val TextLimit = 256
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("left(")
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(s", $TextLimit)")
    }
    def resultType: TypeCode = TypeCode.String
  }
}

object DateTimeOps {
  object Format extends FunctionFormatter {
    val Name = "datetime.format"
    def resultType: TypeCode = TypeCode.Long
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter) = {
      writeDateTime(arguments.head, sqlFormatter)
    }
  }

  def writeDateTime(e: Expression, sqlFormatter: ExpressionSqlFormatter): Unit = {
    e match {
      case Constant(null, _) =>
        sqlFormatter.write("NULL")
      case Constant(v @ (_: ZonedDateTime | _: LocalDate | _: LocalTime), _) =>
        sqlFormatter.write(TypedRefAwareJsonTypeTransformer.serialize(v).asInstanceOf[java.util.List[_]].get(1))
      case Scalar(s: Select) =>
        val propDef = s.properties.head
        val newPropDef = propDef.copy(e = Function(Format.Name, List(propDef.e)))
        sqlFormatter.visit(s.copy(properties = List(newPropDef)))
      case _ =>
        sqlFormatter.write("(")
        sqlFormatter.visit(e)
        sqlFormatter.write("->>1)::bigint")
    }
  }

  def writeZone(e: Expression, sqlFormatter: ExpressionSqlFormatter): Unit = {
    e match {
      case Constant(v: ZonedDateTime, _) =>
        sqlFormatter.write("'")
        sqlFormatter.write(TypedRefAwareJsonTypeTransformer.serialize(v).asInstanceOf[java.util.List[_]].get(2))
        sqlFormatter.write("'")
      case _ =>
        sqlFormatter.visit(e)
        sqlFormatter.write("->>2")
    }
  }

  trait DTCompare extends FunctionFormatter {
    def operator: String
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("(")
      writeDateTime(arguments.head, sqlFormatter)
      sqlFormatter.write(s" $operator ")
      writeDateTime(arguments(1), sqlFormatter)
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  trait DTEqual extends FunctionFormatter {
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      arguments match {
        case List(Constant(null | None, _), a) =>
          sqlFormatter.write("(")
          writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" IS NULL")
          sqlFormatter.write(")")
        case List(a, Constant(null | None, _)) =>
          sqlFormatter.write("(")
          writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" IS NULL")
          sqlFormatter.write(")")
        case List(a, b) =>
          sqlFormatter.write("(")
          writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" = ")
          writeDateTime(b, sqlFormatter)
          sqlFormatter.write(")")
        case as => throw new MatchError(as)
      }
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  trait DTNotEqual extends FunctionFormatter {
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      arguments match {
        case List(Constant(null | None, _), a) =>
          sqlFormatter.write("(")
          writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" IS NOT NULL")
          sqlFormatter.write(")")
        case List(a, Constant(null | None, _)) =>
          sqlFormatter.write("(")
          writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" IS NOT NULL")
          sqlFormatter.write(")")
        case List(a, b) =>
          sqlFormatter.write("(")
          writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" != ")
          writeDateTime(b, sqlFormatter)
          sqlFormatter.write(")")
        case as => throw new MatchError(as)
      }
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  trait DTIn extends FunctionFormatter {
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      val (head :: others) = arguments
      writeDateTime(head, sqlFormatter)
      sqlFormatter.write(" IN (")
      var first = true
      for (v <- others) {
        if (!first) sqlFormatter.write(", ")
        writeDateTime(v, sqlFormatter)
        first = false
      }
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }
}

object DateOps {
  object IsBefore extends DateTimeOps.DTCompare {
    val Name = "date.isBefore"
    override val operator = "<"
  }
  object IsAfter extends DateTimeOps.DTCompare {
    val Name = "date.isAfter"
    override val operator = ">"
  }
  object Equals extends DateTimeOps.DTEqual { val Name = "date.equals" }
  object NotEquals extends DateTimeOps.DTNotEqual { val Name = "date.notEquals" }
  object In extends DateTimeOps.DTIn { val Name = "date.in" }
}

object TimeOps {
  object IsBefore extends DateTimeOps.DTCompare {
    val Name = "time.isBefore"
    override val operator = "<"
  }
  object IsAfter extends DateTimeOps.DTCompare {
    val Name = "time.isAfter"
    override val operator = ">"
  }
  object Equals extends DateTimeOps.DTEqual { val Name = "time.equals" }
  object NotEquals extends DateTimeOps.DTNotEqual { val Name = "time.notEquals" }
  object In extends DateTimeOps.DTIn { val Name = "time.in" }
}

object ZonedDateTimeOps {
  object IsBefore extends DateTimeOps.DTCompare {
    val Name = "datetimez.isBefore"
    override val operator = "<"
  }
  object IsAfter extends DateTimeOps.DTCompare {
    val Name = "datetimez.isAfter"
    override val operator = ">"
  }
  object In extends DateTimeOps.DTIn { val Name = "datetimez.in" }
  object EqualInstant extends DateTimeOps.DTCompare {
    val Name = "datetimez.equalInstant"
    override val operator = "="
  }

  object Equals extends FunctionFormatter {
    val Name = "datetimez.equals"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      arguments match {
        case List(Constant(null | None, _), a) =>
          sqlFormatter.write("(")
          DateTimeOps.writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" IS NULL")
          sqlFormatter.write(")")
        case List(a, Constant(null | None, _)) =>
          sqlFormatter.write("(")
          DateTimeOps.writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" IS NULL")
          sqlFormatter.write(")")
        case List(a, b) =>
          sqlFormatter.write("(")
          sqlFormatter.write("(")
          DateTimeOps.writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" = ")
          DateTimeOps.writeDateTime(b, sqlFormatter)
          sqlFormatter.write(")")
          sqlFormatter.write(" AND (")
          DateTimeOps.writeZone(a, sqlFormatter)
          sqlFormatter.write(" = ")
          DateTimeOps.writeZone(b, sqlFormatter)
          sqlFormatter.write(")")
          sqlFormatter.write(")")
        case as => throw new MatchError(as)
      }
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object NotEquals extends FunctionFormatter {
    val Name = "datetimez.notEquals"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      arguments match {
        case List(Constant(null | None, _), a) =>
          sqlFormatter.write("(")
          DateTimeOps.writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" IS NOT NULL")
          sqlFormatter.write(")")
        case List(a, Constant(null | None, _)) =>
          sqlFormatter.write("(")
          DateTimeOps.writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" IS NOT NULL")
          sqlFormatter.write(")")
        case List(a, b) =>
          sqlFormatter.write("(")
          sqlFormatter.write("(")
          DateTimeOps.writeDateTime(a, sqlFormatter)
          sqlFormatter.write(" != ")
          DateTimeOps.writeDateTime(b, sqlFormatter)
          sqlFormatter.write(")")
          sqlFormatter.write(" OR (")
          DateTimeOps.writeZone(a, sqlFormatter)
          sqlFormatter.write(" != ")
          DateTimeOps.writeZone(b, sqlFormatter)
          sqlFormatter.write(")")
          sqlFormatter.write(")")
        case as => throw new MatchError(as)
      }
    }
    def resultType: TypeCode = TypeCode.Boolean
  }
}

object InstantOps {
  object IsBefore extends FunctionFormatter {
    val Name = "instant.isBefore"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.visit(Binary(BinaryOperator.LessThan, arguments.head, arguments(1)))
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object IsAfter extends FunctionFormatter {
    val Name = "instant.isAfter"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.visit(Binary(BinaryOperator.GreaterThan, arguments.head, arguments(1)))
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object Equals extends FunctionFormatter {
    val Name = "instant.equals"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.visit(Binary(BinaryOperator.Equal, arguments.head, arguments(1)))
    }
    def resultType: TypeCode = TypeCode.Boolean
  }
}

object ConvertOps {
  object ToJsonb extends FunctionFormatter {
    val Name = "convert.toJsonb"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      arguments.head match {
        case Function(ConvertOps.ToText.Name, List(const: Constant)) =>
          sqlFormatter.write("to_jsonb(")
          sqlFormatter.visit(arguments.head)
          sqlFormatter.write(")")
        case _ =>
          sqlFormatter.write("(")
          sqlFormatter.visit(arguments.head)
          sqlFormatter.write(")::jsonb")
      }
    }
    def resultType: TypeCode = TypeCode.Map
  }

  object ToText extends FunctionFormatter {
    val Name = "convert.toText"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("(")
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(")")
      sqlFormatter.write("::text")
    }
    def resultType: TypeCode = TypeCode.String
  }

  object ToEntity extends FunctionFormatter {
    val Name = "convert.toEntity"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.visit(arguments.head)
    }
    def resultType: TypeCode = TypeCode.PersistentEntity
  }

  object ToJsonbArray extends FunctionFormatter {
    val Name = "convert.toJsonbArray"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("jsonb_build_array(")
      var first = true
      for (v <- arguments) {
        if (!first) sqlFormatter.write(", ")
        sqlFormatter.visit(v)
        first = false
      }
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Sequence
  }
}

object EntityOps {
  def writeEntityReference(e: Expression, sqlFormatter: ExpressionSqlFormatter): Unit = {
    e match {
      case Constant(null, _) =>
        sqlFormatter.write("NULL")
      case Constant(v: EntityReference, _) =>
        sqlFormatter.write("'")
        sqlFormatter.write(v.toUuid)
        sqlFormatter.write("'")
      // It is a EntityReference column for PropertyType.Special.
      case Property(PropertyType.Special, names, _) => sqlFormatter.visit(e)
      case Scalar(s: Select) =>
        val propDef = s.properties.head
        val newPropDef = propDef.copy(e = Function(Format.Name, List(propDef.e)))
        sqlFormatter.visit(s.copy(properties = List(newPropDef)))
      case _ =>
        sqlFormatter.write("(")
        sqlFormatter.visit(e)
        sqlFormatter.write("->>1) :: uuid")
    }
  }

  object Format extends FunctionFormatter {
    val Name = "entity.format"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      writeEntityReference(arguments.head, sqlFormatter)
    }
    def resultType: TypeCode = TypeCode.Reference
  }

  object AsTypedRef extends FunctionFormatter {
    val Name = "entity.asTypedRef"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      val List(eref, clsid) = arguments
      sqlFormatter.write("jsonb_build_array('~E', ")
      sqlFormatter.visit(eref)
      sqlFormatter.write(", ")
      sqlFormatter.visit(clsid)
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Reference
  }

  object AsVersionedRef extends FunctionFormatter {
    val Name = "entity.asVersionedRef"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      val List(vref) = arguments
      sqlFormatter.write(s"jsonb_build_array('${StringMarkers.VersionedRef}', ")
      sqlFormatter.visit(vref)
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.VersionedReference
  }

  object Equals extends FunctionFormatter {
    val Name = "entity.equals"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      arguments match {
        case List(Constant(null | None, _), a) =>
          sqlFormatter.write("(")
          EntityOps.writeEntityReference(a, sqlFormatter)
          sqlFormatter.write(" IS NULL")
          sqlFormatter.write(")")
        case List(a, Constant(null | None, _)) =>
          sqlFormatter.write("(")
          EntityOps.writeEntityReference(a, sqlFormatter)
          sqlFormatter.write(" IS NULL")
          sqlFormatter.write(")")
        case List(a, b) =>
          sqlFormatter.write("(")
          EntityOps.writeEntityReference(a, sqlFormatter)
          sqlFormatter.write(" = ")
          EntityOps.writeEntityReference(b, sqlFormatter)
          sqlFormatter.write(")")
        case as => throw new MatchError(as)
      }
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object NotEquals extends FunctionFormatter {
    val Name = "entity.notEquals"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      arguments match {
        case List(Constant(null | None, _), a) =>
          sqlFormatter.write("(")
          EntityOps.writeEntityReference(a, sqlFormatter)
          sqlFormatter.write(" IS NOT NULL")
          sqlFormatter.write(")")
        case List(a, Constant(null | None, _)) =>
          sqlFormatter.write("(")
          EntityOps.writeEntityReference(a, sqlFormatter)
          sqlFormatter.write(" IS NOT NULL")
          sqlFormatter.write(")")
        case List(a, b) =>
          sqlFormatter.write("(")
          EntityOps.writeEntityReference(a, sqlFormatter)
          sqlFormatter.write(" != ")
          EntityOps.writeEntityReference(b, sqlFormatter)
          sqlFormatter.write(")")
        case as => throw new MatchError(as)
      }
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object In extends FunctionFormatter {
    val Name = "entity.in"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      val (head :: others) = arguments
      EntityOps.writeEntityReference(head, sqlFormatter)
      sqlFormatter.write(" IN (")
      var first = true
      for (v <- others) {
        if (!first) sqlFormatter.write(", ")
        EntityOps.writeEntityReference(v, sqlFormatter)
        first = false
      }
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object TypeIs extends FunctionFormatter {
    val Name = "entity.typeIs"
    // We don't need 'write' method since it has been rewritten to Binary or In expression.
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = ???
    def resultType: TypeCode = TypeCode.Boolean
  }
}

object EventOps {
  def writeEventReference(e: Expression, sqlFormatter: ExpressionSqlFormatter): Unit = {
    e match {
      case Constant(null, _) =>
        sqlFormatter.write("NULL")
      case Constant(v: BusinessEventReference, _) =>
        sqlFormatter.write("'")
        sqlFormatter.write(v.toUuid)
        sqlFormatter.write("'")
      // It is a EntityReference column for PropertyType.Special.
      case Property(PropertyType.Special, _, _) => sqlFormatter.visit(e)
      case Scalar(s: Select) =>
        val propDef = s.properties.head
        val newPropDef = propDef.copy(e = Function(Format.Name, List(propDef.e)))
        sqlFormatter.visit(s.copy(properties = List(newPropDef)))
      case _ =>
        sqlFormatter.write("(")
        sqlFormatter.visit(e)
        sqlFormatter.write("->>1) :: uuid")
    }
  }

  object Format extends FunctionFormatter {
    val Name = "event.format"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      writeEventReference(arguments.head, sqlFormatter)
    }
    def resultType: TypeCode = TypeCode.Reference
  }

  object AsTypedRef extends FunctionFormatter {
    val Name = "event.asTypedRef"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      val List(eref, clsid) = arguments
      sqlFormatter.write("jsonb_build_array('~B', ")
      sqlFormatter.visit(eref)
      sqlFormatter.write(", ")
      sqlFormatter.visit(clsid)
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Reference
  }

  object Equals extends FunctionFormatter {
    val Name = "event.equals"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      arguments match {
        case List(Constant(null | None, _), a) =>
          sqlFormatter.write("(")
          writeEventReference(a, sqlFormatter)
          sqlFormatter.write(" IS NULL")
          sqlFormatter.write(")")
        case List(a, Constant(null | None, _)) =>
          sqlFormatter.write("(")
          writeEventReference(a, sqlFormatter)
          sqlFormatter.write(" IS NULL")
          sqlFormatter.write(")")
        case List(a, b) =>
          sqlFormatter.write("(")
          writeEventReference(a, sqlFormatter)
          sqlFormatter.write(" = ")
          writeEventReference(b, sqlFormatter)
          sqlFormatter.write(")")
        case as => throw new MatchError(as)
      }
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object NotEquals extends FunctionFormatter {
    val Name = "event.notEquals"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      arguments match {
        case List(Constant(null | None, _), a) =>
          sqlFormatter.write("(")
          writeEventReference(a, sqlFormatter)
          sqlFormatter.write(" IS NOT NULL")
          sqlFormatter.write(")")
        case List(a, Constant(null | None, _)) =>
          sqlFormatter.write("(")
          writeEventReference(a, sqlFormatter)
          sqlFormatter.write(" IS NOT NULL")
          sqlFormatter.write(")")
        case List(a, b) =>
          sqlFormatter.write("(")
          writeEventReference(a, sqlFormatter)
          sqlFormatter.write(" != ")
          writeEventReference(b, sqlFormatter)
          sqlFormatter.write(")")
        case as => throw new MatchError(as)
      }
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object In extends FunctionFormatter {
    val Name = "event.in"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      val (head :: others) = arguments
      writeEventReference(head, sqlFormatter)
      sqlFormatter.write(" IN (")
      var first = true
      for (v <- others) {
        if (!first) sqlFormatter.write(", ")
        writeEventReference(v, sqlFormatter)
        first = false
      }
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }
}

object JsonbOps {
  object Contains extends FunctionFormatter {
    val Name = "jsonb.contains"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      val (field :: value :: Nil) = arguments
      sqlFormatter.visit(field)
      sqlFormatter.write(" @> ")
      sqlFormatter.visit(value)
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object IsEmpty extends FunctionFormatter {
    val Name = "jsonb.isEmpty"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(" = '[]'")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object NonEmpty extends FunctionFormatter {
    val Name = "jsonb.nonEmpty"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.visit(arguments.head)
      sqlFormatter.write(" != '[]'")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }
}

/*
 * We will create (md5(f::text), left(f::text, Md5IndexLeft.TextLimit)) compound index for embeddable, Tuple property.
 * We generate md5 Equal/In query so that the query could hit index.
 */
object HashLikeBtreeIndexOps {

  /**
   *   1. md5_property == constant
   *      a. constant(NULL) \=> md5(p::text) IS NULL b. size(constant) <= Md5IndexLeft.TextLimit/2 \=> md5(p::text) =
   *         md5((constant::jsonb)::text) AND left(p::text, Md5IndexLeft.TextLimit) = (constant::jsonb)::text c.
   *         size(constant) > Md5IndexLeft.TextLimit/2 \=> md5(p::text) = md5((constant::jsonb)::text) AND p = constant
   *         2. md5_property == another_property \=> md5(p1::text) = md5(p2::text) AND p1 = p2
   */
  object Equals extends FunctionFormatter {
    val Name = "hashLikeBtreeIndex.equals"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      val (left :: right :: Nil) = arguments
      (left, right) match {
        case (
              e: Expression,
              f @ Function(ConvertOps.ToJsonb.Name, List(Function(ConvertOps.ToText.Name, List(const: Constant))))) =>
          writeMd5BinaryEqual(e, f, const, sqlFormatter)

        case (
              f @ Function(ConvertOps.ToJsonb.Name, List(Function(ConvertOps.ToText.Name, List(const: Constant)))),
              e: Expression) =>
          writeMd5BinaryEqual(e, f, const, sqlFormatter)

        case (e: Expression, const: Constant) =>
          writeMd5BinaryEqual(e, const, const, sqlFormatter)

        case (const: Constant, e: Expression) =>
          writeMd5BinaryEqual(e, const, const, sqlFormatter)

        case _ =>
          writeMd5BinaryEqual(left, right, sqlFormatter)
      }
    }

    private def writeMd5BinaryEqual(
        e: Expression,
        originalConst: Expression,
        const: Constant,
        sqlFormatter: ExpressionSqlFormatter): Unit = {
      const.value match {
        case None | null => sqlFormatter.visit(Binary(Equal, getMd5Function(e), const))
        case _ =>
          val constToJsonb = Function(ConvertOps.ToJsonb.Name, List(originalConst))
          val constToMd5 = getMd5Function(constToJsonb)
          val md5Equal = Binary(Equal, getMd5Function(e), constToMd5)

          val v = sqlFormatter.formatConstant(const)
          if (v.length > Left256.TextLimit / 2) {
            writeBinaryAndAlso(md5Equal, e, originalConst, sqlFormatter)
          } else {
            val propertyToText = Function(ConvertOps.ToText.Name, List(e))
            val constJsonbToText = Function(ConvertOps.ToText.Name, List(constToJsonb))
            val leftEqual =
              Binary(Equal, Function(TextOps.Left256.Name, List(propertyToText)), constJsonbToText)
            sqlFormatter.visit(Binary(AndAlso, md5Equal, leftEqual))
          }
      }
    }

    private def writeMd5BinaryEqual(p1: Expression, p2: Expression, sqlFormatter: ExpressionSqlFormatter): Unit = {
      val property1ToMd5 = getMd5Function(p1)
      val property2ToMd5 = getMd5Function(p2)

      val md5Equal = Binary(Equal, property1ToMd5, property2ToMd5)
      writeBinaryAndAlso(md5Equal, p1, p2, sqlFormatter)
    }

    private def writeBinaryAndAlso(
        md5Equal: Binary,
        originalLeft: Expression,
        originalRight: Expression,
        sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.visit(md5Equal)
      sqlFormatter.write(" AND ")

      sqlFormatter.visit(originalLeft)
      sqlFormatter.write(" = ")
      sqlFormatter.visit(originalRight)
    }

    private def getMd5Function(p: Expression): Function = {
      val propertyToText = Function(ConvertOps.ToText.Name, List(p))
      Function(TextOps.Md5.Name, List(propertyToText))
    }

    def resultType: TypeCode = TypeCode.Boolean
  }

  /**
   * md5_property In (c1, c2) \=> md5_property == c1 || md5_property == c2
   */
  object In extends FunctionFormatter {
    val Name = "hashLikeBtreeIndex.in"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      arguments match {
        case List(i: In) =>
          (i.e, i.values) match {
            case (e, Right(values)) =>
              val orExpr = Expression.balancedOr(values.map(v => Binary(Equal, e, v)))
              sqlFormatter.visit(orExpr)
            case _ =>
          }
        case as => throw new MatchError(as)
      }
    }
    def resultType: TypeCode = TypeCode.Boolean
  }
}

object OptionOps {
  object Value extends FunctionFormatter {
    val Name = "option.value"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      throw new IllegalStateException(s"Should not have attempted to sql-format the $Name formatter")
    }
    def resultType: TypeCode = TypeCode.None
  }
}

object KnowableOps {
  object Value extends FunctionFormatter {
    val Name = "knowable.value"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      writeKnowableValue(arguments.head, sqlFormatter)
    }
    def resultType: TypeCode = TypeCode.None
  }

  object IsUnknown extends FunctionFormatter {
    val Name = "knowable.isUnknown"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("(")
      writeKnowableTag(arguments.head, sqlFormatter)
      sqlFormatter.write(" = 0)")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object IsNotApplicable extends FunctionFormatter {
    val Name = "knowable.isNotApplicable"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      sqlFormatter.write("(")
      writeKnowableTag(arguments.head, sqlFormatter)
      sqlFormatter.write(" = 1)")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  object TypeEquals extends FunctionFormatter {
    val Name = "knowable.typeEquals"
    def write(arguments: List[Expression], sqlFormatter: ExpressionSqlFormatter): Unit = {
      val e1 :: e2 :: Nil = arguments
      sqlFormatter.write("(")
      writeKnowableTag(e1, sqlFormatter)
      sqlFormatter.write(" = ")
      writeKnowableTag(e2, sqlFormatter)
      sqlFormatter.write(")")
    }
    def resultType: TypeCode = TypeCode.Boolean
  }

  def writeKnowableTag(e: Expression, sqlFormatter: ExpressionSqlFormatter): Unit = {
    e match {
      case Constant(null, _)           => sqlFormatter.write("NULL")
      case Constant(Seq(i: Int, _), _) => sqlFormatter.write(i)
      case Constant(Seq(i: Int), _)    => sqlFormatter.write(i)
      case _ =>
        sqlFormatter.write("(")
        sqlFormatter.visit(e)
        sqlFormatter.write("->>0)::integer")
    }
  }

  def writeKnowableValue(e: Expression, sqlFormatter: ExpressionSqlFormatter): Unit = {
    e match {
      case Constant(null | Seq(_), _) => sqlFormatter.write("NULL")
      case Constant(Seq(_, v), _)     => sqlFormatter.visit(Constant(v, TypeCode.None))
      case _ =>
        val sqlType = sqlFormatter.getSqlType(Function(Value.Name, List(e)))
        if (sqlType != "")
          sqlFormatter.write("(")
        sqlFormatter.visit(e)
        if (sqlType != "")
          sqlFormatter.write("->>1")
        else
          sqlFormatter.write("->1")
        if (sqlType != "")
          sqlFormatter.write(s")::$sqlType")
    }
  }
}
