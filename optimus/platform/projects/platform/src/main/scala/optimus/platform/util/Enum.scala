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
package optimus.platform.util

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.BeanProperty
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.deser.ContextualDeserializer
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import optimus.core.utils.RuntimeMirror
import spray.json.JsString
import spray.json.JsValue
import spray.json.RootJsonFormat

import scala.reflect.runtime.universe._

/**
 * This is a general purpose Enum implementation using case objects. It is preferable to Scala Enumeration unless it's
 * in the critical path of your program's performance and your program never uses reflection, as this Enum solution uses
 * reflection to implement values and withName. Scala Enumeration has the following problems:
 *
 *   1. Enumeration has the same type after erasure. For example, the following code
 *      {{{
 * object WeekDay extends Enumeration {
 *    type WeekDay = Value
 *    val Mon, Tue, Wed, Thu, Fri, Sat, Sun = Value
 * }
 * import WeekDay._
 * object Color extends Enumeration {
 *    type Color = Value
 *    val Red, Yellow, Blue = Value
 * }
 * import Color._
 *
 * def check(weekDay: WeekDay) = ???
 * def check(color: Color) = ???
 *      }}}
 *
 * will result in compiler error:
 * {{{
 * error: double definition:
 * private def check(weekDay: this.WeekDay.WeekDay): Nothing at line 12 and
 * private def check(color: this.Color.Color): Nothing at line 13
 * have same type after erasure: (weekDay: Enumeration#Value)Nothing
 * def check(color: Color) = ???
 * ^
 * one error found
 * }}}
 *
 * 2. No compile time exhaustive check. For example:
 * {{{
 * def check(weekDay: WeekDay) = {
 *     weekDay match {
 *       case Mon => ???
 *       case Tue => ???
 *     }
 * }
 * }}}
 * will compile but will fail with scala.MatchError when running with weekdays other than Mon or Tue.
 *
 * To effectively use this class, the following rules has to be followed for the values/withName methods to work
 * properly:
 *
 *   - The companion object of T has to extend Enum[T]
 *   - The enumeration values have to extend T
 *   - The enumeration values have to be be defined inside the companion object of T as case objects
 *
 * For example, the following enumeration values (Mon, Tue, Wed, Thu, Fri, Sat, Sun) extend WeekDay and are defined as
 * case objects of object WeekDay, which extends Enum[WeekDay]:
 * {{{
 * sealed trait WeekDay
 * object WeekDay extends Enum[WeekDay] {
 *   case object Mon extends WeekDay
 *   case object Tue extends WeekDay
 *   case object Wed extends WeekDay
 *   case object Thu extends WeekDay
 *   case object Fri extends WeekDay {
 *     override val toString: String = "Friday"
 *   }
 *   case object Sat extends WeekDay
 *   case object Sun extends WeekDay
 * }
 * }}}
 */
class Enum[T: TypeTag] {
  def withName(s: String): T =
    values
      .find(v => v.toString == s)
      .getOrElse(throw new IllegalArgumentException(s"""found no ${getClass.getSimpleName
          .replace("$", "")} enum value for "$s""""))
  lazy val values: Iterable[T] = {
    val rm = RuntimeMirror.forClass(getClass)
    val companion = typeOf[T].companion
    val modules = companion.decls.collect { case d: ModuleSymbol => d }.map(rm.reflectModule)
    modules.map(_.instance.asInstanceOf[T])
  }

  implicit def enumFormat: RootJsonFormat[T] =
    new RootJsonFormat[T] {
      override def write(obj: T): JsValue = JsString(obj.toString)
      override def read(json: JsValue): T = {
        json match {
          case JsString(str) => withName(str)
          case _ => throw new IllegalArgumentException(s"Expected an enum ${this.getClass.getName}, got $json")
        }
      }
    }
}

@JsonDeserialize(using = classOf[EnumDeserializer[_]])
@JsonSerialize(using = classOf[EnumSerializer[_]])
trait DefaultEnumJacksonBindings

/**
 * This is a Jackson deserializer that deserializes an optimus.platform.util.Enum value from JSON/XML. You can use it to
 * annotate the sealed trait of the enum definition, for example,
 * {{{
 * @JsonDeserialize(using = classOf[EnumDeserializer[WeekDay]])
 * sealed trait WeekDay
 * object WeekDay extends Enum[WeekDay] {
 *   case object Mon extends WeekDay
 *   case object Tue extends WeekDay
 *   case object Wed extends WeekDay
 *   case object Fri extends WeekDay {
 *     override val toString: String = "Friday"
 *   }
 *   case object Sat extends WeekDay
 *   case object Sun extends WeekDay
 * }
 * }}}
 * or to annotate an optimus.platform.util.Enum field directly:
 * {{{
 * class Schedule {
 *   ...
 *   @JsonDeserialize(using = classOf[EnumDeserializer[WeekDay]])
 *   val weekDay: WeekDay = WeekDay.Mon
 *   ...
 * }
 * }}}
 */
class EnumDeserializer[T <: AnyRef](val clazz: Class[_]) extends JsonDeserializer[T] with ContextualDeserializer {
  def this() = this(null)
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): T = {
    val spec = p.getValueAsString

    val rm = RuntimeMirror.forClass(this.getClass)
    val companionModule = rm.classSymbol(clazz).companion.asModule
    val enum = rm.reflectModule(companionModule).instance.asInstanceOf[Enum[T]]
    enum.withName(spec)
  }
  override def deserializeWithType(
      p: JsonParser,
      ctxt: DeserializationContext,
      typeDeserializer: TypeDeserializer): AnyRef = deserialize(p, ctxt)

  override def createContextual(ctxt: DeserializationContext, property: BeanProperty): JsonDeserializer[T] = {
    new EnumDeserializer[T](ctxt.getContextualType.getRawClass)
  }
}

class EnumSerializer[T] extends JsonSerializer[T] {
  override def serialize(
      value: T,
      gen: com.fasterxml.jackson.core.JsonGenerator,
      serializers: SerializerProvider): Unit = {
    gen.writeString(value.toString)
  }
  override def serializeWithType(
      value: T,
      gen: JsonGenerator,
      serializers: SerializerProvider,
      typeSer: TypeSerializer): Unit = {
    gen.writeString(value.toString)
  }
}
