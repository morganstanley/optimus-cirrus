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
package optimus.stratosphere.utils

import java.io.PrintWriter
import java.io.StringReader
import java.io.StringWriter
import java.lang.reflect.Constructor
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Properties
import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal

object CaseClassSerializer {
  final implicit class StringOps(prefix: String) {
    def dot(s: String): String = if (prefix == "") s else s"$prefix.$s"
  }

  trait Deserializer {
    val name: String
    def read(values: Map[String, String], prefix: String): Object
    def store(value: Any, values: Map[String, String], fieldName: String): Map[String, String]
  }

  final case class Simple(override val name: String, fromString: String => Any) extends Deserializer {
    override def read(values: Map[String, String], fieldName: String): AnyRef = values.get(fieldName) match {
      case None =>
        throw new IllegalArgumentException(s"Property $fieldName is missing in provided values: $values")
      case Some(value) =>
        try fromString(value).asInstanceOf[Object]
        catch {
          case NonFatal(e) =>
            throw new RuntimeException(s"When loading $value as field $name from $value")
        }
    }
    override def store(value: Any, values: Map[String, String], fieldName: String): Map[String, String] =
      values + (fieldName -> value.toString)
  }

  private[utils] implicit class KeyPath(parentKey: String) {
    private def toPath(k: String) = k.split("\\.")
    private val parentPath = toPath(parentKey)

    def asSubpath(key: String): Option[Array[String]] = {
      val path = toPath(key)
      if (path.startsWith(parentPath)) Some(path.drop(parentPath.length)) else None
    }
  }

  private[utils] final case class OptionDeserializer(override val name: String, nested: Deserializer)
      extends Deserializer {
    override def read(values: Map[String, String], fieldName: String): AnyRef =
      if (values.keys.exists(k => fieldName.asSubpath(k).isDefined)) Some(nested.read(values, fieldName))
      else None

    override def store(value: Any, values: Map[String, String], fieldName: String): Map[String, String] = value match {
      case Some(v) =>
        nested.store(v, values, fieldName)
      case _ =>
        values
    }
  }

  private[utils] final case class MapDeserializer(override val name: String, nested: Deserializer)
      extends Deserializer {
    override def read(values: Map[String, String], fieldName: String): AnyRef = {
      val groupedValues = values.keys.groupBy { key =>
        fieldName.asSubpath(key).flatMap(_.headOption) // If several level deep, only keep the one below parent
      }
      groupedValues.collect { case (Some(name), keys) =>
        name -> nested.read(values, fieldName dot name)
      }
    }
    override def store(value: Any, values: Map[String, String], fieldName: String): Map[String, String] = value match {
      case m: Map[_, _] =>
        m.foldLeft(values) { case (values, (name, value)) =>
          nested.store(value, values, fieldName dot name.toString)
        }
      case _ =>
        values
    }
  }

  private val IntType = typeOf[Int]
  private val LongType = typeOf[Long]
  private val StringType = typeOf[String]
  private val BooleanType = typeOf[Boolean]
  private val PathType = typeOf[Path]
  private val OptionType = typeOf[Option[_]]
  private val MapType = typeOf[Map[_, _]]
}

class CaseClassSerializer[T <: Product: TypeTag: ClassTag](nested: CaseClassSerializer[_]*) {
  import CaseClassSerializer._

  private lazy val constructor: Constructor[_] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass
    val allConstructors = clazz.getConstructors.filter(_.getParameters.length == deserializers.length)
    val Array(default) = allConstructors
    default
  }

  private lazy val deserializers: List[Deserializer] = {
    val params = implicitly[TypeTag[T]].tpe.typeSymbol.asClass.primaryConstructor.asMethod.paramLists
    assert(params.size == 1, "Only simple case classes are supported")
    params.head.map(p => deserializerFor(p.name.toString)(p.typeSignature))
  }

  private def toProperties(obj: T, prefix: String = ""): Properties = {
    val properties = new Properties()
    write(Map.empty, obj, prefix).foreach { case (name, value) => properties.put(name, value) }
    properties
  }

  def toString(obj: T): String = {
    val writer = new StringWriter()
    toProperties(obj).list(new PrintWriter(writer))
    writer.getBuffer.toString
  }

  private def readFromValues(values: Map[String, String], prefix: String = ""): T = {
    val allKeys = values.keySet

    val params = deserializers.map { d =>
      val correctFieldName = prefix dot d.name
      val damagedFieldName = s".$correctFieldName"
      if (allKeys.exists(_.startsWith(damagedFieldName))) // We want to cover nested case classes
        d.read(values, damagedFieldName)
      else
        d.read(values, correctFieldName) // If we can't find it at this point, we have another problem
    }
    constructor.newInstance(params: _*).asInstanceOf[T]
  }

  def fromString(s: String, migrate: Map[String, String] => Map[String, String] = identity): T = {
    val prop = new Properties()
    prop.load(new StringReader(s))
    readFromValues(migrate(prop.asScala.toMap))
  }

  private def asNested(fieldName: String): PartialFunction[Type, Deserializer] = {
    case t if t == typeOf[T] =>
      new Deserializer {
        override def store(value: Any, values: Map[String, String], fieldName: String): Map[String, String] =
          write(values, value.asInstanceOf[T], fieldName)

        override def read(values: Map[String, String], fieldName: String): Object =
          readFromValues(values, fieldName).asInstanceOf[Object]

        override val name: String = fieldName
      }
  }

  private def deserializers(fieldName: String): PartialFunction[Type, Deserializer] = {
    case t if t <:< IntType     => Simple(fieldName, _.toInt)
    case t if t <:< LongType    => Simple(fieldName, _.toLong)
    case t if t <:< StringType  => Simple(fieldName, _.toString)
    case t if t <:< BooleanType => Simple(fieldName, _.toBoolean)
    case t if t <:< PathType    => Simple(fieldName, p => Paths.get(p))
    case option @ TypeRef(_, _, Seq(nested)) if option <:< OptionType =>
      OptionDeserializer(fieldName, deserializerFor(fieldName)(nested))
    case map @ TypeRef(_, _, Seq(key, value)) if map <:< MapType && key <:< StringType =>
      MapDeserializer(fieldName, deserializerFor(fieldName)(value))
  }

  private def noSuchSerializer(name: String): PartialFunction[Type, Deserializer] = { case tpe: Type =>
    throw new IllegalArgumentException(
      s"Type $tpe for field $name is not supported. You can add it by providing new deserializer.")
  }

  private def deserializerFor(name: String) =
    (nested :+ this)
      .foldLeft(deserializers(name))((current, added) => current.orElse(added.asNested(name)))
      .orElse(noSuchSerializer(name))

  private def write(values: Map[String, String], obj: T, prefix: String): Map[String, String] = {
    obj.productIterator.to(Seq).zip(deserializers).foldLeft(values) { case (values, (value, deserializer)) =>
      deserializer.store(value, values, prefix dot deserializer.name)
    }
  }
}
