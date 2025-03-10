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

import optimus.platform.annotations.internal.EmbeddableMetaDataAnnotation
import optimus.platform.pickling._
import optimus.platform.storable.EntityImpl
import optimus.platform.relational.reactive.filter.Binary
import optimus.platform.relational.tree._

import scala.collection.mutable.ArrayBuffer

object FilterClassOption extends Enumeration {
  type FilterClassOption = Value
  val Hierarchical = Value(1)
  val Explicit = Value(2)
}

sealed trait FilterElement
final case class MemberProperty(element: MemberElement) extends FilterElement {
  private val prefixImpl: StringBuilder = {
    val builder = StringBuilder.newBuilder
    builder.append("notification_entry.segment.serialized_entity.properties")
    builder
  }
  private val namesImpl: ArrayBuffer[String] = ArrayBuffer.empty[String]

  final def name = element.memberName
  private def getClass(e: MemberElement): Class[_] = {
    namesImpl += e.memberName
    e.instanceProvider match {
      case m: MemberElement =>
        val clz = getClass(m)
        val res = clz.getMethod(e.memberName).getReturnType()
        if (isEmbeddable(clz)) {
          if (e != element) prefixImpl.append(s""".children[type = "MAP" AND associated_key = "${e.memberName}"]""")
        } else
          throw new UnsupportedOperation(
            s"only properties in embeddable can be indirectly accessed, but access ${e.memberName} in ${clz.getName()}")
        res
      case provider: ProviderRelation =>
        if (e != element) prefixImpl.append(s""".children[type = "MAP" AND associated_key = "${e.memberName}"]""")
        provider.rowTypeInfo.runtimeClass.getMethod(e.memberName).getReturnType()
      case t =>
        throw new IllegalArgumentException("Unknow type " + t)
    }
  }

  private def isSupportedType(propType: Class[_]): Boolean =
    Binary.primitiveTypes.contains(propType) || Binary.stringTraverableTypes.contains(propType)
  private def isEmbeddable(propType: Class[_]): Boolean =
    propType.getAnnotation(classOf[EmbeddableMetaDataAnnotation]) ne null
  private def isScalaEnum(propType: Class[_]): Boolean = propType.getName == "scala.Enumeration$Value"
  private def isJavaEnum(propType: Class[_]): Boolean = propType.isEnum
  private def isOption(propType: Class[_]): Boolean = propType == classOf[Option[_]]

  final lazy val isEmbeddable: Boolean = isEmbeddable(propType)
  final val propType = getClass(element)
  final lazy val pickler = buildPickler(element.rowTypeInfo)

  /**
   * Get proper Pickler for a TypeInfo. For Option (which maybe nested) recursively build inner pickler, and make the
   * inner pickler implicit provided to outer pickler
   *
   * We cannot get Pickler like in initPickler method generated in CompanionObject, because we are missing Manifest of
   * type parameter here
   */
  private def buildPickler[T](typeInfo: TypeInfo[T]): Pickler[_] = {
    val clazz = typeInfo.classes.head
    if (isOption(clazz)) {
      val typeInfoOfTparam: TypeInfo[_] = typeInfo.typeParams.head
      val picklerOfTParam = buildPickler(typeInfoOfTparam)
      DefaultPicklers.optionPickler[Any](picklerOfTParam.asInstanceOf[Pickler[Any]])
    } else {
      if (classOf[EntityImpl].isAssignableFrom(clazz)) {
        EntityPickler
      } else if (isScalaEnum(clazz)) {
        DefaultPicklers.enumPickler
      } else if (isEmbeddable(clazz)) {
        // if the property type is @embeddable, use its pickler (generated by compiler plugin)
        val moduleClass = Class.forName(clazz.getName + "$")
        moduleClass.getField("MODULE$").get(moduleClass).asInstanceOf[{ def pickler: Pickler[_] }].pickler
      } else if (isJavaEnum(clazz)) {

        /**
         * because JavaEnumPickler takes a E <: Enum[E] type parameter. I tried several ways to pass the compilation but
         * failed:
         *   1. new JavaEnumPickler[]: Error:(269, 40) class type required but optimus.platform.pickling.JavaEnumPickler[]
         *      found else if (isJavaEnum(propType)) new JavaEnumPickler[_] 2. new JavaEnumPickler[_ < Enum[_]]:
         *      Error:(269, 40) class type required but optimus.platform.pickling.JavaEnumPickler[_ <: Enum[_]] found else
         *      if (isJavaEnum(propType)) new JavaEnumPickler[_ <: Enum[_]] 3. new JavaEnumPickler[Enum[_]]: Error:(269,
         *      40) type arguments [Enum[_]] do not conform to class JavaEnumPickler's type parameter bounds [E <:
         *      Enum[E]] else if (isJavaEnum(propType)) new JavaEnumPickler[Enum[_]] In a nutshell, I cannot find a way
         *      to pass in type parameter can express "nested up boundary". Says the tparam should be:
         *      1. <: Enum 2. at the same time be tparam of the upper boundary Enum
         */
        new Pickler[Enum[_]] {
          override def pickle(data: Enum[_], visitor: PickledOutputStream) = {
            visitor.writeRawObject(data.name)
          }
        }
      } else if (isSupportedType(clazz)) {
        new IdentityPickler[Any]
      } else {
        throw new UnsupportedFilterCondition(s"can't find pickler for type $clazz") // otherwise, throw exception
      }
    }
  }

  final lazy val prefix = prefixImpl.toString()
  final lazy val names: Seq[String] = namesImpl.toList
}
final case class ValueProperty(value: Any) extends FilterElement
