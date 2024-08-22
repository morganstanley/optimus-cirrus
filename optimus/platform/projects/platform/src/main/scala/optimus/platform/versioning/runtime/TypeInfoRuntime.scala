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
package optimus.platform.versioning.runtime

import optimus.platform._
import optimus.platform.versioning.RegisteredFieldTypeT
import optimus.platform.versioning.RftShape
import optimus.platform.versioning.TypeInfoApiBase
import optimus.platform.versioning.VersioningUtilsBase

import scala.collection.immutable.SortedMap
import scala.reflect.api.JavaUniverse
import scala.reflect.runtime.{currentMirror => cm}
import scala.reflect.runtime.{universe => ru}

object TypeInfoRuntime {
  val runtime = new TypeInfoRuntime(cm)

  def shapeOf(clz: Class[_], matchEmbeddable: Boolean = false) = {
    val (tp, name) = getTypeAndSymbolName(clz)
    runtime.TypeInfo(tp, name, matchEmbeddable).shape
  }

  def getRegisteredFieldType(clz: Class[_], fieldName: String) = {
    val tp = getType(clz)
    val memberSymbol = tp.decl(ru.TermName(fieldName))
    runtime.getType(memberSymbol)
  }

  private def getTypeAndSymbolName(clz: Class[_]) = {
    val classSymbol = cm.classSymbol(clz)
    (classSymbol.selfType, classSymbol.fullName)
  }

  private def getType(clz: Class[_]) = {
    val classSymbol = cm.classSymbol(clz)
    classSymbol.selfType
  }
}

class TypeInfoRuntime(val mirror: JavaUniverse#Mirror) extends VersioningUtilsBase with TypeInfoApiBase {
  type U = mirror.universe.type
  val u = mirror.universe

  def getType(symbol: ru.Symbol) = symbol.typeSignature.asInstanceOf[u.Type].fieldType

  object TypeInfo {
    val invalidTypesMsg = "Valid types are concrete entity/event/embeddable case classes."
    val invalidTypeMsg = "Valid types are concrete entity/event types."

    def apply(tp: ru.Type, className: String, matchEmbeddable: Boolean = false): TypeInfo = {
      val tpe = tp.asInstanceOf[u.Type]
      createTypeInfo(tpe, className, matchEmbeddable)
    }

    def createTypeInfo(tpe: u.Type, className: String, matchEmbeddable: Boolean): TypeInfo = {
      tpe.dealias match {
        case t: u.TypeRefApi if t <:< types.entityCompanionBase =>
          EntityCompanionTypeInfo(t, t.companion, className)
        case t: u.TypeRefApi if t <:< types.entity =>
          EntityCompanionTypeInfo(t.companion.asInstanceOf[u.TypeRef], t, className)

        case t: u.TypeRefApi if t <:< types.eventCompanionBase => EventCompanionTypeInfo(t, t.companion, className)
        case t: u.TypeRefApi if t <:< types.businessEvent =>
          EventCompanionTypeInfo(t.companion.asInstanceOf[u.TypeRef], t, className)

        case t: u.TypeRefApi if matchEmbeddable && t.sym.companion.hasAnnotation[embeddable] =>
          EmbeddableCompanionTypeInfo(t, t.companion, className)
        case t: u.TypeRefApi
            if matchEmbeddable && t.sym.isClass && !t.sym.isAbstract && t.sym.hasAnnotation[embeddable] =>
          EmbeddableCompanionTypeInfo(t.companion.asInstanceOf[u.TypeRef], t, className)

        case e: u.ExistentialTypeApi => createTypeInfo(e.underlying, className, matchEmbeddable)
        case _ =>
          val msg =
            if (matchEmbeddable)
              s"Type $className is not valid. $invalidTypesMsg"
            else s"Type $className is not valid. $invalidTypeMsg"
          throw new IllegalArgumentException(msg)
      }
    }
  }

  protected abstract class TypeInfo(t: u.Type) extends CompanionTypeInfoBase(t) {
    val className: String

    final lazy val shape: RftShape = {
      val storedProps: Seq[(String, RegisteredFieldTypeT)] = allStoredProperties.iterator.map { case (name, meta) =>
        (name, meta.rft)
      }.toIndexedSeq
      new RftShape(className, SortedMap(storedProps: _*), isCanonical)
    }
  }

  abstract class EmbeddableTypeInfo(moduleType: u.TypeRef) extends TypeInfo(moduleType) with EmbeddableTypeInfoApi {
    // To support generic type
    override def getFieldType(field: String): Option[u.Type] =
      super.getFieldType(field).map(_.asSeenFrom(classType, classType.typeSymbol))
  }

  case class EntityCompanionTypeInfo(
      moduleType: u.TypeRef,
      override protected val classType: u.Type,
      override val className: String)
      extends TypeInfo(moduleType)
      with EntityTypeInfoApi

  case class EventCompanionTypeInfo(
      moduleType: u.TypeRef,
      override protected val classType: u.Type,
      override val className: String)
      extends TypeInfo(moduleType)
      with BusinessEventTypeInfoApi

  case class EmbeddableCompanionTypeInfo(
      moduleType: u.TypeRef,
      override protected val classType: u.Type,
      override val className: String)
      extends EmbeddableTypeInfo(moduleType)
}
