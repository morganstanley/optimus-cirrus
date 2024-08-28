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

import optimus.core.utils.RuntimeMirror
import optimus.platform.annotations.parallelizable

import scala.reflect.api
import scala.reflect.api._
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

@parallelizable
object ReflectUtils {

  /** wrap up a Type in to a simple TypeTag (which doesn't support universe migration) */
  def mkTypeTag[T](tpe: Type, mirror: universe.Mirror): TypeTag[T] = {
    TypeTag(
      mirror,
      new TypeCreator {
        override def apply[U <: Universe with Singleton](m: api.Mirror[U]): U#Type = {
          if (m eq mirror) tpe.asInstanceOf[U#Type]
          else throw new IllegalArgumentException(s"Type tag defined in $mirror cannot be migrated to other mirrors.")
        }
      }
    )
  }

  /** gets the TypeTag for a java Class */
  def typeTagForClass[T](cls: Class[T]): TypeTag[T] = {
    val mirror = RuntimeMirror.forClass(cls)
    mkTypeTag[T](mirror.classSymbol(cls).info, mirror)
  }

  // recursively collects up the real (non-structural) types and the methods from the structural refinements
  def extractRealTypesAndStructuralMethods[U <: Universe](u: U)(tpe: u.Type): (List[u.Type], List[u.MethodSymbol]) = {
    import u._

    tpe.dealias match {
      case RefinedType(parents, scope) =>
        val (parentTypes, parentMethods) = parents.map(p => extractRealTypesAndStructuralMethods(u)(p)).unzip
        val methods = scope.toList.collect { case m: MethodSymbol => m }
        (parentTypes.flatten, methods ::: parentMethods.flatten)
      case BoundedWildcardType(TypeBounds(lo, hi)) =>
        extractRealTypesAndStructuralMethods(u)(hi)
      case other =>
        tpe.typeSymbol.typeSignature match {
          case TypeBounds(lo, hi) => extractRealTypesAndStructuralMethods(u)(hi)
          case ClassInfoType(parents, decls, typeSym)
              if (typeSym.name.toString.indexOf(
                "anon") != -1) => // others are concrete init not "new {...}" or "new A{...}"
            val (parentTypes, parentMethods) = parents.map(p => extractRealTypesAndStructuralMethods(u)(p)).unzip
            val methods = decls.toList.collect { case m if m.isMethod && !m.isConstructor => m.asMethod }
            (parentTypes.flatten, methods ::: parentMethods.flatten)
          case rt @ RefinedType(_, _) => extractRealTypesAndStructuralMethods(u)(rt)
          case _                      => (List(other), Nil)
        }

    }
  }

  def getValues[T: WeakTypeTag]: Set[T] = {
    val mirror = RuntimeMirror.forClass(this.getClass)
    val tpe = weakTypeOf[T]

    tpe.companion.members.collect {
      case sym if sym.isModule && sym.typeSignature <:< tpe =>
        mirror.reflectModule(sym.asModule).instance.asInstanceOf[T]
    }.toSet
  }

  def knownDirectSubclasses[T: WeakTypeTag]: Set[Symbol] =
    weakTypeOf[T].typeSymbol.asClass.knownDirectSubclasses

  def getCompanion[T](klass: Class[_]): T = {
    val moduleClass: Class[_] = Class.forName(klass.getName + "$", true, klass.getClassLoader)
    moduleClass.getField("MODULE$").get(moduleClass).asInstanceOf[T]
  }

  /**
   * a global lock which should be held during any scala reflect / toolbox operations to work around the global lack of
   * threadsafety in scala reflect API
   */
  private class ScalaReflectLock
  val scalaReflectLock: Object = new ScalaReflectLock
}
