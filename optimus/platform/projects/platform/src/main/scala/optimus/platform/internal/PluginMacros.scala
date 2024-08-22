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
package optimus.platform.internal

import optimus.graph.ReallyNontweakablePropertyInfo
import optimus.platform._
import optimus.platform.storable.Storable
import optimus.tools.scalacplugins.entity.reporter.PluginMacrosAlarms._

import scala.reflect.macros.whitebox.Context

object PluginMacros {
  // this type alias is a workaround for a weird "macro implementation has incompatible shape" error which we get,
  // when this type is added explicitly to findInheritPropertyInfoForCompoundKey
  type NontweakablePropertyInfo = ReallyNontweakablePropertyInfo[_, _]
  def findInheritPropertyInfoForCompoundKey[E <: Storable](expr: E => Any): NontweakablePropertyInfo =
    macro findInheritPropertyInfoForCompoundKeyImpl[E]
  def findInheritPropertyInfoForCompoundKeyImpl[E <: Storable: c.WeakTypeTag](c: Context)(
      expr: c.Expr[E => Any]): c.Expr[ReallyNontweakablePropertyInfo[_, _]] = {
    import c.universe._

    val etpe = weakTypeTag[E].tpe
    val Function(_, Select(_, prop)) = expr.tree
    etpe.baseClasses.tail
      .find { cls =>
        val cop = cls.companion
        cop.typeSignature.decl(prop) != NoSymbol
      }
      .map { cls =>
        c.Expr[ReallyNontweakablePropertyInfo[_, _]](Select(Ident(cls.companion.name), prop))
      }
      .getOrElse {
        OptimusReporter.alarm(c, KEY_INDEX_COMPONENT_PROP_INFO_MISSING)(c.enclosingPosition, prop)
        reify[ReallyNontweakablePropertyInfo[_, _]] { ??? }
      }
  }

  def getIsCollection[E <: Storable](expr: E => Any): Boolean = macro getIsCollectionImpl[E]
  def getIsCollectionImpl[E <: Storable: c.WeakTypeTag](c: Context)(expr: c.Expr[E => Any]): c.Expr[Boolean] = {
    import c.universe._

    val tpe = expr.tree.tpe.typeArgs(1)

    // we do not want to treat ImmutableArray[Byte] as collection
    val b = ((tpe <:< typeOf[Traversable[_]] && !(tpe =:= typeOf[ImmutableArray[Byte]])) ||
      tpe <:< typeOf[Array[_]]) &&
      // Storable is a parent class of @event and @entity.
      !(tpe <:< typeOf[Storable] || tpe.dealias.erasure.typeSymbol.annotations
        .exists(_.tree.tpe =:= weakTypeOf[embeddable]))
    c.Expr[Boolean](q"$b")
  }
}
