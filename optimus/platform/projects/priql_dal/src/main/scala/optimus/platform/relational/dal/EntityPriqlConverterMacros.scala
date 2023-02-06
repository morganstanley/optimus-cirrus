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

import optimus.platform._
import optimus.platform.internal.MacroBase
import optimus.platform.relational.PriqlConverter
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference

import scala.reflect.macros.blackbox.Context

object EntityPriqlConverterMacros {
  def entityPriqlConverterImpl[T: c.WeakTypeTag, U <: Entity: c.WeakTypeTag](c: Context): c.Expr[PriqlConverter[T, U]] =
    new EntityPriqlConverterMacros[c.type](c).entityConverter[T, U]
}

private[relational] class EntityPriqlConverterMacros[C <: Context](val c: C) extends MacroBase {
  import c.universe._

  val EntityReferencePriqlConverterT = Ident(symbolOf[EntityReferencePriqlConverter[_, _]])
  val EntityConverterTypeT = Ident(symbolOf[EntityPriqlConverter[_, _, _]])
  val PriqlConverterTpe = typeOf[PriqlConverter[_, _]]
  val PriqlConverterT = Ident(typeOf[PriqlConverter.type].termSymbol)
  val EntityReferenceTpe = typeOf[EntityReference]

  def entityConverter[T: c.WeakTypeTag, U <: Entity: c.WeakTypeTag]: c.Expr[PriqlConverter[T, U]] = {
    val tType = weakTypeOf[T]
    val uType = weakTypeOf[U]
    val tree =
      if (tType <:< uType) q"$PriqlConverterT.identityConverter[$tType, $uType]"
      else {
        c.inferImplicitValue(
          appliedType(PriqlConverterTpe, tType, uType),
          silent = true,
          withMacrosDisabled = true) orElse {
          val keySymOpt = uType.members.find(sym => sym.isMethod && hasAnnotation[key](Ident(sym)))
          val companion = Ident(uType.typeSymbol.companion)
          keySymOpt map { sym =>
            val keyType = sym.typeSignatureIn(uType).finalResultType
            val convType = appliedType(PriqlConverterTpe, tType, keyType)
            val converterTree = c.inferImplicitValue(convType, silent = false)
            q"new $EntityConverterTypeT($converterTree, $companion)"
          } getOrElse {
            val keyType = EntityReferenceTpe
            val convType = appliedType(PriqlConverterTpe, tType, keyType)
            val converterTree = c.inferImplicitValue(convType, silent = false)
            q"new $EntityReferencePriqlConverterT($converterTree, $companion)"
          }
        }
      }
    c.Expr[PriqlConverter[T, U]](tree)
  }
}
