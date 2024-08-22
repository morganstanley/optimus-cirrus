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
package optimus.platform.dal

import optimus.platform.internal.OptimusReporter
import optimus.platform.storable.Entity
import optimus.platform.storable.Key
import optimus.tools.scalacplugins.entity.reporter.DALAlarms
import optimus.utils.MacroUtils

object DALMacros {
  import scala.reflect.macros.blackbox.Context

  def backReferenceImpl[A <: Entity: c.WeakTypeTag](c: Context)(f: c.Expr[A => Entity]) = {
    import c.universe._

    val Function(_, fbody @ Select(_, propName)) = f.tree
    val targetType = fbody.tpe

    val enclosingClass = MacroUtils.enclosingClass(c)
    // NB: This is equivalent to sym.thisType (which is a much more intuitive name IMNSHO)
    val expectedType = enclosingClass.asClass.thisPrefix

    if (!(expectedType <:< targetType))
      OptimusReporter.alarm(c, DALAlarms.BACK_REFERENCE_TYPE_MISMATCH)(f.tree.pos, expectedType, targetType)

    val csym = weakTypeTag[A].tpe.typeSymbol.companion

    val companion = internal.gen.mkAttributedIdent(csym)
    val property = csym.typeSignature.member(propName)
    if (property == NoSymbol)
      OptimusReporter.alarm(c, DALAlarms.NO_PROPERTY_IN_COMPANION)(f.tree.pos, propName, csym)
    val propInfo = internal.gen.mkAttributedSelect(companion, property)
    val makeKey = propInfo.tpe.member(TermName("makeKey"))
    if (makeKey == NoSymbol)
      OptimusReporter.alarm(c, DALAlarms.MUST_INDEX_PROPERTY)(f.tree.pos, propInfo)

    val key = c.Expr[Key[A]](Apply(Select(propInfo, makeKey), This(enclosingClass) :: Nil))
    val entityThis = c.Expr[Entity](This(enclosingClass))

    reify {
      DALImpl.resolver.findByIndex(key.splice, entityThis.splice.dal$temporalContext)
    }
  }
}
