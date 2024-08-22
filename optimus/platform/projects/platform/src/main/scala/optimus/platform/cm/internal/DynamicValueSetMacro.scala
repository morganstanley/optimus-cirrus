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
package optimus.platform.cm.internal

import optimus.utils.MacroUtils

/**
 * Helper Macro for the CM Object model DynamicValueSet classes.
 */
object DynamicValueSetMacro {

  import scala.reflect.macros.whitebox.Context

  /**
   * Finds all the predefined values in the DynamicValueSet object and returns them as a Seq[TDynamicValue]
   */
  def findPredefinedValues: Any = macro findPredefinedValues_Impl
  def findCaseObjectInstances: Any = macro findCaseObjectInstances_Impl

  def findPredefinedValues_Impl(c: Context): c.Expr[Any] = {
    import c.universe._

    val module = MacroUtils.enclosingModule(c)

    if (!module.isModule)
      c.error(c.macroApplication.pos, "this macro must be invoked from a top-level object")
    val klass = module.companion
    val matchingDecls = module.typeSignature.decls filter {
      case s if s.isMethod && s.asMethod.isAccessor && !s.isSynthetic && !s.fullName.equals("predefinedValues") =>
        s.asMethod.returnType.typeSymbol.asClass.baseClasses.contains(klass)
      case _ => false
    }
    val ids = matchingDecls map { s =>
      internal.gen.mkAttributedIdent(s)
    }
    val tree = Apply(reify(Seq).tree, ids.toList)
    c.Expr[Any](tree)
  }

  def findCaseObjectInstances_Impl(c: Context): c.Expr[Any] = {
    import c.universe._

    val module = MacroUtils.enclosingModule(c)
    if (!module.isModule)
      c.error(c.macroApplication.pos, "this macro must be invoked from a top-level object")
    val matchingDecls = module.typeSignature.decls.sorted filter {
      case s if !s.isSynthetic && s.isModule && s.companion == NoSymbol => true
      case _                                                            => false
    }
    val ids = matchingDecls map { s =>
      internal.gen.mkAttributedIdent(s)
    }
    val tree = Apply(reify(Seq).tree, ids.toList)
    c.Expr[Any](tree)
  }
}
