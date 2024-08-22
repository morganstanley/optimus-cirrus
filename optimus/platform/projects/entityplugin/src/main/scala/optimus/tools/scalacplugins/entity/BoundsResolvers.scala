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
package optimus.tools.scalacplugins.entity

import scala.collection.mutable
import scala.reflect.internal.Flags
import scala.tools.nsc.Global

/**
 * "Tidy up" types by ensuring they can be referred to away from their definition site. The main purpose of this class
 * is to fix unbound abstract types, so `Foo[A <: String]` becomes `Foo[A] forSome { type A <: String }` (or `Foo[_ <:
 * String]` in shorthand form).
 */
trait BoundsResolvers {
  val global: Global
  import global._

  final def boundsResolverFor(eeSym: Symbol, moduleSym: Symbol) = new BoundsResolver(eeSym, moduleSym)
  class BoundsResolver private[BoundsResolvers] (eeSym: Symbol, moduleSym0: Symbol) {
    def covariantBound(tp: Type): Type = {
      val output = convertAbstractsToBoundsOrExistentials(tp, eeSym)
      import staged.scalaVersionRange
      val stabilizer: TypeMap = if (scalaVersionRange("2.13:"): @staged) {
        new VariancedTypeMap with SymbolStabilizer {
          override val moduleSym = moduleSym0
        }
      } else {
        new TypeMap(trackVariance = true) with SymbolStabilizer {
          override val moduleSym = moduleSym0
        }
      }
      stabilizer(output)
    }

    def contravariantBound(tp: Type): Type = {
      // Cheat here slightly - we get the contravariant bound for a type by dropping it into a contravariant
      // container and getting the covariant (ie. standard) bound for the container.
      covariantBound(appliedType(Contra, tp)).typeArgs.head
    }
  }

  private trait SymbolStabilizer extends TypeMap {
    val moduleSym: Symbol
    override def apply(tp: Type): Type = tp match {
      case atr: AliasTypeRef =>
        // Convert type aliases to types (eg. convert `T` to `Int` if there's an alias `type T = Int`), and
        // widen singleton types (eg. convert `Foo.this.type` or `f.type` (where f is a Foo) to `Foo`).
        apply(atr.dealiasWiden)
      case TypeRef(ThisType(parent), sym, args) if !parent.isStaticOwner && !moduleSym.hasTransOwner(parent) =>
        // Convert `Outer.this.Inner` to `Outer#Inner`, if the module isn't also within Outer.this.
        // And convert abstract types to bounds/existentials (if they exist) on Outer
        apply(TypeRef(convertAbstractsToBoundsOrExistentials(parent.tpe, parent), sym, args))
      case TypeRef(_, definitions.ByNameParamClass, arg :: Nil) =>
        // Convert by-names to the type they wrap (eg. "=> Double" to "Double")
        apply(arg)
      case t => mapOver(t)
    }
  }

  private def convertAbstractsToBoundsOrExistentials(tp: Type, eeSym: Symbol): Type = {
    val seen = mutable.LinkedHashSet[Symbol]()

    // Recursively search the type and all its parents for any abstract type symbols
    def findHidden(t: Type): Unit = for (comp <- t; baseType <- comp.baseTypeSeq.toList; baseComp <- baseType) {
      val sym = baseComp.typeSymbolDirect
      if (!seen(sym) && sym.isAbstractType) {
        seen += sym
        findHidden(baseComp)
      }
    }

    // Find any abstract types that are bound to the containing entity/event or member. Then we use those
    // to 'pack' the symbols for the type we want to fix, which converts the abstract/hidden symbols to existentials.
    findHidden(eeSym.tpe)
    findHidden(tp)
    packSymbols(seen.toList, tp, eeSym)
  }

  // class <contra>[-T]
  private[this] lazy val Contra: Symbol = {
    val res = NoSymbol.newClass(TypeName("<contra>"))
    val tparam = res.newSyntheticTypeParam("T", Flags.CONTRAVARIANT).setInfo(TypeBounds.empty)
    res.setInfo(PolyType(tparam :: Nil, ClassInfoType(definitions.AnyRefTpe :: Nil, newScope, res)))
    res
  }
}
