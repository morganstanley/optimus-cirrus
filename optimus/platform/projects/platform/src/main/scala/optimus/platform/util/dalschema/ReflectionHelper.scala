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
package optimus.platform.util.dalschema

import optimus.core.utils.RuntimeMirror
import optimus.exceptions.RTException
import optimus.platform._
import optimus.platform.annotations.handle
import optimus.platform.annotations.valAccessor
import optimus.scalacompat.reflect.NamedArgTree

import java.lang.reflect.InvocationTargetException
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

@entity private[optimus] object ReflectionHelper {

  @node def reflectValue(instance: Any, symbol: Symbol): Any = {

    val reflectedValue = NodeTry {
      reflectValueOrException(instance, symbol)
    } getOrRecover {
      case e: InvocationTargetException =>
        Option(e.getTargetException).map { _.getMessage } getOrElse { "[error]" }
      case _: ScalaReflectionException => "[unused]"
      case e @ RTException             => s"Exception when accessing field $symbol on $instance: ${e.getMessage}"
    }
    if (reflectedValue == null) "[null]" else reflectedValue
  }

  @node def reflectValueOrException(instance: Any, symbol: Symbol): Any = {

    val reflectedValue = {
      val r = mirror(this.getClass.getClassLoader).reflect(instance)
      symbol match {
        case m: MethodSymbol @unchecked => r.reflectMethod(m)()
        case f: TermSymbol @unchecked   =>
          // Will handle field reflection in a specific way due to
          // https://github.com/scala/bug/issues/12096
          // The scala compiler removes any unused fields so reflection
          // fails on them. This is fixed in 2.12 but we're not on that
          // version yet.
          r.reflectField(f).get
      }
    }
    if (reflectedValue == null) null else reflectedValue
  }

  def mirror(clsLoader: ClassLoader): Mirror = {
    RuntimeMirror.forClassLoader(clsLoader)
  }

  def isKey(symbol: Symbol): Boolean = {
    hasAnnotation(symbol, typeOf[key])
  }

  def isIndexed(symbol: Symbol): Boolean = {
    hasAnnotation(symbol, typeOf[indexed])
  }

  def isUniqueIndexed(symbol: universe.Symbol): Boolean = {
    getAnnotationParameterValue(symbol, typeOf[indexed], "unique").exists(_.asInstanceOf[Boolean])
  }

  def isProjected(symbol: Symbol): Boolean = {
    hasAnnotation(symbol, typeOf[projected])
  }

  def isInheritedAnnotation(tpe: Type): Boolean = {
    tpe =:= typeOf[indexed] || tpe =:= typeOf[key] || tpe =:= typeOf[stored]
  }

  def hasAnnotation(symbol: Symbol, tpe: Type): Boolean = findAnnotation(symbol, tpe).nonEmpty

  def getAnnotationParameters(a: Annotation): Seq[(String, Any)] = {
    // We want to get parameter names but it's not straight forward
    // to do it for scala annotations. Here, we get it from the annotations
    // constructor parameter list
    // If we didn't manage to get the parameter names, i.e. paramNamesOpt
    // is None. Then we'll just use the positional index value as the parameter
    // string name. We take care of this in the below match block.
    val paramNamesOpt = a.tree.tpe.members
      .find(_.isConstructor)
      .map {
        _.asMethod.paramLists.flatten.map {
          _.name.decodedName.toString
        }
      }

    a.tree match {
      case Apply(_, args) =>
        args.zipWithIndex.map {
          case (Literal(Constant(value: Any)), index: Int) =>
            (paramNamesOpt.map { _(index) }.getOrElse(index.toString), value)
          case (NamedArgTree(Ident(name), Literal(Constant(value))), _) =>
            // In the case of a java annotation, we already get the parameter name in the
            // pattern match above
            (name.decodedName.toString, value)
          case (defArg: Tree, index: Int) if defArg.symbol.isMethod && defArg.symbol.asMethod.isParamWithDefault =>
            val module = a.tree.tpe.typeSymbol.asClass.companion.asModule
            val inst = currentMirror.reflectModule(module).instance
            val companionMirror = currentMirror.reflect(inst)
            // Defaulted argument. Need to create an instance of companion of the annotation
            val defArgMethod = companionMirror.reflectMethod(defArg.symbol.asMethod)
            val defaultValue = defArgMethod()
            (paramNamesOpt.map { _(index) }.getOrElse(index.toString), defaultValue)
          case (x: Any, index: Int) =>
            // Fallback for anything we didn't expect to get.
            (paramNamesOpt.map { _(index) }.getOrElse(index.toString), x)
        }
      case _ =>
        // We expect the above case to match every time, but returning no parameters
        // rather than throwing MatchError is preferred here.
        List.empty
    }
  }

  def getAnnotationParameterValue(symbol: Symbol, annot: Type, param: String): Option[Any] = {
    findAnnotation(symbol, annot).flatMap {
      getAnnotationParameters(_).collectFirst { case (name, value) if name == param => value }
    }
  }

  def findAnnotation(symbol: Symbol, tpe: Type): Option[Annotation] = {
    (symbol +: (if (isInheritedAnnotation(tpe)) symbol.asTerm.overrides else Nil))
      .flatMap(_.annotations)
      .find(_.tree.tpe =:= tpe)
  }

  // Returns TermSymbols for vals and nullary defs that are not
  // marked @async or @impure or have Unit return types.
  @scenarioIndependent @node def candidateMembersFor(tpe: Type): Iterable[TermSymbol] = {

    def consideredSideEffecting(m: MethodSymbol) = {
      // we support Unit vals and we support @key defs of Unit type so have to special case them
      // all other Unit types are going to be considered side effecting defs.
      !m.annotations.exists(a => a.tree.tpe =:= typeOf[key] || a.tree.tpe =:= typeOf[valAccessor]) &&
      m.returnType.finalResultType.dealias =:= typeOf[Unit] ||
      m.annotations.exists { a =>
        val aType = a.tree.tpe
        aType =:= typeOf[async] || aType =:= typeOf[impure] || aType =:= typeOf[handle]
      }
    }

    def otherwiseFiltered(symbol: TermSymbol) = {
      symbol.name.decodedName.toString == "argsHash"
    }

    tpe.members
      .flatMap { s: Symbol =>
        s match {
          case m: MethodSymbol if !consideredSideEffecting(m) && !otherwiseFiltered(m) =>
            m.typeSignature match {
              case t if t.paramLists.flatten.isEmpty =>
                // Would have preferred to pattern-match on
                // NullaryMethodType but finding that defs defined
                // as nullary on anonymous types appear not to
                // match so we'll just have to accept nillarys.
                // Silver-lining is that we need to select toString()
                // anyway so we don't have to special case it here.
                Option(m)
              case _ =>
                None
            }
          case v: TermSymbol if !v.isMethod && !otherwiseFiltered(v) =>
            Option(v)
          case _ =>
            None
        }
      }
  }

  def typeString(t: Type, fqn: Boolean, typeArgs: Boolean): String = {
    if (t <:< typeOf[Enumeration#Value]) {
      // Need to replace the final '.' with '$' else the class name
      // won't be loadable with Class.forName
      val typeStr = t.toString
      if (typeStr != "Enumeration#Value") {
        val loc = typeStr.lastIndexOf('.')
        val dollar = "$"
        s"${typeStr.substring(0, loc)}$dollar${typeStr.substring(loc + 1, typeStr.length)}"
      } else typeStr
    } else {
      (fqn, typeArgs) match {
        case (_, true) =>
          val classStr = if (fqn) t.typeSymbol.fullName else t.typeSymbol.name.decodedName.toString
          val typeArgStr = t.typeArgs
            .map {
              case tb: TypeBounds => typeString(tb.hi.dealias, fqn, typeArgs)
              case t              => typeString(t.dealias, fqn, typeArgs)
            }
            .mkString(",")
          if (typeArgStr == "") {
            classStr
          } else {
            s"$classStr[$typeArgStr]"
          }
        case (true, false) =>
          val fullName = t.typeSymbol.fullName
          if (t.typeArgs.nonEmpty) {
            val startIndex = fullName.indexOf('[')
            if (startIndex == -1) {
              // Tuples use the (type1, type2, ...(syntax) so
              // it is expected there is no "[" for them.
              // Use the simpleName from the typeSymbol
              t.typeSymbol.fullName
            } else {
              fullName.substring(0, startIndex)
            }
          } else {
            fullName
          }
        case (false, false) => t.typeSymbol.name.decodedName.toString
      }
    }
  }

  def getClassSymbolFromName(fqn: String, clsLoader: ClassLoader): universe.ClassSymbol = {
    val clazz = Class.forName(fqn, false, clsLoader)
    mirror(clsLoader).classSymbol(clazz)
  }
}
