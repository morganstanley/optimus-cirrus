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

import optimus.platform._
import optimus.platform.storable.Entity
import optimus.platform.storable.Storable
import optimus.platform.util.CopyHelper
import optimus.tools.scalacplugins.entity.reporter.CopyMethodAlarms

import java.time.Instant
import scala.reflect.macros.blackbox.Context

class CopyDynamic[C <: Context](val c: C, val isEvent: Boolean = false) extends MacroBase {
  import c.universe._

  def extract(self: Tree, args: Seq[c.Expr[(Any, Any)]]): Seq[(Literal, Tree)] = {
    self.tpe.typeSymbol.typeSignature // why is this line here?  comments elsewhere indicate that we need this to force certain initializations

    def processArg(arg: c.Expr[(Any, Any)]): (c.universe.Literal, c.universe.Tree) = arg.tree match {
      case Apply(_, (lit @ Literal(Constant(k: String))) :: v :: Nil) =>
        // NB: Should expand this macro after valaccessors or storedprops which does the xform to $impl vars.
        // o/w the symbols/trees are different depending on whether or not self.tpe is compiled during this run or not.
        val getter = self.tpe.member(TermName(k))
        if (!getter.isMethod) alarm(CopyMethodAlarms.NO_SUCH_PROPERTY, k, self.symbol)
        if (!getter.isPublic) alarm(CopyMethodAlarms.NOT_PUBLIC_PROPERTY, k)

        val implGetter: c.universe.Symbol = {
          val getterName = TermName(k + "$impl")

          val fromTopLevel = self.tpe.member(getterName)

          // In case of incremental compilation tpe does not contains optimus-generated members from parents
          if (fromTopLevel == NoSymbol) {
            val parentsSymbols = self.tpe.baseClasses.tail

            val getterFromParent = parentsSymbols.find(_.typeSignature.member(getterName) != NoSymbol)
            getterFromParent.map(_.typeSignature.member(getterName)).getOrElse(NoSymbol)
          } else
            fromTopLevel
        }

        val methSym: MethodSymbol = {
          if (implGetter == NoSymbol) getter.asMethod
          else if (implGetter.isMethod) implGetter.asMethod
          else {
            // handle the override val properly, if the definition is in another compiliation unit
            val alternatives = implGetter.asTerm.alternatives
            alternatives.find(_.isMethod).map(_.asMethod).getOrElse(getter.asMethod)
          }
        }

        val isConcreteType = !(self.tpe.typeSymbol.asClass.isAbstract || self.tpe.typeSymbol.asClass.isTrait)

        implicit class FirstSymbol[A](as: Iterable[A]) {
          def firstSymbol(f: A => c.universe.Symbol): c.universe.Symbol = {
            var s = NoSymbol
            val it = as.iterator
            while (s == NoSymbol && it.hasNext) s = f(it.next())
            s
          }
        }

        // We won't do compile time check if the caller's type is not concrete, because def may be overridden by val
        if (isConcreteType) {
          if (methSym == NoSymbol) alarm(CopyMethodAlarms.NO_SUCH_MEMBER, k, self.tpe)

          val owner = methSym.owner
          val sym =
            if (owner.isClass && owner.asClass.isTrait) {
              // Might be re-typed after redirect-accessors, in which case the original $impl will have migrated
              // to the $backing var.
              val backingName = TermName(k + "$backing")
              val backingSym = owner.info.member(backingName)
              if (backingSym == NoSymbol && methSym.isAccessor && methSym.isMethod && methSym.isVal)
                methSym
              else
                backingSym
            } else if (methSym.isAccessor)
              methSym.accessed
            else
              NoSymbol

          if (sym == NoSymbol)
            alarm(CopyMethodAlarms.NOT_VAL_PROPERTY, k)

          def isValidTime = k == "validTime" && (sym.typeSignature =:= typeOf[Instant])

          // NB sym.typeSignature needs to be forced before we check annotations.
          sym.typeSignature
          if (
            (!isEvent && !sym.annotations
              .exists(_.tree.tpe =:= weakTypeOf[stored])) || // if entity, but doesn't contain @stored
            (isEvent && sym.annotations
              .exists(_.tree.tpe =:= weakTypeOf[transient]) && !isValidTime) // if event, but contains @transient
          ) alarm(CopyMethodAlarms.NOT_STORABLE_PROPERTY, k)

          // TODO (OPTIMUS-0000): we need find user modify @key vals during compile time! Now, the @key is rewritten by previous phase, so we can't find it here
          // if(sym.annotations.exists(_.tpe =:= weakTypeOf[key])) error(c.macroApplication.pos, s"$k is key which can't be changed")

          val ctor = self.tpe.decl(termNames.CONSTRUCTOR) // get the primary constructor
          if (ctor != NoSymbol) {
            val psym = ctor.asMethod.paramLists.head.find { p =>
              p.name.decodedName.toString.trim() == k
            }
            psym foreach { p =>
              if (p.annotations.exists(_.tree.tpe =:= weakTypeOf[copyAsIs]))
                alarm(CopyMethodAlarms.COPY_ASIS_PROPERTY, k)
            }
          }
          if (sym.annotations.exists(_.tree.tpe =:= weakTypeOf[copyAsIs])) alarm(CopyMethodAlarms.COPY_ASIS_PROPERTY, k)
        }

        // we have to check getter, if sym is the $impl var it will have tpe =:= Node[T] instead of T
        val rtpe = getReturnType(getter).asSeenFrom(self.tpe, getter.owner)

        if (!(v.tpe weak_<:< rtpe)) {
          alarm(CopyMethodAlarms.TYPE_MISMATCH, rtpe, v.tpe)
        }

        (lit, v)
    }

    args.map(processArg(_))
  }

  def mkMap(extracted: Seq[(Literal, Tree)]): c.Expr[Map[String, Any]] = {
    c.Expr[Map[String, Any]](
      Apply(
        reify(Map).tree,
        extracted map { case (k, v) =>
          Apply(reify(Tuple2).tree, k :: v :: Nil)
        } toList))
  }

  def mkMapForEntityOrEvent(self: Tree, args: Seq[c.Expr[(Any, Any)]]): c.Expr[Map[String, Any]] = {
    val extracted = extract(self, args)
    mkMap(extracted)
  }

  def getReturnType(symbol: Symbol): Type = {
    symbol.asMethod.returnType
  }

  def checkCopyableEntity(tree: Tree, context: String): Unit = {
    def isTransient(entityTref: TypeRef): Boolean =
      !(entityTref.typeSymbol.annotations exists (_.tree.tpe =:= weakTypeOf[stored]))

    def hasPublicCtor(entityTref: TypeRef): Boolean = {
      val ctors = entityTref.decls collect {
        case mem if mem.name.decodedName.toString.trim() == termNames.CONSTRUCTOR.decodedName.toString.trim() => mem
      }
      if (ctors == Nil) true // for trait @entity, which doesn't have any constructor
      else ctors exists (_.isPublic) // at least have one public constructor

    }

    val typeRef = tree.tpe match {
      case tref: TypeRef => tref // if self type is TypeRef, which class should be @entity
      case other =>
        tree.symbol match {
          case ms: MethodSymbol =>
            getReturnType(tree.symbol).asInstanceOf[TypeRef] // handle self is a method which return a @entity
          case ts: TermSymbol => ts.typeSignature.asInstanceOf[TypeRef] // handle local variable @entity
          case _              =>
            // default behavior - don't report the @transient can't copy error;
            // this should be ok, because all the vals inside the @transient entity are not marked as @stored,
            // we miss here but user still get some error, but it maybe less meaningful for them
            null
        }
    }

    if (null != typeRef) {
      if (!isEvent && isTransient(typeRef)) alarm(CopyMethodAlarms.TRANSIENT_ENTITY, context)

      if (!hasPublicCtor(typeRef)) alarm(CopyMethodAlarms.NO_PUBLIC_CTOR)
    }

  }

  def copyUnique[E <: Storable: c.WeakTypeTag](name: c.Expr[String])(args: Seq[c.Expr[Any]]) = {
    if (args.nonEmpty) abort(CopyMethodAlarms.NEED_NAMED_PARAM, "copyUnique")
    else copyUniqueNamed(name)(Nil)
  }

  def copyUniqueNamed[E <: Storable: c.WeakTypeTag](name: c.Expr[String])(args: Seq[c.Expr[(Any, Any)]]) = {
    val Select(Apply(_, self :: Nil), _) = c.prefix.tree

    checkCopyableEntity(self, "copyUnique")

    val map = mkMapForEntityOrEvent(self, args)

    reify {
      CopyHelper.copyHelp(c.Expr[E](self).splice)(map.splice)
    }
  }

  def dalModify(name: c.Expr[String])(args: Seq[c.Expr[Any]]) = {
    if (args.nonEmpty) abort(CopyMethodAlarms.NEED_NAMED_PARAM, "dalModify")
    else dalModifyNamed(name)(Nil)
  }

  def dalModifyNamed(name: c.Expr[String])(args: Seq[c.Expr[(Any, Any)]]) = {
    if (args == Nil) alarm(CopyMethodAlarms.NEED_SECOND_ARG_LIST)

    val Apply(_, self :: Nil) = c.prefix.tree

    checkCopyableEntity(self, "DAL.modify")

    val map = mkMapForEntityOrEvent(self, args)

    reify {
      val copied = CopyHelper.copyHelp(c.Expr[Entity](self).splice)(map.splice)
      DAL.replace(c.Expr[Entity](self.duplicate).splice, copied)
    }
  }
}

object CopyMethodMacros {

  def dalModifyImpl(c: Context)(name: c.Expr[String])(args: c.Expr[(Any, Any)]*): c.Expr[Unit] =
    (new CopyDynamic[c.type](c)).dalModifyNamed(name)(args)

  def dalModify0Impl(c: Context)(name: c.Expr[String])(args: c.Expr[Any]*): c.Expr[Unit] =
    (new CopyDynamic[c.type](c)).dalModify(name)(args)

  def copyUniqueImpl[E <: Entity: c.WeakTypeTag](c: Context)(name: c.Expr[String])(
      args: c.Expr[(Any, Any)]*): c.Expr[E] =
    (new CopyDynamic[c.type](c)).copyUniqueNamed[E](name)(args)

  def copyUnique0Impl[E <: Entity: c.WeakTypeTag](c: Context)(name: c.Expr[String])(args: c.Expr[Any]*): c.Expr[E] =
    (new CopyDynamic[c.type](c)).copyUnique[E](name)(args)

  def eventCopyUniqueImpl[E <: Storable: c.WeakTypeTag](c: Context)(name: c.Expr[String])(
      args: c.Expr[(Any, Any)]*): c.Expr[E] =
    (new CopyDynamic[c.type](c, true)).copyUniqueNamed[E](name)(args)

  def eventCopyUnique0Impl[E <: Storable: c.WeakTypeTag](c: Context)(name: c.Expr[String])(
      args: c.Expr[Any]*): c.Expr[E] =
    (new CopyDynamic[c.type](c, true)).copyUnique[E](name)(args)
}
