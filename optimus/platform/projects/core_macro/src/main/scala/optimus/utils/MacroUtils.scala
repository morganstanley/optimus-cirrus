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
package optimus.utils

import optimus.exceptions.RTListStatic

import scala.reflect.api.Universe
import optimus.platform.internal.OptimusReporter
import optimus.tools.scalacplugins.entity.reporter.MacroUtilsAlarms._

import scala.annotation.nowarn
import scala.reflect.macros.TypecheckException
import scala.reflect.macros.blackbox
import scala.reflect.macros.contexts

object MacroUtils {
  // For debug/dev use, mostly to show how macros get expanded.
  def prettyTree[U](body: => U): String = macro Show.prettyTreeImpl[U]
  def showCode[U](body: => U): String = macro Show.showCodeImpl[U]

  /**
   * returns a deep copy of the input tree (but does not copy the symbols or types, so you'd better make sure it's an
   * untypechecked tree
   */
  def dupTree(c: blackbox.Context)(t: c.Tree): c.Tree = {
    import c.universe._
    new Transformer {
      // unavoidable casts required to get the strict tree copier
      override val treeCopy = c.asInstanceOf[contexts.Context].global.newStrictTreeCopier.asInstanceOf[TreeCopier]
    }.transform(t)
  }

  /**
   * returns a copy of body with the symbol owners updated to match the context in to which it is being spliced
   */
  def splice(c: blackbox.Context)(body: c.Tree): c.Tree = {
    explicitSplice(c)(body, c.internal.enclosingOwner)
  }

  def explicitSplice(c: blackbox.Context)(body: c.Tree, originalOwner: c.universe.Symbol): c.Tree = {
    import c.universe._, c.internal._, decorators._
    body.updateAttachment(optimus.utils.internal.Splicer.OrigOwnerAttachment(originalOwner))
    q"_root_.optimus.utils.internal.Splicer.changeOwner($body)"
  }

  // Define a method, using a macro. Tricky because, at the current level of macro technology, we have to extract the 'this'
  // pointer by examining the full macro context.
  def macroMethod[T](c: blackbox.Context)(methodName: String, f: c.Expr[_]): c.Expr[T] =
    new MacroMethod[c.type](c).macroMethod[T](methodName, f)

  // Define a method that may be called via a subclass. Allow the type of 'this' to be inferred, since we don't know it yet.
  // Allow the return type to be specified explicitly, since Scala has trouble with higher kinds.
  def macroMethodRT[T](c: blackbox.Context)(methodName: String, returnType: c.Tree, f: c.Expr[_]): c.Expr[T] =
    new MacroMethod[c.type](c).subclassMacroMethod[T](methodName, returnType, f)

  // Get source location of the caller, can be used as follows:
  // def foo(bar: Int)(implicit loc: SourceLocation) {
  //   println("called foo(" + bar + ") at " + loc)
  // }
  final case class SourceLocation(line: Int, method: String, className: String) {
    override def toString: String = s"($className:$line)"
    /** Provide full details (method-class-line) on given SourceLocation. */
    def details: String = s"$method ($className:$line)"
  }
  object SourceLocation {
    implicit def makeSourceLocation: SourceLocation = macro sourceLocationMacro

    final def sourceLocationMacro(c: blackbox.Context): c.Expr[SourceLocation] = {
      import c.universe._
      val line = c.Expr[Int](Literal(Constant(c.enclosingPosition.line)))
      val method = c.Expr[String](Literal(Constant(enclosingMethod(c).fullName)))
      val className = c.Expr[String](Literal(Constant(enclosingClass(c).fullName)))
      reify(SourceLocation(line.splice, method.splice, className.splice))
    }
  }

  /**
   * typechecks the inputTree and validates that:
   *
   * a) the symbol owner hierarchy matches the AST hierarchy b) there are no untypedchecked subtrees
   *
   * Both of these are common mistakes which are not caught by the typechecker and which cause difficult to understand
   * and reproduce errors from the compiler backend, so are worth catching early.
   *
   * Returns the typechecked and validated tree.
   */
  def typecheckAndValidate(c: blackbox.Context)(inputTree: c.Tree): c.Tree = {
    import scala.collection.mutable
    import c.universe._
    class ValidationTraverser extends Traverser {
      // the currentOwner is kept up-to-date as we traverse by super.atOwner
      currentOwner = c.internal.enclosingOwner

      // tracking for symbol -> declaring tree - used only to make the error messages more useful
      var currentTree: Tree = null
      val ownersToTrees = mutable.HashMap[Symbol, Tree]()

      override def traverse(tree: Tree): Unit = {
        // just putting these here to make it clear what's happening inside the match
        def keepGoing: Unit = { super.traverse(tree) }
        def stop: Unit = {}

        val prevTree = currentTree
        currentTree = tree
        tree match {
          // Imports seem not to have types on them even after we typecheck, but that's ok
          case Import(_, _) => stop // OPTIMUS-45654
          // this only happens if the macro spliced an untypechecked tree inside a typechecked tree (the typechecker
          // doesn't visit inside trees which are already typechecked)
          case t if t.tpe == null =>
            OptimusReporter.alarm(c, UNTYPECHECKED_TREE)(tree.pos, tree)
            stop
          case Ident(termNames.WILDCARD | typeNames.WILDCARD_STAR) => // won't have symbol
            stop
          case CaseDef(_, _, _) => // may not have symbol
            keepGoing
          // all other cases should have symbols
          case s: SymTree if s.symbol == null || s.symbol == NoSymbol =>
            OptimusReporter.alarm(c, NO_SYMBOL)(tree.pos, tree)
            stop
          case _ => // validation successful!
            keepGoing
        }
        currentTree = prevTree
      }

      // the supertrait calls this every time we enter a tree which declares a new symbol. we override it
      // to check that the new symbol is owned by the previous symbol and to produce an error if not
      override def atOwner(owner: Symbol)(traverse: => Unit): Unit = {
        ownersToTrees.put(owner, currentTree)
        // the rule for symbol ownership is very simple: every time a new symbol is declared, its owner should
        // be the closest declaration outside of it, which is still in currentOwner at this point
        validateOwnership(symbol = owner, actualOwner = owner.owner, expectedOwner = currentOwner)
        super.atOwner(owner)(traverse)
        ownersToTrees.remove(owner)
      }

      private def validateOwnership(symbol: Symbol, actualOwner: Symbol, expectedOwner: Symbol): Unit = {
        if (!ownerIsValid(actualOwner, expectedOwner)) {
          val expectedOwnerTree = ownersToTrees.get(expectedOwner).getOrElse("?")
          val actualOwnerTree = ownersToTrees.get(actualOwner).getOrElse("?")

          OptimusReporter.alarm(c, INCORRECT_SYMBOL_OWNER)(
            symbol.pos,
            symbol,
            currentTree,
            expectedOwner,
            expectedOwnerTree,
            actualOwner,
            actualOwnerTree)
        }
      }

      private def ownerIsValid(actualOwner: Symbol, expectedOwner: Symbol): Boolean = {
        // the nice case is that our actual owner matches the expected owner, but there are some special exemptions
        (actualOwner eq expectedOwner) || {
          def tryParent: Boolean = ownerIsValid(actualOwner, expectedOwner.owner)

          val tree = ownersToTrees.getOrElse(expectedOwner, EmptyTree)
          tree match {
            // pass through owners of Templates - these are weird symbols which don't own the underlying members
            // (those members are instead owned by the symbol of the class/trait which is the Template symbol's owner
            case Template(_, _, _) => tryParent

            // pass through owners of lifted eta expansions ("<synthetic> val eta$0 = (x: X) => someMethod(a)(x)"),
            // because the RHS seems to always be owned by the synthetic symbol's owner
            case ValDef(mods, name, _, _) if mods.hasFlag(Flag.SYNTHETIC) && name.toString.startsWith("eta$") =>
              tryParent

            // pass through owners of lifted artifacts - these are usually for re-ordering arguments to preserve
            // left-to-right evaluation semantics in the presence of named arguments or inlined method calls,
            // and the owner of symbols on their RHS can either be the owner of the synthetic val, or that owner's owner
            case ValDef(mods, _, _, _) if mods.hasFlag(Flag.ARTIFACT) => tryParent

            // otherwise we either didn't have a tree (in which case the actualOwner was defined outside of the overall
            // tree that we are checking, which is definitely bad), or else it wasn't one of the allowed exceptions
            // (which is also bad), so fail
            case _ => false
          }
        }
      }
    }

    try {
      val typechecked = c.typecheck(inputTree)
      (new ValidationTraverser).traverse(typechecked)
      typechecked
    } catch {
      case t: TypecheckException =>
        // when a macro result fails to typecheck, it's really useful to see the tree that the macro produced.
        // users should "never" see this messy error because if user code doesn't typecheck then it will usually
        // fail before the macro is invoked.
        OptimusReporter.alarm(c, MACRO_TYPECHECK_FAILURE)(inputTree.pos, inputTree, t.msg)
        throw t
    }
  }

  /**
   * same as typecheckAndValidate but for Exprs
   */
  def typecheckAndValidateExpr[T](c: blackbox.Context)(inputExpr: c.Expr[T]): c.Expr[T] =
    c.Expr[T](typecheckAndValidate(c)(inputExpr.tree))

  /**
   * Use this to ensure that compilation is RT
   */
  def relativeSourcePath(c: blackbox.Context)(pos: c.Position): String = {
    val path = pos.source.file.canonicalPath
    if (path.nonEmpty) {
      val osIndependent = path.replace("\\", "/")
      val relative = osIndependent.split("src/").last
      relative
    } else ""
  }

  // purportedly useful APIs deprecated in 2.12
  def enclosingClass(c: blackbox.Context) = enclosingSymbol(c)(sym => sym.isClass)
  def enclosingModule(c: blackbox.Context) = enclosingSymbol(c)(sym => sym.isModuleClass).asClass.module
  def enclosingMethod(c: blackbox.Context) = enclosingSymbol(c)(_.isMethod)
  @nowarn("cat=deprecation")
  def enclosingCode(c: blackbox.Context) = c.enclosingUnit.body

  private def enclosingSymbol(c: blackbox.Context)(predicate: c.Symbol => Boolean) = {
    var currentSym = c.internal.enclosingOwner
    while (currentSym != null && currentSym != c.universe.NoSymbol && !predicate(currentSym)) {
      currentSym = currentSym.owner
    }
    currentSym
  }

  def isRtException(c: blackbox.Context)(exn: c.Symbol): Boolean = {
    val name = exn.fullName
    def configuredWithXMacroSettings = c.settings.exists { s =>
      s.split(':') match {
        case Array("rtException", cname) => cname == name
        case _                           => false
      }
    }
    RTListStatic.members.contains(name) || configuredWithXMacroSettings
  }

  /**
   * Extract a function/closure that might be wrapped in a block for no particular reason
   */
  def functionN(c: blackbox.Context)(tree: c.Tree): (List[c.universe.ValDef], c.universe.Tree) = {
    import c.universe._
    tree match {
      case Function(args, rhs)             => (args.asInstanceOf[List[ValDef]], rhs)
      case Block(Nil, Function(args, rhs)) => (args.asInstanceOf[List[ValDef]], rhs)
      case _ =>
        OptimusReporter.alarm(c, CONFUSING_FUNCTION)(tree.pos, tree)
        null
    }
  }

  /**
   * Given arg { param => rhsStuff(param) } Rewrite to { val param = arg rhsStuff(param) }
   */
  def applyFunctionInline(c: blackbox.Context)(f: c.Tree, argVal: c.Tree): c.Tree = {
    import c.universe._, c.internal._, decorators._
    val (vds, rhs) = MacroUtils.functionN(c)(f)
    if (vds.size != 1)
      OptimusReporter.alarm(c, CONFUSING_FUNCTION)(argVal.pos, argVal)
    val param = vds.head
    // Pull out the wrapped argument from implicit Endoish. We will be assigning this to the parameter.
    // At this point, pieces of the function are still owned by the closure anonfun, which we're about to
    // get rid of, so reassign them one level out.
    val closureSym = param.symbol.owner
    val enclosingOwner = closureSym.owner
    c.internal.changeOwner(rhs, closureSym, enclosingOwner)
    c.internal.setOwner(param.symbol, enclosingOwner)
    // If argument is a block that defines symbols, then they currently belong to the enclosing owner and
    // must be adopted by the parameter.
    changeOwner(argVal, enclosingOwner, param.symbol)
    val newVd = c.internal.valDef(param.symbol, argVal)
    val inlined = q"{$newVd; $rhs}"
    val typechecked = MacroUtils.typecheckAndValidate(c)(inlined)
    typechecked
  }

  /**
   * Apply the function to a reference to the symbol
   */
  def applyFunctionToSym(c: blackbox.Context)(f: c.Tree, argSym: c.Symbol): c.Tree = {
    import c.universe._
    applyFunctionInline(c)(f, q"$argSym")
  }

  /**
   * Create a temporary variable; give it a value and an owner.
   */
  def temporaryValDef(c: blackbox.Context)(
      name: String,
      eo: c.universe.Symbol,
      rhs: c.Tree): (c.universe.ValDef, c.universe.TermSymbol) = {
    import c.internal._
    val tmpSymbol = newTermSymbol(eo, reificationSupport.freshTermName(name))
    setOwner(tmpSymbol, eo)
    changeOwner(rhs, eo, tmpSymbol)
    val rhsTc = c.typecheck(rhs)
    setInfo(tmpSymbol, rhsTc.tpe)
    val vd = valDef(tmpSymbol, rhsTc)
    (vd, tmpSymbol)
  }

}

class MacroMethod[C <: blackbox.Context](val c: C) {

  /**
   * With a context like someObject.method(a,b,c) Extract a list of Trees corresponding to someObject, a, b, c
   *
   * In an ideal world, this AST would be just Apply(Select(someObjectTree, methodName), List(aTree,bTree,cTree)), but
   * the Select can be buried under a TypeApply (and possibly other Tree elements) so we do a find for
   * Select(someObjectTree, methodName).
   */
  def extractThisAndArgs(c: blackbox.Context, method: String): List[c.Tree] = {
    import c.universe._
    var cela: c.Tree = null
    val m = c.macroApplication match {
      // Extract the arguments and the (possibly wrapped) Select tree.
      case Apply(t: Tree, args: List[Tree]) =>
        // Look for the bare Select tree, and pull out the "this" pointer.
        t.find {
          case Select(t: Tree, methodName) if methodName.toString() == method => {
            cela = t // icky
            true
          }
          case _ => false
        } map { _ =>
          cela :: args
        }
      case _ => None
    }
    m.getOrElse {
      c.error(
        c.macroApplication.pos,
        s"Couldn't extract this/args for $method from ${show(c.macroApplication)} ${showRaw(c.macroApplication)}")
      null // never reached
    }
  }

  def macroMethod[T](methodName: String, f: c.Expr[_]): c.Expr[T] = {
    import c.universe._
    import compat._

    val argVals = extractThisAndArgs(c, methodName)
    val t = f.tree match {
      case Function(args: List[Tree], body: Tree) =>
        val argAssigns = (args).zip(argVals).map { case (arg, rhs) =>
          internal.setSymbol(ValDef(Modifiers(), arg.name, arg.tpt, MacroUtils.splice(c)(rhs)), arg.symbol)
        }
        Block(argAssigns, MacroUtils.splice(c)(body))
      case _ =>
        c.error(c.macroApplication.pos, ("Couldn't find function " + Show.prettyTreeInternal(c.universe)(f.tree)))
        null
    }
    c.Expr[T](MacroUtils.typecheckAndValidate(c)(t))
  }

  /**
   * variant of macroMethod that erases type of function arguments, expecting them to be properly inferred when
   * assigned.
   */
  def subclassMacroMethod[T](methodName: String, returnType: c.Tree, f: c.Expr[_]): c.Expr[T] = {
    import c.universe._
    val argVals = extractThisAndArgs(c, methodName)
    val t = f.tree match {
      case Function(args: List[Tree], body: Tree) =>
        val argAssigns = (args).zip(argVals).map { case (arg, rhs) =>
          ValDef(arg.mods, arg.name, TypeTree(), MacroUtils.splice(c)(rhs))
        }
        Block(argAssigns, MacroUtils.splice(c)(body))
      case _ =>
        c.error(c.macroApplication.pos, ("Couldn't find function " + Show.prettyTreeInternal(c.universe)(f.tree)))
        null
    }
    val t2 = TypeApply(Select(t, TermName("asInstanceOf")), returnType :: Nil)
    c.Expr[T](MacroUtils.typecheckAndValidate(c)(t2))
  }

}

object Show {

  def showCodeImpl[U: c.WeakTypeTag](c: blackbox.Context)(body: c.Expr[U]): c.Expr[String] = {
    import c.universe._
    c.Expr[String](Literal(Constant(show(body))))
  }

  def prettyTreeImpl[U: c.WeakTypeTag](c: blackbox.Context)(body: c.Expr[U]): c.Expr[String] = {
    import c.universe._
    val t: c.Tree = body.tree
    c.Expr[String](Literal(Constant(prettyTreeInternal(c.universe)(t))))
  }

  val sp = "  "
  def falsePred(t: Any) = false
  def prettyTreeInternal(g: Universe)(t: g.Tree): String = {
    import g._
    val sb = new StringBuilder

    def abbrev(v: Any) = {
      val s =
        v.toString().replaceAll("\\{\\s*[\\r\\n]+\\s*", "{ ").replaceAll("[\\r\\n]+\\s*", "; ").replaceAll(";+", ";")

      " // " + (if (s.size < 100) s else s.substring(0, 97) + "...")
    }
    def addline(indent: Int, s: String): Unit = {
      sb ++= "\n"
      sb ++= sp * indent;
      sb ++= s;
    }
    def addList(indent: Int, l: List[g.Tree]): Unit = {
      var yet = false
      addline(indent, "List(")
      l.foreach { e =>
        if (yet)
          sb ++= ","
        else
          yet = true
        tearUp(e, indent + 2)

      }

    }

    def tearUp(t: g.Tree, ind: Int): Unit = {

      t match {

        case x @ DefDef(
              mods: g.Modifiers,
              name: g.Name @unchecked,
              tparams: List[TypeDef],
              vparamss: List[List[ValDef]],
              tpt: g.Tree,
              rhs: g.Tree) =>
          addline(ind, "DefDef(" + abbrev(x))
          addline(ind + 2, showRaw(mods) + "," + showRaw(name) + ",")
          addList(ind + 2, tparams); sb ++= ","
          addline(ind + 2, "List(")
          var yet = false
          vparamss.foreach { l: List[ValDef] =>
            {
              if (yet) { sb ++= "," }
              else { yet = true }
              addList(ind + 4, l)
            }
          }
          sb ++= "),"
          tearUp(tpt, ind + 2); sb ++= ","
          tearUp(rhs, ind + 2); sb ++= ")"

        case x @ PackageDef(t, l) =>
          addline(ind, "PackageDef(" + abbrev(x))
          tearUp(t, ind + 2)
          sb ++= ","
          addList(ind + 2, l)
          sb ++= ")"

        case x @ ModuleDef(mods, name, impl) =>
          addline(ind, "ModuleDef(" + abbrev(x))
          addline(ind + 2, showRaw(mods) + "," + showRaw(name) + ",")
          tearUp(impl, ind + 2)
          sb ++= ")"

        case x @ Template(parents, self, body) =>
          addline(ind, "Template(")
          addList(ind + 2, parents); sb ++= ","
          tearUp(self, ind + 2); sb ++= ","
          addList(ind + 2, body); sb ++= ","
          sb ++= ")"

        case x @ Apply(a1, l: List[Tree]) =>
          addline(ind, "Apply(" + abbrev(x))
          tearUp(a1, ind + 2)
          sb ++= ","
          addline(ind + 2, "List(")
          l.foreach { t: Tree =>
            tearUp(t, ind + 4)
          }
          sb ++= ")"
          addline(ind, ")")

        case x @ TypeApply(a1, l: List[Tree]) =>
          addline(ind, "TypeApply(" + abbrev(x))
          tearUp(a1, ind + 2)
          addline(ind + 2, "List(")
          l.foreach { t: Tree =>
            tearUp(t, ind + 4)
          }
          addline(ind + 2, ")")
          sb ++= ")"

        case x @ Select(a1, name) =>
          addline(ind, "Select(" + abbrev(x))
          tearUp(a1, ind + 2)
          sb ++= ","
          addline(ind + 2, showRaw(name) + ")")

        // def apply(mods: c.universe.Modifiers,name: c.universe.TermName,tpt: c.universe.Tree,rhs: c.universe.Tree): c.universe.ValDef
        case x @ ValDef(mods, name, tpt, rhs) =>
          addline(ind, "ValDef(" + abbrev(x))
          addline(ind + 2, showRaw(mods) + "," + showRaw(name) + ",")
          tearUp(tpt, ind + 2); sb ++= ","
          tearUp(rhs, ind + 2); sb ++= ")"

        case x @ Function(vds: List[ValDef], a2) =>
          addline(ind, "Function(List(" + abbrev(x))
          vds.foreach { t: Tree =>
            tearUp(t, ind + 4)
          }
          sb ++= "),"
          tearUp(a2, ind + 2)
          sb ++= ")"

        case x @ Block(l: List[Tree], t: Tree) =>
          addline(ind, "Block(")
          addline(ind + 2, "List(")
          l.foreach { t: Tree =>
            tearUp(t, ind + 4)
          }
          sb ++= "),"
          tearUp(t, ind + 2)
          sb ++= ")"

        // def apply(name: c.universe.TermName,params: List[c.universe.Ident],rhs: c.universe.Tree): c.universe.LabelDef
        case x @ LabelDef(tn, lid, rhs) =>
          addline(ind, "LabelDef(" + abbrev(x))
          addline(ind + 2, showRaw(tn) + ",")
          addline(ind + 2, showRaw(lid) + ",")
          tearUp(rhs, ind + 2)
          sb ++= ")"

        // def apply(cond: c.universe.Tree,thenp: c.universe.Tree,elsep: c.universe.Tree): c.universe.If
        case x @ If(cond, thenp, elsep) =>
          addline(ind, "If(" + abbrev(x))
          tearUp(cond, ind + 2); sb ++= ","
          tearUp(thenp, ind + 2); sb ++= ","
          tearUp(elsep, ind + 2); sb ++= ")"

        // def apply(lhs: c.universe.Tree,rhs: c.universe.Tree): c.universe.Assign
        case x @ Assign(lhs, rhs) =>
          addline(ind, "Assign(" + abbrev(x))
          tearUp(lhs, ind + 2); sb ++= ","
          tearUp(rhs, ind + 2); sb ++= ")"

        case t @ TypeTree() =>
          addline(ind, showRaw(t) + abbrev(t))

        case x @ This(ntn) =>
          addline(ind, s"This(${showRaw(ntn)})")
        case x @ Ident(ntn) =>
          addline(ind, s"${showRaw(ntn)}")

        case x =>
          addline(ind, showRaw(x))

      }

      (false, t)
    }

    tearUp(t, 0)
    sb.toString

  }

}
