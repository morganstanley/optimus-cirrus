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
package optimus.tools.scalacplugins.entity.refchecks
import optimus.tools.scalacplugins.entity.Attachment
import optimus.tools.scalacplugins.entity.TypedUtils
import optimus.tools.scalacplugins.entity.reporter.OptimusErrors
import optimus.tools.scalacplugins.entity.reporter.OptimusNonErrorMessages

import scala.reflect.internal.util.TriState

/**
 * Contextual checks, including scenario independence and @impure errors and warnings.
 */
trait ContextualChecks extends TypedUtils {
  import global._

  final object ContextualChecker {
    sealed trait Context {
      val origin: Symbol
      val onlyAllowScenarioIndependentCalls: Boolean
      val onlyAllowPureCalls: Boolean
    }

    final case object Unchecked extends Context {
      override val origin: Symbol = NoSymbol
      override val onlyAllowScenarioIndependentCalls: Boolean = false
      override val onlyAllowPureCalls: Boolean = false
    }

    // These case classes are not final, see https://github.com/scala/bug/issues/4440
    case class EntityCtor(origin: Symbol) extends Context {
      override val onlyAllowScenarioIndependentCalls: Boolean = true
      override val onlyAllowPureCalls: Boolean = false
    }
    case class EmbeddableCtor(origin: Symbol) extends Context {
      override val onlyAllowScenarioIndependentCalls: Boolean = true
      override val onlyAllowPureCalls: Boolean = false
    }
    case class EventCtor(origin: Symbol) extends Context {
      override val onlyAllowScenarioIndependentCalls: Boolean = false
      override val onlyAllowPureCalls: Boolean = false
    }
    case class DefDefArgs(origin: Symbol) extends Context {
      override val onlyAllowScenarioIndependentCalls: Boolean = false
      override val onlyAllowPureCalls: Boolean = false
    }
    case class DefRHS(origin: Symbol, isSI: Boolean, isRT: Boolean) extends Context {
      override val onlyAllowScenarioIndependentCalls: Boolean = isSI
      override val onlyAllowPureCalls: Boolean = isRT
    }
    case class ArgsToTransparentFunction(origin: Symbol, parent: Context) extends Context {
      override val onlyAllowScenarioIndependentCalls: Boolean = parent.onlyAllowScenarioIndependentCalls
      override val onlyAllowPureCalls: Boolean = parent.onlyAllowPureCalls
    }

  }

  class ContextualChecker extends Traverser {
    import ContextualChecker._
    private val _state = new StateHolder[Context](Unchecked)
    private def state = _state.currentValue
    private def enterCtx(ctx: Context)(f: => Unit) = _state.enterNewContext(ctx)(f)

    override def traverse(tree: global.Tree): Unit = {
      val treeSym = tree.symbol

      // Check for scenario dependencies in SI contexts
      if (state.onlyAllowScenarioIndependentCalls) {
        tree match {
          case Select(_, _) if isNode(treeSym) && !allowedInSIContext(treeSym) =>
            // Direct tweak access:
            if (isTweakable(tree))
              alarm(OptimusNonErrorMessages.TWEAK_IN_SI_CONTEXT, tree.pos, treeSym.name, state.origin.name)

            // Possible tweak accesses:
            // [BACKCOMPAT] Treat @node vals as SI, even though they're not (Fix: Remove val and if statement)
            val isFinalNonTweakableVal = !isTweakable(tree) && (treeSym.isGetter || isValAccessor(treeSym))

            // [BACKCOMPAT] We have two warnings: calling @node from SI and calling anything else that is not a node but
            // is async and not SI from SI (@async, NodeFunction0). These should be only ONE warning.
            if (!isFinalNonTweakableVal) {
              if (treeSym.hasAnnotation(NodeAnnotation))
                alarm(OptimusNonErrorMessages.SI_CALL_NONSI, tree.pos, treeSym.name, state.origin.name)
              else
                alarm(OptimusNonErrorMessages.SI_CALL_NONSI_NOT_NODE, tree.pos, treeSym.name, state.origin.name)
            }

          case _ =>
        }
      }

      // check for @impure errors and warnings but again don't recurse
      if (state.onlyAllowPureCalls) {
        tree match {
          // @node val foo = impureFunction() is ok if we are constructing entity off-graph.
          case func: Select if func.symbol.hasAnnotation(ImpureAnnotation) =>
            alarm(OptimusErrors.IMPURE_IN_NODE, func.pos, func.symbol.name, state.origin.name)
          case func: Select if func.symbol.hasAnnotation(AspirationallyImpureAnnotation) =>
            alarm(OptimusNonErrorMessages.IMPURE_IN_NODE, func.pos, func.symbol.name, state.origin.name)
          case _ =>
        }
      }

      // All the traversal is here
      tree match {
        // @entity
        case _: ClassDef | _: ModuleDef if isEntity(treeSym) =>
          enterCtx(EntityCtor(treeSym))(super.traverse(tree))

        // @event
        case _: ClassDef | _: ModuleDef if isEvent(treeSym) =>
          enterCtx(EventCtor(treeSym))(super.traverse(tree))

        // @embeddable
        case _: ClassDef | _: ModuleDef if isEmbeddable(treeSym) =>
          enterCtx(EmbeddableCtor(treeSym))(super.traverse(tree))

        // SI transparent functions have their arguments checked in a ArgsToTransparentFunction context, which is
        // identical to the current context, except that Function(...) are assumed to be SI / pure if the context is
        // pure.
        case Apply(fun, args) if isSITransparent(treeSym) =>
          traverse(fun)
          enterCtx(ArgsToTransparentFunction(fun.symbol, state)) {
            args.foreach(traverse)
          }

        // For a node lifted function,
        //   - lifted parameters are not checked.
        //   - non-lifted parameters are traversed assuming that the qualifier is executed in this current context and
        //     the name isn't, so we only traverse the qualifier.
        case Apply(fun, args) if hasNodeLiftAnno(treeSym) || treeSym.hasAnnotation(NodeSyncLiftAnnotation) =>
          traverse(fun)
          foreachParamsAndArgs(fun.tpe.params, args) { (param, arg) =>
            if (isNodeLifted(param)) enterCtx(Unchecked)(traverse(arg))
            else {
              arg match {
                case Select(qual, _) => traverse(qual)
                case _               => traverse(arg)
              }
            }
          }

        // By name parameters of normal (not transparent) functions are unchecked. Normal parameters are assumed to be
        // evaluated right here and traversed normally.
        case Apply(fun, args) =>
          traverse(fun)
          if (fun.tpe != null) {
            foreachParamsAndArgs(fun.tpe.params, args) { (param, arg) =>
              if (param.isByNameParam) enterCtx(Unchecked)(traverse(arg))
              else traverse(arg)
            }
          } else {
            super.traverseTrees(args)
          }

        case Function(params, rhs) =>
          // Function parameters of a transparent call f() are traversed in the context where f() is called. If f() is
          // transparent and g() is a normal function, then,
          //
          //   @node @scenarioIndependent def foo() = {
          //      f(() => tweakable)      // this produces a SI -> SD warning
          //      g(() => tweakable)      // this does not produce a SI -> SD warning
          //      val x = () => tweakable // and neither does this
          //   }
          //
          // Note that transparency is not transitive, such that this doesn't warn (and is fine generally)
          //
          //   @node @scenarioIndependent def foo() = {
          //      f(() => (() => tweakable))
          //   }
          //
          // This is why we enter the parent context instead of staying in the ArgsToTransparentFunction context below.
          state match {
            case ArgsToTransparentFunction(_, parent) => enterCtx(parent)(traverse(rhs))
            case _                                    => enterCtx(Unchecked)(traverse(rhs))
          }

        case dd: DefDef =>
          /*
          Function arguments don't have to be SI themselves. Here is an example,

          @node(tweak=true) def context: Context = ...
          @scenarioIndependent @node def foo(c: Context = context) = ...

          The above is fine: foo has an SI body. Calling it with an explicit context in an SI node is OK. But calling
          it without an argument is bad, because then it will need to grab the (tweakable) default value. This is
          already accounted for in AsyncDefaultGetterComponent
           */
          enterCtx(DefDefArgs(dd.symbol)) {
            dd.tparams.foreach(this.traverse)
            dd.vparamss.foreach(_.foreach(this.traverse))
            this.traverse(dd.tpt)
          }

          // Note that really we should probably do the match over attach.rt != False, that is consider only
          // specifically non-RT node as impure-safe. However, DefaultAsyncGetterComponent creates RawNodes, not
          // non-RT nodes, so to support this (totally ok) code,
          //   @async @impure def dangerous() = ...
          //   def foo(x = dangerous) = ...
          // we can't filter RawNodes.

          val isReallyRT = dd.symbol.attachments.get[Attachment.Node] match {
            case Some(attach) => attach.rt != TriState.False
            case None         => false
          }

          enterCtx(DefRHS(dd.symbol, internalsMustBeSI(dd.symbol), isReallyRT)) { this.traverse(dd.rhs) }

        /* Note that here we do not default to switching to unchecked! This means we always check for errors unless we
           have explicitly decided to stop (because we encountered a def body etc.) If there are false positives, the
           solution is to add more exceptions above.
         */
        case _ => super.traverse(tree)
      }
    }
  }
}
