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
package optimus.platform

import optimus.core.needsPlugin
import optimus.graph.{Node, NodeClsIDSupport, NodeKey}
import optimus.platform.PluginHelpers.toNodeFactory
import optimus.platform.annotations._
import optimus.platform.internal.WithOptimusMacroReporter
import optimus.utils.MacroUtils.splice

import scala.reflect.macros.blackbox.Context

/**
 * A zero-arg NodeFunction which allows its result to be tweaked (in some implementation-specific way). This allows you
 * to pass around a tweakable node as a reified object, just like a NodeFunction lets you pass around a node as a
 * reified object (and just like a Function lets you pass around a plain method as a reified object).
 *
 * You can construct a TweakableNodeFunction from a normal "@node(tweak = true)" val or (zero-arg) def by using
 * TweakableNodeFunction.apply.
 */
trait TweakableNodeFunction[T] extends NodeFunction0[T] {

  /**
   * returns tweak(s) that will tweak the return value of this function to t
   */
  @node def tweakTo(t: T): Seq[Tweak]

  /**
   * converts the type of this tweakable node function from T to S by applying an "out" function to the result of #apply
   * and an "in" function to the input of #tweakTo
   *
   * @param out
   *   a function from T to S
   * @param in
   *   a function from S to T, typically the inverse of out (though that is not required)
   */
  @nodeSyncLift
  @nodeLiftByName
  final def convert[S](@nodeLift @withNodeClassID out: T => S)(
      @nodeLift @withNodeClassID in: S => T): TweakableNodeFunction[S] =
    convert$withNode(toNodeFactory(out))(toNodeFactory(in))
  final def convert$withNode[S](out: T => Node[S])(in: S => Node[T]): TweakableNodeFunction[S] = {
    TweakableNodeFunction.ConversionProxy(this, out, in)
  }

  /**
   * Returns a TweakableNodeFunction for T#someField (*).
   *
   * Note: Tweaks to someField are achieved by calling T#copy(someField = x) and then tweaking the underlying. This
   * could have performance implications because a) deeply nested ofField usages could cause a significant amount of
   * copying for every tweak, and b) anything dependent on the underlying tweakable will be invalidated when any field
   * is tweaked (for example, fields on the same underlying node accessed via other TweakableNodeFunctions would also be
   * invalidated and recomputed when next accessed).
   *
   * In many cases, it may be more appropriate to just make T an @entity and someField a tweakable node.
   * TweakableNodeFunction#ofField is designed to reduce boilerplate in cases where that isn't possible.
   *
   * (*) More formally: If T is a case class (or other class with standard "copy" method) a f is a lambda of the form
   * "_.someField", returns a new TweakableNodeFunction where apply() returns "this.apply().someField", and where
   * tweakTo returns "this.tweakTo(this.apply().copy(someField = t))" (and if not, it fails to compile)
   */
  final def ofField[F](f: T => F): TweakableNodeFunction[F] = macro TweakableNodeFunctionMacros.ofField[T, F]

}

object TweakableNodeFunction {

  /**
   * Given a tweakable node t, returns a TweakableNodeFunction where apply() returns the value of t and tweakTo tweaks
   * t.
   *
   * Note that the node is captured "by value", so if you write TweakableNodeFunction(foo.bar) then it refers forever to
   * that "bar" node, even if foo is updated to point to something else (c.f. TweakableNodeFunction#byName)
   */
  @expectingTweaks
  @nodeLift
  def apply[T](node: T): TweakableNodeFunction[T] = needsPlugin
  def apply$node[T](node: NodeKey[T]): TweakableNodeFunction[T] = apply(node)

  /**
   * Given a node key, returns a TweakableNodeFunction where apply() returns the value of that nodeKey and tweakTo
   * tweaks that node key.
   *
   * Normally application code should use apply(node) directly rather than using the nodeKey.
   */
  def apply[T](node: NodeKey[T]): TweakableNodeFunction[T] = FromNodeKey(node)

  /**
   * Creates a TweakableNodeFunction from two node functions, such that #apply() will return get.apply() and #tweakTo(t)
   * will return set(t)
   */
  def apply[T](get: NodeFunction0[T], set: NodeFunction1[T, Seq[Tweak]]): TweakableNodeFunction[T] =
    FunctionProxy(get, set)

  /**
   * Given a tweakable node t, returns a TweakableNodeFunction where apply() returns the value of t and tweakTo tweaks
   * t.
   *
   * Unlike TweakableNodeFunction#apply, this captures the node "by name", so it will respond to changes in the captured
   * expression. For example, if you write val t = TweakableNodeFunction.byName(foo.bar) and then tweak foo so that
   * foo.bar is now a different node, then t will point to that different node, not the original bar.
   */
  def byName[T](node: T): TweakableNodeFunction[T] = macro TweakableNodeFunctionMacros.byNameTweakableNodeFunction[T]

  private[optimus] final case class FromNodeKey[T](key: NodeKey[T]) extends TweakableNodeFunction[T] {
    @node def apply(): T = asyncLookupAndGet(key.tidyKey.asInstanceOf[NodeKey[T]])
    @node def tweakTo(t: T): Seq[Tweak] = SimpleValueTweak(key.tidyKey.asInstanceOf[NodeKey[T]])(t) :: Nil
  }

  private final case class FunctionProxy[T](get: NodeFunction0[T], set: NodeFunction1[T, Seq[Tweak]])
      extends TweakableNodeFunction[T] {
    @node def apply(): T = get()
    @node def tweakTo(newValue: T): Seq[Tweak] = set(newValue)
  }

  // Has to be public since the macro constructs it. The first parameter list participates in the case class key and
  // the second doesn't, so that FieldProxys constructed for the same underlying and name are equal
  final case class FieldProxy[T, F](underlying: TweakableNodeFunction[T], name: String)(
      extract: T => F,
      copyWithChange: (T, F) => T)
      extends TweakableNodeFunction[F] {
    @node def apply(): F = extract(underlying.apply())
    @node def tweakTo(newValue: F): Seq[Tweak] = underlying.tweakTo(copyWithChange(underlying.apply(), newValue))
  }

  private final case class ConversionProxy[T, S](
      private val underlying: TweakableNodeFunction[T],
      private val out: T => Node[S],
      private val in: S => Node[T])
      extends TweakableNodeFunction[S] {

    @node def apply(): S = {
      val t = underlying.apply()
      asyncGet(out(t).enqueue)
    }

    @node def tweakTo(s: S): Seq[Tweak] = {
      val t = asyncGet(in(s).enqueue)
      underlying.tweakTo(t)
    }

    override def equals(o: Any): Boolean = o match {
      case that: ConversionProxy[_, _] =>
        (underlying == that.underlying) &&
        NodeClsIDSupport.equals(in, that.in) &&
        NodeClsIDSupport.equals(out, that.out)
      case _ => false
    }
    override def hashCode: Int =
      underlying.hashCode + 31 * NodeClsIDSupport.hashCode(in) + 19 + NodeClsIDSupport.hashCode(out)
  }

  /**
   * Creates a unique instance of a TweakableNodeFunction with specified initial value (think of it as a unique
   * anonymous tweakable box).
   */
  @impure
  def uniqueWithInitialValue[T](initialValue: T): TweakableNodeFunction[T] = {
    val storage: UniqueValue[T] = UniqueValue.uniqueInstance(initialValue)
    FromNodeKey[T](nodeKeyOf(storage.value))
  }

  @entity
  private class UniqueValue[T](@node(tweak = true) val value: T)
}

class TweakableNodeFunctionMacros(val c: Context) extends ByNameMacros with WithOptimusMacroReporter {
  import c.universe._
  import optimus.tools.scalacplugins.entity.reporter.CopyMethodAlarms._

  private def FieldProxyCls = c.symbolOf[TweakableNodeFunction.FieldProxy[_, _]]

  def ofField[T: c.WeakTypeTag, F: c.WeakTypeTag](f: Tree): Tree = {
    val tTpe = c.weakTypeTag[T]
    val field = f match {
      case q"_.$fld" => fld
      case _         => abort(TWEAKABLE_NODE_FN_OF_FIELD_WRONG_LAMBDA, c.enclosingPosition, f)
    }

    // integrity checks (just to produce better error messages)
    val copyMethod = tTpe.tpe.decl(TermName("copy"))
    if (!copyMethod.isMethod) abort(TWEAKABLE_NODE_FN_OF_FIELD_NO_COPY_METHOD, c.enclosingPosition, tTpe.tpe)

    if (!copyMethod.asMethod.paramLists.exists(_.exists(_.name == field)))
      abort(TWEAKABLE_NODE_FN_OF_FIELD_NO_COPY_PARAM, c.enclosingPosition, field, tTpe.tpe)

    val nameStr = Literal(Constant(field.toString))
    val proxy =
      q"""new $FieldProxyCls(${splice(c)(c.prefix.tree)}, $nameStr)(
                { (t: $tTpe) => t.$field },
                { (t: $tTpe, x: ${c.weakTypeTag[F]}) => t.copy($field = x) })"""
    proxy
  }
}

trait ByNameMacros {
  protected val c: Context
  import c.universe._

  private def TweakableNodeFunctionCompanion = c.symbolOf[TweakableNodeFunction.type].asClass.module
  private def TweaksCls = c.symbolOf[Tweaks.type].asClass.module
  private def asNode = q"_root_.optimus.platform.asNode"

  def byNameTweakableNodeFunction[T: c.WeakTypeTag](node: c.Tree): Tree = {
    q"""$TweakableNodeFunctionCompanion.apply( $asNode { () => ${splice(c)(node)} },
        $asNode { (x: ${c.weakTypeTag[T]}) => $TweaksCls.byValue(${splice(c)(node)} := x) })"""
  }

}
