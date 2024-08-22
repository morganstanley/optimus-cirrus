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
import scala.tools.nsc.transform.{Transform, TypingTransformers}

/**
 * Handles @captureByValue arguments by replacing references to outer vals and outer "this"s with proxies. This is done
 * so that later phases (lambdalift and explicitouter) won't introduce captures of enclosing classes (especially FSMs).
 * By capturing only the required information by value (rather than allowing capture of $outer references), we avoid
 * memory retention and serialization problems, and in conjunction with @withNodeClassID allow lambdas that captured
 * equal values to be equal.
 *
 * *Conceptually* this phase rewrites...
 *
 * @entity
 *   class C(a: Int, b: Int) {
 * @node
 *   def d(x: Int, y: Int) = asNode { (z: Int) => a + x + z } }
 *
 * ...to...
 *
 * @entity
 *   class C(a: Int, b: Int) {
 * @node
 *   def d(x: Int, y: Int) = asNode { val a$capturedVal$1 = C.this.a val x$capturedVal$1 = x (z: Int) => a$capturedVal$1
 *   + x$capturedVal$1 + z } }
 *
 * ...and rewrites...
 *
 * @entity
 *   class C { def m: Int = 1
 * @node
 *   def d = asNode { (z: Int) => m + z } }
 *
 * ...to...
 *
 * @entity
 *   class C { def m: Int = 1
 * @node
 *   def d = asNode { val capturedOuter$1 = C.this (z: Int) => capturedOuter$1.m + z } }
 *
 * ...in both cases preventing capture of $outer (which would be a Node or FSM class). Except that this phase runs quite
 * late, so the trees have already been through optimus_asyncgraph etc. and they don't look as simple as the above!
 */
class CaptureByValueComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with TypedUtils
    with Transform
    with TransformGrep
    with TypingTransformers
    with AsyncTransformers
    with WithOptimusPhase {
  import global._

  protected def newTransformer0(unit: CompilationUnit): Transformer = new FindCaptureByValueSitesTransformer(unit)

  /**
   * Looks for @captureByValue arguments and delegates to a fresh RewriteCaptureByValueSiteTransfomer to transform them
   */
  private class FindCaptureByValueSitesTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    override def transform(tree: global.Tree): global.Tree = {
      tree match {
        // the annotation has to be placed on the method and the param so we can do this quick check
        case a @ Apply(fun, args) if a.symbol.hasAnnotation(CaptureByValueAnnotation) =>
          val paramss = a.symbol.asMethod.paramss
          // (should always be true since we're post-uncurry, so that's why it's a require not a proper alarm)
          require(paramss.size == 1)

          val newArgs = map2Conserve(args, paramss.head) { (arg, param) =>
            if (param.hasAnnotation(CaptureByValueAnnotation))
              // continue in a fresh Rewrite transformer scoped to this particular callsite
              new RewriteCaptureByValueSiteTransfomer(unit, currentOwner, this).transformCaptureByValueArg(arg)
            else
              transform(arg)
          }
          treeCopy.Apply(a, transform(fun), newArgs)
        case _ =>
          super.transform(tree)
      }
    }
  }

  /**
   * Does the actual transform. One of these is created as we recurse into each @captureByValue argument.
   */
  private class RewriteCaptureByValueSiteTransfomer(
      unit: CompilationUnit,
      enclosingOwner: Symbol,
      enclosingTransformer: FindCaptureByValueSitesTransformer)
      extends FindCaptureByValueSitesTransformer(unit) {

    // lazily allocated since many cases have nothing that needs proxying
    private[this] var originalToProxy: mutable.LinkedHashMap[Symbol, ValDef] = _
    private[this] var proxySymbols: mutable.HashSet[Symbol] = _
    private def getOrCreateProxy(original: Symbol)(proxy: => ValDef): Symbol = {
      if (originalToProxy eq null) {
        originalToProxy = mutable.LinkedHashMap()
        proxySymbols = mutable.HashSet()
      }
      val sym = originalToProxy.getOrElseUpdate(original, proxy).symbol
      proxySymbols.add(sym)
      sym
    }

    /**
     * Entry point of this transform. Replaces references with proxies where required, and returns a Block prefaced with
     * ValDefs to initialize those proxies.
     */
    def transformCaptureByValueArg(arg: Tree): Tree = {
      // first transform the tree in order to replace captures with proxies (using super.transform because we don't need
      // to transform the arg itself, only its content)
      val transformed = super.transform(arg)
      if (originalToProxy eq null) transformed
      else {
        // the proxy val defs may themselves refer to outer scopes, so transform them in the scope of the
        // enclosingTransformer which might rewrite them to use its own proxies
        val transformedProxyVals = originalToProxy.values.toList.map(enclosingTransformer.transform)
        treeCopy.Block(transformed, transformedProxyVals, transformed)
      }
    }

    private def isOuterValRef(sym: Symbol): Boolean =
      // only capture vals or accessors of non-mutable fields
      (sym.isVal || sym.isAccessor || sym.hasAnnotation(ValAccessorAnnotation)) && !sym.accessedOrSelf.isMutable &&
        // only capture effectively final values (includes locals) and params to avoid initialization ordering issues
        (sym.isParameter || sym.isEffectivelyFinal || sym.owner.isEffectivelyFinal) &&
        // don't capture vals which are already local to the current method
        sym.enclMethod != currentMethod &&
        // don't capture node calls! (we may reconsider this later...)
        !sym.hasAnnotation(NodeSyncAnnotation) &&
        // don't capture symbols owned by top-level modules (no point since they don't cause $outer capture anyway)
        !(sym.owner.isModuleOrModuleClass && sym.owner.isTopLevel) &&
        // don't capture weird ownerless symbols (seem to come from labeldefs)
        sym.owner.exists &&
        // only capture vals owned by the callsite or one of its owners
        enclosingOwner.hasTransOwner(sym.owner)

    /**
     * Replaces references with proxies. Post-order traversal (children first):
     *   - identify rewritable `Ident` / `Select` / `This` references
     *     - local vals (not vars) of enclosing scopes
     *     - member vals (not methods) of enclosing types
     *     - this references to enclosing classes
     *   - Create a proxy ValDef to be inserted into the callsite's scope and rewrite the reference to this proxy.
     *   - or, re-use an existing proxy for the that target (we only want one proxy per target, not one per reference)
     */
    override def transform(tree: Tree): Tree = {
      // Note that super is FindCaptureByValueSitesTransformer, so if any nested @captureByValue sites are found they
      // will be dealt with first
      val transformed = super.transform(tree)
      transformed match {
        // find any references to local vals or params owned by enclosing scopes
        case id @ Ident(_) if isOuterValRef(id.symbol) =>
          val sym = id.symbol
          val proxy = getOrCreateProxy(sym) {
            val proxy = sym.cloneSymbol(enclosingOwner).setName(currentUnit.freshTermName("" + sym.name + "$captured$"))
            ValDef(proxy, gen.mkAttributedIdent(sym).setType(proxy.info))
          }
          treeCopy.Ident(id, id.name).setSymbol(proxy).modifyType(_.substSym(id.symbol :: Nil, proxy :: Nil))

        // find any references to fields owned by enclosing scopes
        case id @ Select(transformedQual, _) if isOuterValRef(id.symbol) =>
          val sym = id.symbol
          val proxy = getOrCreateProxy(sym) {
            // n.b. since we're running in a late phase, field access is via accessor methods so we can't just clone
            // the (method) symbol and instead must make a fresh val symbol. also see the Apply rewrite in the next case
            val proxy = enclosingOwner
              .newValue(currentUnit.freshTermName("" + sym.name + "$captured$"), enclosingOwner.pos)
              .setInfo(sym.info.resultType)
            // if the qualifier has been proxied (typically with a capturedOuter$1) unproxy it to avoid double indirection
            // which can cause problems when accessing non-field class constructor params ("val x$captured$1 = Outer.this.x"
            // works but "val x$captured$1 = capturedOuter$1.x" causes a runtime NoSuchFieldException)
            val qual =
              if (proxySymbols(transformedQual.symbol)) tree.asInstanceOf[Select].qualifier else transformedQual
            ValDef(proxy, gen.mkAttributedSelect(qual, sym).setType(proxy.info))
          }
          treeCopy
            .Ident(id, id.name)
            .setSymbol(proxy)
            .setType(proxy.tpe)
            .modifyType(_.substSym(id.symbol :: Nil, proxy :: Nil))

        // as noted above, outer field access in this phase is written as Outer.this.accessor(), but we've just
        // rewritten that to accessor$captured$1() where accessor$captured$1 is a proxy val (remember we're visiting in
        // post-order), not an accessor method. So we need to strip off that extraneous ().
        case Apply(id @ Ident(_), Nil) if (proxySymbols ne null) && proxySymbols(id.symbol) =>
          id

        // find any outer "this" references
        case t @ This(_)
            if !t.symbol.isPackageClass &&
              // don't capture symbols owned by top-level modules (no point since they don't cause $outer capture anyway)
              !(t.symbol.isModuleOrModuleClass && t.symbol.isTopLevel) &&
              enclosingOwner.hasTransOwner(t.symbol) =>
          val proxy = getOrCreateProxy(t.symbol) {
            // n.b. we can't use nme.expandedName(..., target) to make a nice name because Constructors phase has a
            // bug whereby it cannot match parameters with expanded names (apparently fixed in Scala 2.13)
            val proxy = enclosingOwner.newTermSymbol(currentUnit.freshTermName("capturedOuter$")).setInfo(t.tpe)
            ValDef(proxy, gen.mkAttributedThis(t.symbol).setType(proxy.info))
          }
          treeCopy.Ident(t, proxy.name).setSymbol(proxy)

        case t => t
      }
    }
  }
}
