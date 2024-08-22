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

import optimus.exceptions.RTExceptionInterface
import optimus.exceptions.RTExceptionTrait
import optimus.exceptions.RTListStatic
import optimus.tools.scalacplugins.entity.reporter._

import scala.collection.mutable
import scala.reflect.internal.Flags
import scala.reflect.internal.Mode
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.transform.TypingTransformers
import scala.util.Failure
import scala.util.Success

object AutoAsyncRegex {
  val SimpleTokenRe = "[A-Za-z0-9error_\\.]+".r
  val ImmutableRe = "scala\\..*\\.immutable\\..*".r
}

/*
 * 1. Where possible, transform the current AST by automatically converting expressions to their async equivalent
 * 2. Log where automatic conversion has taken place
 * 3. Warn about possible bad practices
 *
 * Warnings (not exhaustive, higher than debug level):
 * 10570 - f(args), where f has @suppressAutoAsync annotation
 * 10571 - lazy val x = someAsync()
 * 10564 "with async body" - try someAsync() | catch someAsync() | finally someAsync()
 * 10564 "in @async or @node function" - @node def = try-catch | @async def = try-catch
 * 10561 - mock[SomeEntity]
 * 10563 - optionOrIterable.collect(someAsync)
 * 10553 - optionOrViewOrIterable.asyncOff.combinator(someAsync)
 * 10554 - optionOrIterable.interestingIteration(someAsync), where someAsync contains try-catch (a.k.a. problematic)
 * 10567 - iterable.interestingIteration(someAsync)
 * 20554 - iterable.interestingIteration(someRtAsync)
 * 20555 - iterable.interestingIteration(someExplicitNonRtAsync)
 * 20553/20556 - iterable.interestingIteration(somePossiblyNonRtAsync)
 * 10559 - view.interestingIteration(someAsync)
 *
 * Transformations:
 * option.fold(eitherAsync1)(eitherAsync2) -> Async.seq(option).fold(eitherAsync1)(eitherAsync2)
 * option.interestingOptionComb(someAsync) -> Async.seq(option).interestingOptionComb(someAsync)
 * iterable.find(someAsync) - Async.seq(iterable).find(someAsync)
 * iterable.interestingIteration(someRtAsync) -> Async.par(iterable).interestingIteration(someRtAsync)
 * iterable.interestingIteration(someNonRtAsync) -> Async.seq(iterable).interestingIteration(someNonRtAsync)
 *
 * Main logic is in transform(Tree) method.
 *
 */
class AutoAsyncComponent(val plugin: EntityPlugin, val phaseInfo: OptimusPhaseInfo)
    extends EntityPluginComponent
    with Transform
    with TransformGrep
    with TypingTransformers
    with AsyncTransformers
    with WithOptimusPhase
    with TypedTransformingUtils {

  import global.CompilationUnit
  import global._

  object anames {
    // Note: foreach is handled with inlining
    val interestingIterations: Set[Name] =
      Set(
        "find",
        "map",
        "flatMap",
        "filter",
        "filterNot",
        "groupBy",
        "forall",
        "exists",
        "minBy",
        "maxBy",
        "count"
      ).map(newTermName)
    lazy val interestingPrelifteds: Set[Name] =
      interestingIterations.map(n => newTermName(n.toString + "Prelifted"))

    lazy val combApplyIterableCombinators: Set[Name] =
      interestingIterations ++
        interestingPrelifteds ++
        Set(names.collect)

    lazy val combApplyOptionalCombinators: Set[Name] = Set(names.collect)

    // Prefixes of classes for which all methods are deemed safe. Most are from the standard scala library.
    val safeClasses = Set(
      // collections
      "scala.collection.immutable",
      "scala.collection.Iterable",
      "scala.collection.Seq",
      "scala.collection.Map",
      "scala.collection.Set",
      "scala.collection.IndexedSeq",
      "scala.collection.SortedSet",
      "scala.collection.SortedMap",

      // views
      "scala.collection.SeqView",

      // collection aliases
      "scala.List",
      "scala.Seq",
      "scala.IndexedSeq",
      "scala.Vector",
      "scala.Iterable",
      "scala.IterableOnce",
      "scala.Traversable",
      "scala.TraversableOnce",

      // migration collection aliases
      "optimus.scala212.DefaultSeq",
      "optimus.scala212.DefaultSeq.Seq",
      "optimus.scala212.DefaultSeq.IndexedSeq",

      // collection converters
      "scala.collection.JavaConversions",
      "scala.collection.JavaConverters",
      "scala.jdk.CollectionConverters",

      // 2.12 collections
      "scala.collection.IterableView",
      "scala.collection.TraversableOnce",
      "scala.collection.generic.FilterMonadic",

      // core aliases
      "scala.Int",
      "scala.Long",
      "scala.Float",
      "scala.Double",
      "scala.Byte",
      "scala.Char",
      "scala.Boolean",
      "scala.BigDecimal",
      "scala.Predef",
      "scala.Predef.Set",
      "scala.Predef.Map",
      "scala.Predef.String",
      "scala.Predef.ArrowAssoc",
      "scala.this.Predef.Set",

      // core classes
      "scala.<repeated>", // varargs combinators
      "scala.Throwable", // Not really RT, but perhaps we don't care
      "scala.Any",
      "scala.StringContext",
      "scala.Tuple2",
      "scala.Tuple3",
      "scala.Tuple4",
      "scala.Tuple5",
      "scala.Tuple6",
      "scala.Tuple7", // Punish anyone who uses more than this.
      "scala.Option",
      "scala.Some",
      "scala.util.Right",
      "scala.math.package",
      "scala.math.BigDecimal",
      "scala.util.matching.Regex",

      // xml
      "scala.xml.Node",
      "scala.xml.NodeSeq",
      "scala.xml.XML",
      "scala.xml.Elem",
      "scala.xml.Text",

      // runtime
      "scala.runtime.Tuple2Zipped",
      "scala.runtime.Tuple3Zipped",
      "scala.runtime.Tuple4Zipped",
      "scala.runtime.RichInt",

      // reflection
      "scala.reflect.ClassTag",
      "java.lang.Class",
      "scala.reflect.ManifestFactory",

      // java
      "java.lang.String",
      "java.lang.Double",
      "java.lang.Math",

      // optimus
      "optimus.ui.dsl",
      "optimus.dal.usage.projection",
      "optimus.platform.Tweak",
      "optimus.platform.Async",
      "optimus.platform.OptAsync",
      "optimus.platform.AsyncBase",
      "optimus.platform.asyncPar",
      "optimus.platform.asyncSeq",
      "optimus.platform.CovariantSet",

      // other libraries
      "msjava.slf4jutils.scalalog.Logger",
      "org.slf4j.Logger",
      "org.junit.Assert" // Only RO in circumstances where we're going to fail anyway.
    )

    // Anyone who write impure implementations of these methods deserves runtime bugs.
    val likelySafeMethods: Set[Name] =
      (nme.toString_ :: newTermName("compare") :: newTermName("getClass") :: nme.EQ :: nme.NE :: nme.equals_ ::
        nme.unapply :: nme.unapplySeq :: nme.map :: nme.flatMap :: Nil).toSet[Name]

    val safeMethods: Map[String, Set[Name]] = (("scala.Array", Set[Name](nme.apply)) ::
      ("java.util.concurrent.ConcurrentHashMap", Set[Name](names.get)) ::
      ("java.util.Collection", Set[Name](newTermName("contains"))) ::
      ("scala.PartialFunction", Set[Name](newTermName("lift"))) ::
      Nil).toMap

  }

  var containsMock = false
  //  Possibly speedier match of ".*[mM]ock.*".r
  def maybeMock(s: String): Boolean = {
    val i = s.indexOf("ock")
    i > 0 && { val m: Char = s(i - 1); m == 'm' || m == 'M' }
  }

  def newTransformer0(unit: CompilationUnit) = {
    containsMock = maybeMock(String.valueOf(unit.source.content))
    new AutoAsync(unit)
  }

  class AutoAsync(unit: CompilationUnit) extends TypingTransformer(unit) with TypedTreeCopier {
    import AutoAsyncRegex._
    import global._

    def getAnnotations(tree: Tree): Map[String, String] = {
      tree.collect {
        case t if (t.symbol ne null) && isNode(t.symbol) =>
          (t.symbol.toString(), s"${t.symbol.annotationsString} : ${t.tpe.typeSymbol.annotationsString}")
      }.toMap
    }

    object GeneralApply {
      def unapply(tree: Tree): Option[(Tree, Tree, Name, List[Tree])] = tree match {
        case _: ApplyToImplicitArgs                                  => None
        case Apply(fun @ TypeApply(Select(obj, meth), _targs), args) => Some((fun, obj, meth, args))
        case Apply(fun @ Select(obj, meth), args)                    => Some((fun, obj, meth, args))
        case _                                                       => None
      }
    }

    // Class is safe if list contains fqcn or any prefix of it.
    def isSafeClass(objt: String): Boolean =
      isSafeMemo.getOrElseUpdate(
        objt, {
          val parts = objt.split('.')
          val clazz = new StringBuffer(parts.head)
          anames.safeClasses.contains(parts.head) || parts.tail.exists { p =>
            clazz.append(".")
            clazz.append(p)
            anames.safeClasses.contains(clazz.toString)
          }
        }
      )

    def isProbablyPriqlGenerated(objt: Type, meth: Name): Boolean = {
      val objts = getNiceTypeString(objt)
      val ret = objts.startsWith("scala.reflect.api") ||
        objts.startsWith("optimus.platform.relational.tree") ||
        objt.toString.startsWith("$typecreator") || objt.toString.startsWith("$treecreator") ||
        objt.bounds.hi.toString.startsWith("scala.reflect.api")
      ret
    }

    def isSpecialException(obj: Tree, objt: Type, objts: String, meth: Name): Boolean = {
      val objs = obj.toString
      // Special handling of popular nodebuffer methods that are mutating but difficult to misuse.
      (objts == "scala.xml.NodeBuffer" && (objs == "$buf" || meth.toString == nme.CONSTRUCTOR.toString)) ||
      // Implies that they expect parallelism
      (objts == "java.lang.Object" && meth == nme.synchronized_) ||
      (objts.startsWith("java.time") && meth != names.now)
    }

    private def autoGenerated(appl: Tree, obj: Tree, meth: Name) =
      isNodeOrRef(appl.symbol) || appl.symbol.hasAnnotation(NodeSyncLiftAnnotation) ||
        appl.symbol.hasAnnotation(
          AutoGenCreationAnnotation) || // This does not seem to catch all generated apply methods.  Not sure why.
        // a companion with exactly one apply method should be ok.  (nb. obj.hasSymbolField doesn't compile under 2.10)
        (meth == nme.apply && (obj.symbol ne null) && isEntityCompanion(obj.symbol) && obj.tpe.decls
          .filter(_.name == nme.apply)
          .size == 1)

    private def isSafe(appl: Tree, obj: Tree, meth: Name): Boolean = {
      val asym = appl.symbol
      asym.isCaseApplyOrUnapply || asym.isCaseAccessor || ((obj.symbol ne null) && obj.tpe.typeSymbol.isCaseClass && meth == nme.copy) ||
      // assume implicits are RT
      asym.isImplicit || ((obj.symbol ne null) && obj.symbol.isImplicit) ||
      // TODO: (OPTIMUS-13386): assume constructors to be RT?
      // meth == nme.CONSTRUCTOR ||
      autoGenerated(appl, obj, meth) || {
        val objt = obj.tpe
        anames.likelySafeMethods.contains(meth) || {
          val objts = getNiceTypeString(objt)
          isSafeClass(objts) ||
          anames.safeMethods.get(objts).exists(_.contains(meth)) ||
          ImmutableRe.unapplySeq(objts).isDefined ||
          isProbablyPriqlGenerated(objt, meth) ||
          isSpecialException(obj, objt, objts, meth)
        }
      }
    }

    // Try to get a human-readable type, which is sometimes found in t.underlying but not always.
    private def getNiceTypeString(typ: Type): String = {
      val s = typ match {
        case TypeRef(_, t, _)                                                 => t.fullNameString
        case t if ((t.underlying ne null) && t.underlying.toString == "type") => t.toLongString
        case t if t == t.underlying                                           => t.toLongString
        case t                                                                => getNiceTypeString(t.underlying)
      }
      val i = s.indexOf('[')
      if (i > 0) s.substring(0, i) else s
    }

    case class Opacity(position: Position, alarm: OptimusPluginAlarm, tree: Tree, isDeliberate: Boolean)

    // List of positions and descriptions of possible referential opacities.
    private def opaque(body: Tree, combinator: => String): List[Opacity] = {
      if (parentFlags.assumeParallelizable)
        Nil // By assumption everything is RT.
      else {
        val buf = scala.collection.mutable.ListBuffer.empty[Opacity]
        var varYet = false
        val trav = new Traverser {
          override def traverse(tree: Tree): Unit = tree match {
            case applic @ GeneralApply(objmeth, obj, meth, args) =>
              val applicRT = applic.symbol.hasAnnotation(ParallelizableAnnotation)
              val applicRS = applic.symbol.hasAnnotation(SequentialAnnotation)
              val objRT = obj.tpe.typeSymbol.hasAnnotation(ParallelizableAnnotation)
              val objRS = obj.tpe.typeSymbol.hasAnnotation(SequentialAnnotation)
              val applicAsync = applic.symbol.hasAnnotation(AsyncAnnotation)

              val annotRS = applicRS || (applicRS || ((applicAsync || objRS) && !applicRT))
              val annotRT = applicRT || (objRT && !applicRS)

              if (!isInstance(applic.tpe, tAsyncIterableMarker)) {
                if (applicRT)
                  alarm(OptimusNonErrorMessages.AUTO_ASYNC_DEBUG(s"RTANNOT ${obj.tpe.typeSymbol}:${meth}"), tree.pos)
                else if (applicRS)
                  alarm(OptimusNonErrorMessages.AUTO_ASYNC_DEBUG(s"RSANNOT ${obj.tpe.typeSymbol}.${meth}"), tree.pos)
                else if (objRT)
                  alarm(OptimusNonErrorMessages.AUTO_ASYNC_DEBUG(s"RTANNOT ${obj.tpe.typeSymbol}"), tree.pos)
                else if (objRS)
                  alarm(OptimusNonErrorMessages.AUTO_ASYNC_DEBUG(s"RSANNOT ${obj.tpe.typeSymbol}"), tree.pos)
              }

              if (annotRS) {
                buf += Opacity(
                  obj.pos,
                  OptimusNonErrorMessages.AUTO_ASYNC_DEBUG("Marked sequential or @async"),
                  applic,
                  true)
                // don't traverse, because we know already going to be aseq
              } else if (annotRT) {
                // Specifically require the inlineRT wrapper, rather than just something annotated RS
                if ((obj.symbol ne null) && obj.symbol.toString.contains("inlineRT")) {
                  alarm(
                    OptimusNonErrorMessages.AUTO_ASYNC_DEBUG(s"INLINERT $obj ${obj.symbol} ${obj.tpe.typeSymbol}"),
                    tree.pos)
                } else {
                  super.traverse(tree)
                }
              } else {
                if (!isSafe(applic, obj, meth)) {
                  val annotInfo =
                    if (annotRT || annotRS) s" applicRT=$applicRT, objRT=$objRT, applicRS=$applicRS, objRS=$objRS"
                    else ""
                  buf += Opacity(
                    obj.pos,
                    OptimusNonErrorMessages
                      .POSSIBLE_NON_RT(s"${getNiceTypeString(obj.tpe)}.$meth$annotInfo", combinator),
                    applic,
                    false)
                }
                super.traverse(tree)
              }
            case v if hasVar(v.id) && !varYet =>
              varYet = true
              buf += Opacity(body.pos, OptimusNonErrorMessages.VAR_IN_ASYNC_CLOSURE(), tree, false)
              super.traverse(tree)
            case _ =>
              super.traverse(tree)
          }
        }
        trav.traverse(body)
        buf.toList
      }
    }

    def isAsync(tree: Tree): Boolean = tree match {
      case Select(q, _) => isNodeOrRef(tree.symbol)
      case _: Apply     => isNodeOrRef(tree.symbol)
      case _            => false
    }

    override def transformUnit(unit: CompilationUnit): Unit = {
      try {
        super.transformUnit(unit)
      } catch {
        case ex: Throwable =>
          ex.printStackTrace()
          println("Current: " + curTree)

          reporter.error(curTree.pos, s"Error in auto-async: ${throwableAsString(ex)}")
      }
    }

    private val nmeIterator = newTermNameCached("iterator")

    private val tCanBuildFrom = CanBuildFromClass
    private val tOption = definitions.OptionClass
    private val tIterable = IterableClass
    private val tIterator = definitions.IteratorClass
    private val tPartialFunction = PartialFunctionClass
    private val tView = IterableViewClass
    private val tIterableLike = IterableLikeClass
    private val tAsyncBase = AsyncBaseClass
    private val tAsyncIterableMarker = AsyncIterableMarkerClass
    private val tAsyncPar = AsyncParClass
    private val tAsyncParImp = AsyncParImpClass
    private val tOptAsync = OptAsyncClass
    private val tAsyncLazy = AsyncLazyClass

    lazy val NodeFunctionTrait = (0 to 9).map(i => rootMirror.getRequiredClass(s"optimus.platform.NodeFunction$i"))
    lazy val AsyncFunctionTrait = (0 to 9).map(i => rootMirror.getRequiredClass(s"optimus.platform.AsyncFunction$i"))

    // cache <:< checks
    val instanceMemo = scala.collection.mutable.Map[(Type, ClassSymbol), Boolean]()
    val isSafeMemo = scala.collection.mutable.Map[String, Boolean]()
    def isInstance(tl: Type, tr: ClassSymbol): Boolean =
      instanceMemo.getOrElseUpdate((tl, tr), tl.resultType.typeSymbol isNonBottomSubClass tr)
    def isIterable(tl: Type): Boolean = isInstance(tl, tIterable) && isInstance(tl, tIterableLike)
    def isCBF(iarg: Tree): Boolean = isInstance(iarg.tpe, tCanBuildFrom)
    def isOrdering(iarg: Tree): Boolean = isInstance(iarg.tpe, OrderingClass)

    // Destructuring for various representations of applying a function to a function.
    object CombApply {

      sealed trait MonadType
      case object IsOption extends MonadType
      case object IsIterable extends MonadType
      case object IsView extends MonadType
      trait IsAsyncIterableAlready extends MonadType
      case object IsAparImpAlready extends IsAsyncIterableAlready
      case object IsAparAlready extends IsAsyncIterableAlready
      case object IsAseqAlready extends IsAsyncIterableAlready
      case object IsAsyncOptionAlready extends MonadType
      case object NotMonad extends MonadType

      private[entity] def monadType(coll: Tree): MonadType =
        // TODO (OPTIMUS-11637) Properly auto-async withFilter someday.
        // if (isInstance(coll.tpe, tOption)|| isInstance(coll.tpe,OptionWithFilterClass))
        if (isInstance(coll.tpe, tOption))
          IsOption
        else if (isInstance(coll.tpe, tAsyncParImp))
          IsAparImpAlready
        else if (isInstance(coll.tpe, tAsyncPar))
          IsAparAlready
        else if (isInstance(coll.tpe, tAsyncBase))
          IsAseqAlready
        else if (isInstance(coll.tpe, tOptAsync))
          IsAsyncOptionAlready
        else if (isInstance(coll.tpe, tIterable) && isInstance(coll.tpe, tIterableLike)) {
          if (isInstance(coll.tpe, tView))
            IsView
          else
            IsIterable
        } else NotMonad

      private def isValidCombinatorType(comb: Name, monadType: MonadType): Boolean =
        monadType != NotMonad && (monadType match {
          case IsOption | IsAsyncOptionAlready =>
            anames.combApplyOptionalCombinators.contains(comb)
          case _ =>
            anames.combApplyIterableCombinators.contains(comb)
        })

      /*
      Would like to identify
      1. Monads with async function bodies
      2. Explicit asyncs with async function bodies
      3. Explicit asyncs with non-async function bodies
       */
      //                               app1   tapp,      sel     coll  comb  func  targs       iargs       mtype
      def unapply(tree: Tree): Option[(Apply, TypeApply, Select, Tree, Name, Tree, List[Tree], List[Tree], MonadType)] =
        if (!tree.isInstanceOf[Apply]) // bail out early
          None
        else {
          // we check the expected number of param lists for the method to ensure we're seeing a non-partial
          // application here (otherwise we can think we've found a combinator inside a partial application)
          val paramsCount = tree.symbol.info.paramss.size
          tree match {
            case Apply(app1 @ Apply(tapp @ TypeApply(sel @ Select(coll, comb), targs), f :: Nil), iargs @ (iarg :: Nil))
                if (isCBF(iarg) || isOrdering(iarg)) && paramsCount == 2 && isValidCombinatorType(
                  comb,
                  monadType(coll)) =>
              Some((app1, tapp, sel, coll, comb, f, targs, iargs, monadType(coll)))
            case app2 @ Apply(app1 @ Apply(sel @ Select(coll, comb), f :: Nil), iargs @ (iarg :: Nil))
                if isCBF(iarg) && paramsCount == 2 && isValidCombinatorType(comb, monadType(coll)) =>
              Some((app1, null, sel, coll, comb, f, Nil, iargs, monadType(coll)))
            case app1 @ Apply(tapp @ TypeApply(sel @ Select(coll, comb), targs), f :: Nil)
                if paramsCount == 1 && isValidCombinatorType(comb, monadType(coll)) =>
              Some((app1, tapp, sel, coll, comb, f, targs, Nil, monadType(coll)))
            case app1 @ Apply(sel @ Select(coll, comb), f :: Nil)
                if paramsCount == 1 && isValidCombinatorType(comb, monadType(coll)) =>
              Some((app1, null, sel, coll, comb, f, Nil, Nil, monadType(coll)))
            case _ => None
          }
        }

      def isApplyToMethodWithAsyncBody(app: Apply, comb: Name, vds: Option[List[ValDef]], body: Tree) =
        hasAsync(body.id) && !isClosure(body) &&
          // Normal Function with actual parameters
          (vds.isDefined ||
            // Partial Function in a collect block
            (isInstance(body.tpe, tPartialFunction) && comb == names.collect) ||
            // by-name parameter not yet converted to Function
            app.fun.tpe.params.headOption.exists(_.isByNameParam))

      // Reassemble a CombApply from its component parts.
      def apply(
          origTree: Tree,
          origInnerApply: Apply,
          origTypeApply: TypeApply,
          origSelect: Select,
          newSelect: Select,
          newFunc: Tree,
          typeArgs: List[Tree],
          implicitArgs: List[Tree],
          copy: Boolean
      ): Tree = {
        val args = newFunc :: Nil
        if ((typeArgs ne Nil) && (implicitArgs ne Nil)) {
          if (copy)
            treeCopy.Apply(
              origTree,
              treeCopy.Apply(origInnerApply, treeCopy.TypeApply(origTypeApply, newSelect, typeArgs), args),
              implicitArgs)
          else
            Apply(Apply(TypeApply(newSelect, typeArgs), args), implicitArgs)
        } else if (typeArgs ne Nil) {
          if (copy)
            treeCopy.Apply(origTree, treeCopy.TypeApply(origTypeApply, newSelect, typeArgs), args)
          else
            Apply(TypeApply(newSelect, typeArgs), args)
        } else if (implicitArgs ne Nil) {
          if (copy)
            treeCopy.Apply(origTree, treeCopy.Apply(origInnerApply, newSelect, args), implicitArgs)
          else
            Apply(Apply(newSelect, args), implicitArgs)
        } else {
          if (copy)
            treeCopy.Apply(origTree, newSelect, args)
          else
            Apply(newSelect, args)
        }
      }
    }

    val platformModule = rootMirror.getRequiredModule("optimus.platform")
    def wrapWithAsync(coll: global.Tree, wrapper: global.Name, comb: global.Name): Tree = {
      alarm(
        OptimusNonErrorMessages.AUTO_ASYNC_DEBUG(s"""WRAP ${coll.pos.line} ${coll.pos.column} $comb $wrapper $coll"""),
        coll.pos)
      val ret = Apply(Select(Select(gen.mkAttributedQualifier(platformModule.tpe), names.Async), wrapper), coll :: Nil)
      ret
    }

    // Detect multiple manifestations of .asyncOff
    def isNoAsync(coll: Tree): Boolean = coll match {
      case Apply( // optimus.platform.asyncOff.apply[Int, List[Int](list)
            // optimus.platform.asyncOff.apply[Int, List[Int]
            TypeApply(
              // optimus.platform.asyncOff.apply
              Select(Select(Select(Ident(names.optimus), names.platform), names.asyncOff), nme.apply),
              // [Int, List[Int]]
              _targs),
            // list
            _comb) =>
        true
      case Select(qual, names.asyncOff) => true
      case _                            => false
    }

    def hasProblematic(coll: Tree, f: Tree, iargs: List[Tree]): Boolean =
      (coll :: f :: iargs).exists(t => hasProblematic(t.id))

    sealed trait Translucency
    case object Accidental extends Translucency
    case object Deliberate extends Translucency
    case object Transparent extends Translucency

    private def opacityWarnings(
        opacities: Seq[Opacity],
        body: Tree,
        maxOpacities: Int = 5,
        showDeliberates: Boolean = false): Translucency = {
      if (opacities.isEmpty)
        Transparent
      else {
        var ret: Translucency = Deliberate
        var shown = 0
        val oi = opacities.iterator
        while (oi.hasNext && shown <= maxOpacities) {
          oi.next() match {
            case Opacity(pos, msg, _, true) =>
              if (showDeliberates) {
                alarm(msg, pos)
                shown += 1
              }
            case Opacity(pos, msg, _, false) =>
              alarm(msg, pos)
              shown += 1
              ret = Accidental
          }
        }
        ret
      }
    }
    private def opacityWarnings(body: Tree, comb: => String): Translucency = opacityWarnings(opaque(body, comb), body)

    def checkLegacyApi(coll: Tree, body: Tree, translucency: => Translucency): Boolean = {
      coll match {
        // The collection is a legacy async()
        case Apply(
              as1 @ TypeApply(Select(Select(Select(Ident(optimus), names.platform), names.Async), nme.apply), _),
              args @ (unwrapped :: _)) =>
          if (args.size > 1) {
            // Specifying parallelism level.
            alarm(
              OptimusNonErrorMessages.AUTO_ASYNC_DEBUG(s"""LEGACYASYNCTODO ${coll.pos.line} ${coll.pos.column}"""),
              coll.pos)
          }
          // Legacy async with possibly non-RT body.  The owner needs to make a decision.
          else if (translucency == Accidental) {
            alarm(
              OptimusNonErrorMessages.AUTO_ASYNC_DEBUG(s"""NONRTASYNCTODO ${coll.pos.line} ${coll.pos.column}"""),
              coll.pos)
          } else {
            // Legacy, redundant async.
            val p = coll.pos
            unwrapped.toString match {
              // Should be able to remove the async() automatically.
              case SimpleTokenRe() =>
                alarm(
                  OptimusNonErrorMessages.AUTO_ASYNC_DEBUG(s"""REMOVEEXPLICITASYNC ${p.line} ${p.column} $unwrapped"""),
                  p)
              // Won't be able to remove it.  Alert the owner.
              case _ =>
                alarm(OptimusNonErrorMessages.AUTO_ASYNC_DEBUG(s"""EXPLICITASYNCTODO ${p.line} ${p.column}"""), p)
            }
          }
          true
        // Async'd with new API.  Assume they know what they're doing.
        case _ => false
      }
    }

    // Wrap the collection with asyncWrapper, reassemble the tree, and retype
    def wrapAndRetype(
        origTree: Tree,
        origInnerApply: Apply,
        origTypeApply: TypeApply,
        origSelect: Select,
        newCollection: Tree,
        origCombinator: Name,
        newFunction: Tree,
        typeArgs: List[Tree],
        implicitArgs: List[Tree],
        asyncWrapper: Name): Tree = {
      val untypedTree =
        if ((origCombinator == nme.map || origCombinator == nme.flatMap) && isAtLeastScala2_13) {
          // For Scala 2.13, we construct the BuildFrom based on the evidence arguments that are directly passed to
          // overloaded combinators.
          CombinatorOverload.buildFromFor(origSelect.symbol, typeArgs, implicitArgs) match {
            case None =>
              reporter.error(origSelect.pos, "Unexpected combinator")
              return origTree
            case Some((buildFromTree, elemTypeTypeArgTree, upcast, overloadMethod)) =>
              val newCollectionUpCast = if (upcast) {
                val upCastTo = newCollection.tpe.baseClasses
                  .find(x =>
                    x.typeParams.size == 1 && x.info
                      .member(overloadMethod.name)
                      .alternatives
                      .exists(_.overriddenSymbol(overloadMethod.owner) == overloadMethod))
                  .get
                val upCastTpe = newCollection.tpe.baseType(upCastTo)
                Typed(newCollection, TypeTree(upCastTpe))
              } else if (newCollection.tpe.typeSymbol.isNonBottomSubClass(Enumeration_ValueSet)) {
                val upCastTpe = newCollection.tpe.baseType(symbolOf[scala.collection.immutable.SortedSet[Any]])
                Typed(newCollection, TypeTree(upCastTpe))
              } else newCollection
              val asyncCollection =
                localTyper.typedPos(origTree.pos.makeTransparent)(
                  wrapWithAsync(newCollectionUpCast, asyncWrapper, origCombinator))
              Apply(
                Select(
                  Apply(
                    TypeApply(
                      Select(asyncCollection, names.auto),
                      elemTypeTypeArgTree :: TypeTree(origTree.tpe) :: Nil),
                    buildFromTree :: Nil),
                  origCombinator),
                newFunction :: Nil
              )
          }
        } else {
          val asyncCollection =
            localTyper.typedPos(origTree.pos.makeTransparent)(
              wrapWithAsync(newCollection, asyncWrapper, origCombinator))
          val newSelect = Select(asyncCollection, origCombinator)
          CombApply(
            origTree,
            origInnerApply,
            origTypeApply,
            origSelect,
            newSelect,
            newFunction,
            typeArgs,
            implicitArgs,
            copy = false)
        }
      markAsync(localTyper.typedPos(origTree.pos)(untypedTree))
    }

    // Called when have found a CombApply (monad.interestingCombinator(f))
    // Displays warnings, and rewrites the CombApply to its async equivalent if necessary
    def checkAndRewriteCombinator(
        origTree: Tree,
        origApp1: Apply,
        origTapp: TypeApply,
        origSel: Select,
        newColl: Tree,
        origComb: Name,
        newFunc: Tree,
        newFuncBody: Tree,
        targs: List[Tree],
        iargs: List[Tree],
        monadType: CombApply.MonadType,
        hasAsyncBody: Boolean,
        componentsChanged: Boolean
    ) = {

      // Reassemble tree using transformed components
      def reassemble(): Tree =
        if (componentsChanged)
          CombApply.apply(
            origTree = origTree,
            origInnerApply = origApp1,
            origTypeApply = origTapp,
            origSelect = origSel,
            newSelect = treeCopy.Select(origSel, newColl, origComb),
            newFunc = newFunc,
            typeArgs = targs,
            implicitArgs = iargs,
            copy = true
          )
        else origTree

      def defaultWrapAndRetype(asyncWrapper: Name) =
        wrapAndRetype(
          origTree = origTree,
          origInnerApply = origApp1,
          origTypeApply = origTapp,
          origSelect = origSel,
          newCollection = newColl,
          origCombinator = origComb,
          newFunction = newFunc,
          typeArgs = targs,
          implicitArgs = iargs,
          asyncWrapper = asyncWrapper
        )

      (monadType, hasAsyncBody) match {
        // Nothing async within
        case (CombApply.IsOption | CombApply.IsIterable, false) =>
          reassemble()

        // Warn about possible non-RT calls if we have automatically apar-ed the collection
        case (CombApply.IsAparImpAlready, true) =>
          // note that opacityWarnings generate alarms as a side effect, so we must always call it
          val translucency = opacityWarnings(newFuncBody, origComb.toString)
          checkLegacyApi(newColl, newFuncBody, translucency)
          reassemble()

        // Prelifted combinator (i.e. one that returns Node).  Complain about legacy API, but otherwise
        // assume they know what they're doing.
        case (_: CombApply.IsAsyncIterableAlready, _) if anames.interestingPrelifteds.contains(origComb) =>
          // note that opacityWarnings generate alarms as a side effect, so we must always call it
          val translucency = opacityWarnings(newFuncBody, origComb.toString)
          checkLegacyApi(newColl, newFuncBody, translucency)
          reassemble()

        // Unnecessarily async'd collection or option
        // Generate usual TODOs if using the legacy API, otherwise a friendly warning.
        case (_: CombApply.IsAsyncIterableAlready, false) | (CombApply.IsAsyncOptionAlready, _) =>
          if (!checkLegacyApi(newColl, newFuncBody, opacityWarnings(newFuncBody, origComb.toString)))
            // hasASync doesn't propagate through nested closures, because we don't know that the async
            // call will actually be made, so based on that alone, we might conclude that .apar/.aseq was
            // unnecessary.  However, we can't conclude that it's definitely unnecessary - particularly if the
            // async call is made, inadvertently, in an unlifted closure.
            if (!hasSyncStack(newFuncBody.id))
              alarm(OptimusNonErrorMessages.UNNECESSARY_ASYNC_DEBUG(), newColl.pos)
          reassemble()

        // .collect must be async'd manually on an option
        case (CombApply.IsOption, true) if origComb == names.collect =>
          alarm(OptimusNonErrorMessages.MANUAL_OPTION_ASYNC_COLLECT(), newColl.pos)
          reassemble()

        // .collect must be async'd manually on an iterable
        case (CombApply.IsIterable, true) if origComb == names.collect =>
          // This is to give the developer a hint about whether to use .apar or .aseq by checking for
          // non-RT calls.  We must be careful not to check only the user code here, and not the construction of
          // the PartialFunction, especially as the PartialFunction will disappear once the collection is async'd.
          val opacities = newFuncBody
            .collect {
              case DefDef(mods, name, _, _, _, rhs) if name == nme.applyOrElse || name == nme.isDefinedAt =>
                opaque(rhs, origComb.toString)
            }
            .flatten
            .filter(_.tree match {
              // exclude the failure clause
              case GeneralApply(_, qual, nme.apply, _) => qual.symbol.name != newTermName("default")
              case _                                   => true
            })
          if (opacityWarnings(opacities, newFuncBody, 5, true) == Transparent)
            alarm(OptimusNonErrorMessages.MANUAL_ASYNC_COLLECT_APAR(), newColl.pos)
          else
            alarm(OptimusNonErrorMessages.MANUAL_ASYNC_COLLECT_ASEQ(), newColl.pos)
          reassemble()

        // .asyncOff with async closure
        case (CombApply.IsOption | CombApply.IsIterable | CombApply.IsView, true) if isNoAsync(newColl) =>
          alarm(OptimusNonErrorMessages.AUTO_ASYNC_OFF(), newColl.pos)
          reassemble()

        // Problematic constructs in async closure
        case (CombApply.IsOption | CombApply.IsIterable, true)
            if hasProblematic(newColl, newFunc, iargs) && anames.interestingIterations.contains(origComb) =>
          alarm(OptimusNonErrorMessages.ASYNC_CONTAINS_INCOMPATIBLE(), newColl.pos)
          reassemble()

        case (CombApply.IsIterable, true) if origComb == nme.find_ =>
          alarm(OptimusNonErrorMessages.ASYNC_WRAPPING_DEBUG(newColl.tpe, "aseq"), newColl.pos)
          defaultWrapAndRetype(names.seq)

        // Wrap iterables
        case (CombApply.IsIterable, true) if anames.interestingIterations.contains(origComb) =>
          val opacities = opaque(newFuncBody, origComb.toString)

          if (!parentFlags.assumeParallelizable)
            newFuncBody.find(isAsync).foreach { t =>
              alarm(OptimusNonErrorMessages.ASYNC_CLOSURE_AUTO(s"Iterable.$origComb"), t.pos)
            }

          // Explain why we chose what we chose.
          val res = opacityWarnings(opacities, newFuncBody, 5, true) match {
            case Transparent =>
              // We can parallelize!
              alarm(OptimusNonErrorMessages.ASYNC_WRAPPING_DEBUG(newColl.tpe, "apar"), newColl.pos)
              if (!parentFlags.assumeParallelizable)
                alarm(OptimusErrors.UNMARKED_ASYNC_CLOSURE_T, newColl.pos, newColl)
              defaultWrapAndRetype(names.par)
            case Deliberate =>
              // We will not parallelize
              alarm(OptimusErrors.UNMARKED_ASYNC_CLOSURE_D, newColl.pos, newColl)
              alarm(OptimusNonErrorMessages.ASYNC_WRAPPING_DEBUG(newColl.tpe, "aseq"), newColl.pos)
              defaultWrapAndRetype(names.seq)
            case Accidental =>
              alarm(OptimusErrors.UNABLE_TO_PARALLELIZE_COLLECTION, newColl.pos, newColl)
              alarm(OptimusErrors.UNMARKED_ASYNC_CLOSURE_A, newColl.pos, newColl)
              defaultWrapAndRetype(names.seq)
          }

          alarm(OptimusNonErrorMessages.POSSIBLE_ASYNC_DEBUG, newFuncBody.pos, getAnnotations(newFuncBody))
          res

        case (CombApply.IsView, true) if anames.interestingIterations.contains(origComb) =>
          alarm(OptimusNonErrorMessages.VIEW_WITH_ASYNC, newColl.pos)
          reassemble()

        case _ =>
          alarm(OptimusNonErrorMessages.UNHANDLED_COMBINATOR_DEBUG, origTree.pos, newColl)
          reassemble()
      }
    }

    class Flags {
      var hasAsyncChild = false
      var hasVar = false
      var hasProblematic = false
      var hasSyncStack = false
      var inNodeDef = false
      var inAsyncDef = false
      var assumeParallelizable = false
    }

    // (Keyed by Tree id)
    def asyncTree(t: Tree): Boolean = hasAsync(t.id)
    val hasAsync = mutable.Set[Int]()
    def markAsync[T <: Tree](tree: T): T = {
      hasAsync.add(tree.id)
      tree
    }
    def maybeMarkAsync[T <: Tree](async: Boolean, tree: T): T = if (async) markAsync(tree) else tree
    val hasVar = mutable.Set[Int]()
    // Currently not distinguishing between different problematic constructs like async guards and try/catch.  We only report
    // that we found one.  If we change our mind:
    // val hasProblematic = new scala.collection.mutable.HashMap[Int, Set[String]] with scala.collection.mutable.MultiMap[Int, String]
    val hasProblematic = mutable.Set[Int]()
    val hasSyncStack = mutable.Set[Int]()
    val pluginSupportSymbol = scala.util.Try { rootMirror.getRequiredModule("optimus.graph.PluginSupport") }.toOption

    var parentFlags = new Flags

    def inContext(
        propagateFlagsUp: Boolean = true,
        inNodeDef: Boolean = parentFlags.inNodeDef,
        inAsyncDef: Boolean = parentFlags.inAsyncDef,
        assumeParallelizable: Boolean = false)(f: => global.Tree): Tree = {
      val flags = new Flags
      flags.inNodeDef = inNodeDef
      flags.inAsyncDef = inAsyncDef
      flags.assumeParallelizable = parentFlags.assumeParallelizable || assumeParallelizable
      val origParentFlags = parentFlags
      parentFlags = flags
      val res = f
      parentFlags = origParentFlags
      updateFlags(propagateFlagsUp, res, flags)
      res
    }

    def updateFlags(propagateFlagsUp: Boolean, tree: Tree, flags: Flags): Unit = {
      flags.hasAsyncChild ||= (isAsync(tree) || hasAsync.contains(tree.id))
      if (flags.hasAsyncChild) {
        parentFlags.hasAsyncChild |= propagateFlagsUp
        markAsync(tree)
      }

      parentFlags.hasProblematic ||= (propagateFlagsUp && (flags.hasProblematic || (tree match {
        case Try(_, _, _) => true
        case _            => false
      })))

      if (flags.hasProblematic) {
        hasProblematic.add(tree.id)
      }

      parentFlags.hasSyncStack ||= (propagateFlagsUp && flags.hasSyncStack)

      if (flags.hasSyncStack)
        hasSyncStack.add(tree.id)

      // isMutable, not isVar, because vals on traits are "var" if they're not STABLE, and potentially-tweakable nodes
      // are by definition not STABLE (but they won't change during a collection operation, which is the point)
      if (flags.hasVar || ((tree.symbol ne null) && tree.symbol.isMutable)) {
        parentFlags.hasVar = propagateFlagsUp
        hasVar.add(tree.id)
      }
    }

    // Track temporary artifacts holding async closures
    private val asyncArtifacts = mutable.HashMap.empty[Symbol, ValDef]
    // The async artifacts that we've re-inlined
    private val asyncArtifactsToRemove = mutable.HashSet.empty[Symbol]
    // Extract the async artifact
    def extractAsyncArtifact(f: Symbol, sym: Symbol): Tree = {
      val vd = asyncArtifacts(sym)
      alarm(OptimusNonErrorMessages.ARTIFACT_ASYNC_CLOSURE(f, vd), vd.pos)
      val ValDef(mods, name, tpt, rhs) = vd
      val vdNew = treeCopy.ValDef(vd, mods, name, tpt, rhs)
      vdNew.rhs.changeOwner(vdNew.symbol, vd.symbol.owner)
      // Mark the artifact to be removed from the stats list
      asyncArtifactsToRemove += sym
      vdNew.rhs
    }

    private val suppressInliningTermName = newTermName("suppressInlining")
    private var suppressInlining = false

    private def doInline(tree: Tree): Boolean = {
      if (suppressInlining) {
        debug(tree.pos, "Not inlining due to suppression")
        false
      } else if (enclosingOwner.isClass || enclosingOwner.isModuleOrModuleClass) {
        debug(tree.pos, "Not inlining in constructor")
        false
      } else
        true
    }
    private var enclosingOwner: Symbol = NoSymbol
    override def atOwner[A](tree: global.Tree, owner: global.Symbol)(trans: => A): A = {
      val oldOwner = enclosingOwner
      enclosingOwner = owner
      val result = super.atOwner(tree, owner)(trans)
      enclosingOwner = oldOwner
      result
    }

    private abstract class FunctionN {
      val n: Int
      val This = this

      //                               fn        body  stats
      def unapply(tree: Tree): Option[(Function, Tree, List[Tree])] = tree match {
        case fn @ Function(args, body) if args.size == n => Some((fn, body, Nil))
        case Block(stats, This(fn, body, Nil))           => Some(fn, body, stats)
        case _                                           => None
      }

      /*
      Insert a new function body into the original structure.
       */
      def apply(origWrappedFunction: Tree, origInnerFunction: Function, newBody: Tree, newStats: List[Tree]): Tree = {
        var etaBlocks = 0
        def insert(wrappedFunction: Tree, newBody: Tree): Tree =
          wrappedFunction match {
            case fn @ Function(params, oldBody) =>
              val newFn = treeCopy.Function(fn, params, newBody)
              newBody.changeOwner((origInnerFunction.symbol, newFn.symbol))
              newFn
            case b @ Block(Nil, inner) => treeCopy.Block(b, Nil, insert(inner, newBody))
            case b @ Block(_, inner) =>
              etaBlocks += 1
              assert(etaBlocks == 1, s"Can't reconstruct function AST with $etaBlocks eta blocks")
              treeCopy.Block(b, newStats, insert(inner, newBody))
          }

        insert(origWrappedFunction, newBody)
      }

    }

    private object FunctionOne extends FunctionN { override val n = 1 }
    private object FunctionTwo extends FunctionN { override val n = 2 }

    // Extractor for application of a method, possibly with two multiple argument lists
    // We require full application to at least one argument, i.e. same shape of paramList and argsList
    private object FullMethodApply {

      def unapply(tree: Tree): Option[(Select, List[Tree], List[List[Tree]], List[List[Symbol]])] = {
        if (!tree.isInstanceOf[Apply] || (tree.symbol eq null) || !tree.symbol.isMethod)
          None
        else {
          val treeInfo.Applied(f, ts, argss) = tree
          val paramss = f.symbol.info.paramss
          if (!f.isInstanceOf[Select] || paramss.isEmpty) None
          else {
            // Fix paramss by adjusting it so that each params matches the length of the corresponding args, accounting
            // for repeated args.
            val adjustedParamss =
              map2Conserve(paramss, argss)((params, args) => adjustForRepeatedParameters(params, args))
            Some((f.asInstanceOf[Select], ts, argss, adjustedParamss))
          }
        }
      }
    }

    /**
     * create a temporary symbol and valdef val qual$123 = qual properly typed and recursively transformed. (Enclosing
     * tree is only used for position information.)
     */
    private def extractQualifier(tree: Tree, qual: Tree): (global.TermSymbol, global.ValDef) = {
      val qualName = unit.freshTermName("qual$")
      val qs = enclosingOwner.newValue(qualName, tree.pos, Flags.SYNTHETIC)
      // qual has potentially changed type, eg a different WithFilter path
      qs.setInfo(qual.tpe)
      val qualValDef = ValDef(qs, qual)
      (qs, qualValDef)
    }

    /**
     * Give a pre-identified monadic qualifier, applied to a function arg => body Create temporary symbols and valdefs
     * val mon$123 = qual val arg = mon$123.get properly typed and recursively transformed, as well as the transformed
     * and body, with ownership transformed to the encloser (rather than the anonymous function).
     */
    private def extractAndSetFunctionArg(
        tree: Tree,
        qual: Tree,
        fn: Function,
        body: Tree,
        get: TermSymbol => Tree
    ): (global.TermSymbol, global.ValDef, global.TermSymbol, global.ValDef) = {

      val (qs, qualValdef) = extractQualifier(tree, qual)

      val Function((vd @ ValDef(mods, argName, argType, _)) :: Nil, _) = fn

      val argSym = vd.symbol.asTerm
      argSym.resetFlags()
      argSym.setFlag(Flags.SYNTHETIC)
      argSym.owner = enclosingOwner

      assert(enclosingOwner != NoSymbol)

      val argValDef = ValDef(argSym, get(qs))

      body.changeOwner((fn.symbol, enclosingOwner))

      (qs, qualValdef, argSym, argValDef)
    }

    private def transformApplicationParts(
        fn: Function, // used only for ownership of body
        qual: Tree,
        body: Tree,
        stats: List[Tree]): (global.Tree, global.Tree, List[global.Tree], Boolean) = {
      val qual_x0 = inContext()(transform(qual))
      val bx = inContext()(atOwner(fn.symbol)(transform(body)))
      val stats_x = stats.map { stat =>
        inContext()(transform(stat))
      }
      val a = hasAsync(qual_x0.id) || hasAsync(bx.id) || stats_x.exists(t => hasAsync(t.id))
      (qual_x0, bx, stats_x, a)
    }

    /**
     * Extractor to detect and inline option method calls.
     */
    private object InlineMethod {

      private val mapTN = newTermName("map")
      private val flatMapTN = newTermName("flatMap")
      private val getOrElseTN = newTermName("getOrElse")
      private val filterTN = newTermName("filter")
      private val withFilterTN = newTermName("withFilter")
      private val foreachTN = newTermName("foreach")
      private val filterNotTN = newTermName("filterNot")
      private val foldTN = newTermName("fold")
      private val orElseTN = newTermName("orElse")
      private val existsTN = newTermName("exists")
      private val forallTN = newTermName("forall")

      private def method(cs: ClassSymbol, tn: TermName): global.MethodSymbol = cs.tpe.decl(tn).asMethod

      private val optionCls = rootMirror.getRequiredClass("scala.Option")
      private val withFilterCls = optionCls.tpe.decl(newTypeName("WithFilter")).asClass
      private val eitherCls: ClassSymbol = rootMirror.getRequiredClass("scala.util.Either")
      private val rightProjectionCls = rootMirror.getRequiredClass("scala.util.Either.RightProjection")
      private val leftProjectionCls = rootMirror.getRequiredClass("scala.util.Either.LeftProjection")

      /**
       * Convert WithFilter to Option so we can call methods like .hasValue on it. This is only necessary when the
       * WithFilter qualifier has not itself been inlined.
       */
      private def maybeConvertWithFilterToOption(tree: Tree) = {
        if (tree.tpe.typeSymbol == withFilterCls) {
          val asOption = localTyper.typedPos(tree.pos)(q"$tree.map(scala.Predef.identity)")
          assert(asOption.tpe.typeSymbol isSubClass definitions.OptionClass)
          asOption
        } else tree
      }

      /**
       * Conversely, the original qualifier was a WithFilter, but it got inlined and transformed into an Option, we
       * can't use the old WithFilter#method.
       */
      private def maybeSetOptionMethod(
          tree: Tree,
          oldQual: Tree,
          newQual: Tree,
          optionMethodSymbol: MethodSymbol): Unit = {
        if (oldQual.tpe.typeSymbol == withFilterCls && newQual.tpe.typeSymbol != withFilterCls)
          tree.setSymbol(optionMethodSymbol)
      }

      // Each of the inlining methods below returns Option[(Tree, Boolean)]
      //   None =>                 The tree argument wasn't touched
      //   Some(newTree, false) => Qualifier and body were transformed and potentially changed, but
      //                           this call itself was not inlined.
      //   Some(newTree, true)  => The call was inlined.

      /**
       * xyz.getOrElse(default) \==> val mon$123 = xyz if(xyz.isEmpty) default else mon$123.get
       */
      private def inlineGetOrElse(tree: Tree): Option[InliningResult] = tree match {
        case a @ Apply(ta @ TypeApply(Select(qual, _), _), default :: Nil) =>
          // Occurs irrespective of whether the body is async
          val qual_x = maybeConvertWithFilterToOption(inContext()(transform(qual)))
          val default_x = inContext()(transform(default))
          val (os, optValDef) = extractQualifier(tree, qual_x)
          val ifElse = If(q"$os.isEmpty", default_x, q"$os.get")
          val ret = localTyper.typedPos(tree.pos)(Block(optValDef, ifElse))
          if (hasAsync(default_x.id) || hasAsync(qual_x.id))
            markAsync(ret)
          qual_x.changeOwner((enclosingOwner, os)) // occurs after typer
          Some(InliningResult(ret, true))
        case _ => None
      }

      private def inlineEitherishGetOrElse(pred_get: TermSymbol => (Tree, Tree))(tree: Tree): Option[InliningResult] =
        tree match {
          case a @ Apply(ta @ TypeApply(Select(qual, _), _), default :: Nil) =>
            // Occurs irrespective of whether the body is async
            val qual_x = inContext()(transform(qual))
            val default_x = inContext()(transform(default))
            val (os, optValDef) = extractQualifier(tree, qual_x)
            val (pred, get) = pred_get(os)
            val ifElse = If(pred, get, default_x)
            val ret = localTyper.typedPos(tree.pos)(Block(optValDef, ifElse))
            if (hasAsync(default_x.id) || hasAsync(qual_x.id))
              markAsync(ret)
            qual_x.changeOwner((enclosingOwner, os)) // occurs after typer
            Some(InliningResult(ret, true))
          case _ => None
        }

      /**
       * xyz.orElse(default) \==> val mon$123 = xyz if(xyz.isEmpty) default else mon$123
       */
      private def inlineOrElse(tree: Tree): Option[InliningResult] = tree match {
        case ap @ Apply(ta @ TypeApply(sel @ Select(qual, n), tpt), default :: Nil) =>
          val default_x = inContext()(transform(default))
          val qual_x0 = inContext()(transform(qual))
          if (!hasAsync(default_x.id)) {
            val ret = copyIfReplacements(ap, default -> default_x, qual -> qual_x0)
            Some(InliningResult(ret, false))
          } else {
            val qual_x = maybeConvertWithFilterToOption(qual_x0)
            val (os, optValDef) = extractQualifier(tree, qual_x)
            val ifElse = If(q"$os.isEmpty", default_x, Ident(os))
            val ret = markAsync(localTyper.typedPos(tree.pos)(Block(optValDef, ifElse)))
            qual_x.changeOwner((enclosingOwner, os)) // occurs after typer
            Some(InliningResult(ret, true))
          }
        case _ => None
      }

      object IgnoreTypeApply {
        def unapply(tree: Tree) = tree match {
          case TypeApply(fun, _) => Some(fun)
          case _                 => Some(tree)
        }
      }

      /**
       * qual.op( x => body ) \==> val mon$123 = xyx if(mon$123.isDefined) { val x = mon$123.get ~resExpr(qs, body,
       * xValDef)
       */
      private def inlineOp(
          meth: MethodSymbol, // e.g. Option#map
          tree: Tree,
          get: TermSymbol => Tree,
          resExpr: (
              Symbol, // os
              Tree, // body
              ValDef, // argValDef
              Type // expression type
          ) => Tree): Option[InliningResult] = {
        tree match {
          case ap @ Apply(IgnoreTypeApply(sel @ Select(qual, n)), List(fw @ FunctionOne(fn, body, stats))) =>
            val (qual_x0, bx, stats_x, a) = transformApplicationParts(fn, qual, body, stats)
            if (!hasAsync(bx.id)) {
              val ret = maybeMarkAsync(a, copyIfReplacementsS(ap, qual -> qual_x0 :: body -> bx :: stats.zip(stats_x)))
              maybeSetOptionMethod(ret, qual, qual_x0, meth)
              Some(InliningResult(ret, false))
            } else {
              val qual_x = maybeConvertWithFilterToOption(qual_x0)
              val (qs, qualValDef, _, argValDef) = extractAndSetFunctionArg(tree, qual_x, fn, bx, get)
              val block = Block(stats_x :+ qualValDef, resExpr(qs, bx, argValDef, tree.tpe))
              val ret = markAsync(localTyper.typedPos(tree.pos)(block))
              qual_x.changeOwner((enclosingOwner, qs)) // occurs after typer
              Some(InliningResult(ret, true))
            }
          case _ => None
        }
      }

      private def optGet(qs: TermSymbol) = q"$qs.get"
      private def projGet(qs: TermSymbol) = q"$qs.get"
      private def rightGet(qs: TermSymbol) = q"$qs.right.get"
      private def leftGet(qs: TermSymbol) = q"$qs.left.get"

      private val S = q"_root_.scala.Some"
      private val N = Ident(names.None)
      private def B(ts: Tree*): Block = Block(ts: _*)

      private type Sy = Symbol
      private type Tr = Tree
      private type VD = ValDef
      private val ET = typeOf[Either[_, _]]

      // Given a symbol qs of type Either[A,B] and a tree bx of type C, wrap bx as a Right[A,C]
      private def asRight(qs: Symbol, bx: Tree): Tree = {
        val tps = qs.tpe.dealiasWiden.typeArgs
        assert(tps.size == 2)
        val lt = tps.head
        val rt = bx.tpe
        q"_root_.scala.util.Right.apply[$lt, $rt]($bx)"
      }
      // Given a symbol qs of type Either[A,B] and a tree bx of type C, wrap bx as a Left[C,B]
      private def asLeft(qs: Symbol, bx: Tree): Tree = {
        val tps = qs.tpe.dealiasWiden.typeArgs
        assert(tps.size == 2)
        val lt = bx.tpe
        val rt = tps.last
        q"_root_.scala.util.Left.apply[$lt, $rt]($bx)"
      }

      /**
       * xyz.fold(default)(x => expr) \==> val mon$123 = xyz if(mon$123.isDefined) { val x = mon$123.get expr } else
       * default
       */
      private def inlineFold(tree: Tree): Option[InliningResult] = tree match {
        case ap1 @ Apply(
              ap2 @ Apply(ta @ TypeApply(sel @ Select(qual, n), tpt), default :: Nil),
              List(fw @ FunctionOne(fn, body, stats))) =>
          val (qual_x0, bx, stats_x, a) = transformApplicationParts(fn, qual, body, stats)
          val default_x = inContext()(transform(default))
          if (!hasAsync(bx.id) && !hasAsync(default_x.id)) {
            val ret =
              maybeMarkAsync(
                a,
                copyIfReplacementsS(tree, qual -> qual_x0 :: body -> bx :: default -> default_x :: stats.zip(stats_x)))
            Some(InliningResult(ret, false))
          } else {
            val qual_x = maybeConvertWithFilterToOption(qual_x0)
            val (os, optValDef, argSym, argValDef) = extractAndSetFunctionArg(tree, qual_x, fn, body, qs => q"$qs.get")
            val ifElse = If(q"$os.isDefined", Block(argValDef, bx), default_x)
            bx.changeOwner(fn.symbol, enclosingOwner)
            val ret = markAsync(localTyper.typedPos(tree.pos)(Block(stats_x :+ optValDef, ifElse)))
            qual_x.changeOwner((enclosingOwner, os)) // occurs after typer
            Some(InliningResult(ret, true))
          }
        case _ => None
      }

      // The boolean indicates whether the operation was inlined.  (Even if wasn't, the tree could still differ
      // because the components have been recursively traversed.)
      private case class InliningResult(newTree: Tree, didInline: Boolean)
      private type Inliner = Function1[Tree, Option[InliningResult]]

      // So we can define an alias in the dispatch map before defining the thing it's aliasing.
      private def alias(classSymbol: ClassSymbol, methodName: TermName): Inliner = { tree: Tree =>
        val ms = method(classSymbol, methodName)
        dispatch(ms)(tree)
      }

      // Define an element of the dispatch tree
      private def inlineComb(classSymbol: ClassSymbol, methodName: TermName, get: TermSymbol => Tree)(
          res: (Sy, Tree, VD, Type) => Tree): (MethodSymbol, Inliner) = {
        val m = method(classSymbol, methodName)
        m -> { tree: Tree =>
          inlineOp(m, tree, get, res)
        }
      }

      private val dispatch0 = Map[Symbol, Inliner](
        method(optionCls, getOrElseTN) -> inlineGetOrElse,
        method(optionCls, orElseTN) -> inlineOrElse,
        method(optionCls, foldTN) -> inlineFold,
        method(eitherCls, getOrElseTN) -> inlineEitherishGetOrElse(os => (q"$os.isRight", q"$os.right.get")),
        method(rightProjectionCls, getOrElseTN) -> inlineEitherishGetOrElse(os => (q"$os.e.isRight", q"$os.get")),
        method(leftProjectionCls, getOrElseTN) -> inlineEitherishGetOrElse(os => (q"$os.e.isLeft", q"$os.get"))
      )

      private val dispatchAliases = Map[Symbol, Inliner](
        method(withFilterCls, mapTN) -> alias(optionCls, mapTN),
        method(withFilterCls, flatMapTN) -> alias(optionCls, flatMapTN),
        method(withFilterCls, foreachTN) -> alias(optionCls, foreachTN),
        method(optionCls, filterTN) -> alias(optionCls, withFilterTN),
        method(withFilterCls, withFilterTN) -> alias(optionCls, withFilterTN)
      )

      private val dispatchEither: Map[Symbol, Inliner] = Map.empty[Symbol, Inliner] +
        inlineComb(eitherCls, mapTN, rightGet) { case (qs, bx, avd, tp) =>
          If(q"$qs.isRight", Block(avd, asRight(qs, bx)), q"$qs.asInstanceOf[$tp]")
        } +
        inlineComb(eitherCls, flatMapTN, rightGet) { case (qs, bx, avd, tp) =>
          If(q"$qs.isRight", Block(avd, bx), q"$qs.asInstanceOf[$tp]")
        } +
        inlineComb(eitherCls, foreachTN, rightGet) { case (qs, bx, avd, _) =>
          If(q"$qs.isRight", Block(avd, bx), q"()")
        } +
        inlineComb(eitherCls, forallTN, rightGet) {
          case (qs, bx, avd, _) => { val pred = Block(avd, bx); q"$qs.isLeft || $pred" }
        } +
        inlineComb(eitherCls, existsTN, rightGet) {
          case (qs, bx, avd, _) => { val pred = Block(avd, bx); q"$qs.isRight && $pred" }
        }

      private val rightProjections: Map[Symbol, Inliner] = Map.empty[Symbol, Inliner] +
        inlineComb(rightProjectionCls, mapTN, projGet) { case (qs, bx, avd, tp) =>
          If(q"$qs.e.isRight", Block(avd, asRight(qs, bx)), q"$qs.e.asInstanceOf[$tp]")
        } +
        inlineComb(rightProjectionCls, flatMapTN, projGet) { case (qs, bx, avd, tp) =>
          If(q"$qs.e.isRight", Block(avd, bx), q"$qs.e.asInstanceOf[$tp]")
        } +
        inlineComb(rightProjectionCls, filterTN, projGet) { case (qs, bx, avd, _) =>
          If(q"$qs.e.isRight", Block(avd, q"if($bx) $S($qs.e) else $N"), N)
        } +
        inlineComb(rightProjectionCls, foreachTN, projGet) { case (qs, bx, avd, _) =>
          If(q"$qs.e.isRight", Block(avd, bx), q"()")
        } +
        inlineComb(rightProjectionCls, forallTN, projGet) {
          case (qs, bx, avd, _) => { val pred = Block(avd, bx); q"$qs.e.isLeft || $pred" }
        } +
        inlineComb(rightProjectionCls, existsTN, projGet) {
          case (qs, bx, avd, _) => { val pred = Block(avd, bx); q"$qs.e.isRight && $pred" }
        }

      private val leftProjections: Map[Symbol, Inliner] = Map.empty[Symbol, Inliner] +
        inlineComb(leftProjectionCls, mapTN, projGet) { case (qs, bx, avd, tp) =>
          If(q"$qs.e.isLeft", Block(avd, asLeft(qs, bx)), q"$qs.e.asInstanceOf[$tp]")
        } +
        inlineComb(leftProjectionCls, flatMapTN, projGet) { case (qs, bx, avd, tp) =>
          If(q"$qs.e.isLeft", Block(avd, bx), q"$qs.e.asInstanceOf[$tp]")
        } +
        inlineComb(leftProjectionCls, filterTN, projGet) { case (qs, bx, avd, _) =>
          If(q"$qs.e.isLeft", Block(avd, q"if($bx) $S($qs.e) else $N"), N)
        } +
        inlineComb(leftProjectionCls, foreachTN, projGet) { case (qs, bx, avd, _) =>
          If(q"$qs.e.isLeft", Block(avd, bx), q"()")
        } +
        inlineComb(leftProjectionCls, forallTN, projGet) {
          case (qs, bx, avd, _) => { val pred = Block(avd, bx); q"$qs.e.isRight || $pred" }
        } +
        inlineComb(leftProjectionCls, existsTN, projGet) {
          case (qs, bx, avd, _) => { val pred = Block(avd, bx); q"$qs.e.isLeft && $pred" }
        }

      private val dispatchOption: Map[Symbol, Inliner] = Map.empty[Symbol, Inliner] +
        inlineComb(optionCls, mapTN, optGet) { case (qs, bx, avd, _) =>
          If(q"$qs.isDefined", B(avd, q"$S($bx)"), N)
        } +
        inlineComb(optionCls, flatMapTN, optGet) { case (os, bx, avd, _) =>
          If(q"$os.isDefined", B(avd, bx), N)
        } +
        inlineComb(optionCls, foreachTN, optGet) { case (os, bx, avd, _) =>
          If(q"$os.isDefined", Block(avd, bx), EmptyTree)
        } +
        inlineComb(optionCls, withFilterTN, optGet) { case (os, bx, avd, _) =>
          If(q"$os.isDefined", Block(avd, q"if($bx) $os else $N"), N)
        } +
        inlineComb(optionCls, filterNotTN, optGet) { case (os, bx, avd, _) =>
          If(q"$os.isDefined", Block(avd, q"if($bx) $N else $os"), N)
        } +
        inlineComb(optionCls, existsTN, optGet) {
          case (os, bx, avd, _) => { val pred = Block(avd, bx); q"$os.isDefined && $pred" }
        } +
        inlineComb(optionCls, forallTN, optGet) {
          case (os, bx, avd, _) => { val pred = Block(avd, bx); q"$os.isEmpty || $pred" }
        }

      private val dispatch: Map[Symbol, Inliner] =
        dispatch0 ++ dispatchAliases ++ dispatchOption ++ dispatchEither ++ rightProjections ++ leftProjections

      def unapply(tree: Tree): Option[Tree] = tree match {
        case app: Apply =>
          val extractor: Option[Inliner] = dispatch.get(app.symbol)
          if (extractor.isDefined && doInline(app)) {
            val o1 = getOwners(tree)
            extractor.get(tree).map { case InliningResult(t, didInline) =>
              if (pluginData.alarmConfig.debug) {
                alarm(
                  OptimusNonErrorMessages
                    .INLINING_DEBUG("" + didInline + ":" + symDiffs(o1, t)._3.toString + "Option", app.symbol.name),
                  tree.pos)
              }
              t
            }
          } else None
        case _ => None
      }
    }

    private type Sym = (Symbol, Int)
    private def getOwners(tree: Tree): Map[Sym, Sym] = {
      val m = mutable.HashMap[Symbol, Symbol]()
      val trav = new Traverser {
        override def traverse(tree: Tree): Unit = {
          if ((tree.symbol ne null) && tree.symbol != NoSymbol)
            m += ((tree.symbol, tree.symbol.owner))
          super.traverse(tree)
        }
      }
      trav.traverse(tree)
      m.map(e => ((e._1, e._1.id), (e._2, e._2.id))).toMap
    }

    private def symDiffs(tree1: Tree, tree2: Tree): (Map[Sym, Sym], Map[Sym, Sym], Set[(Sym, Sym, Sym)]) = {
      val o1 = getOwners(tree1)
      val o2 = getOwners(tree2)
      symDiffs(o1, o2)
    }
    private def symDiffs(o1: Map[Sym, Sym], tree2: Tree): (Map[Sym, Sym], Map[Sym, Sym], Set[(Sym, Sym, Sym)]) =
      symDiffs(o1, getOwners(tree2))

    private def symDiffs(o1: Map[Sym, Sym], o2: Map[Sym, Sym]): (Map[Sym, Sym], Map[Sym, Sym], Set[(Sym, Sym, Sym)]) = {
      val only1 = o1 -- o2.keys
      val only2 = o2 -- o1.keys
      val both = o1.keySet -- only1.keys
      val diffs = both.map(s => (s, o1(s), o2(s))).filter { case (_, d1, d2) => d1 != d2 }
      (only1, only2, diffs)
    }

    /**
     * Extractor to detect and inline Iterable foreach calls
     */
    private object InlineIterableForeach {

      def unapply(tree: Tree): Option[Tree] = {
        // Quick first pass to avoid the deeper unapply if possible.
        if ((tree.symbol eq null) || nme.foreach != tree.symbol.name)
          None
        else
          tree match {
            case Apply(ta @ TypeApply(sel @ Select(qual, meth), tpt), List(fw @ FunctionOne(fn, body, stats)))
                if (qual.tpe.typeSymbol isSubClass definitions.IterableClass) && doInline(tree) =>
              assert(meth == nme.foreach)
              val (qual_x, bx, stats_x, a) = transformApplicationParts(fn, qual, body, stats)
              if (!hasAsync(bx.id)) {
                // Reconstruct the original application.
                val ret = maybeMarkAsync(
                  a,
                  treeCopy.Apply(
                    tree,
                    treeCopy.TypeApply(ta, treeCopy.Select(sel, qual_x, meth), tpt),
                    FunctionOne(fw, fn, bx, stats_x) :: Nil))
                Some(ret)
              } else {
                val Function((vd @ ValDef(mods, argName, argTpt, _)) :: Nil, _) = fn
                assert(enclosingOwner != NoSymbol)

                // E.g. if qual is a Foo[A,B] extends Iterable[B], we want
                // val iterator$x: Iterator[B] = (qualifier).iterator
                // Extracting from transformed qual
                // val argType = qual_x.tpe.baseType(typeOf[Iterable[_]].typeSymbol).typeArgs.head
                val argType = vd.symbol.tpe
                val iteratorTpe = appliedType(definitions.IteratorClass, argType :: Nil)
                val iteratorName = unit.freshTermName("iterator$")
                val iteratorSym = enclosingOwner.newValue(iteratorName, tree.pos, Flags.SYNTHETIC)
                iteratorSym.setInfo(iteratorTpe)
                val iteratorValDef = ValDef(iteratorSym, q"($qual_x).iterator.asInstanceOf[$iteratorTpe]")

                // Create: val fnParam = iterator$x.next()
                val argSym = vd.symbol
                argSym.resetFlags()
                argSym.setFlag(Flags.SYNTHETIC)
                argSym.owner = enclosingOwner
                // argSym.setInfo(NoType)
                // argSym.setInfo(argType)
                val argValDef = ValDef(argSym, q"$iteratorSym.next()")

                val whileBody = Block(stats_x :+ argValDef :+ bx: _*)
                val whileLoop = q"while($iteratorSym.hasNext) {$whileBody}"

                val block = Block(iteratorValDef, whileLoop)
                val ret = localTyper.typedPos(tree.pos)(block)
                qual_x.changeOwner((enclosingOwner, iteratorSym))
                bx.changeOwner((fn.symbol, enclosingOwner))
                markAsync(ret)
                alarm(OptimusNonErrorMessages.INLINING_DEBUG("Iterable", "foreach"), tree.pos)
                Some(ret)
              }

            case _ => None

          }

      }

    }

    private object InlineMapGetOrElse {
      def unapply(tree: Apply): Option[Tree] = {
        // cheap initial check that this might be a method we're interested in
        if ((tree.symbol eq null) || (tree.symbol.name ne names.getOrElse)) None
        else
          tree match {
            case Apply(tapp @ TypeApply(fun @ Select(qual, _), targs), key :: default :: Nil)
                if fun.symbol.overriddenSymbol(
                  GenMapLikeCls) ne NoSymbol => // GenMapLike is the base class containing getOrElse
              // There are two acceptable ways to inline map.getOrElse(key, default):
              //   - if (map.contains(key)) map.apply(key) else default
              //   - val tmp = map.get(key); if (tmp.isDefined) tmp.get else default
              // The first looks better but will likely be slow on large maps (since there's a duplicate lookup);
              // the latter incurs an extra allocation of a Some. I'm picking the latter because I think the overhead
              // will be generally less, especially in cases of big maps, and we're only doing this in the case of async
              // calls, which means that we're already going to go off and do (relatively) expensive other stuff.
              val qual_x = inContext()(transform(qual))
              val key_x = inContext()(transform(key))
              val default_x =
                inContext()(transform(default)) // we're running before uncurry, so this by-name arg looks normal
              if (!hasAsync(default_x.id)) {
                // so we have to do the sub-transforms to check for async, but we're not going to inline (it's a pessimization generally)
                // but having done the work we may as well use it, so reconstruct the result tree here:
                Some(
                  treeCopy.Apply(
                    tree,
                    treeCopy.TypeApply(tapp, treeCopy.Select(fun, qual_x, names.getOrElse), targs),
                    key_x :: default_x :: Nil))
              } else { // inlining time!
                val optionalValue = localTyper.typedPos(tree.pos.focus) {
                  Apply(Select(qual_x, GenMapLike_get), key_x :: Nil)
                }
                val tmp = currentOwner
                  .newTermSymbol(unit.freshTermName("getOrElse$"), tree.pos.focus, Flags.SYNTHETIC)
                  .setInfo(optionalValue.tpe) // cheat by pre-typing the map.get(key) and using whatever it returns
                optionalValue.changeOwner(currentOwner, tmp)
                Some(localTyper.typedPos(tree.pos.focus) {
                  Block(
                    ValDef(tmp, optionalValue),
                    If(
                      cond = Select(Ident(tmp), currentRun.runDefinitions.Option_isDefined),
                      thenp = Select(Ident(tmp), currentRun.runDefinitions.Option_get),
                      elsep = default_x
                    )
                  )
                })
              }
            case _ => None
          }
      }
    }

    override def transform(tree: Tree): Tree = {
      try {
        transformSafe(tree)
      } catch {
        case ex: Throwable =>
          ex.printStackTrace()
          alarm(OptimusErrors.TRANSFORM_ERROR_AUTOASYNC, tree.pos, throwableAsString(ex))
          tree
      }
    }

    def transformSafe(tree: Tree): Tree = {
      curTree = tree

      tree match {
        // Skip this sub-tree if annotated with @suppressAutoAsync
        case Apply(func, _) if (func.symbol ne null) && func.symbol.hasAnnotation(SuppressAutoAsyncAnnotation) =>
          alarm(OptimusNonErrorMessages.SUPRESS_ASYNC_LABEL_DEBUG, func.pos)
          tree

        case CaptureCode(expr) =>
          val transformedExpr = inContext()(transform(expr))
          CaptureCode(transformedExpr, localTyper)

        // Keep track of whether we're within an @node
        // TODO (OPTIMUS-26094): keep track of this with something better than @node/@async
        case DefDef(_, _, _, _, _, _) if tree.symbol.hasAnnotation(NodeAnnotation) =>
          inContext(inNodeDef = true)(super.transform(tree))

        // Keep track of whether we're within an @async
        case DefDef(_, _, _, _, _, _) if tree.symbol.hasAnnotation(AsyncAnnotation) =>
          inContext(inAsyncDef = true)(super.transform(tree))

        case DefDef(_, nme.CONSTRUCTOR, _, _, _, _) =>
          /* We don't support async within constructors, but it's extensively attempted, so warning about it will just
           * upset everyone.
            if (hasAsync(tree.id))
              reporter.info(tree.pos, "Note: Constructors should not contain async calls", true)
           */
          tree

        case vd @ ValOrDefDef(_, name, _, _) =>
          val vdx @ ValOrDefDef(_, _, _, rhs) = inContext()(super.transform(tree))
          // Complain about lazy async
          if (hasAsync(rhs.id)) {
            if (
              vd.hasSymbolWhich(_.isLazy) && !isInstance(rhs.tpe, tAsyncLazy) && !vd.symbol
                .hasAnnotation(ClosuresEnterGraph)
            ) {
              val pos = rhs.find(isAsync).fold(rhs.pos)(_.pos)
              alarm(OptimusNonErrorMessages.ASYNC_LAZY(), pos)
            }
          } else {
            if (isInstance(rhs.tpe, tAsyncLazy)) {
              rhs match {
                // We should probably do this at the point where we create the AsyncLazy node...
                case Apply(_, args) if args.flatMap(_.find(isAsync)).isEmpty =>
                  alarm(OptimusNonErrorMessages.STOP_ADDING_ASYNC_VAL_EVERYWHERE, vd.pos)
                case _ =>
              }
            }
          }
          vdx

        case Import(expr, ImportSelector(n, _, _, _) :: Nil)
            if (expr.symbol == AsyncCollection && n == suppressInliningTermName) =>
          reporter.echo(tree.pos, s"Suppressing Option inlining for $unit")
          suppressInlining = true
          super.transform(tree)

        case Import(_, _) =>
          tree

        case Try(_, _, _) =>
          val res @ Try(body, cases, finalizer) = inContext()(super.transform(tree))
          if (hasAsync(body.id) || hasAsync(finalizer.id) || cases.exists(cd => hasAsync(cd.id)))
            alarm(OptimusNonErrorMessages.ASYNC_TRY, tree.pos)
          if (parentFlags.inNodeDef) {
            cases.foreach { case CaseDef(pat, _, _) =>
              pat
                .find { t =>
                  t.tpe <:< definitions.ThrowableTpe &&
                  !(t.tpe <:< RTExceptionTraitTpe || t.tpe <:< RTExceptionInterfaceTpe ||
                    RTListStatic.members.contains(t.tpe.typeSymbol.fullName))
                }
                .foreach { t =>
                  alarm(OptimusNonErrorMessages.TRY_CATCH_NODE(t.tpe.typeSymbol.fullName), t.pos)
                }
            }
          }
          res

        // Detect and warn about applying what looks like a mocking library to an entity.
        case TypeApply(fun, args)
            if containsMock &&
              maybeMock(fun.symbol.toString) &&
              args.exists(i => isEntity(i.symbol)) =>
          alarm(OptimusNonErrorMessages.MOCK_ENTITY(), args.head.pos)
          inContext()(super.transform(tree))

        case Apply(fun, meth :: Nil)
            if fun.symbol.name == names.expect &&
              fun.toString.startsWith("org.easymock.EasyMock.expect") &&
              isNode(meth.symbol) =>
          alarm(OptimusNonErrorMessages.MOCK_NODE(), fun.pos)
          inContext()(super.transform(tree))

        // If this case matches, the extracted value will have been transformed and possibly inlined.
        case InlineMethod(inlined) =>
          checkOwnership(inlined, currentOwner)
          inlined

        case Apply(
              ap2 @ Apply(TypeApply(sel @ Select(qual, names.foldLeft), _), z :: Nil),
              (fn @ FunctionTwo(fi, expr, stats)) :: Nil) if isIterable(qual.tpe) =>
          val (qual_x, expr_x, stats_x, a) = transformApplicationParts(fi, qual, expr, stats)
          val z_x = inContext()(transform(z))
          if (!hasAsync(expr_x.id))
            maybeMarkAsync(
              a,
              copyIfReplacementsS(tree, qual -> qual_x :: expr -> expr_x :: z -> z_x :: stats.zip(stats_x)))
          else {
            val newSel = Select(wrapWithAsync(qual_x, names.seq, names.foldLeft), names.foldLeft)
            val ret =
              markAsync(
                typedPosWithReplacements(tree, sel -> newSel, fn -> FunctionTwo(fn, fi, expr_x, stats_x), z -> z_x))
            ret
          }

        // If this case matches, the extracted value will have been transformed and possibly inlined.
        case InlineIterableForeach(inlined) =>
          checkOwnership(inlined, currentOwner)
          inlined

        // If this case matches, the extracted value will have been transformed and possibly inlined.
        case InlineMapGetOrElse(inlined) =>
          checkOwnership(inlined, currentOwner)
          inlined

        // We match expressions of the form: monad.combinator(f), where the relevant monads/combinators are defined above
        // If f has async calls, then we check to see whether monad can be automatically async'd or parallelised
        case CombApply(app1, tapp, sel, coll, comb, f, targs, iargs, mtype) =>
          inContext() {
            val collRes = inContext()(transform(coll))
            val closure @ Closure(vds, body, fRes) = inContext()(transform(f))
            val componentsChanged = (collRes ne coll) || (closure ne f)
            val hasAsync = CombApply.isApplyToMethodWithAsyncBody(app1, comb, vds, body)
            checkAndRewriteCombinator(
              tree,
              app1,
              tapp,
              sel,
              collRes,
              comb,
              fRes,
              body,
              targs,
              iargs,
              mtype,
              hasAsync,
              componentsChanged)
          }

        // We don't want to propagate async-ness from a function to the enclosing scope
        // Nor from the enclosing scope to the function
        case Function(_, _) =>
          val res = inContext(
            propagateFlagsUp = false,
            inNodeDef = false,
            inAsyncDef = false
          )(super.transform(tree))
          res

        case FullMethodApply(f, ts, argss, paramss)
            if !pluginSupportSymbol.exists(_ == f.symbol.owner.module) => { // plugin support is exempt

          if (containsMock && maybeMock(f.toString) && ts.exists(tt => isEntity(tt.symbol)))
            alarm(OptimusNonErrorMessages.MOCK_ENTITY(), ts.head.pos)

          // If a method has @nodeLift, then arguments will be converted to NodeKeys rather than
          // being executed directly, so we don't need to async.
          val functionIsNodeLift = hasNodeLiftAnno(f.symbol) || hasNodeLiftByName(f.symbol)
          val isClosuresEnterGraphFn = f.symbol.hasAnnotation(ClosuresEnterGraph)

          // If a method has @assumeParallelizableInClosures, assume that any collection operations
          // found in closures in arguments passed to the function are parallelizable and should be
          // silently apar'd.
          // If a particular parameter has this annotation, assume the above for collection operations
          // found in closures in the corresponding argument.
          inContext(assumeParallelizable = f.symbol.hasAnnotation(AssumeParallelizableInClosures)) {

            val f_x: Tree = inContext()(transform(f))
            assert(f_x.isInstanceOf[Select]) // better still be a select, or the world has gone mad
            val Select(qual_x, methName) = f_x

            val argss_x: List[List[Tree]] = map2Conserve(argss, paramss) { case (args, params) =>
              // NOTE: params and args are the same length which is enforced in FullMethodApply
              map2Conserve(args, params) { (arg, param) =>
                def finish(ret: Tree, closure: Tree): Tree = {
                  ret.setType(arg.tpe)
                  markAsync(ret)
                  // This is a little silly, but I have an inkling that will keep us from generating a 17001 in asyncgraph.
                  if (isClosuresEnterGraphFn || param.hasAnnotation(ClosuresEnterGraph))
                    closure.symbol.addAnnotation(EntersGraphAnnotation)
                  ret
                }
                val extractedArg = arg match {
                  // Look for arguments that are artifacts holding async closures...
                  case i: Ident if asyncArtifacts.contains(i.symbol) =>
                    val rh = extractAsyncArtifact(f.symbol, i.symbol)
                    finish(rh, rh)
                  // or, strangely, for expressions of the specific form: artifact.apply()
                  case Apply(sel @ Select(i @ Ident(_), nme.apply), Nil) if asyncArtifacts.contains(i.symbol) =>
                    val rh = extractAsyncArtifact(f.symbol, i.symbol)
                    finish(Apply(Select(rh, nme.apply).setType(sel.tpe), Nil), rh)
                  case a => a
                }

                // The async-ness of the argument propagates to the apply, but there are exceptions:
                val propagateFlagsUp =
                  // Don't propagate if the function is @nodeLift, in which case all arguments are converted to new nodes
                  !functionIsNodeLift &&
                    // Don't propagate if the parameter is a regular by-name, as the argument's execution will be deferred
                    (!param.isByNameParam ||
                      // unless the by-name argument is itself marked @nodeLift but not @nodeLiftByName, in which case it will be converted immediately
                      // into an AlreadyCompletedNode, so we DO want to propagate its async-ness.
                      (hasNodeLiftAnno(param) && !hasNodeLiftByName(param))) &&
                    // Don't propagate if the the argument is a closure
                    !isClosure(extractedArg)

                inContext(
                  propagateFlagsUp,
                  assumeParallelizable = param.hasAnnotation(AssumeParallelizableInClosures)
                )(transform(extractedArg))
              }
            }

            def bailout = copyIfReplacements(tree, ((f -> f_x) :: argss.flatten.zip(argss_x.flatten)): _*)

            val wrapWithAsNode: Tree => Tree = (fn: Tree) => {
              val tps = fn.tpe.typeArgs
              q"_root_.optimus.platform.asNode[..$tps](${wrapAssign(fn)})"
            }
            val wrapWithAsNodeByName: Tree => Tree = (arg: Tree) => {
              q"_root_.optimus.platform.asNode.apply0(${wrapAssign(arg)})"
            }

            // Force param info to exist.
            mforeach(paramss)(_.info)

            val alwaysAutoAsNode = f.symbol.hasAnnotation(AlwaysAutoAsNodeAnnotation)

            // First pass to look for any async closures in non-nodeLifted position:
            lazy val nonLiftedBodies: List[Tree] =
              if (alwaysAutoAsNode) Nil
              else {
                val flat = paramss.flatten.zip(argss_x.flatten)
                flat.collect {
                  case (param, Closure(_, body, _))
                      if hasAsync(body.id) &&
                        (param.isByNameParam || definitions.isFunctionType(param.tpe)) &&
                        !hasNodeLiftAnno(param) && !param.hasAnnotation(ClosuresEnterGraph) =>
                    body
                }
              }

            if ((!alwaysAutoAsNode && nonLiftedBodies.isEmpty) || isClosuresEnterGraph(f.symbol))
              bailout // No async closures.
            else {
              val origSymbol = f_x.symbol

              // Quickly exclude candidates that don't meet basic criteria.
              def quickMemberCheck(origSymbol: Symbol, candidateSymbol: Symbol): Boolean = {
                (candidateSymbol.isMethod && candidateSymbol != origSymbol) &&
                // Must have matching name
                (candidateSymbol.name == origSymbol.name || candidateSymbol.name.toString == origSymbol.name.toString + "$NF") &&
                // Same number of type params
                sameLength(origSymbol.tpe.typeParams, candidateSymbol.tpe.typeParams) &&
                // Same shape of method parameters
                (candidateSymbol.tpe.paramss corresponds origSymbol.tpe.paramss)(sameLength)
              }

              // Check for equivalence of parameter (argument) types.
              def fullParamCheck(candParam: Symbol, origParam: Symbol): Boolean = {
                // Case match here seems to make IntelliJ very unhappy.

                // Exact type match, but excluding cases where only one is nodelifted.
                if (
                  (candParam.tpe =:= origParam.tpe) &&
                  !(hasNodeLiftAnno(candParam) ^ hasNodeLiftAnno(origParam))
                )
                  true

                // Original's parameter is byName, and candidate's parameter is NodeFunction0
                else if (
                  definitions.isByNameParamType(origParam.tpe) &&
                  (candParam.tpe.typeSymbol == NodeFunctionTrait(0) ||
                    candParam.tpe.typeSymbol == AsyncFunctionTrait(0))
                )
                  origParam.tpe.typeArgs.head =:= candParam.tpe.typeArgs.head

                // Original's parameter is as closure, and candidate's parameter is as NodeFunctionN of same arity.
                else if (definitions.isFunctionType(origParam.tpe) && origParam.tpe.typeSymbol.typeParams.size < 10) {
                  val arity = origParam.tpe.typeSymbol.typeParams.size - 1
                  (candParam.tpe.typeConstructor.typeSymbol == NodeFunctionTrait(arity) ||
                    candParam.tpe.typeConstructor.typeSymbol == AsyncFunctionTrait(arity)) && {
                    (candParam.tpe.typeArgs corresponds origParam.tpe.typeArgs)(_ matches _)
                  }
                } else
                  false

              }

              // Find candidate for conversion.
              val newMethodOpt: Option[Symbol] = origSymbol.owner.info.decls.find { candSymbol: Symbol =>
                {
                  quickMemberCheck(origSymbol, candSymbol) && {
                    // Apply type params of the original to candidate, turning ensuring that
                    // the argument type parameters will match.  Note use of tpeHK, to strip of parameters of the
                    // parameters (e.g. F[_] --> F ).
                    val dummyTargs = origSymbol.tpe.typeParams.map(_.tpeHK)
                    val candType = appliedType(candSymbol.tpe.typeConstructor, dummyTargs)
                    // This isn't really necessary, but it's irksome to have one signature with
                    // parameters when the other's were applied away.
                    val origType = appliedType(origSymbol.tpe.typeConstructor, dummyTargs)
                    (candType.paramss corresponds origType.paramss)((c, o) => (c corresponds o)(fullParamCheck)) || {
                      alarm(
                        OptimusNonErrorMessages.FUNCTION_CONVERSION_CANDIDATE_REJECTED(
                          s"$origSymbol${origSymbol.typeSignature}",
                          s"$candSymbol${candSymbol.typeSignature}"),
                        tree.pos
                      )
                      false
                    }
                  }
                }
              }

              def oldMethType = f.symbol.toString + f.symbol.typeSignature.toString
              if (alwaysAutoAsNode && newMethodOpt.isEmpty)
                alarm(OptimusErrors.NO_ASNODE_VARIANT_FOUND(oldMethType), tree.pos)

              // Construct new method call.
              val newTreeOpt = newMethodOpt.flatMap { newMeth =>
                val newArgss = map2Conserve(argss_x, paramss) {
                  map2Conserve(_, _) {
                    // Fast check for parameters that don't require wrapping
                    case (arg, param)
                        if (!definitions.isFunctionType(param.tpe) && !definitions.isByNameParamType(param.tpe))
                          || hasNodeLiftAnno(param) =>
                      arg
                    // Wrap closures
                    case (arg @ Closure(Some(vds), body, fn: Function), param)
                        if definitions.isFunctionType(param.tpe) && vds.size < NodeFunctionTrait.size =>
                      wrapWithAsNode(fn)
                    // Wrap byNames
                    case (arg @ Closure(None, body, _), param) if definitions.isByNameParamType(param.tpe) =>
                      wrapWithAsNodeByName(body)
                    // This can only happen if we ran out of NodeFunctions
                    case (arg, _) =>
                      arg
                  }
                }

                val newFun = Select(qual_x, newMeth)

                // Try to construct and type the new function applied to the new arguments.
                // If this fails, the compiler cannot recover, so we generate an error.
                val newTree = q"$newFun[..$ts](...$newArgss)"
                def newMethType = newMeth.toString + newMeth.typeSignature.toString

                scala.util.Try {
                  localTyper.typedPos(tree.pos)(newTree)
                } match {
                  case Failure(e) =>
                    alarm(OptimusErrors.FAILED_CONVERSION(oldMethType, newMethType, e), tree.pos)
                    None
                  case Success(newApply) =>
                    alarm(OptimusNonErrorMessages.FUNCTION_CONVERSION(oldMethType, newMethType), tree.pos)
                    Some(newApply)
                }
              }

              newTreeOpt.getOrElse {
                // If we didn't find a suitable method, generate a warning for each offending closure
                nonLiftedBodies.foreach { body =>
                  tree match {
                    case treeInfo.Applied(Select(qual, meth), _, _) =>
                      // Try hard to find something async with an actual range position
                      val asyncBodyWithRange = body.find(t => isAsync(t) && t.pos.isRange)
                      val asyncBody = asyncBodyWithRange orElse body.find(t => isAsync(t))
                      val pos = asyncBody.fold(body.pos)(_.pos)
                      // Try to locate an actual definition of method to which the closure is passed
                      val found = qual.tpe.findMember(meth, Flags.DEFERRED, 0, false).owner
                      val fname =
                        if (found == NoSymbol)
                          s"${qual.tpe.dealias}.$meth"
                        else
                          s"${found.name}.$meth"
                      parentFlags.hasSyncStack = true
                      // If the closure follows a call to `.iterator` then hard error
                      val callChainHasIterator = Iterator
                        .iterate(qual) {
                          case Select(qual, _) => qual
                          case t =>
                            val core = treeInfo.dissectCore(t)
                            if (core eq t) null else core
                        }
                        .takeWhile(_ != null)
                        .exists(t => t.symbol != null && t.symbol.name == nmeIterator)
                      val upgradeToError =
                        callChainHasIterator && found.isNonBottomSubClass(tIterator)
                      if (upgradeToError)
                        alarm(OptimusNonErrorMessages.ASYNC_CLOSURE_ERROR(fname), pos)
                      else
                        alarm(OptimusNonErrorMessages.ASYNC_CLOSURE(fname), pos)
                    case _ =>
                  }
                }
                bailout
              }
            }
          }
        }

        case b @ Block(stats, expr) if b.hasAttachment[analyzer.NamedApplyInfo] =>
          // Look for artifact symbols used exactly once:
          val artifactCounts = mutable.HashMap.empty[Symbol, Int]
          stats.foreach {
            case vd: ValDef if vd.symbol.isArtifact && vd.rhs.isInstanceOf[Function] =>
              artifactCounts += vd.symbol -> 0
            case _ =>
          }
          b.foreach {
            case i: Ident =>
              artifactCounts.get(i.symbol).foreach { count =>
                artifactCounts.put(i.symbol, count + 1)
              }
            case _ =>
          }

          // Iterate through stats, transforming accumulating async artifacts, which will then
          // be used in transforms of subsequent artifacts
          val newAsyncArtifacts = mutable.HashSet.empty[Symbol]
          val statsx = stats.mapConserve { s =>
            val sx = inContext()(transform(s))
            sx match {
              case vd @ ValDef(_, _, _, rhs) =>
                if (
                  hasAsync(rhs.id) &&
                  rhs.isInstanceOf[Function] &&
                  artifactCounts.getOrElse(vd.symbol, 0) == 1
                ) {
                  newAsyncArtifacts += vd.symbol
                  asyncArtifacts += vd.symbol -> vd
                }
              case _ =>
            }
            sx
          }

          val exprx = inContext()(transform(expr))
          val statsFiltered = statsx.filterNot(stat => asyncArtifactsToRemove(stat.symbol))
          val ret = treeCopy.Block(tree, statsFiltered, exprx)
          if (asyncTree(exprx) || statsFiltered.exists(asyncTree))
            markAsync(ret)
          asyncArtifactsToRemove --= newAsyncArtifacts
          asyncArtifacts --= newAsyncArtifacts
          ret

        case _ =>
          inContext()(super.transform(tree))
      }

    }
  }

  private class CombinatorOverload(
      val method: Symbol,
      val buildFromMethod: TermName,
      tupled: Boolean = false,
      val upcast: Boolean = false) {
    def typeArg(combinatorArgs: List[Tree]): Tree = (combinatorArgs: @unchecked) match {
      case k :: v :: Nil if tupled =>
        TypeTree(definitions.TupleClass.specificType(k.tpe :: v.tpe :: Nil))
      case t :: Nil =>
        t
    }
  }
  private object CombinatorOverload {
    def decl(owner: Symbol, name: TermName) = {
      val sym = owner.info.decl(name)
      assert(sym != NoSymbol, (owner, name))
      assert(!sym.isOverloaded, (owner, sym.alternatives))
      sym
    }
    def member(owner: Symbol, name: TermName) = {
      val sym = owner.info.member(name)
      assert(sym != NoSymbol, (owner, name))
      assert(!sym.isOverloaded, (owner, sym.alternatives))
      sym
    }
    class MapAndFlatMap(owner: Symbol, buildFromMethod: TermName, tupled: Boolean = false, upcast: Boolean = false) {
      assert(owner != NoSymbol, buildFromMethod)
      val Map = new CombinatorOverload(decl(owner, nme.map), buildFromMethod, tupled, upcast)
      val FlatMap = new CombinatorOverload(decl(owner, nme.flatMap), buildFromMethod, tupled, upcast)
      val all = Map :: FlatMap :: Nil
    }
    lazy val SortedMap = new MapAndFlatMap(SortedMapLikeClass, TermName("buildFromSortedMapOps"), tupled = true)
    lazy val Map = new MapAndFlatMap(MapLikeClass, TermName("buildFromMapOps"), tupled = true)

    lazy val BitSet = new MapAndFlatMap(BitSetLikeClass, TermName("buildFromBitSetOps"))
    lazy val SortedSet = new MapAndFlatMap(SortedSetLikeClass, TermName("buildFromSortedSetOps"))

    lazy val IterableOps =
      new MapAndFlatMap(IterableLikeClass, TermName("buildFromIterableOps"), upcast = true)
    lazy val all = List(SortedMap, Map, BitSet, SortedSet, IterableOps).flatMap(_.all)
    def buildFromFor(
        combinator: Symbol,
        typeArgs: List[Tree],
        implicitArgs: List[Tree]): Option[(Tree, Tree, Boolean, Symbol)] = {
      all.collectFirst {
        case overload
            if combinator.overriddenSymbol(
              overload.method.owner) == overload.method && combinator.paramss.flatten.size == overload.method.paramss.flatten.size =>
          val fun = gen.mkAttributedSelect(
            gen.mkAttributedStableRef(CanBuildFromClass.companionModule),
            member(CanBuildFromClass.companionModule.moduleClass, overload.buildFromMethod))
          val buildFromTree = if (implicitArgs.nonEmpty) Apply(fun, implicitArgs) else fun
          (buildFromTree, overload.typeArg(typeArgs), overload.upcast, overload.method)
      }
    }
  }
}
