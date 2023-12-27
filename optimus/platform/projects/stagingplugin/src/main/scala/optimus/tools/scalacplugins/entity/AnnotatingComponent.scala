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

import scala.tools.nsc._
import scala.tools.nsc.plugins._

object AnnotatingComponent {
  val lazyReason =
    "Lazy collections may cause code to get executed outside of the optimus scope that is expected: http://codetree-docs/optimus/docs/QualityAssurance/ReviewRules.html#Lazy-Collections"
  val streamFunctions: Seq[String] = Seq("cons", "empty", "iterate", "from", "continually", "fill", "tabulate", "range")
}

/**
 * Adds annotations to pre-existing (i.e. library) symbols. Currently only supports @deprecated but could easily be
 * modified to add other annotations.
 */
class AnnotatingComponent(
    val pluginData: PluginData,
    val global: Global,
    val phaseInfo: OptimusPhaseInfo
) extends PluginComponent
    with WithOptimusPhase {
  import global._

  /**
   * lookup class / object or term member. For `x.y.z`, returns all type and term symbols `z` located either in class or
   * module `x.y`.
   *
   * If `z` ends with `$`, only returns terms. If `z` ends in `#`, only return types.
   */
  private def lookup(name: String): Seq[Symbol] = {
    val last = name.last
    val trm = last == '$' || last != '#'
    val tpe = last == '#' || last != '$'
    def owners(n: String) =
      Set(rootMirror.getModuleIfDefined(n), rootMirror.getModuleIfDefined(n), rootMirror.getClassIfDefined(n))
        .filter(_.exists)
    def members(o: Symbol, n: String) =
      ((if (tpe) o.info.member(TypeName(n)) else NoSymbol) ::
        (if (trm) o.info.member(TermName(n)) else NoSymbol).alternatives).filter(_.exists)
    val dot = name.lastIndexOf(".")
    val memberName = name.substring(dot + 1).stripSuffix("$").stripSuffix("#")
    owners(name.substring(0, dot)).flatMap(o => members(o, memberName)).toSeq
  }

  private def useInstead(alt: String, neu: Boolean = false) =
    s"${if (neu) "[NEW]" else ""}use $alt instead (deprecated by optimus staging)"

  private val view = "view"
  private val mapValuesReasonSuffix =
    ". Please import optimus.scalacompat.collection._ and use mapValuesNow / filterKeysNow instead."
  private val toStream = "toStream"
  private val streamPrefix = "scala.collection.immutable.Stream."
  private val lazyListPrefix = "scala.collection.immutable.LazyList."

  private val discourageds = Seq[(String, List[String])](
    // 2.12
    "scala.collection.TraversableLike.view" -> List(view, AnnotatingComponent.lazyReason),
    "scala.collection.IterableLike.view" -> List(view, AnnotatingComponent.lazyReason),
    "scala.collection.SeqLike.view" -> List(view, AnnotatingComponent.lazyReason),
    "scala.collection.GenTraversableOnce.toStream" -> List(toStream, AnnotatingComponent.lazyReason),
    "scala.collection.IterableLike.toStream" -> List(toStream, AnnotatingComponent.lazyReason),
    "scala.collection.TraversableLike.toStream" -> List(toStream, AnnotatingComponent.lazyReason),
    // 2.13
    "scala.collection.IterableOps.view" -> List(view, AnnotatingComponent.lazyReason),
    "scala.collection.SeqOps.view" -> List(view, AnnotatingComponent.lazyReason),
    "scala.collection.IndexedSeqOps.view" -> List(view, AnnotatingComponent.lazyReason),
    "scala.collection.MapOps.view" -> List(view, AnnotatingComponent.lazyReason)
  )

  private val discouragedStream =
    AnnotatingComponent.streamFunctions
      .flatMap(s =>
        List(
          streamPrefix + s -> List(s, AnnotatingComponent.lazyReason),
          lazyListPrefix + s -> List(s, AnnotatingComponent.lazyReason)))

  private val deprecations = Seq[(String, List[String])](
    // "org.something.Deprecated" -> useInstead("org.something.ToUseInstead")
  )

  private val deprecatings = Seq[(String, List[String])](
    "scala.Predef.StringFormat.formatted" -> List(
      useInstead("`formatString.format(value)` or the `f\"\"` string interpolator")
    ),
    "scala.collection.breakOut" -> List(
      """collection.breakOut does not exist on Scala 2.13.
        |For operations on standard library collections:
        |  - use `coll.iterator.map(f).toMap`
        |  - for less common target types, add `import scala.collection.compat._` and use `.to(SortedSet)`
        |  - for target types with 0 or 2 type parameters, also add `import optimus.scalacompat.collection._` and use `.convertTo(SortedMap)`
        | For operations on async collections (`apar` or `aseq`):
        |   - add `import optimus.scalacompat.collection._` and use `coll.apar.map(f)(TargetType.breakOut)`""".stripMargin
    ),
    "scala.Unit$" -> List("`Unit` value is not allowed in source; use `()` for the value of type `Unit`"),
    "scala.Predef.any2stringadd" -> List(
      "instead of `object + string`, use a string interpolation or `\"\" + object + string`."),
    "scala.collection.JavaConverters" -> List(useInstead("scala.jdk.CollectionConverters")),
    "scala.collection.JavaConversions" -> List(useInstead("scala.jdk.CollectionConverters")),
    "scala.collection.mutable.MutableList" -> List(useInstead("scala.collection.mutable.ListBuffer")),
    "org.mockito.MockitoEnhancer.withObjectMocked" -> List("This is incompatible with with Scala 2.13. Refer to documentation of optimus.utils.MockableObject for an alternative")
  ) ++ List(
    "MapLike",
    "GenMapLike",
    "SortedMapLike",
    "concurrent.TrieMap",
    "immutable.MapLike",
    "immutable.SortedMap",
    "mutable.LinkedHashMap",
    "parallel.ParMapLike")
    .flatMap(c => List(s"scala.collection.$c.mapValues", s"scala.collection.$c.filterKeys"))
    .map(_ -> List(AnnotatingComponent.lazyReason + mapValuesReasonSuffix))

  private lazy val deprecatedAnno = rootMirror.getRequiredClass("scala.deprecated")
  private lazy val deprecatingAnno = rootMirror.getClassIfDefined("optimus.platform.annotations.deprecating")
  private lazy val constAnno = rootMirror.getClassIfDefined("scala.annotation.ConstantAnnotation")
  private lazy val discouragedAnno = rootMirror.getClassIfDefined("optimus.platform.annotations.discouraged")

  private lazy val genTraversableFactory =
    rootMirror.getClassIfDefined("scala.collection.generic.GenTraversableFactory")

  def newPhase(prev: scala.tools.nsc.Phase): StdPhase = new StdPhase(prev) {
    private def add(targets: Seq[(String, List[String])], annot: Symbol, pred: Symbol => Boolean = _ => true): Unit =
      annot match {
        case annotClass: ClassSymbol =>
          targets.foreach { case (oldName, msg) =>
            lookup(oldName).foreach { oldSym =>
              if (pred(oldSym))
                addAnnotationInfo(oldSym, annotClass, msg)
            }
          }

        case _ => // annot class not found on classpath, do nothing
      }
    def apply(unit: global.CompilationUnit): Unit = {
      add(deprecations, deprecatedAnno)
      add(deprecatings, deprecatingAnno)
      add(discourageds, discouragedAnno)
      add(discouragedStream, discouragedAnno, _.owner != genTraversableFactory)
      pluginData.forceLoad.foreach(fqn => rootMirror.getClassIfDefined(fqn).andAlso(_.initialize))
    }
  }

  private def addAnnotationInfo(target: Symbol, annotCls: ClassSymbol, args: List[Any]): Unit =
    if (target.exists) {
      val isConst = constAnno.exists && annotCls.isNonBottomSubClass(constAnno)
      def annotArgs = args.iterator.map(a => Literal(Constant(a))).toList
      def annotAssocs = {
        def toConstArg(a: Any): ClassfileAnnotArg = a match {
          case a: Array[_] => ArrayAnnotArg(a.map(toConstArg))
          case c           => LiteralAnnotArg(Constant(c))
        }
        args
          .zip(annotCls.primaryConstructor.info.params)
          .iterator
          .map { case (arg, paramSym) =>
            (paramSym.name, toConstArg(arg))
          }
          .toList
      }

      val anno = AnnotationInfo(annotCls.tpe, if (isConst) Nil else annotArgs, if (isConst) annotAssocs else Nil)
      target.addAnnotation(anno)
    }
}
