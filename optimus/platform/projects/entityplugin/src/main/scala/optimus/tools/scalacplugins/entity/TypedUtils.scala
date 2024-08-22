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
import optimus.tools.scalacplugins.entity.reporter.OptimusAlarmBuilder1
import optimus.tools.scalacplugins.entity.reporter.OptimusPluginReporter
import optimus.tools.scalacplugins.entity.staged.scalaVersionRange

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.reflect.internal.util.TriState
import scala.tools.nsc.Global
import scala.tools.nsc.ast.TreeDSL
import scala.tools.nsc.symtab.Flags
import scala.tools.nsc.transform.Transform
import scala.tools.nsc.transform.TypingTransformers

trait SharedUtils extends TreeDSL { this: OptimusNames =>
  val global: Global

  import global._
  lazy val CollectionSeqClass = rootMirror.requiredClass[scala.collection.Seq[_]]

  lazy val EmbeddableAnnotation = rootMirror.getRequiredClass("optimus.platform.embeddable")
  lazy val StableAnnotation = rootMirror.getRequiredClass("optimus.platform.stable")
  lazy val NotPartOfIdentityAnnotation = rootMirror.getRequiredClass("optimus.platform.notPartOfIdentity")

  lazy val BusinessEventClass = rootMirror.getClassIfDefined("optimus.platform.BusinessEvent")
  lazy val BusinessEventImplClass = rootMirror.getClassIfDefined("optimus.platform.BusinessEventImpl")
  lazy val EntityClass = rootMirror.getRequiredClass("optimus.platform.storable.Entity")

  lazy val EmbeddableCompanionBaseClass =
    rootMirror.getRequiredClass("optimus.platform.storable.EmbeddableCompanionBase")
  lazy val EmbeddableTraitCompanionBaseClass =
    rootMirror.getRequiredClass("optimus.platform.storable.EmbeddableTraitCompanionBase")

  lazy val Node: ClassSymbol = rootMirror.getRequiredClass("optimus.graph.Node")
  lazy val NodeGet: String = s"${Node.fullName}.get"
  lazy val AlreadyCompletedPropertyNode = rootMirror.getRequiredClass("optimus.graph.AlreadyCompletedPropertyNode")
  lazy val NodeDebugAnnotation = rootMirror.getRequiredClass("optimus.platform.nodeDebug")
  lazy val NodeConstructors = rootMirror.getRequiredClass("optimus.graph.NodeConstructors")
  lazy val NodeConstructors_newConstantPropertyNode =
    definitions.getMemberMethod(NodeConstructors.linkedClassOfClass, newTermName("newConstantPropertyNode"))

  private lazy val CoreSupport = rootMirror.getRequiredModule("optimus.core.CoreSupport")
  private lazy val CoreSupport_pluginShouldHaveReplacedThis =
    definitions.getMember(CoreSupport, newTermName("pluginShouldHaveReplacedThis"))
  lazy val PoisonedAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.poisoned")
  object PoisonedPlaceholder {
    // Create a call to a generic @poisoned function
    def shouldHaveReplacedThis: global.Apply =
      Apply(gen.mkAttributedRef(CoreSupport.thisType, CoreSupport_pluginShouldHaveReplacedThis), Nil)

    def unapply(tree: Tree): Option[Option[String]] = tree match {
      case Apply(fun, _) if fun.symbol.hasAnnotation(PoisonedAnnotation) =>
        Some(fun.symbol.getAnnotation(PoisonedAnnotation).get.stringArg(0))
      case _ => None
    }
  }

  lazy val ReferenceHolder = rootMirror.getClassIfDefined("optimus.platform.storable.ReferenceHolder")
  lazy val IterableOnceOpsClass = rootMirror.getClassIfDefined("scala.collection.IterableOnceOps")
  def isAtLeastScala2_13 = IterableOnceOpsClass != NoSymbol
  def isScala2_12 = !isAtLeastScala2_13

  def isEvent(symbol: Symbol) =
    (symbol.tpe.typeSymbol ne BusinessEventClass) &&
      (symbol.tpe.typeSymbol ne BusinessEventImplClass) &&
      (symbol.tpe.typeSymbol isNonBottomSubClass BusinessEventClass)

  def isBlockingProperty(prop: Symbol): Boolean = {
    def isBlockingPropertyType(tpe: Type): Boolean = {
      val seen = mutable.Set.empty[Type]

      def isBlockingEmbeddable(tsym: Symbol): Boolean = {
        // force the info to ensure annotations are populated
        tsym.info
        (tsym.hasAnnotation(EmbeddableAnnotation) && {
          val params = tsym.primaryConstructor.paramss.flatten
          params exists (t => containsBlockingType(t.tpe))
        }) ||
        (tsym.isTrait && {
          // TODO (OPTIMUS-10006):
          // maybe it works in this specific case but anyway knownDirectSubclasses is broken and won't be fixed
          val sub = tsym.knownDirectSubclasses
          sub exists isBlockingEmbeddable
        })
      }

      def containsBlockingType(tpe: Type): Boolean = {
        // custom traverser (instead of just using Type#exists) so that we can avoid traversing certain subtrees
        var isBlocking = false
        (new TypeTraverser {
          def traverse(tp: Type): Unit = {
            // we don't traverse into ReferenceHolder because they are lazy (non-blocking) by design, so we
            // don't need to check the Entity they are referring to
            if (!isBlocking && seen.add(tp) && !tp.typeSymbol.isNonBottomSubClass(ReferenceHolder)) {
              if ((tp <:< EntityClass.tpe) || (tp exists (t => isBlockingEmbeddable(t.typeSymbol))))
                isBlocking = true
              else {
                mapOver(tp)
              }
            }
          }
        })(tpe)
        isBlocking
      }

      containsBlockingType(tpe) || containsBlockingType(tpe.bounds.hi)
    }

    isBlockingPropertyType(prop.info.finalResultType)
  }

  final def toTriState(ob: Option[Boolean]): TriState =
    ob.fold(TriState.Unknown)(TriState.booleanToTriState)

  final def map3[A, B, C, R](xs1: List[A], xs2: List[B], xs3: List[C])(f: (A, B, C) => R): List[R] = {
    val lb = new ListBuffer[R]
    var ys1 = xs1
    var ys2 = xs2
    var ys3 = xs3
    while (ys1.nonEmpty && ys2.nonEmpty && ys3.nonEmpty) {
      lb += f(ys1.head, ys2.head, ys3.head)
      ys1 = ys1.tail
      ys2 = ys2.tail
      ys3 = ys3.tail
    }
    lb.toList
  }

}

//noinspection TypeAnnotation
trait TypedUtils extends SharedUtils with PluginUtils with AsyncUtils with OptimusNames {
  import CODE._
  import global._
  import definitions._

  def entitySettings: EntitySettings

  trait PropertyInfoWrapping {
    val propertyPrefix: String
    val onModule: Boolean
  }

  case class InfoHolderModule(module: Symbol, propertyPrefix: String, onModule: Boolean) extends PropertyInfoWrapping

  case class InnerEntity(sym: Symbol, prefix: String)
  case class InnerEntities(syms: List[InnerEntity])

  /*
    To be used in conjuction with optimus.plugin.CaptureCode.
    The extractor
       CaptureCode(expr)
    will match code looking like and extract someCode
       optimus.plugin.CaptureCode.apply("string_literal_matching_current_phase", someCode)

    You would typically then call the apply method
       CaptureCode(transform(expr),localTyper)
    to produce the tuple of ("string representation of tranformed expr", transformedExpr)

   */
  object CaptureCode {
    lazy val CaptureCodeFunc =
      definitions.getMember(rootMirror.getRequiredModule("optimus.plugin.CaptureCode"), nme.apply)
    def unapply(tree: Tree) = tree match {
      case Apply(Apply(func, Literal(Constant(phase)) :: Nil), expr :: Nil)
          if func.symbol == CaptureCodeFunc && phase == globalPhase.toString =>
        Some(expr)
      case _ => None
    }

    def apply(trans: Tree, typer: analyzer.Typer) = {
      val lit = LIT.typed(trans.toString)
      val args = lit :: trans :: Nil
      typer.typedPos(trans.pos)(gen.mkTuple(args))
    }
  }

  object Closure {
    //                                valdefs if found,    body  function
    def unapply(tree: Tree): Some[(Option[List[ValDef]], Tree, Tree)] = tree match {
      case f @ Function(vds, body)                 => Some((Some(vds), body, f))
      case Block(Nil, Closure(Some(vds), body, f)) => Some((Some(vds), body, f))
      case _                                       => Some(None, tree, tree)
    }
  }

  // Check that a closure is expected and that the argument is itself a closure
  def isClosureParamAndArg(param: Symbol, tree: Tree): Boolean =
    definitions.isFunctionType(param.tpe) && isClosure(tree)

  def isClosure(tree: Tree): Boolean = tree match {
    case Closure(Some(_), _, _) => true // really, truly has args list
    case _                      => false
  }

  lazy val NodifyCall = definitions.getMember(rootMirror.getRequiredModule("optimus.core.CoreAPI"), names.nodify)
  def NodifySelect = gen.mkAttributedRef(NodifyCall)
  lazy val NodifyPropertyCall =
    definitions.getMember(rootMirror.getRequiredModule("optimus.core.CoreAPI"), names.nodifyProperty)
  def NodifyPropertySelect = gen.mkAttributedRef(NodifyPropertyCall)

  lazy val KnowableClass = rootMirror.getRequiredClass("optimus.platform.cm.Knowable")
  lazy val OwnershipMetadata = rootMirror.getRequiredClass("optimus.platform.OwnershipMetadata")
  lazy val DalMetadata = rootMirror.getRequiredClass("optimus.platform.DalMetadata")
  lazy val OptOut = rootMirror.getRequiredClass("optimus.platform.OptOut")
  // User code annotations
  lazy val EntityAnnotation = rootMirror.getRequiredClass("optimus.platform.entity")
  lazy val EventAnnotation = rootMirror.getRequiredClass("optimus.platform.event")
  lazy val KeyAnnotation = rootMirror.getRequiredClass("optimus.platform.key")
  lazy val IndexedAnnotation = rootMirror.getRequiredClass("optimus.platform.indexed")
  lazy val NodeAnnotation = rootMirror.getRequiredClass("optimus.platform.node")
  lazy val AsyncAnnotation = rootMirror.getRequiredClass("optimus.platform.async")
  lazy val JobAnnotation = rootMirror.getRequiredClass("optimus.platform.job")
  lazy val RecursiveAnnotation = rootMirror.getRequiredClass("optimus.platform.recursive")
  lazy val AsyncAlwaysUniqueAnnotation = rootMirror.getRequiredClass("optimus.platform.asyncAlwaysUnique")
  lazy val ElevatedAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.elevated")
  lazy val ScenarioIndependentRhsAnnotation = rootMirror.getRequiredClass("optimus.platform.siRhs")
  lazy val GivenRuntimeEnvAnnotation = rootMirror.getRequiredClass("optimus.platform." + tpnames.givenRuntimeEnv)
  lazy val AlwaysAutoAsNodeAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.alwaysAutoAsyncArgs")
  lazy val MiscFlagsAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.miscFlags")

  lazy val StoredAnnotation = rootMirror.getRequiredClass("optimus.platform.stored")
  lazy val BackedAnnotation = rootMirror.getRequiredClass("optimus.platform.backed")

  lazy val ScenarioIndependentAnnotation = rootMirror.getRequiredClass("optimus.platform.scenarioIndependent")
  lazy val ScenarioIndependentTransparentAnnotation =
    rootMirror.getRequiredClass("optimus.platform.annotations.scenarioIndependentTransparent")
  lazy val EntersGraphAnnotation = rootMirror.getRequiredClass("optimus.platform.entersGraph")
  lazy val AsyncOffAnnotation = rootMirror.getRequiredClass("optimus.platform.asyncOff")
  lazy val LoomAnnotation = rootMirror.getRequiredClass("optimus.platform.loom")
  lazy val LoomOffAnnotation = rootMirror.getRequiredClass("optimus.platform.loomOff")
  lazy val ImpureAnnotation = rootMirror.getRequiredClass("optimus.platform.impure")
  lazy val AspirationallyImpureAnnotation = rootMirror.getRequiredClass("optimus.platform.aspirationallyimpure")
  lazy val UpcastingTargetAnnotation = rootMirror.getRequiredClass("optimus.platform.upcastingTarget")
  lazy val ExportedAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.exported")
  lazy val BackingStoreAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.backingStore")
  lazy val ByInstanceOnlyAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.byInstanceOnly")

  // Internal annotations
  lazy val ReifiedAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.internal._reified")
  lazy val ProjectedAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.internal._projected")
  lazy val FullTextSearchAnnotation =
    rootMirror.getRequiredClass("optimus.platform.annotations.internal._fullTextSearch")
  lazy val TweakableAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.tweakable")
  lazy val NonTweakableAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.nonTweakable")
  lazy val ScenarioIndependentInternalAnnotation =
    rootMirror.getRequiredClass("optimus.platform.annotations.scenarioIndependentInternal")
  lazy val OnlyTweakableArgAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.onlyTweakableArg")
  lazy val DefNodeCreatorAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.defNodeCreator")
  lazy val DefAsyncCreatorAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.defAsyncCreator")
  lazy val DefJobCreatorAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.defJobCreator")
  lazy val ValNodeCreatorAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.valNodeCreator")
  lazy val NodeLiftAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.nodeLift")
  lazy val NodeLiftQueuedAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.nodeLiftQueued")
  lazy val PropertyNodeLiftAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.propertyNodeLift")
  lazy val WithNodeClassIDAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.withNodeClassID")
  lazy val CaptureByValueAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.captureByValue")
  lazy val NodeLiftByNameAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.nodeLiftByName")
  lazy val NodeLiftByValueAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.nodeLiftByValue")
  lazy val handleAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.handle")
  lazy val ExpectingTweaksAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.expectingTweaks")
  lazy val TweakOperatorAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.tweakOperator")
  lazy val AutoGenCreationAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.autoGenCreation")
  lazy val C2PAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.c2p")
  lazy val MetaAnnotation = rootMirror.getRequiredClass("optimus.platform.meta")
  lazy val EntityMetaDataAnnotation =
    rootMirror.getRequiredClass("optimus.platform.annotations.internal.EntityMetaDataAnnotation")
  lazy val AccelerateInfoAnnotation =
    rootMirror.getRequiredClass("optimus.platform.annotations.internal.AccelerateInfoAnnotation")
  lazy val SuppressAutoAsyncAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.suppressAutoAsync")
  lazy val AssumeParallelizableInClosures =
    rootMirror.getRequiredClass("optimus.platform.annotations.assumeParallelizableInClosure")
  lazy val ClosuresEnterGraph =
    rootMirror.getRequiredClass("optimus.platform.annotations.closuresEnterGraph")
  lazy val ForceNodeClassAnnotations = List(ElevatedAnnotation, ExportedAnnotation, JobAnnotation)
  lazy val WithLocationTagAnnotation = rootMirror.getRequiredClass("optimus.platform.annotations.withLocationTag")

  // methods to treat as if they have been marked @closuresEnterGraph (i.e. ignore sync stacks in lambdas)
  private lazy val TreatAsClosuresEnterGraph =
    Set("require", "assert").flatMap(m =>
      definitions.getMemberMethod(definitions.PredefModule, TermName(m)).alternatives)

  def isClosuresEnterGraph(sym: Symbol): Boolean =
    sym.hasAnnotation(ClosuresEnterGraph) || TreatAsClosuresEnterGraph(sym) ||
      (ScalaTestPackage.exists && sym.hasTransOwner(ScalaTestPackage))

  final def hasTweakableOption(sym: Symbol): Option[Boolean] = hasTweakableOption(sym.getAnnotation(NodeAnnotation))
  final def hasTweakableOption(anno: Option[AnnotationInfo]): Option[Boolean] = anno.flatMap(extractTweakableArg)
  final private def extractTweakableArg(anno: AnnotationInfo): Option[Boolean] = extractAnnoJavaArg(anno, names.tweak)

  final def hasExposeArgTypes(sym: Symbol): Boolean = hasExposeArgTypesOnNode(sym) || hasExposeArgTypesOnAsync(sym)
  final def hasExposeArgTypesOnNode(sym: Symbol): Boolean = hasExposeArgTypes(sym, NodeAnnotation)
  final def hasExposeArgTypesOnAsync(sym: Symbol): Boolean = hasExposeArgTypes(sym, AsyncAnnotation)
  final private def hasExposeArgTypes(sym: Symbol, anno: ClassSymbol): Boolean =
    sym.getAnnotation(anno).flatMap(extractArgTypes).getOrElse(false)
  final private def extractArgTypes(anno: AnnotationInfo): Option[Boolean] =
    extractAnnoJavaArg(anno, names.exposeWithArgTypes)

  final def hasUniqueOption(anno: Option[AnnotationInfo]): Option[Boolean] =
    anno.flatMap(extractAnnoJavaArg(_, names.unique))

  final def hasQueryableOption(anno: Option[AnnotationInfo]): Option[Boolean] =
    anno.flatMap(extractAnnoJavaArg(_, names.queryable))
  final def hasQueryableByRefOption(anno: Option[AnnotationInfo]): Option[Boolean] =
    anno.flatMap(extractAnnoJavaArg(_, names.queryByEref))

  final private def extractAnnoJavaArg(anno: Annotation, arg: TermName): Option[Boolean] = {
    anno.javaArgs.get(arg).collect { case LiteralAnnotArg(const) => const.booleanValue }
  }

  final def getLoomNodesArg(anno: AnnotationInfo): Option[Array[ClassfileAnnotArg]] =
    anno.javaArgs.get(tpnames.loomNodes).map(_.asInstanceOf[Array[ClassfileAnnotArg]])
  final def getLoomLambdasArg(anno: AnnotationInfo): Option[Array[ClassfileAnnotArg]] =
    anno.javaArgs.get(tpnames.loomLambdas).map(_.asInstanceOf[Array[ClassfileAnnotArg]])

  // NB These may not be on classpath, so they will resolve to NoSymbol.
  lazy val JunitTestAnnotation = rootMirror.getClassIfDefined("org.junit.Test")
  lazy val JunitBeforeAnnotation = rootMirror.getClassIfDefined("org.junit.Before")
  lazy val JunitAfterAnnotation = rootMirror.getClassIfDefined("org.junit.After")
  lazy val JunitBeforeClassAnnotation = rootMirror.getClassIfDefined("org.junit.BeforeClass")
  lazy val JunitAfterClassAnnotation = rootMirror.getClassIfDefined("org.junit.AfterClass")
  lazy val hasJunitAnnotations = JunitTestAnnotation.exists

  lazy val ScalaTestSuiteClass = rootMirror.getClassIfDefined("org.scalatest.TestSuite")
  lazy val ScalaTestPackage = rootMirror.getPackageIfDefined("org.scalatest").map(_.moduleClass)

  lazy val xFuncAnnotation = rootMirror.getClassIfDefined("optimus.export.xFunc")
  lazy val hasXFuncAnnotation = xFuncAnnotation.exists

  lazy val SoapObjectAnnotation = rootMirror.getClassIfDefined("optimus.stratstudio.soap.Annotations.soapobject")
  lazy val SoapMethodAnnotation = rootMirror.getClassIfDefined("optimus.stratstudio.soap.Annotations.soapmethod")

  lazy val EntityImplClass = rootMirror.getRequiredClass("optimus.platform.storable.EntityImpl")
  lazy val InlineEntityClass = rootMirror.getRequiredClass("optimus.platform.storable.InlineEntity")
  lazy val EntityReference = rootMirror.getRequiredClass("optimus.platform.storable.EntityReference")

  lazy val StorableInfo = rootMirror.getRequiredClass("optimus.entity.StorableInfo")
  lazy val EntityInfo = rootMirror.getRequiredClass("optimus.entity.EntityInfo")
  lazy val EventInfoCls = rootMirror.getRequiredClass("optimus.platform.storable.EventInfo")
  lazy val ClassEntityInfoCls = rootMirror.getRequiredClass("optimus.entity.ClassEntityInfo")
  lazy val ModuleEntityInfoCls = rootMirror.getRequiredClass("optimus.entity.ModuleEntityInfo")
  lazy val StorableClass = rootMirror.getRequiredClass("optimus.platform.storable.Storable")
  lazy val KeyedEntityCompanionBaseCls =
    rootMirror.getClassIfDefined("optimus.platform.storable.KeyedEntityCompanionBase")

  lazy val IndexInfo = rootMirror.getRequiredClass("optimus.entity.IndexInfo")

  lazy val OGSchedulerContext = rootMirror.getRequiredClass("optimus.graph.OGSchedulerContext")

  // Node[T] and its members
  lazy val NodeKey = rootMirror.getRequiredClass("optimus.graph.NodeKey")
  lazy val NodeSync = rootMirror.getRequiredClass("optimus.graph.profiled.NodeSync")
  lazy val NodeSyncWithExecInfo = rootMirror.getRequiredClass("optimus.graph.profiled.NodeSyncWithExecInfo")
  lazy val NodeSyncAlwaysUnique = rootMirror.getRequiredClass("optimus.graph.profiled.NodeSyncAlwaysUnique")
  lazy val NodeFSM = rootMirror.getRequiredClass("optimus.graph.profiled.NodeFSM")
  lazy val NodeFSMWithExecInfo = rootMirror.getRequiredClass("optimus.graph.profiled.NodeFSMWithExecInfo")
  lazy val NodeFSMAlwaysUnique = rootMirror.getRequiredClass("optimus.graph.profiled.NodeFSMAlwaysUnique")
  lazy val NodeFSMStoredClosure = rootMirror.getRequiredClass("optimus.graph.profiled.NodeFSMStoredClosure")
  lazy val NodeDelegate = rootMirror.getRequiredClass("optimus.graph.profiled.NodeDelegate")
  lazy val NodeDelegateWithExecInfo = rootMirror.getRequiredClass("optimus.graph.profiled.NodeDelegateWithExecInfo")
  lazy val NodeDelegateAlwaysUnique = rootMirror.getRequiredClass("optimus.graph.profiled.NodeDelegateAlwaysUnique")
  lazy val NodeDelegateStoredClosure = rootMirror.getRequiredClass("optimus.graph.profiled.NodeDelegateStoredClosure")
  lazy val CompletableNode = rootMirror.getRequiredClass("optimus.graph.CompletableNode")
  lazy val defGet = definitions.getMember(Node, nme.get)
  lazy val defGetJob = definitions.getMember(Node, names.getJob) // [JOB_EXPERIMENTAL]
  lazy val defEnqueue = definitions.getMember(Node, names.enqueue)
  lazy val defEnqueueJob = definitions.getMember(Node, names.enqueueJob) // [JOB_EXPERIMENTAL]
  lazy val AlreadyCompletedNode = rootMirror.getRequiredClass("optimus.graph.AlreadyCompletedNode")
  lazy val PropertyNode = rootMirror.getRequiredClass("optimus.graph.PropertyNode")
  lazy val PropertyNodeDelegate = rootMirror.getRequiredClass("optimus.graph.PropertyNodeDelegate")
  lazy val defLookupAndGet = definitions.getMember(PropertyNode, names.lookupAndGet)
  lazy val defLookupAndGetJob = definitions.getMember(PropertyNode, names.lookupAndGetJob) // [JOB_EXPERIMENTAL]
  lazy val defLookupAndGetSI = definitions.getMember(PropertyNode, names.lookupAndGetSI)
  lazy val defLookupAndEnqueue = definitions.getMember(PropertyNode, names.lookupAndEnqueue)
  lazy val defLookupAndEnqueueJob = definitions.getMember(PropertyNode, names.lookupAndEnqueueJob) // [JOB_EXPERIMENTAL]
  lazy val PropertyNodeSync = rootMirror.getRequiredClass("optimus.graph.PropertyNodeSync")
  lazy val PropertyNodeFSM = rootMirror.getRequiredClass("optimus.graph.PropertyNodeFSM")

  lazy val CovariantPropertyNode =
    definitions.getMember(PropertyNode.companionModule, TypeName("CovariantPropertyNode"))

  lazy val ScenarioStack = rootMirror.getRequiredClass("optimus.platform.ScenarioStack")
  lazy val TweakNode = rootMirror.getRequiredClass("optimus.graph.TweakNode")
  lazy val Tweak = rootMirror.getRequiredClass("optimus.platform.Tweak")

  lazy val AsyncCollection = rootMirror.getRequiredModule("optimus.platform.Async")
  lazy val AsyncBaseClass = rootMirror.getRequiredClass("optimus.platform.AsyncBase")
  lazy val AsyncBaseAutoAsyncClass = definitions.getMemberClass(AsyncBaseClass, TypeName("AutoAsync"))
  lazy val AsyncIterableMarkerClass = rootMirror.getRequiredClass("optimus.platform.AsyncIterableMarker")
  lazy val AsyncParClass = rootMirror.getRequiredClass("optimus.platform.asyncPar")
  lazy val AsyncParImpClass = rootMirror.getRequiredClass("optimus.platform.asyncParImp")
  lazy val OptAsyncClass = rootMirror.getRequiredClass("optimus.platform.OptAsync")
  lazy val AsyncLazyClass = rootMirror.getRequiredClass("optimus.platform.asyncLazyWithAnyRuntimeEnv.Lazy")

  def setOfSymbols(prefix: String, additionalSymbols: String*) = {
    val syms = HashSet.empty[Symbol]
    var i = 0
    do {
      val cls = rootMirror.getClassIfDefined(prefix + i)
      if (cls ne NoSymbol) { i += 1; syms += cls }
      else i = -1 // break
    } while (i >= 0)

    for (sym <- additionalSymbols) syms += rootMirror.getRequiredClass(sym)
    syms
  }

  lazy val NodeClsID = rootMirror.getRequiredClass("optimus.graph.NodeClsID")

  final val PropertyInfoName = "optimus.graph.PropertyInfo"
  lazy val PropertyInfo = rootMirror.getRequiredClass(PropertyInfoName)
  abstract class PropertyInfoClassBase(rootName: String, maxArity: Int) extends VarArityClassApi {
    private def isDefinedAt(i: Int) = i < seq.length && i >= 0
    val seq: collection.immutable.IndexedSeq[ClassSymbol] = (0 to maxArity).map { i =>
      rootMirror.getRequiredClass(rootName + i)
    }.toVector
    def apply(i: Int): Symbol = if (isDefinedAt(i)) seq(i) else NoSymbol
    def specificType(entityClass: Type, args: List[Type], resultType: Type): Type = {
      val arity = args.length
      if (!isDefinedAt(arity)) NoType
      else appliedType(apply(arity), entityClass :: args ::: resultType :: Nil)
    }
  }
  object PropertyInfoClass extends PropertyInfoClassBase(PropertyInfoName, 21)
  object TwkPropertyInfoClass extends PropertyInfoClassBase("optimus.graph.TwkPropertyInfo", 21)
  object C2PPropertyInfoClass extends PropertyInfoClassBase("optimus.graph.Child2ParentPropertyInfo", 0)
  object C2PTwkPropertyInfoClass extends PropertyInfoClassBase("optimus.graph.Child2ParentTwkPropertyInfo", 0)
  lazy val TweakablePropertyInfoN = setOfSymbols("optimus.graph.TwkPropertyInfo")
  lazy val PropertyInfoBase = rootMirror.getRequiredClass("optimus.graph.DefPropertyInfo")
  lazy val GenPropertyInfo = rootMirror.getRequiredClass("optimus.graph.GenPropertyInfo")
  lazy val ReallyNontweakablePropertyInfo = rootMirror.getRequiredClass("optimus.graph.ReallyNontweakablePropertyInfo")
  lazy val C2PReallyNontweakablePropertyInfo =
    rootMirror.getRequiredClass("optimus.graph.Child2ParentReallyNontweakablePropertyInfo")

  lazy val BaseIndexPropertyInfo = rootMirror.getRequiredClass("optimus.graph.BaseIndexPropertyInfo")
  lazy val IndexPropertyInfo = rootMirror.getRequiredClass("optimus.graph.IndexPropertyInfo")
  lazy val EventIndexPropertyInfo = rootMirror.getRequiredClass("optimus.graph.EventIndexPropertyInfo")
  lazy val UniqueIndexPropertyInfo = rootMirror.getRequiredClass("optimus.graph.UniqueIndexPropertyInfo")
  lazy val NonEntityJobPropertyInfo = rootMirror.getClassIfDefined("optimus.graph.NonEntityJobPropertyInfo")
  lazy val EventUniqueIndexPropertyInfo =
    rootMirror.getRequiredClass("optimus.graph.EventUniqueIndexPropertyInfo")
  lazy val IndexPropertyInfoForErefFilter =
    rootMirror.getRequiredClass("optimus.graph.IndexPropertyInfoForErefFilter")

  lazy val NodeTaskInfo = rootMirror.getRequiredClass("optimus.graph.NodeTaskInfo")
  lazy val AsyncNodeTaskInfo = rootMirror.getRequiredClass("optimus.graph.AsyncNodeTaskInfo")
  lazy val ElevatedPropertyInfo = rootMirror.getClassIfDefined("optimus.dist.elevated.ElevatedPropertyInfo")
  lazy val ensureNotCacheable = definitions.getMember(NodeTaskInfo, names.ensureNotCacheable).asInstanceOf[MethodSymbol]

  lazy val StorageInfo = rootMirror.getRequiredClass("optimus.platform.storable.StorageInfo")
  lazy val AppliedStorageInfo =
    definitions.getMember(rootMirror.getRequiredModule("optimus.platform.storable"), names.AppliedStorageInfo)
  lazy val DALEventInfo = rootMirror.getRequiredClass("optimus.platform.dal.DALEventInfo")
  lazy val BusinessEventReference = rootMirror.getRequiredClass("optimus.platform.storable.BusinessEventReference")

  lazy val PickledInputStream = rootMirror.getRequiredClass("optimus.platform.pickling.PickledInputStream")
  lazy val PickledOutputStream = rootMirror.getRequiredClass("optimus.platform.pickling.PickledOutputStream")
  lazy val TemporalContext = rootMirror.getRequiredClass("optimus.platform.TemporalContext")
  lazy val IncompatibleVersionException =
    rootMirror.getRequiredClass("optimus.platform.dal.IncompatibleVersionException")

  lazy val EvaluationContext = rootMirror.getRequiredModule("optimus.platform.EvaluationContext")
  lazy val CoreAPI = rootMirror.getRequiredClass("optimus.core.CoreAPI")

  lazy val DAL = rootMirror.getRequiredModule("optimus.platform.DAL")

  lazy val TweakTarget = rootMirror.getRequiredClass("optimus.graph.TweakTarget")
  lazy val TweakTargetColonEquals = definitions.getMember(TweakTarget, names.colonEquals)

  lazy val SeqOfTweaksType =
    TypeRef(NoType, rootMirror.requiredClass[collection.Seq[_]], List(TypeRef(NoType, Tweak, Nil)))
  lazy val ImmutableSetClass = rootMirror.getRequiredClass("scala.collection.immutable.Set")
  lazy val IterableClass = rootMirror.getRequiredClass("scala.collection.Iterable")
  lazy val IterableLikeClass =
    requiredClassForScalaVersion("scala.collection.IterableLike", "scala.collection.IterableOps")
  lazy val MapLikeClass =
    requiredClassForScalaVersion("scala.collection.MapLike", "scala.collection.MapOps")
  lazy val SortedMapLikeClass =
    requiredClassForScalaVersion("scala.collection.SortedMapLike", "scala.collection.SortedMapOps")
  lazy val SortedSetLikeClass =
    requiredClassForScalaVersion("scala.collection.SortedSetLike", "scala.collection.SortedSetOps")
  lazy val BitSetLikeClass =
    requiredClassForScalaVersion("scala.collection.BitSetLike", "scala.collection.BitSetOps")
  lazy val TraversableLikeClass =
    requiredClassForScalaVersion("scala.collection.TraversableLike", "scala.collection.IterableOps")
  lazy val CovariantSetClass = rootMirror.getRequiredClass("optimus.platform.CovariantSet")
  lazy val CanBuildFromClass =
    requiredClassForScalaVersion("scala.collection.generic.CanBuildFrom", "scala.collection.BuildFrom")
  lazy val IterableViewClass = requiredClassForScalaVersion("scala.collection.IterableView", "scala.collection.View")
  lazy val PartialFunctionClass = rootMirror.getRequiredClass("scala.PartialFunction")
  lazy val OrderingClass = rootMirror.getRequiredClass("scala.math.Ordering")
  lazy val ProgressReporter = rootMirror.getRequiredClass("optimus.interop.ProgressReporter")
  lazy val Enumeration = rootMirror.getRequiredClass("scala.Enumeration")
  lazy val Enumeration_ValueSet = definitions.getMember(Enumeration, TypeName("ValueSet"))
  lazy val InstantCls = rootMirror.getRequiredClass("java.time.Instant")
  lazy val GenMapLikeCls = requiredClassForScalaVersion("scala.collection.GenMapLike", "scala.collection.MapOps")
  lazy val GenMapLike_get = definitions.getMemberMethod(GenMapLikeCls, nme.get)
  lazy val GenTraversableOnceClass =
    requiredClassForScalaVersion("scala.collection.GenTraversableOnce", "scala.collection.IterableOnce")
  lazy val HasDefaultUnpickleableClass =
    rootMirror.getRequiredClass("optimus.platform.storable.HasDefaultUnpickleableValue")
  lazy val piiElement = rootMirror.getRequiredClass("optimus.datatype.PIIElement")
  lazy val dataSubjectCategory = rootMirror.getModuleByName("optimus.datatype.Classification$DataSubjectCategory")

  // mixed-in will be disabled for these annotations!
  private lazy val optimusGraphAnnos: Set[String] = Set(
    EmbeddableAnnotation,
    StableAnnotation,
    NotPartOfIdentityAnnotation,
    NodeDebugAnnotation,
    EntityAnnotation,
    EventAnnotation,
    KeyAnnotation,
    IndexedAnnotation,
    NodeAnnotation,
    AsyncAnnotation,
    JobAnnotation,
    RecursiveAnnotation,
    AsyncAlwaysUniqueAnnotation,
    ElevatedAnnotation,
    ScenarioIndependentRhsAnnotation,
    GivenRuntimeEnvAnnotation,
    MiscFlagsAnnotation,
    StoredAnnotation,
    BackedAnnotation,
    ScenarioIndependentAnnotation,
    ScenarioIndependentTransparentAnnotation,
    EntersGraphAnnotation,
    AsyncOffAnnotation,
    LoomAnnotation,
    LoomOffAnnotation,
    ImpureAnnotation,
    AspirationallyImpureAnnotation,
    UpcastingTargetAnnotation,
    ExportedAnnotation,
    BackingStoreAnnotation,
    ByInstanceOnlyAnnotation,
    ReifiedAnnotation,
    ProjectedAnnotation,
    FullTextSearchAnnotation,
    TweakableAnnotation,
    NonTweakableAnnotation,
    ScenarioIndependentInternalAnnotation,
    OnlyTweakableArgAnnotation,
    DefNodeCreatorAnnotation,
    DefAsyncCreatorAnnotation,
    DefJobCreatorAnnotation,
    ValNodeCreatorAnnotation,
    NodeLiftAnnotation,
    NodeLiftQueuedAnnotation,
    PropertyNodeLiftAnnotation,
    WithNodeClassIDAnnotation,
    CaptureByValueAnnotation,
    NodeLiftByNameAnnotation,
    NodeLiftByValueAnnotation,
    handleAnnotation,
    ExpectingTweaksAnnotation,
    TweakOperatorAnnotation,
    AutoGenCreationAnnotation,
    C2PAnnotation,
    MetaAnnotation,
    EntityMetaDataAnnotation,
    AccelerateInfoAnnotation,
    SuppressAutoAsyncAnnotation,
    WithLocationTagAnnotation
  ).map(_.fullNameString)

  def isOptimusGraphSpecific(anno: AnnotationInfo): Boolean =
    optimusGraphAnnos.contains(anno.symbol.fullNameString)

  def requiredClassForScalaVersion(scala212: String, scala213: String): ClassSymbol =
    if (scalaVersionRange("2.13:"): @staged) {
      rootMirror.getRequiredClass(scala213)
    } else {
      rootMirror.getRequiredClass(scala212)
    }

  lazy val RTExceptionTraitTpe = typeOf[RTExceptionTrait]
  lazy val RTExceptionInterfaceTpe = typeOf[RTExceptionInterface]

  def isUserDefinedEntity(p: Symbol) =
    (p != EntityClass) && (p != InlineEntityClass) && (p != EntityImplClass) && (p isSubClass EntityClass)
  def isEntity(symbol: Symbol) =
    (symbol.tpe.typeSymbol ne EntityClass) && (symbol.tpe.typeSymbol isSubClass EntityClass)
  def isStoredEntity(symbol: Symbol) = {
    isEntity(symbol) && symbol.hasAnnotation(StoredAnnotation)
  }
  def isEntityOrEvent(symbol: Symbol) = isEntity(symbol) || isEvent(symbol)
  def isEmbeddable(symbol: Symbol) = symbol.hasAnnotation(EmbeddableAnnotation)
  def isStable(symbol: Symbol) = symbol.hasAnnotation(StableAnnotation)
  def isEntityCompanion(symbol: Symbol) = {
    assert(!(NoSymbol isSubClass EntityClass), "NoSymbol should not be SubClass of Entity")
    symbol.companionClass isSubClass EntityClass
  }

  def isStorableCompanion(symbol: Symbol) = symbol.isModuleOrModuleClass && isEntityOrEvent(symbol.companionClass)

  def isKey(symbol: Symbol) = symbol.hasAnnotation(KeyAnnotation)
  def isIndex(symbol: Symbol) = symbol.hasAnnotation(IndexedAnnotation)
  def isKeyOrIndex(symbol: Symbol) = isKey(symbol) || isIndex(symbol)

  def isIndexInfo(symbol: Symbol) = symbol.tpe.typeSymbol isSubClass IndexInfo

  def isFullTextSearch(symbol: Symbol) = symbol.hasAnnotation(FullTextSearchAnnotation)

  def isEntityProjected(s: Symbol): Boolean = {
    s.getAnnotation(EntityMetaDataAnnotation).exists(a => getProjectedValue(a.original))
  }
  def isEntityFullTextSearchable(s: Symbol): Boolean = {
    s.getAnnotation(EntityMetaDataAnnotation).exists(a => getFullTextSearchValue(a.original))
  }
  def isEventProjected(s: Symbol): Boolean =
    s.getAnnotation(EventAnnotation).flatMap(extractAnnoJavaArg(_, names.projected)).getOrElse(false)

  def isEventContained(s: Symbol): Boolean =
    s.getAnnotation(EventAnnotation).flatMap(extractAnnoJavaArg(_, names.contained)).getOrElse(false)

  def isEmbeddableProjected(s: Symbol): Boolean =
    s.getAnnotation(EmbeddableAnnotation).flatMap(extractAnnoJavaArg(_, names.projected)).getOrElse(false)

  private def getProjectedValue(tree: Tree): Boolean = {
    tree match {
      case Apply(_, args) =>
        args.exists {
          case NamedArgTree(Ident(name: TermName), Literal(proj)) if names.projected == name => proj.booleanValue
          case _                                                                             => false
        }
      case Literal(proj) => proj.booleanValue
      case _             => false
    }
  }

  private def getFullTextSearchValue(tree: Tree): Boolean = {
    tree match {
      case Apply(_, args) =>
        args.exists {
          case NamedArgTree(Ident(name: TermName), Literal(doc)) if names.fullTextSearch == name => doc.booleanValue
          case _                                                                                 => false
        }
      case Literal(doc) => doc.booleanValue
      case _            => false
    }
  }

  lazy val PlatformPkgModule = rootMirror.getRequiredModule("optimus.platform")
  lazy val DALModule = rootMirror.getModuleIfDefined("optimus.platform.DAL")

  // Only for tweakNontweak error checking
  lazy val Value2TweakTarget = definitions.getMember(PlatformPkgModule, newTermName("value2TweakTarget"))

  lazy val LocationTagClass = rootMirror.getRequiredClass("optimus.platform.LocationTag")
  lazy val LocationTagPkg = rootMirror.getPackage("optimus.platform.LocationTag")
  lazy val WithDerivedTag = definitions.getMember(LocationTagClass, newTermName("withDerivedTag"))

  lazy val CopyUniqueCls =
    if (PlatformPkgModule.exists) definitions.getMemberIfDefined(PlatformPkgModule, TypeName("CopyUnique"))
    else NoSymbol
  lazy val DALModifyMod = if (DALModule.exists) definitions.getMember(DALModule, TermName("Modify")) else NoSymbol

  // use rather than isAccessor post-valaccessors
  def isValAccessor(sym: Symbol) = sym.hasAnnotation(ValAccessorAnnotation)
  def hasSIAnnotation(sym: Symbol) = sym.hasAnnotation(ScenarioIndependentAnnotation)
  // TODO (OPTIMUS-0000): use attachments rather than tag annos
  def isTweakable(tree: Tree) = tree.symbol.hasAnnotation(TweakableAnnotation)
  def isNonTweakable(tree: Tree) = tree.symbol.hasAnnotation(NonTweakableAnnotation)

  // hasSIInternalAnnotation => this function is marked SI
  // internalsMustBeSI => this function should only call SI functions
  // allowedInSIContext => this function can be called within SI contexts
  def hasSIInternalAnnotation(sym: Symbol) = sym.hasAnnotation(ScenarioIndependentInternalAnnotation)
  def internalsMustBeSI(sym: Symbol) =
    sym.hasAnnotation(ScenarioIndependentInternalAnnotation) || sym.hasAnnotation(ScenarioIndependentAnnotation)
  def allowedInSIContext(sym: Symbol) =
    sym.hasAnnotation(ScenarioIndependentAnnotation) ||
      sym.hasAnnotation(ScenarioIndependentInternalAnnotation) ||
      sym.hasAnnotation(ScenarioIndependentTransparentAnnotation)
  def hasNodeLiftByName(sym: Symbol) = sym.hasAnnotation(NodeLiftByNameAnnotation)
  def hasNodeLiftByValue(sym: Symbol) = sym.hasAnnotation(NodeLiftByValueAnnotation)
  def hasNodeLiftAnno(sym: Symbol) = sym.hasAnnotation(NodeLiftAnnotation)
  def hasNodeSyncLiftAnno(sym: Symbol) = sym.hasAnnotation(NodeSyncLiftAnnotation)
  def isNodeLifted(sym: Symbol) = hasNodeLiftByName(sym) || hasNodeLiftByValue(sym) || hasNodeLiftAnno(sym)
  def hasNodeAnnotation(tree: Tree) = tree.symbol.hasAnnotation(NodeAnnotation)
  def hasAsyncAnnotation(tree: Tree) = tree.symbol.hasAnnotation(AsyncAnnotation)
  def hasNodeSyncAnnotation(tree: Tree) = tree.symbol.hasAnnotation(NodeSyncAnnotation)
  def addLoomIfMissing(symbol: Symbol): Unit = if (curInfo.isLoom) addIfMissing(symbol, LoomAnnotation)
  def addIfMissing(sym: Symbol, annotation: ClassSymbol, assoc: List[(TermName, ClassfileAnnotArg)] = Nil): Unit =
    if (!sym.hasAnnotation(annotation)) sym.addAnnotation(AnnotationInfo(annotation.tpe, Nil, assoc))

  lazy val OptimusJavaAnnos: Set[Symbol] =
    Set(
      NodeAnnotation,
      NodeSyncAnnotation,
      AsyncAnnotation,
      ScenarioIndependentAnnotation,
      EmbeddableAnnotation,
      EntityAnnotation,
      EventAnnotation,
      IndexedAnnotation,
      StoredAnnotation
    )

  private var _curInfo: AsyncScopeInfo = _
  def curInfo: AsyncScopeInfo = {
    if (_curInfo eq null)
      _curInfo = new AsyncScopeInfo(canAsync = false, stopAsync = false, isLoom = entitySettings.loom)
    _curInfo
  }

  def atPropClass(canAsync: Boolean)(f: => Tree): Tree = atPropClass(canAsync, curInfo.stopAsync)(f)

  private def hasLoomOffAnnotation(tree: Tree): Boolean = {
    val sym = tree.symbol
    sym.hasAnnotation(LoomOffAnnotation) || (sym.isModule && sym.companionClass.hasAnnotation(LoomOffAnnotation))
  }

  def atAsyncOffScope(tree: Tree)(f: => Tree): Tree = {
    if (curInfo.isLoom && hasLoomOffAnnotation(tree))
      atPropClass(canAsync = false, stopAsync = false, isLoom = false)(f)
    else if (curInfo.isLoom || tree.symbol.hasAnnotation(LoomAnnotation))
      atPropClass(canAsync = false, stopAsync = true, isLoom = true)(f)
    else if (curInfo.stopAsync)
      atPropClass(canAsync = false, stopAsync = true)(f)
    else if (tree.symbol.hasAnnotation(AsyncOffAnnotation))
      atPropClass(canAsync = false, stopAsync = true)(f)
    else
      atPropClass(canAsync = false, stopAsync = false)(f)
  }

  def atPropClass(canAsync: Boolean, stopAsync: Boolean, isLoom: Boolean = false)(f: => Tree): Tree = {
    val oldInfo = curInfo
    _curInfo = new AsyncScopeInfo(canAsync = !stopAsync && canAsync, stopAsync = stopAsync, isLoom = isLoom)
    val result = f
    _curInfo = oldInfo
    result
  }

  private def isFunctionOrPF(sym: Symbol) =
    definitions.isFunctionSymbol(sym) || sym.isNonBottomSubClass(PartialFunctionClass)
  def isSITransparent(sym: Symbol): Boolean = {
    // we have marked optimus code like the 'async' collection wrapper with ScenarioIndependentTransparentAnnotation
    sym.hasAnnotation(ScenarioIndependentTransparentAnnotation) ||
    // until we have some kind of tool to inspect the bytecode and figure out which library functions are
    // SI transparent (i.e. execute any function arguments internally), let's just assume that methods in the Scala
    // collections that have Function/PartialFunction params _ARE_.
    (sym != NoSymbol && sym.owner != NoSymbol && sym.owner.fullName.startsWith("scala.collection") &&
      mexists(sym.info.paramss)(param =>
        isFunctionOrPF(definitions.dropByName(definitions.dropRepeated(param.info)).typeSymbol)))
  }

  /**
   * Is the symbol top-level (i.e. owned by a package class) or owned by a REPL wrapper? We treat entities in
   * class-based REPL wrappers as "effectively" top-level because from the user's perspective the wrapper is the top
   * level - they have no way to get at anything above that level.
   */
  def isEffectivelyTopLevel(sym: Symbol): Boolean = sym.isTopLevel || nme.isReplWrapperName(sym.owner.name)

  def miscFlags(sym: Symbol): Int = sym.getAnnotation(MiscFlagsAnnotation).flatMap(_.intArg(0)).getOrElse(0)
  def miscFlags(tree: Tree): Int = miscFlags(tree.symbol)
  def hasMiscFlag(sym: Symbol, flag: Int): Boolean = (miscFlags(sym) & flag) != 0
  def hasMiscFlag(tree: Tree, flag: Int): Boolean = tree.hasSymbolField && hasMiscFlag(tree.symbol, flag)
  def setMiscFlags(tree: Tree, flag: Int): Unit = {
    val flags = miscFlags(tree)
    tree.symbol.removeAnnotation(MiscFlagsAnnotation)
    tree.symbol.withAnnotation(
      AnnotationInfo(MiscFlagsAnnotation.tpe, Literal(Constant(flag | flags)) :: Nil, List.empty))
  }
  def setMiscFlags(sym: Symbol, flag: Int): Unit = {
    val flags = miscFlags(sym)
    sym.removeAnnotation(MiscFlagsAnnotation)
    sym.withAnnotation(AnnotationInfo(MiscFlagsAnnotation.tpe, Literal(Constant(flag | flags)) :: Nil, List.empty))
  }

  // def argsHash =  PluginHelpers.hashOf( PluginHelpers.hashOf(start, arg1), arg2 )....
  def mkArgsHashT(vals: List[ValOrDefDef], start: Tree): Tree =
    mkArgsHashImpl(
      vals map { v =>
        gen.mkAttributedRef(v.symbol)
      },
      start)
  def mkArgsHashImpl(vals: List[Tree], start: Tree = LIT(1)): Tree = {
    // If we have a lazy property, select the result out.  It is invariant that if we take this path we have an AlreadyCompletedPropertyNode.
    // Might be good to assert this to prevent regressions/violations.
    vals.foldLeft(start) { (body, arg) =>
      val newArg =
        if (arg.tpe.resultType.typeSymbol isSubClass Node) Apply(PluginSupport.safeResult, arg :: Nil) else arg
      Apply(PluginSupport.hashOf, List(body, newArg))
    }
  }

  def mkTypedApply(fun: Tree, argss: List[List[Tree]], targs: List[Type]): Tree = {
    // first apply the type parameters (if any)
    val GenPolyType(tparams, polyRestpe) = fun.tpe
    val monoTpe = polyRestpe.instantiateTypeParams(tparams, targs)
    val tapp = gen.mkTypeApply(fun, targs.map(TypeTree)).setType(monoTpe)

    // then apply the argument lists. each list requires that we wrap f with another Apply().
    argss.foldLeft(tapp) { (f, a) =>
      Apply(f, a).setType(f.tpe.resultType)
    }
  }

  /**
   * creates a synthetic nullary DefDef with the supplied name and rhs, and creates and adds its MethodSymbol to
   * classSym. note that rhs must have a correct type set already
   */
  def addNullarySynthMethod(name: TermName, classSym: ClassSymbol, pos: Position, flags: Long)(
      rhsGen: MethodSymbol => Tree) = {
    val methodSym = classSym.newMethod(name, pos).setFlag(flags)
    val rhs = rhsGen(methodSym)
    val methodDef = DefDef(Modifiers(flags | Flags.SYNTHETIC), name, Nil, Nil :: Nil, TypeTree(), rhs)
    methodSym setInfo NullaryMethodType(rhs.tpe)
    methodDef setSymbol methodSym
    classSym.info.decls enter methodSym
    methodDef
  }

  /**
   * Creates def getCldID and static var `__nclsID` in order to implement the NodeClsID interface. Adds both to the cls
   * symbol and returns the def Tree only (we don't actually need a tree for the static var)
   */
  def mkNodeClsIdGetter(cls: ClassSymbol): Tree = {
    val nclsIDsym = cls.newValue(newTermName("__nclsID"), NoPosition, Flags.STATIC | Flags.MUTABLE)
    nclsIDsym.setInfo(IntClass.tpe)
    cls.info.decls enter nclsIDsym

    val nclsIDdef =
      addNullarySynthMethod(newTermName("getClsID"), cls, NoPosition, Flags.PROTECTED | Flags.OVERRIDE) { _ =>
        gen.mkAttributedRef(nclsIDsym)
      }
    nclsIDdef
  }

  def resetOverrideIfNecessary(methodSym: Symbol): Unit = {
    if (methodSym.hasFlag(Flags.OVERRIDE | Flags.ABSOVERRIDE)) {
      if (!methodSym.isOverridingSymbol)
        methodSym.resetFlag(Flags.OVERRIDE | Flags.ABSOVERRIDE)
    }
  }

  private class ForAllTraverser(f: Tree => Boolean) extends Traverser {
    var failed = false
    override def traverse(tree: Tree): Unit = {
      if (!f(tree)) failed = true
      if (!failed) super.traverse(tree)
    }
  }

  /**
   * Returns true iff predicate f is true for all entries in the tree, else returns false. Short-circuits as soon as f
   * returns false.
   */
  def forAll(tree: Tree)(f: Tree => Boolean) = {
    val t = new ForAllTraverser(f)
    t.traverse(tree)
    !t.failed
  }

  // NB: "Logical" is punned here because TreeDSL.AND produces EmptyTree for AND(Nil: _*), which is clearly illogical.
  def mkLogicalAnd(trees: List[Tree]) = if (trees.isEmpty) Literal(Constant(true)) else trees.reduceLeft(gen.mkAnd)

  /**
   * Return a list of parameters corresponding to each arg in a function application, unwrapping any repeated params T*
   * to copies of T.
   *
   * In most cases, this returns the original params list.
   */
  def adjustForRepeatedParameters(params: List[Symbol], args: List[_]): List[Symbol] = {
    // This is nasty because we want to avoid doing too much work, but there is a whole bunch of conditions we need
    // to fulfill.
    @scala.annotation.tailrec
    def walk(p: List[Symbol], a: List[_], repeatedParam: Option[Symbol])(accum: List[Symbol]): Option[List[Symbol]] = {
      (p, a) match {
        // Done both.
        case (Nil, Nil) => repeatedParam.map(_ => accum)

        // There are more parameters than args. This is the case when the last parameter is a repeated parameter for
        // which no arg was given. If that's the case, we truncate the repeated parameter.
        case (_ :: ptail, Nil) =>
          if (ptail.isEmpty) Some(accum)
          else throw new NoSuchElementException("More parameters than corresponding args")

        // There are more args than parameters because the last parameter is a repeated one. We repeat that last
        // parameter.
        case (Nil, _ :: atail) =>
          val clone = repeatedParam.map(p => p.cloneSymbol(p.owner))
          walk(Nil, atail, repeatedParam)(
            clone.getOrElse(
              throw new TypeError(
                params.headOption.map(_.pos).getOrElse(NoPosition),
                "not a repeated parameter but args is longer than params")
            ) :: accum)

        // Otherwise we just keep iterating.
        case (phead :: ptail, _ :: atail) =>
          val repeated =
            Some(phead)
              .filter(definitions.isRepeated)
              .map(p => p.cloneSymbol(p.owner).modifyInfo(definitions.dropRepeated))
          walk(ptail, atail, repeated)(repeated.getOrElse(phead) :: accum)
      }
    }

    walk(params, args, None)(Nil) match {
      case None    => params
      case Some(p) => p.reverse
    }
  }

  @scala.annotation.tailrec
  final def foreachParamsAndArgs(params: List[Symbol], args: List[Tree])(f: (Symbol, Tree) => Unit): Unit = args match {
    case Nil =>
    case a :: as =>
      params match {
        case p :: Nil => f(p, a); foreachParamsAndArgs(params, as)(f) // repeated parameter as last element of params
        case p :: ps  => f(p, a); foreachParamsAndArgs(ps, as)(f)
        case Nil      => throw new IllegalStateException() // empty params, nonempty args?
      }
  }

  def checkOwnership(inputTree: Tree, callingOwner: Symbol = rootMirror.RootClass): Unit = {
    import scala.collection.mutable

    class ValidationTraverser extends Traverser {
      // the currentOwner is kept up-to-date as we traverse by super.atOwner
      currentOwner = callingOwner

      // tracking for symbol -> declaring tree - used only to make the error messages more useful
      var currentTree: Tree = _
      val symToTree = mutable.HashMap[Symbol, Tree]()

      var enclAppl: Tree = EmptyTree

      override def traverse(tree: Tree): Unit = {

        val prevTree = currentTree
        currentTree = tree

        tree match {

          case _: PackageDef =>
            super.traverse(tree)

          case d: DefTree =>
            val symbol = d.symbol
            val actualOwner = symbol.owner
            val expectedOwner = currentOwner
            if (!ownerIsValid(actualOwner, expectedOwner)) {
              val expectedOwnerTree = symToTree.getOrElse(currentOwner, "?")
              val actualOwnerTree = symToTree.getOrElse(actualOwner, "?")
              // Try to show 10 plausibly interesting lines of stack
              val trace = new InternalError("Wrong owner").getStackTrace
              val i0 = trace.indexWhere(_.getMethodName.contains("checkOwnership"))
              val st = if (i0 < 0) "" else trace.slice(i0, i0 + 10).mkString(";")
              reporter.error(
                d.pos,
                s"""Wrong owner for $symbol: expected=$expectedOwner actual=$actualOwner
                defTree=$d
                expectedTree=$expectedOwnerTree
                actualTree=$actualOwnerTree
                enclAppl=$enclAppl
                $st
                  """.stripMargin
              )

            } // else reporter.info(symbol.pos, s"As expected, $symbol < $actualOwner", false)

            super.traverse(tree)

          case _: Apply =>
            val oldEnclAppl = enclAppl
            enclAppl = tree
            super.traverse(tree)
            enclAppl = oldEnclAppl
          case _ =>
            super.traverse(tree)
        }

        currentTree = prevTree
      }

      // the supertrait calls this every time we enter a tree which declares a new symbol. we override it
      // to check that the new symbol is owned by the previous symbol and to produce an error if not
      override def atOwner(owner: Symbol)(traverse: => Unit): Unit = {

        symToTree.put(owner, currentTree)
        // the rule for symbol ownership is very simple: every time a new symbol is declared, its owner should
        // be the closest declaration outside of it, which is still in currentOwner at this point
        // validateOwnership(symbol = owner, actualOwner = owner.owner, expectedOwner = currentOwner)
        super.atOwner(owner)(traverse)
        symToTree.remove(owner)
      }

      def ownerIsValid(actualOwner: Symbol, expectedOwner: Symbol): Boolean = {
        def tryParent: Boolean = ownerIsValid(actualOwner, expectedOwner.owner)
        if (expectedOwner eq actualOwner)
          // the nice case is that our actual owner matches the expected owner, but there are some special exemptions
          true
        else if (expectedOwner eq callingOwner) {
          // In this case, we aren't going to have a tree, but we still might need to climb the ownership
          // to pass through allowed exceptions
          @tailrec
          def tryParent(eo: Symbol): Boolean =
            (eo eq actualOwner) ||
              ((eo.isSynthetic && eo.name.toString.startsWith("eta$") || eo.isArtifact) && tryParent(eo.owner))
          tryParent(expectedOwner)
        } else {
          val tree = symToTree.getOrElse(expectedOwner, EmptyTree)
          tree match {

            case _: PackageDef =>
              true

            // pass through owners of Templates - these are weird degenerate symbols which don't own the underlying members
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

    (new ValidationTraverser).traverse(inputTree)
  }

  def enclosingClassesOrModules(sym: Symbol): List[Symbol] = {
    def enclosingClassesOrModulesIter(sym: Symbol): List[Symbol] = {
      if (sym.isClass && !sym.isPackageClass) {
        if (sym.isTopLevel) {
          sym :: Nil
        } else {
          sym :: enclosingClassesOrModulesIter(sym.owner)
        }
      } else if (sym.isPackageClass) {
        Nil
      } else enclosingClassesOrModulesIter(sym.owner)
    }

    enclosingClassesOrModulesIter(sym).reverse
  }

  // [PROP_INFO_TOP_LEVEL] Get name of property info on top-level companion
  //
  // Only used for generating references to property infos of property nodes, not for other types of field that
  // we generate property infos for, e.g. @node(exposeArgTypes = true), @async(exposeArgTypes = true) and @elevated
  //
  // Used when:
  //    1 - Generating entity infos
  //    2 - Generating backing vals on stored entities
  //    3 - Generating property node methods
  //    4 - Generating stored property loads
  def infoHolderForSym(sym: Symbol): InfoHolderModule = {
    def module(sym: Symbol) = if (sym.isModuleClass) sym.module else sym.companion

    lazy val enclosingSymbols = enclosingClassesOrModules(sym.owner)
    if (isEffectivelyTopLevel(sym) || enclosingSymbols.isEmpty) {
      InfoHolderModule(module(sym), "", sym.isModuleClass)
    } else {
      val topLevelSym = enclosingSymbols.head
      val prefix = (enclosingSymbols.tail.map(_.name) :+ sym.name).mkString("", "_", "_")
      InfoHolderModule(module(topLevelSym), prefix, sym.isModuleClass)
    }
  }

  // Comment moved from original location in TypedNodeClassGenerator
  //
  // If it's not a top-level module we need to write Outer.this.Module instead of just Module (though we don't seem
  // to need to do that recursively in the case of multiple levels of nesting since typer doesn't do that either).
  // If we don't do this correctly, lambdalift gets confused and marks the module accessor (i.e. Outer.this.Module())
  // as public even if Module is private, and this then breaks name mangling, causing AbstractMethodError at
  // run time. Exception: if the entity is method-local we cannot (and aren't expected to) do this correctly.
  def genQualifiedModuleRef(module: Symbol): RefTree = {
    if (module.isDefinedInPackage || module.owner.isVal) {
      gen.mkAttributedIdent(module)
    } else {
      gen.mkAttributedSelect(gen.mkAttributedThis(module.owner), module)
    }
  }
}

trait TypedTransformingUtils extends TypedUtils {
  this: TypingTransformers =>
  import global._
  trait TypedTreeCopier {
    this: TypingTransformer =>

    def typedPosWithReplacements(tree: Tree, subst: (Tree, Tree)*): Tree =
      localTyper.typedPos(tree.pos)(inject(tree, copy = false, subst.map(t => (t._1.id, t._2)).toMap))
    def copyIfReplacements(tree: Tree, subst: (Tree, Tree)*): Tree =
      if (subst.exists(t => t._1 ne t._2))
        inject(tree, copy = true, subst.map(t => (t._1.id, t._2)).toMap)
      else tree
    // Scala seems to get confused by an overload:
    def copyIfReplacementsS(tree: Tree, subst: Seq[(Tree, Tree)]): Tree = copyIfReplacements(tree, subst: _*)
    def inject(tree: Tree, copy: Boolean, subst: Map[Int, Tree]): Tree = {
      class CopyTronTransformer extends Transformer {
        override def transform(tree: Tree): Tree =
          subst.getOrElse(
            tree.id,
            tree match {
              case Apply(qual, args) =>
                if (copy)
                  treeCopy.Apply(tree, transform(qual), args.map(transform))
                else
                  Apply(transform(qual), args.map(transform))

              case TypeApply(fun, args) =>
                if (copy)
                  treeCopy.TypeApply(tree, transform(fun), args.map(transform))
                else
                  TypeApply(transform(fun), args.map(transform))

              case Select(qual, name) =>
                if (copy)
                  treeCopy.Select(tree, transform(qual), name)
                else
                  Select(transform(qual), name)

              case Block(stats, expr) =>
                if (copy)
                  treeCopy.Block(tree, stats.map(transform), transform(expr))
                else
                  Block(stats.map(transform), transform(expr))

              case fn1 @ Function(args, body) =>
                val fn2 =
                  if (copy)
                    treeCopy.Function(tree, args, transform(body))
                  else
                    Function(args, transform(body))
                fn2.body.changeOwner((fn1.symbol, fn2.symbol))
                fn2

              case t => super.transform(t)
            }
          )
      }
      val ret = (new CopyTronTransformer).transform(tree)
      checkOwnership(ret, currentOwner)
      ret
    }
  }
}

trait SafeTransform extends Transform {
  self: OptimusPluginReporter =>

  import global._

  protected val transformError: OptimusAlarmBuilder1

  trait SafeTransformer extends Transformer {
    override def transform(tree: Tree): Tree = {
      try {
        transformSafe(tree)
      } catch {
        case ex: Throwable =>
          ex.printStackTrace()
          alarm(transformError, tree.pos, ex)
          EmptyTree
      }
    }

    protected def transformSafe(tree: Tree): Tree = super.transform(tree)
  }

}

trait SafeTypingTransformers extends TypingTransformers {
  self: OptimusPluginReporter =>

  import global._

  protected val transformError: OptimusAlarmBuilder1

  abstract class SafeTypingTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {

    override def transform(tree: Tree): Tree = {
      try {
        transformSafe(tree)
      } catch {
        case ex: Throwable =>
          ex.printStackTrace() // should we remove such kind of stack trace? [Unattributed comment]
          // Assume that if it's OK to print a stack trace rather than use reporter, then it's ok to print other things.
          // These have been very useful for debugging compiler crashes:
          System.err.println("Internal current: " + curTree)
          System.err.println(showRaw(curTree))
          System.err.println("Our current: " + tree)
          alarm(transformError, tree.pos, tree)
          EmptyTree
      }
    }

    protected def transformSafe(tree: Tree): Tree = super.transform(tree)
  }

}
