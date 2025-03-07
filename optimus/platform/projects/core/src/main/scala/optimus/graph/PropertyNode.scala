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
package optimus.graph

import optimus.config.NodeCacheConfigs
import optimus.debug.InstrumentationConfig
import optimus.debug.InstrumentedHashCodes
import optimus.graph.cache.UNodeCache
import optimus.graph.diagnostics.NodeName
import optimus.graph.loom.LNodeFunction1
import optimus.graph.loom.TrivialNode
import optimus.graph.OGTrace.AsyncSuffix
import optimus.platform._
import optimus.platform.storable.Entity
import optimus.platform.temporalSurface.TemporalSurface
import optimus.platform.util.PrettyStringBuilder

import java.io.Externalizable
import java.io.IOException
import java.io.ObjectInput
import java.io.ObjectInputStream
import java.io.ObjectOutput
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import scala.util.control.NonFatal

/** Mostly a marker interface for DependencyTracker */
trait TweakableKey {
  def propertyInfo: NodeTaskInfo
  // Cleanup the key from as much memory as possible
  def tidyKey: TweakableKey
  def trackKey: TweakableKey
}

final case class ExtractorTweakableKey(keyValue: AnyRef, info: NodeTaskInfo) extends TweakableKey {
  override def propertyInfo: NodeTaskInfo = info
  override def tidyKey: TweakableKey = this
  override def trackKey: TweakableKey = this
}

/**
 * Key that identifies the Node (excluding scenarioStack). NodeKey can be converted to a runnable Node with
 * prepareForExecutionIn call
 */
sealed trait NodeKey[+T] extends TweakableKey {

  /**
   * @note
   *   Abstract, is defined by Node instance type.
   */
  def propertyInfo: NodeTaskInfo

  /**
   * The entity instance that created this node.
   */
  def entity: Entity

  /**
   * Collects all arguments as an array TODO (OPTIMUS-0000): Consider not using it or even generating (in plug-in) in
   * the future
   */
  def args: Array[AnyRef]

  /**
   * Currently used for scenario tweaks only to generate the tweak's when or set clause given the arguments of the
   * current key node
   */
  def argsCopy(generator: AnyRef /* Function[E, A1, A2, Any] */ ): Node[Any]

  /**
   * In order to avoid creating temporary array and boxing value type plug-in generates and overrides these functions
   * with equivalent implementations TODO (OPTIMUS-0000): For common arguments (types and number) create "build-in"
   * classes with those implementations. and reuse them from plug-in. This will reduce jar size, compile size and most
   * importantly instruction cache polluting
   */
  def argsEquals(that: NodeKey[_]): Boolean = java.util.Arrays.equals(args, that.args)
  def argsHash: Int = java.util.Arrays.hashCode(args)

  /**
   * NodeKey can be prepared to become a full Node at any point.
   *
   * @return a fully computable Node in the given scenarioStack
   */
  def prepareForExecutionIn(scenarioStack: ScenarioStack): PropertyNode[_]

  /** Cleanup the key from as much memory as possible */
  def tidyKey: PropertyNode[_]

  /** Returns NodeHash or the key itself */
  override def trackKey: TweakableKey = if (propertyInfo.trackTweakableByHash) NodeHash(this) else this

  def toKeyString: String

  protected[optimus] def toDebugString: String =
    getClass.getName + AsyncSuffix + System.identityHashCode(this) + ":" + toKeyString
}

object PropertyNode {
  final def observedValueNode[T](v: T, entity: Entity, propertyInfo: NodeTaskInfo): PropertyNode[T] = {
    val tc = entity.dal$temporalContext
    if (tc != null && tc.isInstanceOf[TemporalSurface] && tc.asInstanceOf[TemporalSurface].canTick)
      new InitedPropertyNodeSync(
        v,
        entity,
        propertyInfo.asInstanceOf[DefPropertyInfo0[Entity, Entity => Boolean, Entity => T, T]])
    else
      new AlreadyCompletedPropertyNode(v, entity, propertyInfo)
  }

  /** A supertype of PropertyNode containing only covariant uses of `A`. */
  // noinspection ScalaUnusedSymbol: used by plugin
  private[optimus] type CovariantPropertyNode[+A] = Node[A] with NodeKey[A]
}

abstract class PropertyNode[T] extends CompletableNode[T] with NodeKey[T] with TweakableKey {
  initTrackingValue() // [SEE_INIT_CLONE_RESET_DESERIALIZE]
  override def tidyKey: PropertyNode[T] = asKeyIn(ScenarioStack.constant)

  /**
   * Returns a node that could be used as a cache key for this node in the specified scenario.
   */
  def asKeyIn(ss: ScenarioStack): PropertyNode[T] =
    attachOrClone(ss).asInstanceOf[PropertyNode[T]]

  /**
   * The entity instance that created this node.
   */
  def entity: Entity

  /**
   * Currently used for scenario tweaks only to generate the tweak's when or set clause given the arguments of the
   * current key node
   */
  override def argsCopy(generator: AnyRef /* Function[E, A1, A2, Any] */ ): Node[Any] = {
    generator.asInstanceOf[Entity => Node[Any]](entity) // For no arguments property and relies on erasure for entity
  }

  /**
   * Collects all arguments as an array TODO (OPTIMUS-0000): Consider not using and not generating in plug-in this
   * method in the future
   */
  override def args: Array[AnyRef] = NodeTask.argsEmpty

  /** Collects 'this' which for properties is always entity */
  final override def methodThis: AnyRef = entity

  /**
   * Most of the time this member is overriden in the derived classes However for anonymous node this is not important
   * and "default" can be reused
   */
  def propertyInfo: NodeTaskInfo = NodeTaskInfo.Default

  /**
   * Most of the time executionInfo is the same as propertyInfo, but see TweakNode for an important exception
   */
  override def executionInfo: NodeTaskInfo = propertyInfo

  protected def initTrackingValue(): Unit = doInitTrackingValue()

  /** Initializes tweak tracking fields */
  protected final def doInitTrackingValue(): Unit = {
    val info = propertyInfo
    if (info eq null)
      throw new GraphInInvalidState(s"Unexpected null PropertyInfo ${getClass.getName}")
    if (info.isDirectlyTweakable) {
      markAsTrackingValue()
      if (DiagnosticSettings.enablePerNodeTPDMask) {
        info.ensureTweakMaskIsSet()
        setTweakPropertyDependency(info.tweakMask)
      }
    }
  }

  /** Some state needs to be retained between node resets and deserialization */
  override def reset(): Unit = {
    super.reset()
    initTrackingValue() // [SEE_INIT_CLONE_RESET_DESERIALIZE]
  }

  // must be protected to be called when serializing derived classes
  // noinspection ScalaUnusedSymbol (@Serial)
  protected def readResolve(): AnyRef = {
    initTrackingValue() // [SEE_INIT_CLONE_RESET_DESERIALIZE]
    this
  }

  /**
   * [PLUGIN_ENTRY] Looks up the node in cache, applies tweaks and enqueues in the local scheduler queue
   */
  // noinspection ScalaUnusedSymbol: used by plugin
  override final def lookupAndEnqueue: Node[T] = {
    val ec = OGSchedulerContext.current()
    OGTrace.setPotentialEnqueuer(ec, this, false)
    val node = ec.scenarioStack.getNode(this, ec)
    ec.enqueueDirect(node)
    node
  }

  /**
   * [JOB_EXPERIMENTAL] This is the variant for @job-annotated nodes, it also publishes edge crumbs to track the
   * dependency graph. [PLUGIN_ENTRY] Looks up the node in cache, applies tweaks and enqueues in the local scheduler
   * queue
   */
  // noinspection ScalaUnusedSymbol
  override final def lookupAndEnqueueJob: Node[T] = {
    val ec = OGSchedulerContext.current()
    OGTrace.setPotentialEnqueuer(ec, this, false)
    val node = Edges.ensurePropertyJobNodeTracked(this, ec)
    ec.enqueueDirect(node)
    node
  }

  /**
   * [PLUGIN_ENTRY] Looks up the node in cache, applies tweaks, blocks until node executes and returns result
   */
  override final def lookupAndGet: T = {
    val ec = OGSchedulerContext.current()
    OGTrace.setPotentialEnqueuer(ec, this, true)
    val node = ec.scenarioStack.getNode(this, ec)
    ec.runAndWait(node)
    ec.getCurrentNodeTask.combineInfo(node, ec)
    node.result
  }

  /**
   * [JOB_EXPERIMENTAL] This is the variant for @job-annotated nodes, it also publishes edge crumbs to track the
   * dependency graph. [PLUGIN_ENTRY] Looks up the node in cache, applies tweaks, blocks until node executes and returns
   * result
   */
  // noinspection ScalaUnusedSymbol
  override final def lookupAndGetJob: T = {
    val ec = OGSchedulerContext.current()
    OGTrace.setPotentialEnqueuer(ec, this, true)
    val node = Edges.ensurePropertyJobNodeTracked(this, ec)
    ec.runAndWait(node)
    ec.getCurrentNodeTask.combineInfo(node, ec)
    node.result
  }

  /** A wrapper for lookAndGet so that we can easily identify SI nodes */
  // noinspection ScalaUnusedSymbol: used by plugin
  override final def lookupAndGetSI: T = lookupAndGet

  /** Only called on a node that is tweakable and/or x-scenario */
  override protected def reportTweakableUseBy(node: NodeTask): Unit = {
    if (Settings.schedulerAsserts && !propertyInfo.isDirectlyTweakable) throw new GraphInInvalidState()
    node.scenarioStack.tweakableListener.onTweakableNodeUsedBy(this, node)
  }

  /**
   * Called when `PropertyInfo.hasTweakHandler = true` to transform an applied tweaks into the actual tweaks to apply.
   */
  final def transformTweak(value: TweakNode[T], pss: ScenarioStack): collection.Seq[Tweak] =
    transformTweak(value.cloneWithDirectAttach(pss, this).get)

  /**
   * The plugin implements this method to call the user written foo_:= tweak handler
   */
  // noinspection ScalaUnusedSymbol
  def transformTweak(value: Any): collection.Seq[Tweak] =
    throw new UnsupportedOperationException("No tweak-handler for: " + propertyInfo)

  /**
   * Returns a node that one can evaluate to get the value of this node under the given scenario
   *
   * <p>Overridden here to make return type more specific.
   *
   * <p>Should only be further overridden by nodes which implement their own caching internally.
   * Don't override this without discussing in detail with Graph team first.
   */
  override def prepareForExecutionIn(ss: ScenarioStack): PropertyNode[T] =
    super.prepareForExecutionIn(ss).asInstanceOf[PropertyNode[T]]

  /** override to type to PropertyNode from NodeTask */
  override def cacheUnderlyingNode: PropertyNode[T] = this

  /**
   * Tests two PropertyNodes for equality, by checking: (entity, propertyInfo, args). argsEquals can safely assume that
   * it's presented with nodes of the same type
   */
  override def equals(that: Any): Boolean = {
    that match {
      case key: PropertyNode[_] =>
        (this eq key) || ((propertyInfo eq key.propertyInfo) && entity == key.entity && argsEquals(key))
      case _ => false
    }
  }

  /**
   * Tests two PropertyNodes for equality, by checking: (entity, propertyInfo, args). Allows to tighten equality for the
   * purposes of caching (as opposed to tweak equality)
   *
   * Note: it's always cachedValue.equalsForCaching(key) because equalsForCaching isn't symmetric. Proxies override
   * argsEqual so that they compare equal with their srcNodeTemplates (even if they aren't the same class) because a
   * cached proxy should be reused on lookups of its srcNodeTemplate. Most other PN only compare with equal with nodes
   * of the same class. [SEE_NO_SYMMETRY_EQUALS_FOR_CACHING]
   */
  final def equalsForCaching(key: PropertyNode[_]): Boolean = {
    (this eq key) || (
      (propertyInfo eq key.propertyInfo) && // Pinfo matches
        (entity.eq(key.entity) || entity.equalsForCachingInternal(key.entity)) && // entities are equal
        argsEquals(key) // args are equal too
    )
  }

  /** Entity caches its own hashCode. To share most of the cost with hashForCaching we store partially computed value */
  @transient private[this] var $hashCodeArgsAndInfo: Int = _
  private def hashCodeArgsAndInfo: Int = {
    if ($hashCodeArgsAndInfo == 0) {
      val info = propertyInfo
      $hashCodeArgsAndInfo = (argsHash * 31 + info.hashCode) * 31
    }
    $hashCodeArgsAndInfo
  }

  /** Used for general store and tweaks */
  override def hashCode: Int = {
    if (InstrumentationConfig.instrumentAllHashCodes)
      InstrumentedHashCodes.enterReporting()
    val v_entity = entity
    val r = if (v_entity ne null) hashCodeArgsAndInfo + v_entity.hashCode else hashCodeArgsAndInfo
    if (InstrumentationConfig.instrumentAllHashCodes)
      InstrumentedHashCodes.exitReporting()
    r
  }

  /** Used only for caching */
  final def hashCodeForCaching: Int = {
    if (InstrumentationConfig.instrumentAllHashCodes)
      InstrumentedHashCodes.enterReporting()
    val vEntity = entity
    val r = if (vEntity ne null) hashCodeArgsAndInfo + vEntity.hashCodeForCachingInternal else hashCodeArgsAndInfo
    if (InstrumentationConfig.instrumentAllHashCodes)
      InstrumentedHashCodes.exitReporting()
    r
  }

  def toKeyString: String =
    (if (entity ne null) entity.getClass.getSimpleName + AsyncSuffix + System.identityHashCode(entity).toHexString
     else "-") +
      "." + (if (propertyInfo ne null) propertyInfo.name else "-") +
      (if (!args.isEmpty) "(" + args.mkString(",") + ")" else "")

  protected[optimus] override def toDebugString: String =
    super.toDebugString +
      "=" + (if (isDoneWithException) "!\"" + exception + "\""
             else if (isDoneWithResult) result.toString
             else stateAsString)

  override def subProfile(): Entity = entity
  override def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder = {
    if (sb.useEntityType && (entity ne null)) {
      val skey = if (sb.showKeys && (entity.$info.keys ne null)) {
        val keys = entity.allKeyInfos[Entity]
        keys.map(k => k.getRaw(entity).toString)
      }.mkString("(", ",", ").")
      else if (sb.showEntityToString) {
        try "{" + entity + "}."
        catch {
          case NonFatal(e) => s"{entity toString threw $e}."
        }
      } else "."

      val name = entity.getClass.getName
      val className = name.substring(name.lastIndexOf('.') + 1)
      sb.append(className + skey + propertyInfo.name)
    } else {
      classMethodName(sb)
    }

    if (sb.showNodeArgs) {
      sb.append("(")
      val as: Array[AnyRef] = args
      if (args.nonEmpty) {
        var first: Boolean = true
        for (a <- as) {
          if (first) first = false else sb.append(",")
          try sb.append(a)
          catch {
            case NonFatal(ex) => sb.append("argument toString threw ").append(ex)
          }
        }
      }
      sb.append(")")
    }

    if (sb.showNodeState) {
      sb.append(":")
      sb.append(stateAsString)
    }
    sb
  }

  override def classMethodName: String = {
    val sb = new PrettyStringBuilder()
    classMethodName(sb)
    sb.toString
  }

  private def classMethodName(sb: PrettyStringBuilder): PrettyStringBuilder = {
    entity match {
      case nd: NodeDebug =>
        sb ++= "[" ++= entity.getClass.getSimpleName ++= "] "
        nd.debugKey.asInstanceOf[PropertyNode[_]].writePrettyString(sb)
      case _ =>
        sb ++= NodeName.cleanNodeClassName(propertyInfo.nodeName().toString(sb.simpleName, sb.includeHint), '.')
    }
  }
}

/**
 * Base class for Node implementation created by plug-in for @node members that are NOT calling other async functions.
 * [SEE_PLUGIN_OVERRIDES] This is not the class you are looking to override!
 */
abstract class PropertyNodeSync[T] extends PropertyNode[T] {
  override def argsEquals(that: NodeKey[_]): Boolean = NodeClsIDSupport.equals(this, that)
  override def argsHash: Int = NodeClsIDSupport.hashCode(this)
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(func, ec)
}

private[optimus] final class ConstantPropertyNodeSync[T](
    v: T,
    val entity: Entity,
    override val propertyInfo: NodeTaskInfo)
    extends PropertyNodeSync[T] {
  if (Settings.schedulerAsserts && !Settings.auditing)
    throw new GraphException("CPNS for auditing only")

  override def func: T = v
  override def tidyKey: PropertyNode[T] = new ConstantPropertyNodeSync(v, entity, propertyInfo)

  // noinspection ScalaUnusedSymbol [SEE_INIT_CLONE_RESET_DESERIALIZE]
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    initTrackingValue()
  }
}

/**
 * In the ticking environment, the storable values can't be AlreadyCompleted, But we should support the
 * PluginHelper.safeResult which handle use the node in another @stored @entity's constructor
 *
 * '@stored @entity class Foo(@node val name: String) '@stored @entity class Bar(pn: PropertyNode[Sting]) val foo =
 * Foo("foo") val bar = Bar(nodeOf(foo.name)) Map[Bar, Any] we need to calculate the hashcode of Bar, and we will use
 * safeResult
 */
class InitedPropertyNodeSync[T](
    private val v: T,
    override val entity: Entity,
    override val propertyInfo: DefPropertyInfo0[Entity, Entity => Boolean, Entity => T, T])
    extends PropertyNode[T] {

  override def run(ec: OGSchedulerContext): Unit = completeWithResult(func, ec)

  final def refreshedValue: T = propertyInfo.createNodeKey(entity).asInstanceOf[InitedPropertyNodeSync[T]].v
  override def func: T = {
    refreshedValue
  }

  // noinspection ScalaUnusedSymbol
  // Take a snapshot of ticking value if serialised
  private def writeReplace: Any = new AlreadyCompletedPropertyNode(refreshedValue, entity, propertyInfo)
}

/**
 * Generated classes will derive from this class [SEE_PLUGIN_OVERRIDES] This is not the class you are looking to
 * override!
 */
abstract class PropertyNodeFSM[T] extends PropertyNode[T] {
  override def argsEquals(that: NodeKey[_]): Boolean = NodeClsIDSupport.equals(this, that)
  override def argsHash: Int = NodeClsIDSupport.hashCode(this)
  override def isFSM = true
  @transient final private var __k: NodeStateMachine = _
  final override def setContinuation(kx: NodeStateMachine): Unit = {
    __k = kx
  }
  override def reset(): Unit = {
    __k = null
    super.reset()
  }

  override def completeWithException(ex: Throwable, ec: EvaluationQueue): Unit = {
    __k = null // If cancelled via CS cancellation (i.e. while not actually running) we want to cleanup
    super.completeWithException(ex, ec)
  }

  override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    val k = __k
    if (k != null && (k.childNode eq child))
      super.onChildCompleted(eq, child)
  }

  final override def run(ec: OGSchedulerContext): Unit = {
    val lk = __k
    if (lk eq null)
      funcFSM(this, ec)
    else {
      __k = null
      lk.run(this, ec)
    }
  }
}

/*
 * AlreadyCompletedPropertyNode is always under ScenarioStack.constant
 * [PLUGIN_ENTRY]
 */
class AlreadyCompletedPropertyNode[T](
    v: T,
    final override val entity: Entity,
    final override val propertyInfo: NodeTaskInfo)
    extends PropertyNode[T]
    with TrivialNode {

  initAsCompleted(v)

  if (propertyInfo.shouldLookupPlugin()) {
    throw new RuntimeException(
      s"Cannot set plugin on trivial computation! Attempting to run ACPN $propertyInfo with plugin")
  }

  override def getProfileId: Int = propertyInfo.profile // We want to pretend to be a different node type
  override def reset(): Unit = {}
  override def isStable: Boolean = scenarioStack() eq ScenarioStack.constant
  override def func: T = result
  override def run(ec: OGSchedulerContext): Unit = { throw new UnsupportedOperationException(s"Can't run ACPN $this") }
  override def tidyKey: PropertyNode[T] = new AlreadyCompletedPropertyNode(result, entity, propertyInfo)

  // noinspection ScalaUnusedSymbol
  private def writeReplace(): AnyRef = new ACPNMoniker(this)

  override def argsCopy(generator: AnyRef): Node[Any] = generator match {
    case predicate: LNodeFunction1[Entity @unchecked, Boolean @unchecked] =>
      predicate.apply$newNode(entity)
    case _ => super.argsCopy(generator)
  }
}

/*
 * This node is generated for @embeddable class with @node constructor.
 * This node will be looked up in cache directly, and won't be scheduled
 * Note that the key is simply the value since our aim is to intern the object
 */
class ConstructorNode[T](v: T, propertyInfo: NodeTaskInfo)
    extends AlreadyCompletedPropertyNode[T](v, null, propertyInfo) {
  final override def argsHash: Int = v.##
  final override def argsEquals(other: NodeKey[_]): Boolean = {
    other match {
      case cNode: ConstructorNode[T @unchecked] => cNode.result == result
      case _                                    => false
    }
  }
  final override def run(ec: OGSchedulerContext): Unit = {
    throw new GraphInInvalidState(s"Can't run ConstructorNode $this")
  }
}

private[optimus] object ConstructorNode {
  final val suffix = "-constructor"
  private val class2ConstructorInfo: ConcurrentMap[Class[_], NodeTaskInfo] = new ConcurrentHashMap

  def buildConstructorInfo(cls: Class[_]): NodeTaskInfo = {
    val nti = class2ConstructorInfo.computeIfAbsent(
      cls,
      key => {
        // default settings for constructor node
        val ctorInfo = new NodeTaskInfo(s"${key.getName}$suffix", NodeTaskInfo.SCENARIOINDEPENDENT)
        ctorInfo.setCustomCache(UNodeCache.constructorNodeGlobal)
        ctorInfo
      }
    )

    // this must be outside of the NTI creation! Otherwise config will only be applied once on creation, which might
    // be incorrect behaviour if constructor node had already been constructed by the time optconf is applied
    val constructorConfig = NodeCacheConfigs.constructorConfigs.get(cls.getName)
    if (constructorConfig.isDefined)
      nti.setExternalConfig(constructorConfig.get)
    nti
  }
}

/** Serialization helper to reduce public surface of the ACPN classes and to reduce serialized size */
@SerialVersionUID(1L)
private class ACPNMoniker[T](var node: AlreadyCompletedPropertyNode[T]) extends Externalizable {

  def this() = this(null)

  // noinspection ScalaUnusedSymbol
  private def readResolve(): AnyRef = node

  @throws[IOException]
  def writeExternal(out: ObjectOutput): Unit = {
    // write only the fields we care about
    out.writeObject(node.result)
    out.writeObject(node.entity)
    out.writeObject(node.propertyInfo)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  def readExternal(in: ObjectInput): Unit = {
    val result = in.readObject().asInstanceOf[T]
    val entity = in.readObject().asInstanceOf[Entity]
    val propertyInfo = in.readObject().asInstanceOf[NodeTaskInfo]
    node = new AlreadyCompletedPropertyNode[T](result, entity, propertyInfo)
  }
}

/**
 * Plugin override this class for @node def that simply forward to another async node [SEE_PLUGIN_OVERRIDES] This is not
 * the class you are looking to override!
 */
abstract class PropertyNodeDelegate[A] extends PropertyNode[A] {
  override def argsEquals(that: NodeKey[_]): Boolean = NodeClsIDSupport.equals(this, that)
  override def argsHash: Int = NodeClsIDSupport.hashCode(this)
  override def isFSM = true // It's FSM as far as outside world is concerned

  override def run(ec: OGSchedulerContext): Unit = {
    val node = childNode
    node.continueWith(this, ec)
  }

  override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
    completeFromNode(child.asInstanceOf[Node[A]], eq)
  }

  /* Implemented by plug-in, must return just queued up node */
  protected def childNode: Node[A]
}
