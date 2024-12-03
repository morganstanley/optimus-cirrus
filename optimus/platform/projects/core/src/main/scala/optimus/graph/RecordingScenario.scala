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

import java.io.Serializable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import optimus.core.SparseBitSet
import optimus.graph.{PropertyNode => PN}
import optimus.platform.NodeHash
import optimus.platform.ScenarioStack
import optimus.platform.Tweak
import optimus.platform.TweakableListener
import optimus.platform.util.PrettyStringBuilder
import optimus.platform.util.html._

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// format: off
/*
Nested structure with RecordedTweakables (see example in optimus.profiler.RecordedTweakablesTests)

Given the following code:

d = nonTweakable + c
nonTweakable = a + b

given(x := 1) {
  given(a := 1, b := 1, c := x + 1) {
     d
  }
}

The resulting structure is:
RT {
  tweaked = [TTN {
     a := 1
     RT = RecordedTweakables.empty
  },
  TTN {
     b := 1
     RT = RecordedTweakables.empty
  },
  TTN {
     c := x + 1
     nested = RT {
         tweaked = [TTN {
            x := 1
            RT = RecordedTweakables.empty
         }]
     }
  }]
}
 */
// format: on
final class TweakTreeNode(val tweak: Tweak, val key: PN[_], val trackingDepth: Int, val nested: RecordedTweakables)
    extends Serializable {

  def withDepth(ktweak: Tweak, depth: Int): TweakTreeNode = {
    if (DiagnosticSettings.traceTweaksEnabled && OGTrace.observer.traceTweaks)
      if (depth == trackingDepth && (tweak ne null) && ktweak.id == tweak.id) this
      else new TweakTreeNode(ktweak, key, depth, nested)
    else {
      if (depth == trackingDepth) this
      else new TweakTreeNode(tweak, key, depth, nested)
    }
  }

  def withDepth(ktweak: Tweak, depth: Int, tweaked: Array[TweakTreeNode]): TweakTreeNode = {
    if (DiagnosticSettings.traceTweaksEnabled && OGTrace.observer.traceTweaks) {
      if (depth == trackingDepth && (nested.tweaked eq tweaked) && (tweak ne null) && ktweak.id == tweak.id) this
      else new TweakTreeNode(ktweak, key, depth, nested.withTweaked(tweaked))
    } else {
      if (depth == trackingDepth && (nested.tweaked eq tweaked)) this
      else new TweakTreeNode(tweak, key, depth, nested.withTweaked(tweaked))
    }
  }

  override def toString: String = writeHtml(new HtmlBuilder()).toPlaintext

  def writeHtml(hb: HtmlBuilder): HtmlBuilder = {
    if (tweak ne null) tweak.writeHtml(hb, ignoreTracing = true)
    else if (key.propertyInfo.hasTweakHandler) {
      val psb = new PrettyStringBuilder(false, hb.short)
      val keyString = key.writePrettyString(psb).toString
      hb += keyString += " [tweak handler]" // see OPTIMUS-54061
    } else {
      val msg =
        "How did we end up with a null tweak in TweakTreeNode if our tweakable has no tweak handler? Contact Optimus Graph Team "
      throw new GraphInInvalidState(msg)
    }
    hb += " at depth " += trackingDepth.toString += " "
    if (nested ne null) {
      hb.squareBracketsIndent {
        nested.writeHtml(hb)
      }
    } else
      hb.newLine()
    hb
  }

  /**
   * Used in ValueInspector not to mis-highlight diffs in TweakTreeNodes And used in assertion Note: we don't compare
   * trackingDepth because it's not really important for TweakTreeNode to be equal
   */
  override def equals(obj: Any): Boolean = obj match {
    case that: TweakTreeNode =>
      if (key == that.key) {
        if (tweak == that.tweak) nested == that.nested
        else false
      } else false
    case _ => false
  }

  override def hashCode(): Int = {
    val nestedHash = if (nested ne null) nested.hashCode() else 0
    31 * (31 * key.hashCode + nestedHash) + (if (tweak ne null) tweak.hashCode else 0)
  }
}

private[optimus] object RecordedTweakables {
  val emptyTweakables = new Array[PropertyNode[_]](0)
  val emptyTweakableHashes = new Array[NodeHash](0)
  val emptyTweakTreeNodes = new Array[TweakTreeNode](0)
  val empty = new RecordedTweakables(emptyTweakables, emptyTweakableHashes, emptyTweakTreeNodes)

  def apply(ttns: Array[TweakTreeNode]): RecordedTweakables = {
    if (ttns.length == 0) empty
    else new RecordedTweakables(emptyTweakables, emptyTweakableHashes, ttns)
  }
}

/**
 * All fields MUST be val(s) (Scala translates into 'final' jvm fields) When this class gets 'published' , there is a
 * race condition and the class has to have all fields 'published'
 */
private[optimus] class RecordedTweakables(
    val tweakable: Array[PropertyNode[_]],
    val tweakableHashes: Array[NodeHash],
    val tweaked: Array[TweakTreeNode]
) extends TweakableListener
    with Serializable {

  override def toString: String = writeHtml(new HtmlBuilder).toPlaintext

  def writeHtml(hb: HtmlBuilder, separateBy: Int = 2): HtmlBuilder = {
    if (tweaked.length + tweakable.length + tweakableHashes.length != 0) {
      hb.namedGroup("XSTweaks", separateBy = separateBy) {
        if (tweaked.length != 0) {
          hb.add(TweakHeader("Tweaked:")).newLine()
          for (t <- tweaked) {
            t.writeHtml(hb)
          }
        }

        if (tweakable.length != 0) {
          hb.add(TweakHeader("Tweakable:")).newLine()
          for (t <- tweakable) {
            hb.noStyle(t.toString).newLine()
          }
        }

        if (tweakableHashes.length != 0) {
          hb.add(TweakHeader("Hashes:")).newLine()
          for (t <- tweakableHashes) {
            hb.noStyle(t.toString).newLine()
          }
        }
      }
    }
    hb
  }

  override def recordedTweakables: RecordedTweakables = this

  def withTweaked(tweaked: Array[TweakTreeNode]): RecordedTweakables =
    new RecordedTweakables(tweakable, tweakableHashes, tweaked)

  def isEmpty: Boolean = this eq RecordedTweakables.empty

  def this(rtl: RecordingTweakableListener) = this(
    if (rtl._tweakable.isEmpty) RecordedTweakables.emptyTweakables
    else rtl._tweakable.values().toArray[PropertyNode[_]](new Array[PropertyNode[_]](rtl._tweakable.size())),
    if (rtl._tweakableHashes.isEmpty) RecordedTweakables.emptyTweakableHashes
    else rtl._tweakableHashes.values().toArray(new Array[NodeHash](rtl._tweakableHashes.size())),
    if (rtl._tweaked.isEmpty) RecordedTweakables.emptyTweakTreeNodes
    else rtl._tweaked.values().toArray(new Array[TweakTreeNode](rtl._tweaked.size()))
  )

  def this(ttn: TweakTreeNode) =
    this(RecordedTweakables.emptyTweakables, RecordedTweakables.emptyTweakableHashes, Array(ttn))

  final override def reportTweakableUseBy(node: NodeTask): Unit = {
    node.scenarioStack.combineTrackData(this, node)
  }

  final def getTweakInfection: SparseBitSet = {
    var r = SparseBitSet.empty
    var i = 0
    while (i < tweaked.length) {
      val tnt = tweaked(i)
      r = r.merge(new SparseBitSet(tnt.tweak.id))
      if (tnt.nested ne null) r = r.merge(tnt.nested.getTweakInfection)
      i += 1
    }
    r
  }

  /** There is a possibility of those methods calls, and it can be safely ignored */
  override def onTweakableNodeCompleted(node: PropertyNode[_]): Unit = {}
  override def onTweakableNodeUsedBy(key: PropertyNode[_], node: NodeTask): Unit = {}
  override def onTweakableNodeHashUsedBy(key: NodeHash, node: NodeTask): Unit = {}
  override def onTweakUsedBy(ttn: TweakTreeNode, node: NodeTask): Unit = {}

  // returns TweakTreeNodes (containing nested RecordedTweakables) grouped by key - used to compare tweaks to diagnose
  // cross-scenario cache misses
  def xsTweaks: Map[PropertyNode[_], TweakTreeNode] = {
    val tweaks = mutable.Map.empty[PropertyNode[_], TweakTreeNode]
    var i = 0
    while (i < tweaked.length) {
      tweaks.put(tweaked(i).key, tweaked(i))
      i += 1
    }
    tweaks.toMap
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: RecordedTweakables => new XSDiffer(stopEarly = true).allXsDiff(this, that)
    case _                        => false
  }

  override def hashCode(): Int = {
    (tweakable ++ tweakableHashes ++ tweaked).foldLeft(19) { case (result, element) =>
      result * 41 + element.hashCode
    }
  }
}

/*
 * Recursively compare two cross-scenario stacks (to include diffs in nested RecordedTweakables):
 * extraTweaks      => rt1 contains the tweak but rt2 does not
 * missingTweaks    => rt2 does NOT contain the tweak but rt1 does
 * mismatchedTweaks => both contain the tweak but the tweaks aren't equal
 */
private[optimus] class XSDiffer(
    extraTweaks: mutable.Set[TweakTreeNode],
    missingTweaks: mutable.Set[TweakTreeNode],
    mismatchedTweaks: ArrayBuffer[(TweakTreeNode, TweakTreeNode)],
    stopEarly: Boolean
) {

  def this(stopEarly: Boolean) = this(mutable.Set.empty, mutable.Set.empty, ArrayBuffer.empty, stopEarly)

  private[this] var notDifferent = true

  def addExtraTweak(t: TweakTreeNode): Unit = extraTweaks += t
  def addMissingTweak(t: TweakTreeNode): Unit = missingTweaks += t
  def addMismatchedTweaks(t0: TweakTreeNode, t1: TweakTreeNode): Unit = mismatchedTweaks += t0 -> t1

  private def tweaksMap(rt: RecordedTweakables): Map[PropertyNode[_], TweakTreeNode] =
    if (rt == null) Map.empty[PropertyNode[_], TweakTreeNode] else rt.xsTweaks

  def tweaksEqual(rt1: RecordedTweakables, rt2: RecordedTweakables): Boolean = {
    val tweaks1 = tweaksMap(rt1)
    val tweaks2 = tweaksMap(rt2)

    val rt1Tweaked = if (rt1 != null) rt1.tweaked else Array.empty[TweakTreeNode]
    for (t1 <- rt1Tweaked) {
      if (!tweaks2.contains(t1.key)) {
        addExtraTweak(t1)
        if (stopEarly) return false
        else notDifferent = false
      } else {
        val t2 = tweaks2(t1.key)
        if (t1.tweak != t2.tweak) {
          addMismatchedTweaks(t1, t2)
          if (stopEarly) return false
          else notDifferent = false
        } else if (!allXsDiff(t1.nested, t2.nested)) {
          if (stopEarly) return false
          else notDifferent = false
        }
      }
    }

    val rt2Tweaked = if (rt2 != null) rt2.tweaked else Array.empty[TweakTreeNode]
    for (t2 <- rt2Tweaked) {
      if (!tweaks1.contains(t2.key)) {
        addMissingTweak(t2)
        if (stopEarly) return false
        else notDifferent = false
      }
    }
    notDifferent
  }

  def allXsDiff(rt1: RecordedTweakables, rt2: RecordedTweakables): Boolean = {
    if (rt1 == null && rt2 == null) return true
    tweaksEqual(rt1, rt2)
  }
}

private[optimus] class WhenXSDiffer extends XSDiffer(null, null, null, stopEarly = true) {

  override def addExtraTweak(t: TweakTreeNode): Unit = ()
  override def addMissingTweak(t: TweakTreeNode): Unit = ()
  override def addMismatchedTweaks(t0: TweakTreeNode, t1: TweakTreeNode): Unit = ()

  def tweakablesEqual(rt1: RecordedTweakables, rt2: RecordedTweakables): Boolean = (rt1, rt2) match {
    case (null, null)          => true
    case (_, null) | (null, _) => false
    case (a, b) => tweakablesEqual(a.tweakable, b.tweakable) && tweakablesEqual(a.tweakableHashes, b.tweakableHashes)
  }

  def tweakablesEqual[A](t1: Array[A], t2: Array[A]): Boolean = (t1, t2) match {
    case (null, null)          => true
    case (_, null) | (null, _) => false
    case (a, b)                => a.toSet == b.toSet
  }

  def whenClauseEqual(wc1: Map[WhenNodeKey, Boolean], wc2: Map[WhenNodeKey, Boolean]): Boolean = {
    for ((k, v) <- wc1) {
      if (!wc2.contains(k)) return false
      if (wc2(k) != v) return false
    }
    for (k <- wc2.keys) {
      if (!wc1.contains(k)) return false
    }
    true
  }

  override def allXsDiff(rt1: RecordedTweakables, rt2: RecordedTweakables): Boolean = {
    if (rt1 == null && rt2 == null) return true
    if (rt1 == null || rt2 == null) return false

    if (!tweakablesEqual(rt1, rt2)) return false
    if (!tweaksEqual(rt1, rt2)) return false
    (rt1, rt2) match {
      case (rt1: WhenNodeRecordedTweakables, rt2: WhenNodeRecordedTweakables) =>
        whenClauseEqual(rt1.whenNodes, rt2.whenNodes)
      case (_: WhenNodeRecordedTweakables, _) => false
      case (_, _: WhenNodeRecordedTweakables) => false
      case _                                  => true
    }
  }
}

/* While XS node is executing this Listener will be collecting the tweaks and tweakables */
class RecordingTweakableListener(
    var scenarioStack: ScenarioStack,
    @transient override val trackingProxy: NodeTask,
    var parent: ScenarioStack,
    earlyUpReport: Boolean)
    extends TweakableListener
    with Serializable {

  /* This ctor call is running (potentially) concurrently with speculative nodes reporting */
  override def recordedTweakables = new RecordedTweakables(this)

  /** It is critical that this and other tables allow for concurrent iteration and publishing */
  @transient private[optimus] var _tweakable: ConcurrentHashMap[PropertyNode[_], PropertyNode[_]] = _
  @transient private[optimus] var _tweakableHashes: ConcurrentHashMap[NodeHash, NodeHash] = _
  @transient private[optimus] var _tweaked: ConcurrentHashMap[PropertyNode[_], TweakTreeNode] = _

  {
    _tweakable = new ConcurrentHashMap[PropertyNode[_], PropertyNode[_]]
    _tweakableHashes = new ConcurrentHashMap[NodeHash, NodeHash]
    _tweaked = new ConcurrentHashMap[PropertyNode[_], TweakTreeNode]
  }

  /**
   * If we aren't done, we don't know what our used tweakables are and can't report them to another node.
   *
   * To avoid a data race here, we *must* run onXSOriginalCompleted before we mark an XS node or its proxy as DONE.
   */
  final override def reportTweakableUseBy(node: NodeTask): Unit = throw new GraphInInvalidState()

  /** RecordingScenarioStack doesn't need this callback! */
  override def onTweakableNodeCompleted(node: PropertyNode[_]): Unit = throw new GraphInInvalidState()

  private def shouldUpReport: Boolean = earlyUpReport && parent.isRecordingTweakUsage

  def opaqueIndexForHash(key: PropertyNode[_]): Int = -1

  /** Save the relevant info, and automatically downgrade tweakables into tweak hashes based on the property info */
  override def onTweakableNodeUsedBy(key: PropertyNode[_], node: NodeTask): Unit = {
    if (key.propertyInfo.trackTweakableByHash) {
      val hash = NodeHash(key, opaqueIndexForHash(key))
      val prev = _tweakableHashes.putIfAbsent(hash, hash)
      if (shouldUpReport && prev == null) {
        if (parent.isRecordingWhenDependencies) parent.combineTweakableData(key, node)
        else parent.combineTweakableData(hash, node)
      }
    } else {
      val tkey = key.tidyKey
      val prev = _tweakable.putIfAbsent(tkey, tkey)
      if (shouldUpReport && prev == null) parent.combineTweakableData(tkey, node)
    }
  }

  /** Save the relevant info */
  override def onTweakableNodeHashUsedBy(hash: NodeHash, node: NodeTask): Unit = {
    val prev = _tweakableHashes.putIfAbsent(hash, hash)
    if (shouldUpReport && prev == null) parent.combineTweakableData(hash, node)
  }

  /** Save the relevant info */
  override def onTweakUsedBy(ttn: TweakTreeNode, node_not_tobe_used: NodeTask): Unit = {
    if (ttn.trackingDepth <= scenarioStack.trackingDepth) {
      val prev = _tweaked.putIfAbsent(ttn.key, ttn)
      if (Settings.schedulerAsserts && (prev ne null) && prev != ttn)
        throw new GraphException(s"Inconsistent TweakTreeNodes: $ttn vs $prev")

      if (shouldUpReport && prev == null) parent.combineTweakData(ttn, node_not_tobe_used)

    } else if (ttn.nested ne null) {
      // [SEE_ON_TWEAK_USED]
      // although the tweak was found in a child of this ScenarioStack (and therefore doesn't affect us)
      // it may have nested dependencies which resolved in our parent(s) and we can't just drop that information
      // so we replay it in to our parent (which may record or ignore it depending on whether it cares or not)
      scenarioStack.combineTrackData(ttn.nested, node_not_tobe_used)
    }
  }
}

private[optimus] final class WhenNodeRecordedTweakables(
    tweakable: Array[PropertyNode[_]],
    tweakableHashes: Array[NodeHash],
    tweaked: Array[TweakTreeNode],
    val whenNodes: Map[WhenNodeKey, Boolean]
) extends RecordedTweakables(tweakable, tweakableHashes, tweaked) {
  def this(rtl: WhenNodeRecordingTweakableListener) = this(
    rtl._tweakable.values().toArray[PropertyNode[_]](new Array[PropertyNode[_]](rtl._tweakable.size())),
    rtl._tweakableHashes.values().toArray(new Array[NodeHash](rtl._tweakableHashes.size())),
    rtl._tweaked.values().toArray(new Array[TweakTreeNode](rtl._tweaked.size())),
    rtl._whenNodes.asScala.toMap
  )

  override def writeHtml(hb: HtmlBuilder, separateBy: Int = 2): HtmlBuilder = {
    super.writeHtml(hb)
    if (whenNodes.nonEmpty) {
      hb.namedGroup("XSWhenNodes", separateBy = 2) {
        hb.add(TweakHeader("WhenClauses:")).newLine()
        for ((key, bool) <- whenNodes) {
          hb.noStyle(key.key.toString)
          hb.noStyle(s"[index=${key.opaqueIndex},result=$bool]")
          hb.squareBracketsIndent {
            key.rt.writeHtml(hb, separateBy = 0)
          }
        }
      }
    }
    hb
  }

  override def withTweaked(tweaked: Array[TweakTreeNode]): RecordedTweakables =
    new WhenNodeRecordedTweakables(tweakable, tweakableHashes, tweaked, whenNodes)

  override def equals(obj: Any): Boolean = obj match {
    case that: WhenNodeRecordedTweakables => new WhenXSDiffer().allXsDiff(this, that)
    case _                                => false
  }

  override def hashCode(): Int = {
    (tweakable ++ tweakableHashes ++ tweaked).foldLeft(19) { case (result, element) =>
      result * 41 + element.hashCode
    } * 41 + whenNodes.hashCode
  }
}

final case class WhenNodeKey(predicate: AnyRef, key: PropertyNode[_], opaqueIndex: Int, rt: RecordedTweakables) {
  override def hashCode(): Int = (predicate.hashCode * 41 + key.hashCode) * 41 + opaqueIndex
  override def equals(obj: Any): Boolean = obj match {
    case that: WhenNodeKey =>
      predicate.equals(that.predicate) && key.equals(that.key) && opaqueIndex == that.opaqueIndex && new WhenXSDiffer()
        .allXsDiff(rt, that.rt)
    case _ => false
  }
}

final class WhenNodeRecordingTweakableListener(
    parent: ScenarioStack,
    trackingProxy: NodeTask,
    earlyUpReport: Boolean = true)
    extends RecordingTweakableListener(parent, trackingProxy, parent, earlyUpReport) {

  def this(parent: ScenarioStack) = this(parent, null)

  def this(parent: ScenarioStack, earlyUpReport: Boolean) = this(parent, null, earlyUpReport)

  val _opaqueIndexCounter = new AtomicInteger(0)
  val _opaqueIndexForHash = new ConcurrentHashMap[PropertyNode[_], Int]
  val _whenNodes = new ConcurrentHashMap[WhenNodeKey, Boolean]

  override def recordedTweakables = new WhenNodeRecordedTweakables(this)

  override def extraFlags: Int = EvaluationState.RECORD_WHEN_DEPENDENCIES

  override def opaqueIndexForHash(key: PropertyNode[_]): Int =
    _opaqueIndexForHash.getOrDefault(key, super.opaqueIndexForHash(key))

  override def reportWhenClause(key: PropertyNode[_], predicate: AnyRef, res: Boolean, rt: RecordedTweakables): Unit = {
    val opaqueIndex = _opaqueIndexForHash.computeIfAbsent(key, _ => _opaqueIndexCounter.getAndIncrement)
    val compositeKey = WhenNodeKey(predicate, key, opaqueIndex, rt)
    _whenNodes.putIfAbsent(compositeKey, res)
  }
}
