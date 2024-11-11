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

import scala.annotation.nowarn
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicLong
import java.util.{HashMap => JHashMap}
import com.github.benmanes.caffeine.cache.Caffeine
import optimus.core.TPDMask
import optimus.platform._
import optimus.platform.annotations.deprecating

import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
import scala.collection.mutable

/**
 * This class supports scenario stack caching. The use case is as follows: <code> someCollection map {
 * given(someScenario) { _.something } } </code> Here we want the memos for someScenario not to be lost immediately upon
 * exiting the given block so that the next iteration over the collection will benefit from those memos. In order to do
 * that, we need to cache the scenarioStacks associated with each scenario within a parent scenario stack.
 *
 *   - process wide singleton cache of scenario stacks keyed against parent ScenarioStack and the scenario being entered
 *     for which the child scenarioStack is being created (and cached)
 */
object CacheIDs {
  def createWeakCache(): ConcurrentMap[AnyRef, ScenarioStack] =
    Caffeine.newBuilder.weakValues().build[AnyRef, ScenarioStack]().asMap()

  /** Map from (parent scenarioID and Scenario) -> ScenarioStack */
  private val global = new ConcurrentHashMap[ScenarioStackCacheKey, SSCacheIDRef]()

  @deprecating(
    "Temporary implementation to aggressively clear the caches for GUI instances leaking old scenarios in the cache. Also used in stress tests to reset between runs")
  def clear(): Unit = global.clear()

  private val removedCacheIDs = new ReferenceQueue[SSCacheID]()
  private[optimus] def putIfAbsent(key: ScenarioStackCacheKey, newCacheID: SSCacheID): SSCacheID = {
    var cacheID: SSCacheID = newCacheID
    global.compute( // Read the word 'compute' as 'update'
      key,
      (_, oldRef) => {
        if (oldRef ne null) { // There is previous entry
          val prevCacheID = oldRef.get // Now are holding a strong reference
          if (prevCacheID ne null) {
            cacheID = prevCacheID
            oldRef
          } else { // Current state: global[oldKey] == oldRef and oldRef.cacheID == null
            val oldKey = oldRef.key // and it's NOT equals 'key'
            val newRef = new SSCacheIDRef(oldKey, cacheID, removedCacheIDs)
            oldKey.updateKeyWithValue(newRef, cacheID) // Notice: oldRef.key is updated and not key/k
            newRef
          }
        } else { // No entry in global at all for this key
          val newRef = new SSCacheIDRef(key, cacheID, removedCacheIDs)
          key.updateKeyWithValue(newRef, cacheID)
          newRef
        }
      }
    )

    var cleanUp = removedCacheIDs.poll()
    while (cleanUp ne null) {
      global.remove(cleanUp.asInstanceOf[SSCacheIDRef].key, cleanUp)
      cleanUp = removedCacheIDs.poll()
    }

    // essential to ensure the one we are going to use is initialized because we need the property infos to be
    // markInstanceTweaked or markPropertyTweaked appropriately
    cacheID.ensureInitialized()
    cacheID
  }

  private[graph] def validateInputScenario(scenario: Scenario): Unit = {
    if (scenario.nestedScenarios.nonEmpty)
      throw new GraphInInvalidState("SSCacheKey scenario should not be nested")
    if (scenario.hasUnresolvedOrMarkerTweaks)
      throw new GraphInInvalidState("SSCacheKey scenario should not have unresolved or marker tweaks")
  }
}

private class SSCacheIDRef(val key: ScenarioStackCacheKey, cacheID: SSCacheID, q: ReferenceQueue[SSCacheID])
    extends WeakReference[SSCacheID](cacheID, q) {
  override def toString: String = {
    val id = get()
    if (id eq null) "ssref()" else "ssref:" + id.toString
  }
}

sealed private[optimus] class ScenarioStackCacheKey {
  def updateKeyWithValue(cacheIDRef: SSCacheIDRef, ssCacheID: SSCacheID): Unit = {}
}

/** Tracking overlay ScenarioStack is defined by just 2 ids */
final private[optimus] case class TOSSCacheKey(originalSSCacheID: Long, overlaySSCacheID: Long)
    extends ScenarioStackCacheKey

/** Used in distribution to share cache key across different runs with same RuntimeEnvironment */
final private[optimus] case class RuntimeEnvironmentCacheKey(env: RuntimeEnvironment) extends ScenarioStackCacheKey

final private[optimus] class SSCacheKey(val parentID: Long, private var _scenario: Scenario)
    extends ScenarioStackCacheKey {

  private var cacheIDRef: SSCacheIDRef = _ // Only one is valid scenario or cacheIDRef / cacheID
  @volatile
  private var cacheIDID: Long = -1

  /** cacheIDRef points to ssCacheID, but the reason to pass it explicitly is to ensure that it's not collected */
  override def updateKeyWithValue(cacheIDRef: SSCacheIDRef, ssCacheID: SSCacheID): Unit = {
    if ((_scenario ne null) && (ssCacheID.inputScenario ne _scenario))
      throw new GraphInInvalidState()

    this.cacheIDRef = cacheIDRef
    this.cacheIDID = ssCacheID.id // volatile ensures MB
    this._scenario = null
  }

  private def accessScenario: Scenario = if (_scenario ne null) _scenario
  else {
    val cacheID = cacheIDRef.get()
    if (cacheID eq null) null else cacheID.asInstanceOf[SSScenarioCacheID]._inputScenario
  }

  def equals(otherKey: SSCacheKey): Boolean = parentID == otherKey.parentID && {
    val s1 = accessScenario
    val s2 = otherKey.accessScenario
    if ((s1 ne null) && (s2 ne null))
      s1 == s2
    else
      cacheIDID == otherKey.cacheIDID
  }

  override def equals(otherKey: Any): Boolean = (otherKey.asInstanceOf[AnyRef] eq this) || (otherKey match {
    case ssKey: SSCacheKey => equals(ssKey)
    case _                 => false
  })

  /** Has to be a val otherwise when scenario is nulled out we will NPE */
  override val hashCode: Int = _scenario.hashCode * 41 + parentID.hashCode()
  override def toString: String = s"SSCacheKey(id = $cacheIDID)"
}

private[optimus] object SSCacheID {

  val cacheIDGenerator = new AtomicLong()

  /** creates a new SSCacheID with a unique ID but no tweaks */
  final def newUnique(): SSCacheID = new SSCacheID()

  /** create new SSCacheID that forwards all the calls to the original */
  final def newForwarding(cacheID: SSCacheID): SSCacheID = new SSForwardingCacheID(cacheID)

  /**
   * creates a new uninitialized SSCacheID for inputScenario - this is cheap to create but must not be used until you
   * have called ensureInitialized (note that CacheIDs does that for you)
   */
  def newUninitialized(inputScenario: Scenario): SSCacheID = new SSScenarioCacheID(inputScenario)
}

/**
 * Notes:
 *   1. We only cache flat scenarios. i.e. nested scenarios are flattened out before cache is accessed
 *   1. We generate an 'id' as opposed to using SSCacheID identity itself to reduce the number of GC cycles we will need
 *      to remove key/values from cache
 */
sealed private[optimus] class SSCacheID {
  val id: Long = SSCacheID.cacheIDGenerator.incrementAndGet()

  /**
   * Entries can be of 4 forms:
   *   1. NodeKey (PropertyNode) -> Tweak [aka instance tweak]
   *   1. propertyInfo -> Tweak [aka full blown property tweak]
   *   1. propertyInfo -> KeyExtractor [aka faster property tweaks]
   *   1. ExtractorTweakableKey -> Tweak [second part of faster property tweaks]
   */
  protected[graph] var tweaks: JHashMap[AnyRef, AnyRef] = new JHashMap()

  /** Don't use system one, this one is probably better and faster! */
  // noinspection HashCodeUsesVar (id is basically a val, but during deserialization needs to be updated)
  override def hashCode(): Int = id.asInstanceOf[Int]

  @volatile // This field also serves as isInitialized in a classic double checked locking
  protected[graph] var twkHash: JHashMap[AnyRef, AnyRef] = _ // Node hash or NodeTaskInfo hash
  protected[graph] var tweakMask: TPDMask = TPDMask.empty

  def tweakMaskString = tweakMask.stringEncoded()

  def this(original: SSCacheID) = {
    this()
    this.tweaks = original.tweaks
    this.tweakMask = original.tweakMask
  }

  protected def putAllInternal(expandedTweaks: Iterable[Tweak]): Unit = {
    val it = expandedTweaks.iterator
    while (it.hasNext) {
      val tweak = it.next()
      tweak.target match {
        case tweakTarget: InstancePropertyTarget[_, _] =>
          put(tweakTarget.hashKey, tweak) // last one wins in case of dups
        case tweakTarget @ (_: PredicatedPropertyTweakTarget[_, _, _, _] | _: PropertyTarget[_, _, _, _]) =>
          put(tweakTarget.propertyInfo, tweak) // regular property tweaks
        case tweakTarget: TweakKeyExtractorTarget[_, _] =>
          put(tweakTarget.info, tweakTarget.keyExtractor)
          put(tweakTarget.key, tweak)
      }
    }
  }

  def allTweaksAsIterable: Iterable[Tweak] = tweaks.values().asScala.collect { case twk: Tweak => twk }
  def allInstanceTweaksInfos: Seq[NodeTaskInfo] =
    tweaks.keySet().asScala.collect { case key: PropertyNode[_] => key.propertyInfo }.toSeq
  def allPropertyTweaksInfos: Seq[NodeTaskInfo] =
    tweaks.keySet().asScala.collect { case nti: NodeTaskInfo => nti }.toSeq

  // use unordered API since SnapshotScenarioStack identity uses Scenario.equals and hashcode (for better reuse)
  @nowarn("msg=10500 optimus.platform.Scenario.unordered")
  private[optimus] def toScenario: Scenario = {
    if (tweaks.isEmpty) Scenario.empty
    else
      Scenario.unordered(allTweaksAsIterable)
  }

  /** Valid for CacheID created from a set of tweaks aka Scenario */
  def inputScenario: Scenario = toScenario

  /** returns the instance tweak for key, if one exists, else null */
  def get(key: TweakableKey): Tweak = tweaks.get(key).asInstanceOf[Tweak]

  /**
   * Returns the property tweak for key, if one exists, else null. Does *not* check for property tweaks affecting
   * parents of key (i.e. the same properties on super-types of the owning entity) OR Returns KeyExtractor for
   * propertyTweaks that support quick lookup
   */
  def get(key: NodeTaskInfo): AnyRef = tweaks.get(key)

  def containsInstanceTweak(key: TweakableKey): Boolean = tweaks.containsKey(key)

  /**
   * This is a quick test to short circuit hashmap lookup: there should never be false negatives (ie mayContainTweak
   * never returns false when there IS a tweak) but there is a small chance of false positives (ie mayContainTweak
   * returns true when there is no tweak). These are OK because they just result in doing the lookup we would've done
   * without the mask anyway
   */
  def mayContainTweak(key: NodeTaskInfo): Boolean = tweakMask.intersects(key.tweakMask)

  // REMEMBER: If we support XSFT for tracking scenario, we need to consider overriding in SSForwardingCacheID
  def mayContainTweak(mask: TPDMask): Boolean = tweakMask.intersects(mask)

  /**
   * Returns true if this SSCacheID *may* contain instance or property tweaks (*including* to parent properties) which
   * affect the key. May return true even in the absence of a specific tweak but will never return false incorrectly.
   * Used in XS.
   */
  def mayContainTweakAffecting(key: NodeHash): Boolean = {
    var twkHash = this.twkHash
    if (twkHash eq null) synchronized {
      twkHash = this.twkHash
      if (twkHash eq null) {
        twkHash = new JHashMap[AnyRef, AnyRef]()
        for (k <- allTweaksAsIterable) {
          val hashKey = k.target.hashKey
          if (hashKey ne null)
            twkHash.putIfAbsent(NodeHash(hashKey), null)
          else
            twkHash.putIfAbsent(k.target.propertyInfo, null)
        }
        this.twkHash = twkHash // publish (aka  isInitialized = true)
      }
    }

    val keyProp = key.propertyInfo
    if (twkHash.isEmpty) false
    else if (keyProp.wasTweakedByInstance && twkHash.containsKey(key)) true
    else if (keyProp.wasTweakedByProperty) {
      // need to check all parent properties of the key to see if any were tweaked (note that we check the parents here
      // instead of adding the children to the twkHash above because more children could be classloaded after the
      // twkHash is built; matchOn gets updated when that happens but of course the twkHash would not be updated)
      var found = twkHash.containsKey(keyProp)
      val parents = keyProp.matchOn()
      if (parents ne null) {
        var i = 0
        while (!found && i < parents.size) {
          found = twkHash.containsKey(parents(i))
          i += 1
        }
      }
      found
    } else false
  }

  // Must be called on a brand new SSCacheID or on a mutable version
  protected def put(key: TweakableKey, twk: Tweak): Unit = {
    if (tweakMask eq TPDMask.empty) tweakMask = new TPDMask()
    key.propertyInfo.markInstanceTweaked()
    tweakMask.merge(key.propertyInfo.tweakMask)
    tweaks.put(key, twk)
  }

  protected def put(key: NodeTaskInfo, twk: AnyRef): Unit = {
    if (tweakMask eq TPDMask.empty) tweakMask = new TPDMask()
    key.markPropertyTweaked()
    tweakMask.merge(key.tweakMask)
    tweaks.put(key, twk)
  }

  /**
   * Creates a new id, but copies expanded tweaks. The copy doesn't expose mutation, but you'll get horrible errors if
   * you aliased a mutating cache id and it mutates!
   */
  def dup: SSCacheID = {
    ensureInitialized()
    val nid = new SSCacheID(this)
    nid
  }

  /**
   * Note that this method recomputes tweakMask based on the instanceTweaks and propertyTweaks. It should be called to
   * recompute the mask after tweaks are mutated, unless the mutation method itself updates tweakMask as it goes -- for
   * example, the two put(key, twk) methods below update tweakMask anyway in super.put calls.
   *
   * In the remove(key) and removeInstance/PropertyTweaks methods, we cannot just unset the bit as we go, since we lose
   * information when we do the compression in the first place. For example, if we have calls to the same node with
   * different arguments, then removing a tweak to one should not remove the tweak to the other: e.b(1) e.b(2)
   *
   * given(e.b(1) := ...) given(e.b(2) := ...)
   *
   * If we simply unset the bit when we remove the tweak to e.b(1), then we will incorrectly end up unsetting the bit
   * that represents the tweak to e.b(2)
   *
   * Future improvement: add counting or other bloom filter techniques
   */
  protected def recomputeTweakMask(): Unit = {
    tweakMask = new TPDMask()
    val it = tweaks.keySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      entry match {
        case k: NodeKey[_] =>
          tweakMask.merge(k.propertyInfo.tweakMask)
        case info: PropertyInfo[_] =>
          tweakMask.merge(info.tweakMask)
        case _ => // this would be [ExtractorTweakableKey -> Tweak] case, but we already catch the PropertyInfo above
        // and recompute the mask accordingly even for extractor tweaks
      }
    }
  }

  def isEmpty: Boolean = tweaks.isEmpty

  def ensureInitialized(): Unit = {}

  override def toString: String = s"SSCacheID(id = $id)"
}

/* Delay initialized cache ID based on a scenario */
final private class SSScenarioCacheID(val _inputScenario: Scenario) extends SSCacheID {
  @volatile
  private var isInitialized: Boolean = _

  if (Settings.schedulerAsserts) CacheIDs.validateInputScenario(_inputScenario)

  override def isEmpty: Boolean = _inputScenario.isEmpty
  override def inputScenario: Scenario = _inputScenario

  override def ensureInitialized(): Unit = {
    if (!isInitialized) synchronized {
      // (double checked lock)
      if (!isInitialized) {
        putAllInternal(_inputScenario.topLevelTweaks)
        isInitialized = true
      }
    }
  }

}

/**
 * An SSCacheID with mutable tweak maps - TrackingScenario is using this Note: after serialization it becomes SSCacheID
 * Note: alternative implementation would use immutable.HashMap (need to benchmark to make a call) Note: synchronized{}
 * around put/remove/dup is not needed! The reason is that dup is called during an 'evaluate' cycle on a given TS and
 * never runs concurrently with an 'invalidate' cycle on the same TS
 */
final private[optimus] class MutableSSCacheID extends SSCacheID {
  def changesFrom(allTweaks: collection.Seq[Tweak]): collection.Seq[Tweak] = {
    val it = allTweaks.iterator
    val changed = mutable.ArrayBuffer[Tweak]()
    while (it.hasNext) {
      val t = it.next()
      val info = t.target.propertyInfo
      val hashKey = t.target.hashKey
      val cached = if (hashKey ne null) {
        if (info.wasTweakedByInstance()) get(hashKey) else null
      } else {
        if (info.wasTweakedByProperty()) get(info) else null // SEE_TWEAKED_BY_PROPERTY: consider unifying with tpdMask
      }
      if (t.alwaysInvalidateTrack || cached != t) changed += t
    }
    changed
  }

  var modifyOnWrite: Boolean = _

  /** Creates a new id, but copies expanded tweaks and the copy is read-only */
  override def dup: SSCacheID = {
    modifyOnWrite = true // notes that we observed tweaks we can't update them anymore
    super.dup
  }

  private def copyOnWrite(): Unit = {
    if (modifyOnWrite) {
      tweaks = new JHashMap(tweaks)
      tweakMask = tweakMask.dup()
      modifyOnWrite = false
    }
    twkHash = null // always invalid if we changed something
  }

  def putAll(expandedTweaks: Iterable[Tweak]): Unit = putAllInternal(expandedTweaks)

  override def put(key: TweakableKey, twk: Tweak): Unit = {
    copyOnWrite()
    super.put(key, twk)
  }

  override def put(key: NodeTaskInfo, twk: AnyRef): Unit = {
    copyOnWrite()
    super.put(key, twk)
  }

  def remove(key: NodeKey[_]): Tweak = {
    copyOnWrite()
    val result = tweaks.remove(key)
    recomputeTweakMask()
    result.asInstanceOf[Tweak]
  }

  /* Note: removes property tweaks and tweak extractor tweaks */
  def remove(info: NodeTaskInfo): Unit = {
    copyOnWrite()
    val it = tweaks.keySet().iterator()
    while (it.hasNext) {
      val key = it.next()
      key match {
        case nti: NodeTaskInfo if nti == info                         => it.remove()
        case xkey: ExtractorTweakableKey if xkey.propertyInfo == info => it.remove()
        case _                                                        =>
      }
    }
    recomputeTweakMask()
  }

  def clearTweaks(): Unit = {
    copyOnWrite()
    tweaks.clear()
    recomputeTweakMask()
  }
}

final private class SSForwardingCacheID(val orgID: SSCacheID) extends SSCacheID {
  tweaks = null // Cleanup of unused empty HashMap
  override def dup: SSCacheID = orgID.dup
  override def isEmpty: Boolean = orgID.isEmpty

  override def allTweaksAsIterable: Iterable[Tweak] = orgID.allTweaksAsIterable

  override def get(key: TweakableKey): Tweak = orgID.get(key)
  override def get(key: NodeTaskInfo): AnyRef = orgID.get(key)

  override def containsInstanceTweak(key: TweakableKey): Boolean = orgID.containsInstanceTweak(key)
  override def mayContainTweakAffecting(key: NodeHash): Boolean = orgID.mayContainTweakAffecting(key)

  override def mayContainTweak(key: NodeTaskInfo): Boolean = orgID.mayContainTweak(key)
  override def put(key: TweakableKey, twk: Tweak): Unit = throw new GraphInInvalidState()
  override def put(key: NodeTaskInfo, twk: AnyRef): Unit = throw new GraphInInvalidState()

  override def ensureInitialized(): Unit = orgID.ensureInitialized()
}
