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

import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import optimus.graph.diagnostics.messages.ProfilerMessages
import optimus.graph.PThreadContext._
import optimus.graph.diagnostics.OGCounterView
import optimus.graph.diagnostics.PNodeTaskInfo

import java.util.{HashMap => JHashMap}
import java.util.{ArrayList => JArrayList}
import java.util.{Arrays => JArrays}
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import optimus.core.TPDMask
import optimus.graph.OGTraceReader.DistributedEvents
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfoRecorded
import optimus.graph.diagnostics.PNodeTaskRecorded
import optimus.graph.diagnostics.messages.OGCounter
import optimus.graph.diagnostics.messages.OGEvent

class OGTraceReaderEventProcessor(constructPNodes: Boolean, ignoreBlocks: Boolean) extends ProfilerMessages {
  val root = new PNodeTaskRecorded(OGSchedulerContext.NODE_ID_ROOT)
  root.info = PNodeTask.startNodeInfo

  var ctx: PThreadContext = _
  var timeOffset: Long = _ // for cross-process time synchronization

  // temporary structures to support id mapping across processes
  val __inProcessTaskIDs = new Int2IntOpenHashMap
  var __inProcessInfoIDs: Int2IntOpenHashMap = _ // assume only blk ID 0 for now, this will be fixed with taskIDs
  private var __highestPntiIDSeen: Int = 2 // skip 0 --> fake PNTI, 1 --> startNode

  val tasks = new JArrayList[PNodeTaskRecorded]()
  private var stringID = new Array[String](16)

  val pntisByBlockID = new JArrayList[Array[PNodeTaskInfo]]()
  pntisByBlockID.add(new Array[PNodeTaskInfo](2)) // for BLOCK_ID_PROPERTY
  pntisByBlockID.get(0)(1) = root.info

  val counters = new JArrayList[OGCounterView[_]]()

  val distributedEvents = new JHashMap[String, DistributedEvents]()

  def distributedEventOf(chainedID: String): DistributedEvents = {
    distributedEvents.computeIfAbsent(chainedID, id => new DistributedEvents(id))
  }

  def taskOf(tid: Int): PNodeTaskRecorded = {
    if (tid == OGSchedulerContext.NODE_ID_ROOT)
      return root // come back to this - multiple roots per process?

    val newID = tasks.size + OGSchedulerContext.NODE_ID_FIRST_VALID
    var virtualID = __inProcessTaskIDs.putIfAbsent(tid, newID)
    if (virtualID == 0) { // default return Int2IntOpenHashMap is 0
      virtualID = newID
      tasks.add(new PNodeTaskRecorded(virtualID))
    }
    tasks.get(virtualID - OGSchedulerContext.NODE_ID_FIRST_VALID)
  }

  def infoOf(iid: Int, blk: Int): PNodeTaskInfo = {
    val blockID = if (ignoreBlocks) 0 else blk
    while (pntisByBlockID.size <= blockID) {
      pntisByBlockID.add(new Array[PNodeTaskInfo](16))
    }
    var pntis = pntisByBlockID.get(blockID)

    val newID =
      if (__inProcessInfoIDs eq null) iid
      else {
        var virtualID = __inProcessInfoIDs.putIfAbsent(iid, __highestPntiIDSeen)
        if (virtualID == 0) {
          virtualID = __highestPntiIDSeen // can't use ArrayList.size here
          __highestPntiIDSeen += 1
        }
        virtualID
      }

    if (pntis.length <= newID) {
      pntis = JArrays.copyOf(pntis, newID * 2)
      pntisByBlockID.set(blockID, pntis)
    }
    var pnti = pntis(newID)
    if (pnti eq null) {
      pnti = new PNodeTaskInfoRecorded(newID, ctx.process)
      pntis(newID) = pnti
    }
    pnti
  }

  // come back to string interning
  private def stringOf(id: Int): String = {
    if (id >= stringID.length) "NA"
    else stringID(id)
  }

  private def counterOf(id: Int): OGCounterView[_] = {
    while (counters.size <= id) counters.add(null)
    var counter = counters.get(id)
    if (counter eq null) {
      val counterDesc = OGCounter.counters.get(id)
      if (counterDesc eq null) {
        PThreadContext.log.warn("Unrecognized Counter!")
        counter = new OGCounterView(id, null)
      } else {
        counter = new OGCounterView(id, counterDesc, ctx.process)
        counters.set(id, counter)
      }
    }
    counter
  }

  override def stringID(id: Int, text: String): Unit = {
    if (id >= stringID.length)
      stringID = JArrays.copyOf(stringID, id * 2)
    stringID(id) = text
  }

  override def threadName(id: Long, name: String): Unit = {
    ctx.name = name
  }

  override def enterGraph(timestamp: Long): Unit = {
    val time = timestamp - timeOffset
    ctx.eventRecord.add(new Span(GRAPH_ENTER, time))
    ctx.push(SPAN_RUN, time)
  }

  override def exitGraph(timestamp: Long): Unit = {
    val time = timestamp - timeOffset
    ctx.eventRecord.add(new Span(GRAPH_EXIT, time))
    ctx.lastSpan(SPAN_RUN, time)
  }

  override def enterWait(causalityID: Int, timestamp: Long): Unit = {
    val time = timestamp - timeOffset
    ctx.eventRecord.add(new Span(GRAPH_ENTER_WAIT, time))
    ctx.lastSpan(SPAN_RUN, time)
    val spanWait = if (causalityID > 0) SPAN_WAIT_SYNC else SPAN_WAIT
    ctx.push(spanWait, time)
  }

  override def exitWait(timestamp: Long): Unit = {
    val time = timestamp - timeOffset
    ctx.eventRecord.add(new Span(GRAPH_EXIT_WAIT, time))
    ctx.lastSpan(SPAN_WAIT, time)
    ctx.push(SPAN_RUN, time)
  }

  override def enterSpin(timestamp: Long): Unit = {
    val time = timestamp - timeOffset
    ctx.eventRecord.add(new PThreadContext.Span(GRAPH_ENTER_SPIN, time))
    // Spin signal can appear inside of wait and we will ignore it
    if (ctx.lastSpan(SPAN_RUN, time))
      ctx.push(SPAN_SPIN, time)
  }

  override def exitSpin(timestamp: Long): Unit = {
    val time = timestamp - timeOffset
    ctx.eventRecord.add(new PThreadContext.Span(GRAPH_EXIT_SPIN, time))
    // Spin signal can appear inside of wait and we will ignore it
    if (ctx.lastSpan(SPAN_SPIN, time))
      ctx.push(SPAN_RUN, time)
  }

  // Local and remote distribution call this method
  override def publishNodeSending(
      id: Int,
      pid: Int,
      blk: Int,
      chainedID: String,
      label: String,
      timestamp: Long): Unit = {
    val event = distributedEventOf(chainedID)
    val task = taskOf(id)
    task.info = infoOf(pid, OGTrace.BLOCK_ID_PROPERTY)
    event.sentTask = task
    event.sentTime = timestamp - timeOffset
    event.sentThreadContext = ctx
    event.label = label
  }

  /** Recorded for distributed/remoted nodes See: publishNodeReceivedLocally */
  override def publishNodeReceived(id: Int, pid: Int, blk: Int, chainedID: String, timestamp: Long): Unit = {
    val event = distributedEventOf(chainedID)
    val task = taskOf(id)
    task.info = infoOf(pid, OGTrace.BLOCK_ID_PROPERTY)
    event.receivedTask = task
    event.receivedTime = timestamp - timeOffset
    event.receivedThreadContext = ctx
  }

  /** Recorded for locally executed nodes See: publishNodeReceived */
  override def publishNodeReceivedLocally(id: Int, pid: Int, blk: Int, chainedID: String, timestamp: Long): Unit = {
    val event = distributedEventOf(chainedID)
    val task = taskOf(id)
    task.info = infoOf(pid, OGTrace.BLOCK_ID_PROPERTY)
    event.receivedTask = task
    event.receivedTime = timestamp - timeOffset
    event.receivedThreadContext = ctx
  }

  /** Recorded for distributed/remoted nodes */
  override def publishNodeStarted(id: Int, chainedID: String, timestamp: Long): Unit = {
    val event = distributedEventOf(chainedID)
    event.startedTime = timestamp - timeOffset
    event.startedThreadContext = ctx
  }

  /** Recorded for distributed/remoted nodes */
  override def publishTaskCompleted(id: Int, chainedID: String, timestamp: Long): Unit = {
    val event = distributedEventOf(chainedID)
    event.completedTime = timestamp - timeOffset
    event.completedThreadContext = ctx
  }

  /** Recorded for local and distributed/remoted nodes */
  override def publishNodeResultReceived(id: Int, chainedID: String, timestamp: Long): Unit = {
    val event = distributedEventOf(chainedID)
    event.resultReceivedTime = timestamp - timeOffset
    event.resultReceivedThreadContext = ctx
  }

  override def publishSerializedNodeResultArrived(id: Int, chainedID: String, timestamp: Long): Unit = {
    val event = distributedEventOf(chainedID)
    event.serializedResultArrivedTime = timestamp - timeOffset
    event.serializedResultArrivedThreadContext = ctx
  }

  override def postComplete(blk: Int, pid: Int, postCompleteTime: Long): Unit = {
    var pnti: PNodeTaskInfo = null
    pnti = infoOf(pid, blk)
    pnti.postCompleteAndSuspendTime += postCompleteTime
  }

  private def lookupEndCommon(blk: Int, pid: Int, collisionCount: Int, time: Long): PNodeTaskInfo = {
    val pnti = infoOf(pid, blk)
    pnti.cacheTime += time
    pnti.collisionCount += collisionCount
    pnti
  }

  override def cacheHit(blk: Int, pid: Int, collisionCount: Int, time: Long): Unit = {
    val pnti = lookupEndCommon(blk, pid, collisionCount, time)
    pnti.cacheHit += 1
  }

  override def cacheMiss(blk: Int, pid: Int, collisionCount: Int, time: Long): Unit = {
    val pnti = lookupEndCommon(blk, pid, collisionCount, time)
    pnti.cacheMiss += 1
  }

  override def cacheProxy(blk: Int, pid: Int, hit: Boolean, countMiss: Boolean, ownerTime: Long): Unit = {
    val pnti = infoOf(pid, blk) // underlying node accounting

    // When proxy uses owner (i.e. called proxy.completeFromNode(src)) we should exclude that time, it's not a real use
    pnti.nodeUsedTime -= ownerTime

    if (hit) // ie, hit != key
      pnti.cacheHit += 1
    else if (countMiss)
      pnti.cacheMiss += 1
  }

  override def cacheEvicted(blk: Int, pid: Int): Unit = {
    val pnti = infoOf(pid, blk)
    pnti.evicted += 1
  }

  override def cacheInvalidated(blk: Int, pid: Int): Unit = {
    val pnti = infoOf(pid, blk)
    pnti.invalidated += 1
  }

  override def cacheReuse(blk: Int, pid: Int, ancTime: Long): Unit = {
    val pnti = infoOf(pid, blk)
    pnti.nodeUsedTime += ancTime
  }

  override def tweakLookup(pid: Int, tweakLookupTime: Long): Unit = {
    val pnti = infoOf(pid, OGTrace.BLOCK_ID_UNSCOPED)
    pnti.tweakLookupTime += tweakLookupTime
  }

  override def cacheLookup(blk: Int, iid: Int, time: Long): Unit = {
    val pnti = infoOf(iid, blk)
    pnti.cacheTime += time
  }

  override def reuseCycle(pid: Int, rcount: Int): Unit = {
    val pnti = infoOf(pid, OGTrace.BLOCK_ID_UNSCOPED)
    pnti.reuseCycle = rcount
  }

  override def reuseStats(pid: Int, register: Long): Unit = {
    val pnti = infoOf(pid, OGTrace.BLOCK_ID_UNSCOPED)
    pnti.reuseStats = register
  }

  override def nodeHashCollision(pid: Int): Unit = {
    val pnti = infoOf(pid, OGTrace.BLOCK_ID_UNSCOPED)
    pnti.overInvalidated += 1
  }

  override def counterEvent(serializedEvent: Array[Byte]): Unit = {
    val bis = new ByteArrayInputStream(serializedEvent)
    val in = new ObjectInputStream(bis)
    val event = in.readObject().asInstanceOf[OGEvent]
    event.time = event.time - timeOffset // [SEE_OFFSET]
    event.process = ctx.process
    counterOf(event.counterID).asInstanceOf[OGCounterView[OGEvent]].events.add(event)
  }

  override def counterEventComplete(counterID: Int, eventID: Int, timestamp: Long): Unit = {
    counterOf(counterID).addCompletion(eventID, timestamp - timeOffset)
  }

  override def summary(
      blk: Int,
      taskId: Int,
      pid: Int,
      rawFirstStartTime: Long,
      rawCompletedTime: Long,
      selfTime: Long,
      ancSelfTime: Long,
      suspendingTime: Long,
      childNodeLookupCount: Int,
      childNodeLookupTime: Long,
      enqueuingPropertyId: Int,
      initAsCompleted: Boolean): Unit = {
    // TODO (OPTIMUS-25314): debugging aid
    if (pid < 0) {
      val dbgPntis = if (pntisByBlockID.isEmpty) "n/a" else pntisByBlockID.get(0).length.toString
      val counter = NodeTaskInfoRegistry.currentTotal()
      val msg =
        s"Error reading NODE_SUMMARY, iid = $pid taskId = $taskId pnti[0].length = $dbgPntis _counter = $counter"
      throw new GraphException(msg)
    }

    val firstStartTime = rawFirstStartTime - timeOffset
    val completedTime = rawCompletedTime - timeOffset

    if (constructPNodes) {
      val tsk = taskOf(taskId)
      tsk.info = infoOf(pid, OGTrace.BLOCK_ID_PROPERTY) // TODO (OPTIMUS-44108): rationalize blk ID
      tsk.firstStartTime = firstStartTime
      tsk.completedTime = completedTime
      tsk.selfTime = selfTime
      tsk.ancSelfTime = ancSelfTime
      tsk.postCompleteAndSuspendTime = suspendingTime
      tsk.childNodeLookupCount = childNodeLookupCount
      tsk.childNodeLookupTime = childNodeLookupTime
    }

    val pnti = infoOf(pid, blk)
    if (!initAsCompleted) pnti.start += 1 // want the timestamps for these for more sensible timeline, but no starts
    pnti.selfTime += selfTime
    pnti.ancAndSelfTime += ancSelfTime + selfTime
    pnti.postCompleteAndSuspendTime += suspendingTime
    // note that we don't update postComplete time here because we generally haven't seen the event yet (except in
    // cases where we see events out of order) - it's handled in NODE_POST_COMPLETE directly
    if (firstStartTime != 0) pnti.wallTime += completedTime - firstStartTime
    pnti.childNodeLookupCount += childNodeLookupCount
    pnti.childNodeLookupTime += childNodeLookupTime
    if (enqueuingPropertyId != 0)
      pnti.enqueuingProperty = enqueuingPropertyId
  }

  private def combineHash(scenarioStackHash: Int, entityHash: Int, argsHash: Int): Int = {
    (((scenarioStackHash * 31) + entityHash) * 31) + argsHash
  }

  override def summaryHashes(
      tid: Int,
      pid: Int,
      ssHash: Int,
      entityHash: Int,
      argsHash: Int,
      resultHash: Int,
      ssIdHash: Int,
      entityIdHash: Int,
      argsIdHash: Int,
      resultIdHash: Int): Unit = {
    val pnti = infoOf(pid, OGTrace.BLOCK_ID_PROPERTY)

    if (constructPNodes) {
      val tsk = taskOf(tid)
      if (tsk.values eq null) tsk.values = new PNodeTaskRecorded.Values()
      tsk.values.resultHash = resultHash
      tsk.values.thisHash = entityHash
      tsk.values.argHash = argsHash
    }

    if (pnti.distinct eq null) pnti.distinct = new PNodeTaskInfo.Distinct()
    val distinct = pnti.distinct

    val combinedHash: Int = combineHash(ssHash, entityHash, argsHash)
    // intentionally use ssHash here as lots of value-equal scenario stacks are created,
    // but they don't appear to cause any significant performance issues (beyond amount of garbage)
    val combinedIdHash: Int = combineHash(ssHash, entityIdHash, argsIdHash)

    if (ssHash != 0) distinct.scenarioStacks.addEx(ssHash)
    if (entityHash != 0) distinct.entities.addEx(entityHash)
    if (argsHash != 0) distinct.args.addEx(argsHash)
    if (resultHash != 0) distinct.results.addEx(resultHash)
    if (combinedHash != 0) distinct.combined.addEx(combinedHash)
    if (ssIdHash != 0) distinct.IdScenarioStacks.addEx(ssIdHash)
    if (entityIdHash != 0) distinct.IdEntities.addEx(entityIdHash)
    if (argsIdHash != 0) distinct.IdArgs.addEx(argsIdHash)
    if (resultIdHash != 0) distinct.IdResults.addEx(resultIdHash)
    if (combinedIdHash != 0) distinct.IdCombined.addEx(combinedIdHash)
  }

  override def profileDesc(
      id: Int,
      flags: Long,
      name: String,
      pkgName: String,
      modifier: String,
      cacheName: String,
      cachePolicy: String): Unit = {
    val pnti = infoOf(id, OGTrace.BLOCK_ID_PROPERTY)
    pnti.flags = flags
    pnti.name = name
    pnti.pkgName = pkgName
    pnti.modifier = modifier
    pnti.cacheName = cacheName
    pnti.cachePolicy = cachePolicy
  }

  override def profileData(
      id: Int,
      start: Long,
      cacheHit: Long,
      cacheMiss: Long,
      cacheHitTrivial: Int,
      evicted: Long,
      invalidated: Int,
      reuseCycle: Int,
      reuseStats: Long,
      selfTime: Long,
      ancAndSelfTime: Long,
      postCompleteAndSuspendTime: Long,
      tweakLookupTime: Long,
      wallTime: Long,
      cacheTime: Long,
      nodeUsedTime: Long,
      tweakID: Int,
      tweakDependencyMask: String): Unit = {

    val pnti = infoOf(id, OGTrace.BLOCK_ID_PROPERTY)
    pnti.start = start
    pnti.cacheHit = cacheHit
    pnti.cacheMiss = cacheMiss
    pnti.cacheHitTrivial = cacheHitTrivial
    pnti.evicted = evicted
    pnti.invalidated = invalidated
    pnti.reuseCycle = reuseCycle
    pnti.reuseStats = reuseStats
    pnti.selfTime = selfTime
    pnti.ancAndSelfTime = ancAndSelfTime
    pnti.postCompleteAndSuspendTime = postCompleteAndSuspendTime
    pnti.tweakLookupTime = tweakLookupTime
    pnti.wallTime = wallTime
    pnti.cacheTime = cacheTime
    pnti.nodeUsedTime = nodeUsedTime
    pnti.tweakID = tweakID
    pnti.tweakDependencies = TPDMask.stringDecoded(tweakDependencyMask)
  }
}
