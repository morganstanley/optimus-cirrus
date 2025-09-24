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
package optimus.graph.diagnostics.ap

import optimus.graph.diagnostics.ap.CrumbParser._
import optimus.graph.diagnostics.ap.StackAnalysis.CleanName
import optimus.platform.util.Log
import optimus.scalacompat.collection._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.compat._
import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

/**
 * final in-memory columnar data for tree build, target on ultra fast filtering and use minimum memory.
 * @param id2Engine unique engine root names, ordered by id
 * @param id2Tpe unique type names, ordered by id
 * @param id2Psid unique pSid names, ordered by id
 * @param id2Frame unique frame names, for example a;b;c == Array('a', 'b', 'c')
 * @param sid2FrameIndexes map from pSid(in flame samples) to frame indexes
 *                         Columnar Samples
 *                All arrays below have the same size
 * @param tpes type
 * @param tStartSeconds start time
 * @param engineRoots engine root
 * @param gsfEngineIds gsf engine
 * @param pSids pSid
 * @param stackValues stack values
 */
class ColumnarFlameSamples(
    val id2Engine: ArraySeq[String],
    val id2Tpe: ArraySeq[String],
    val id2Psid: ArraySeq[String],
    val id2Frame: ArraySeq[CleanName],
    val id2GsfEngineId: ArraySeq[String],
    val sid2FrameIndexes: Map[String, ArraySeq[Int]],
    // All arrays below have the same size
    val tpes: ArraySeq[Int],
    val tStartSeconds: ArraySeq[Int],
    val engineRoots: ArraySeq[Int],
    val gsfEngineIds: ArraySeq[Int],
    val pSids: ArraySeq[Int],
    val stackValues: ArraySeq[Long]
) extends Log {

  val length: Int = stackValues.length

  private def getFrameNodeName(frameIndex: Int): String = id2Frame(frameIndex).name
  def frames(pSid: String): Option[ArraySeq[String]] = {
    val frameIds = sid2FrameIndexes.get(pSid)
    frameIds.map(a => a.map(getFrameNodeName))
  }
}

final case class TestColumnarSamplesBuilder(throwOnInconsistency: Boolean = false)
    extends ColumnarSamplesBuilder(throwOnInconsistency)

/**
 * A builder that constructs columnar data row by row.
 * Each download has its own dedicated builder instance.
 */
abstract class ColumnarSamplesBuilder(throwOnInconsistency: Boolean = false) extends Log {
  private[optimus] val frameBuilder = new FrameBuilder(throwOnInconsistency)
  // tmp vals for parser
  private[optimus] val tpePool = new AtomicInteger(0)
  private[optimus] val engineRootPool = new AtomicInteger(0)
  private[optimus] val gsfEngineIdPool = new AtomicInteger(0)
  private[optimus] val pSidPool = new AtomicInteger(0)
  private[optimus] val tpeToId = new ConcurrentHashMap[String, Int]
  private[optimus] val engineRootToId = new ConcurrentHashMap[String, Int]
  private[optimus] val gsfEngineIdToId = new ConcurrentHashMap[String, Int]
  private[optimus] val pSidToId = new ConcurrentHashMap[String, Int]

  private def getTpeId(tpe: String): Int = tpeToId.computeIfAbsent(tpe, _ => tpePool.getAndIncrement())
  private def getEngineId(engineRoot: String): Int =
    engineRootToId.computeIfAbsent(engineRoot, _ => engineRootPool.getAndIncrement())
  private def getGsfEngineId(gsfEngineId: String): Int =
    gsfEngineIdToId.computeIfAbsent(gsfEngineId, _ => gsfEngineIdPool.getAndIncrement())
  private[optimus] def getPSidId(pSid: String): Int = pSidToId.computeIfAbsent(pSid, _ => pSidPool.getAndIncrement())
  private[optimus] def msToSec(ms: Long): Int = Math.ceil(ms.toDouble / 1000).toInt

  private[optimus] val stackTypeA = ArrayBuffer.empty[Int]
  private[optimus] val tStartSecsA = ArrayBuffer.empty[Int]
  private[optimus] val engineRootA = ArrayBuffer.empty[Int]
  private[optimus] val gsfEngineIdA = ArrayBuffer.empty[Int]
  private[optimus] val pSidA = ArrayBuffer.empty[Int]
  private[optimus] val stackValueA = ArrayBuffer.empty[Long]

  def columnar(
      tpe: String,
      startSec: Int,
      engineRoot: String,
      gsfEngineId: String,
      pSid: String,
      samples: Long): Unit = {
    stackTypeA += getTpeId(tpe)
    tStartSecsA += startSec
    engineRootA += getEngineId(engineRoot)
    gsfEngineIdA += getGsfEngineId(gsfEngineId)
    pSidA += getPSidId(pSid)
    stackValueA += samples
  }

  def toArrayFromMap[X: ClassTag](x2i: ConcurrentHashMap[X, Int]): ArraySeq[X] = {
    val arr = new Array[X](x2i.size)
    x2i.forEach { (name, id) =>
      arr(id) = name
    }
    ArraySeq.from(arr)
  }

  def crumbToProfireSamples(sample: Sample): Unit = {
    sample.pulse.profStacks.foreach { stack =>
      val self = stack.pSlf
      if (self > 0) {
        val tpe = stack.pTpe
        val sid = stack.pSID
        val tSnap = sample.pulse.snapTimeMs
        val snapPeriod = sample.pulse.snapPeriod
        val engineRoot = sample.engineRoot
        val gsfEngineId = sample.gsfEngineId.getOrElse("")
        val tStart = msToSec(tSnap - snapPeriod)
        columnar(tpe, tStart, engineRoot, gsfEngineId, sid, self)
      }
    }
  }

  def addFrame(json: String): Unit = CrumbParser.safeJsonRead[Frame](json).map(frameBuilder.incorporateFrame)
  def addSample(json: String): Unit = CrumbParser.safeJsonRead[Sample](json).map(crumbToProfireSamples)

  // for unit tests
  def framesBeforeComplete(pSid: String): ArraySeq[String] = {
    val currentMap = frameBuilder.buildPSidToFrameIndexesMap()

    currentMap
      .getOrElse(
        pSid, {
          log.warn(s"sample $pSid not found in loaded test frames list!")
          ArraySeq.empty
        })
      .map(frameBuilder.buildIdToFrame()(_).name)
  }
  // for unit tests
  def allFramesBeforeComplete(): Iterable[ArraySeq[String]] = {
    val currentMap = frameBuilder.buildPSidToFrameIndexesMap()
    val frameMap = frameBuilder.buildIdToFrame()
    currentMap.map { case (k, frameIndexes) => frameIndexes.map(frameMap(_).name) }
  }

}

/**
 * A frame builder that constructs pSid to folded frame Map row by row.
 * Each download has its own dedicated builder instance.
 */
class FrameBuilder(throwOnInconsistency: Boolean = false) {

  private val frameNameToId = new ConcurrentHashMap[CleanName, Int]
  private val sid2frames = new ConcurrentHashMap[String, ArraySeq[Int]]
  private val idPool = new AtomicInteger(0)
  // faster than split, we should use while loop to avoid string allocation
  private def collapsed2Frames(collapsed: String, getId: String => Int): ArraySeq[Int] = {
    val frameIds = new ArrayBuffer[Int]()
    val chars = collapsed.toCharArray
    var start = 0
    var i = 0

    while (i <= chars.length) {
      if (i == chars.length || chars(i) == ';') {
        if (i > start) {
          val frame = collapsed.substring(start, i)
          val id = getId(frame)
          frameIds += id
        }
        start = i + 1
      }
      i += 1
    }
    frameIds.to(ArraySeq)
  }

  def buildIdToFrame(): ArraySeq[CleanName] = {
    val arr = new Array[CleanName](frameNameToId.size)
    frameNameToId.forEach { (name, id) =>
      arr(id) = name
    }
    ArraySeq.from(arr)
  }

  def buildPSidToFrameIndexesMap(): Map[String, ArraySeq[Int]] = {
    // this is pSid loaded from the frame samples, which may be different from the pSid in the main samples
    val sid2FrameIndexes = sid2frames.asScala.toMap
    sid2FrameIndexes
  }

  private def getLocalId(frame: String): Int = {
    val cn = CleanName.cleanName(frame, hasFrameNum = false, abbreviate = false, flags = 0)
    frameNameToId.computeIfAbsent(cn, _ => idPool.getAndIncrement())
  }

  def addFrames(pSid: String, profCollapsed: String): Unit = if (throwOnInconsistency) {
    sid2frames.compute(
      pSid,
      { (_, v) =>
        val stack = collapsed2Frames(profCollapsed, { s => getLocalId(s) })
        if ((v ne null) && (stack != v)) {
          val msg = s"duplicate pSid $pSid pointing at different stacks: $v and $stack"
          throw new IllegalStateException(msg)
        }
        stack
      }
    )
  } else sid2frames.computeIfAbsent(pSid, { _ => collapsed2Frames(profCollapsed, { s => getLocalId(s) }) })

  def incorporateFrame(frame: Frame): Unit = addFrames(frame.pSID, frame.profCollapsed)
}
