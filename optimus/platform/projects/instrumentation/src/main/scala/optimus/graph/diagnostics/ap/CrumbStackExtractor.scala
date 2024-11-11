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
import optimus.graph.diagnostics.sampling.SamplingProfiler.LoadData
import optimus.graph.diagnostics.sampling.TaskTracker.AppInstance
import optimus.breadcrumbs.crumbs.Properties.MapStringToJsonOps
import optimus.breadcrumbs.crumbs.Properties
import spray.json._
import DefaultJsonProtocol._
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.diagnostics.ap.CrumbStackExtractor.CrumbMap
import optimus.graph.diagnostics.sampling.SampleCrumbConsumer
import optimus.platform.util.Log
import optimus.utils.CountLogger

import java.util.concurrent.ConcurrentHashMap
import java.util.regex.MatchResult
import java.util.regex.Pattern
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import optimus.scalacompat.collection._
object CrumbStackExtractor extends Log {

  type CrumbMap = Map[String, JsValue]
  type Sid2Frames = util.Map[String, Seq[Int]]

  private val q = "\""
  private val ProfStacksMarker = s"$q${Properties.profStacks.name}$q:[{"
  def likelyStacksSample(s: String): Boolean = s.contains(ProfStacksMarker)
  private val ProfCollapsedMarker = s"$q${Properties.profCollapsed}$q:$q"
  def likelyStacksFrames(s: String): Boolean = s.contains(ProfCollapsedMarker)
  private val StackPathMarker = s"$q${Properties.profMS}$q:$q"
  def likelyStackPathFrames(s: String): Boolean = s.contains(StackPathMarker)

  final case class FullSampleData(
      source: String,
      samples: Array[Sample],
      sidFrames: Sid2Frames,
      speedscope: Speedscope,
      failures: Array[String] = Array.empty)

  final case class Sample(
      key: SampleKey,
      stackValue: Long,
      stackId: String,
      loadData: LoadData,
      private val extractor: CrumbStackExtractor) {
    def frames: Iterable[String] = extractor.frames(stackId).getOrElse(Iterable.empty)

    def fullString = s"Sample($key, $stackValue $stackId $loadData, ${frames.mkString("->")})"
  }

  final case class SampleKey(
      stackType: String,
      rootId: String,
      appId: String,
      tStart: Long,
      tSnap: Long,
      extraLabels: Map[String, String] = Map.empty) {
    def appInstance = AppInstance(rootId, appId)
  }

  final case class AccruedValue(
      var n: Int = 0,
      var cpu: Double = 0.0,
      var work: Double = 0.0,
      var sid2weight: java.util.Map[String, Long] = new ConcurrentHashMap()) {
    def loadData = LoadData(cpu, work)

    def condition(): AccruedValue = {
      sid2weight = sid2weight.asScala.filter(_._2 > 0).asJava
      this
    }
    def accrue(stackId: String, stackValue: Long): Long = {
      sid2weight.compute(
        stackId,
        { case (_, prev) =>
          if (prev eq null) stackValue else prev + stackValue
        })
    }
    def accrue(sample: Sample): Unit = synchronized {
      accrue(sample.stackId, sample.stackValue)
      n += 1
      cpu += sample.loadData.cpuLoad
      work += sample.loadData.graphUtilization
    }
  }

  def mergeIntoSpeedscope(
      key2Accrued: Map[String, AccruedValue],
      sid2frames: Sid2Frames,
      speedscope: Speedscope): Unit = {
    val count = new CountLogger("Accruing speedscope", 5000, log)
    for {
      (tpe, flame) <- key2Accrued
      (sid, weight) <- flame.sid2weight.asScala
      fids <- Option(sid2frames.get(sid)).toIterable
    } {
      count()
      // Conflates all other aspects of key together
      speedscope.addTrace(tpe, weight, fids.toArray)
    }
    count.done()
  }

  private lazy val lorem = new Iterator[String] {
    val words =
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque elit velit, rutrum ut tincidunt et, elementum et mauris. Maecenas pellentesque consectetur risus, in venenatis nulla euismod in. Vivamus euismod, diam at posuere ultricies, quam ipsum ullamcorper sem, vel lobortis est est eu lorem. Donec eros lectus, fringilla ac commodo vitae, facilisis rhoncus turpis. Phasellus ante elit, consequat non consectetur a, sodales sit amet elit. Vestibulum neque libero, accumsan et commodo et, elementum sodales nisi. Donec lobortis dictum sem sit amet ultricies. Nam eget augue sit amet lacus semper hendrerit nec vitae ipsum. Sed vestibulum risus placerat lectus volutpat suscipit. Suspendisse potenti. Ut non mi at odio ultrices laoreet id sed nisi. Proin tincidunt pretium bibendum. Morbi hendrerit porttitor leo sed fermentum. Vivamus malesuada vestibulum neque, adipiscing tempor dolor accumsan nec"
        .split("[\\., ]+")
        .toList
    var curr = words
    override def hasNext: Boolean = true
    override def next(): String = synchronized {
      val w :: ws = curr
      curr = if (ws.isEmpty) words else ws
      w
    }
  }
  private lazy val splitter = Pattern.compile("[^\\W_]+")
  private lazy val loremized = new ConcurrentHashMap[String, String]()

  class TestCrumbConsumer(throwOnInconsistency: Boolean = true, stackPath: Boolean = false)
      extends SampleCrumbConsumer {
    private val crumbsRef = new AtomicReference[List[Crumb]](List.empty)
    override def consume(id: ChainedID, source: Crumb.Source, elems: Elems): Unit = {
      val crumb = PropertiesCrumb(id, source, elems)
      crumbsRef.updateAndGet(crumb :: _)
    }
    def crumbs: Seq[Crumb] = crumbsRef.get()
    def samples: Seq[Sample] = {
      val crumbs = crumbsRef.get()
      val extractor = new CrumbStackExtractor(throwOnInconsistency = throwOnInconsistency, stackPath = stackPath)
      val crumbsMap = crumbs.map(_.asJMap)
      val samples = crumbsMap.map(extractor.crumbToSamples).flatten
      crumbsMap.foreach(extractor.incorporateFrames(_, true))
      samples
    }
  }

}

class CrumbStackExtractor(
    anonymize: Boolean = false,
    throwOnInconsistency: Boolean = false,
    withVerboseLabels: Boolean = false,
    crumbFilter: CrumbMap => Boolean = _ => true,
    val stackPath: Boolean = false)
    extends Log {
  import CrumbStackExtractor._

  val speedscope = new Speedscope(cleanLambdas = true)
  val sid2frames = new ConcurrentHashMap[String, Seq[Int]]

  private val knownSIDs = ConcurrentHashMap.newKeySet[String]()
  private val interner = new ConcurrentHashMap[AnyRef, AnyRef]
  private def intern[T <: AnyRef](t: T): T = interner.computeIfAbsent(t, identity[AnyRef]).asInstanceOf[T]

  private val ignored = new AtomicInteger(0)

  def info =
    s"interned=${interner.size}, sids=${knownSIDs.size}, ignored=${ignored.get()}, distinctStacks=${sid2frames.size()}"

  def results: (util.Map[String, Seq[Int]], Speedscope) = (sid2frames, speedscope)

  private def processFrame(fqmn: String): String =
    if (!anonymize || fqmn.matches("\\w+")) fqmn
    else {
      def f(word: String): String = if (word.isEmpty) word
      else
        loremized.computeIfAbsent(
          word,
          { word =>
            if (word.head.isUpper) lorem.next().capitalize else lorem.next()
          })
      splitter.matcher(fqmn).replaceAll { mr: MatchResult => f(mr.group(0)) }
    }

  def crumbToSamples(crumb: CrumbMap): Iterable[Sample] = {
    if (!crumbFilter(crumb)) {
      ignored.incrementAndGet()
      Iterable.empty
    } else
      for {
        pulse <- crumb.getAsMap(Properties.pulse).toIterable
        rid <- crumb.getAs[String]("uuid").toIterable
        stacks <- pulse.getAsSeqMap(Properties.profStacks).toIterable
        tSnap <- pulse.geti(Properties.snapTimeMs)
        snapPeriod <- pulse.geti(Properties.snapPeriod)
        stack <- stacks
        self <- stack.get(Properties.pSlf)
        tpe <- stack.get(Properties.pTpe)
        sid <- stack.get(Properties.pSID)
      } yield {
        val aid = (crumb.getKey(Properties.appId) orElse pulse.getKey(Properties.appIds).flatMap(_.headOption))
          .getOrElse("UnknownApp")
        val cores = pulse.getKeyOrElse(Properties.cpuCores, 1)
        val cpu = pulse.getKeyOrElse(Properties.profJvmCPULoad, 0.0)
        val work = pulse.getKeyOrElse(Properties.profWorkThreads, 1).toDouble
        val extraLabelsMap =
          if (withVerboseLabels) {
            val gsfEng = crumb.getKey(Properties.gsfEngineId)
            gsfEng.fold(Map.empty[String, String])(eng => Map(Properties.gsfEngineId.name -> intern(eng)))
          } else Map.empty[String, String]

        val key = SampleKey(
          stackType = intern(tpe),
          tStart = tSnap - snapPeriod,
          tSnap = tSnap,
          rootId = intern(rid.takeWhile(_ != '#')),
          appId = intern(aid),
          extraLabels = extraLabelsMap)
        val load = LoadData(cpu, work / cores).round(10)
        val isid = intern(sid)
        knownSIDs.add(isid)
        Sample(key = intern(key), stackValue = self, stackId = isid, loadData = intern(load), extractor = this)
      }
  }

  private def collapsed2Frames(collapsed: String): Seq[Int] =
    collapsed
      .split(';')
      .map(m => speedscope.methodIndex(processFrame(m)))
      .toSeq

  def frames(sid: String): Option[Iterable[String]] = Option(sid2frames.get(sid)).map(speedscope.frames(_))

  def allFrames: Map[String, Iterable[String]] = sid2frames.asScala.toMap.mapValuesNow(speedscope.frames(_))

  private val stackCache = mutable.HashMap.empty[String, StackNode]
  class StackNode(val frame: String, val fid: Int, val parent: StackNode) {
    private val sn = this
    def fids: Iterator[Int] = {
      var ll: List[StackNode] = Nil
      var node = sn
      while (node ne null) {
        ll = node :: ll
        node = node.parent
      }
      ll.iterator.map(_.fid)
    }
  }

  private val waitingForDefinitions = mutable.HashMap.empty[String, List[StackNode => Unit]]

  private def extractAndSave(encoded: String, parent: StackNode): StackNode = {
    assert(encoded.startsWith("="))
    val i = encoded.indexOf('=', 1) + 1
    assert(i > 1 && i < encoded.size)
    val m = encoded.substring(i)
    val fid = speedscope.methodIndex(processFrame(m))
    val sn = new StackNode(m, fid, parent)
    val idx = encoded.substring(1, i - 1)
    stackCache.put(idx, sn)
    waitingForDefinitions.remove(idx).foreach { _.foreach(_(sn)) }
    sn
  }

  private def processStackPathTail(sid: String, head: StackNode, it: Iterator[String]): Unit = {
    val fids = ArrayBuffer.empty[Int]
    if (head ne null) fids ++= head.fids
    var prev = head
    it.foreach { encoded =>
      val curr = extractAndSave(encoded, prev)
      prev = curr
      fids += curr.fid
    }
    sid2frames.put(sid, fids)
  }

  def incorporateFrames(crumb: CrumbMap, includeUnknown: Boolean = true): Unit = if (stackPath) {
    for {
      sid <- crumb.get(Properties.pSID)
      collapsed <- crumb.get(Properties.profMS)
    } if (!sid2frames.contains(sid) && (includeUnknown || knownSIDs.contains(sid))) {
      val it = StackAnalysis.SplitIterator(collapsed, ';')
      if (!it.hasNext) return
      val first = it.next()
      if (first.startsWith("==")) {
        assert(first.length > 3)
        val root = first.substring(2)
        stackCache.get(root) match {
          case Some(head) =>
            processStackPathTail(sid, head, it)
          case None =>
            val fns = { head: StackNode =>
              log.info(s"Got definition for $root")
              processStackPathTail(sid, head, it)
            } :: waitingForDefinitions.getOrElseUpdate(root, Nil)
            waitingForDefinitions.put(root, fns)
            log.info(s"Missing definition for $root; added to waitlist (${waitingForDefinitions.size}), ${fns.size}")
        }
      } else {
        val head = extractAndSave(first, null)
        processStackPathTail(sid, head, it)
      }
    }
  } else {
    for {
      sid <- crumb.get(Properties.pSID)
      collapsed <- crumb.get(Properties.profCollapsed)
    } if (includeUnknown || knownSIDs.contains(sid)) {
      if (throwOnInconsistency) {
        sid2frames.compute(
          sid,
          { (_, v) =>
            val stack = collapsed2Frames(collapsed)
            if ((v ne null) && (stack != v)) {
              val msg = s"duplicate pSid $sid pointing at different stacks: $v and $stack"
              throw new IllegalStateException(msg)
            }
            stack
          }
        )
      } else {
        sid2frames.computeIfAbsent(
          sid,
          { _ => collapsed2Frames(collapsed) }
        )
      }
    }
  }
}
