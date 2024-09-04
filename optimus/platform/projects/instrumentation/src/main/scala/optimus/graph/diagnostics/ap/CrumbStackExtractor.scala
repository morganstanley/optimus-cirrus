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
import optimus.breadcrumbs.crumbs.Crumb
import optimus.graph.diagnostics.ap.CrumbStackExtractor.CrumbMap
import optimus.platform.util.Log
import optimus.utils.CountLogger

import java.util.concurrent.ConcurrentHashMap
import java.util.regex.MatchResult
import java.util.regex.Pattern
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable
import scala.jdk.CollectionConverters._

object CrumbStackExtractor extends Log {

  type CrumbMap = Map[String, JsValue]
  type Sid2Frames = util.Map[String, Seq[Int]]

  private val q = "\""
  private val ProfStacksMarker = s"$q${Properties.profStacks.name}$q:[{"
  def likelyStacksSample(s: String): Boolean = s.contains(ProfStacksMarker)
  private val ProfCollapsedMarker = s"$q${Properties.profCollapsed}$q:$q"
  def likelyStacksFrames(s: String): Boolean = s.contains(ProfCollapsedMarker)

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

  class CrumbConsumer(throwOnInconsistency: Boolean = true) extends Function1[Crumb, Unit] {
    private val crumbsRef = new AtomicReference[List[Crumb]](List.empty)
    override def apply(crumb: Crumb): Unit = {
      crumbsRef.updateAndGet(crumb :: _)
    }
    def crumbs: Seq[Crumb] = crumbsRef.get()
    def samples: Seq[Sample] = {
      val crumbs = crumbsRef.get()
      val extractor = new CrumbStackExtractor(throwOnInconsistency = throwOnInconsistency)
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
    crumbFilter: CrumbMap => Boolean = _ => true) {
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
        total <- stack.get(Properties.pTot)
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
        Sample(key = intern(key), stackValue = total, stackId = isid, loadData = intern(load), extractor = this)
      }
  }

  private def collapsed2Frames(collapsed: String): Seq[Int] =
    collapsed
      .split(';')
      .map(m => speedscope.methodIndex(processFrame(m)))
      .toSeq

  def frames(sid: String): Option[Iterable[String]] = Option(sid2frames.get(sid)).map(speedscope.frames)

  def incorporateFrames(crumb: CrumbMap, includeUnknown: Boolean = true): Unit = for {
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
