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
package optimus.observability

import optimus.config.OptconfContent
import optimus.config.OptconfPath
import optimus.config.OptconfProvider
import optimus.exceptions.RTExceptionTrait
import optimus.graph.diagnostics.gridprofiler.Filter
import optimus.graph.diagnostics.gridprofiler.FilterType
import optimus.graph.diagnostics.gridprofiler.HotspotFilter
import optimus.graph.diagnostics.gridprofiler.Level
import optimus.graph.diagnostics.gridprofiler.Level.Level
import optimus.graph.diagnostics.tsprofiler.TemporalSurfProfilingLevel
import optimus.graph.diagnostics.tsprofiler.TemporalSurfProfilingLevel.TemporalSurfProfilingLevel
import optimus.platform._
import org.slf4j.LoggerFactory

import java.time.Duration
import java.time.Instant
import java.util
import scala.jdk.CollectionConverters._

class JobNotSerializableException(msg: String) extends Exception(msg) with RTExceptionTrait
class JobNotDeserializableException(msg: String) extends Exception(msg) with RTExceptionTrait

object JobValue {
  private val logger = LoggerFactory.getLogger(getClass)
  private[observability] def makePrefixedOptconfValue(prefix: String, content: String): String =
    s"${PrefixedJobValue.optconfCategory}[$prefix$content]"
  def toJobValue(o: Any, debugValue: AnyRef = null): JobValue = o match {
    case o: OptconfProvider =>
      // only store content in dal if it does not have a classpath
      if ((o.optconfPath eq null) || !o.optconfPath.startsWith("classpath"))
        PrefixedJobValue(makePrefixedOptconfValue(PrefixedJobValue.optconfContent, o.jsonContent))
      else PrefixedJobValue(makePrefixedOptconfValue(PrefixedJobValue.optconfPath, o.optconfPath))
    case hf: HotspotFilter =>
      PrefixedJobValue(s"${PrefixedJobValue.hotspotFilter}[${hf.filter};${hf.ftype};${hf.ratio}]")
    case tl: TemporalSurfProfilingLevel =>
      PrefixedJobValue(s"${PrefixedJobValue.temporalSurfProfilingCategory}[$tl]")
    case l: Level        => PrefixedJobValue(s"${PrefixedJobValue.levelCategory}[$l]")
    case s: String       => StrJobValue(s)
    case i: Int          => IntJobValue(i)
    case t: Instant      => InstantJobValue(t)
    case b: Boolean      => BoolJobValue(b)
    case d: Duration     => DurationJobValue(d)
    case q: Seq[_]       => SeqJobValue(q.map(v => toJobValue(v)))
    case l: util.List[_] => JListJobValue(l.asScala.map(v => toJobValue(v)))
    case _ =>
      if (debugValue ne null) {
        logger.error("Caught exception while trying to serialize {} [value={}]", debugValue, o)
      }
      throw new JobNotSerializableException("Cannot serialize to DAL object of type " + o.getClass.getName)
  }
}

@embeddable
sealed trait JobValue {
  def value: Any
}

object PrefixedJobValue {
  private val Pattern = "(.+)\\[(.|\\s)+\\]".r
  private[observability] val optconfContent = "Content:"
  private[observability] val optconfPath = "Path:"
  private[observability] val optconfCategory = "optconf"
  private[observability] val temporalSurfProfilingCategory = "tempSurfProfLvl"
  private[observability] val levelCategory = "level"
  private[observability] val hotspotFilter = "hotspotFilter"
}

/**
 * For [[PrefixedJobValue]] the format should be: category[content] where content value of what you want to store and
 * category refers to what type the input deals with.
 *
 * Optconfs: format will be: "optconf[path--content]" (if the path is null it willjust be "optconf[--content]"
 */
@embeddable
final case class PrefixedJobValue(s: String) extends JobValue {
  override def value: AnyRef = s match {
    case PrefixedJobValue.Pattern(category, _) =>
      category match {
        case PrefixedJobValue.optconfCategory =>
          // getting rid of the optconf[...]
          val content = s.drop(PrefixedJobValue.optconfCategory.length + 1).dropRight(1)
          if (content.startsWith(PrefixedJobValue.optconfPath))
            OptconfPath(content.drop(PrefixedJobValue.optconfPath.length))
          else OptconfContent(content.drop(PrefixedJobValue.optconfContent.length))

        case PrefixedJobValue.temporalSurfProfilingCategory =>
          TemporalSurfProfilingLevel.withName(
            s.drop(PrefixedJobValue.temporalSurfProfilingCategory.length + 1).dropRight(1))
        case PrefixedJobValue.levelCategory =>
          Level.withName(s.drop(PrefixedJobValue.levelCategory.length + 1).dropRight(1))
        case PrefixedJobValue.hotspotFilter =>
          val Array(filter, ftype, ratio) = s.drop(PrefixedJobValue.hotspotFilter.length + 1).dropRight(1).split(";")
          HotspotFilter(Filter.withName(filter), FilterType.withName(ftype), ratio.toDouble)
        case _ =>
          throw new JobNotDeserializableException(s"Cannot deserialize prefixed job value with category $category")
      }
    case _ =>
      throw new JobNotDeserializableException(
        "Cannot deserialize prefixed job value not of format \"category[content]\"")
  }
}

@embeddable
final case class StrJobValue(value: String) extends JobValue

@embeddable
final case class IntJobValue(value: Int) extends JobValue

@embeddable
final case class InstantJobValue(value: Instant) extends JobValue

@embeddable
final case class BoolJobValue(value: Boolean) extends JobValue

@embeddable
final case class DurationJobValue(value: Duration) extends JobValue

@embeddable
final case class SeqJobValue(values: Seq[JobValue]) extends JobValue {
  override def value: Seq[_] = values.map(_.value)
}

@embeddable
final case class JListJobValue(values: Seq[JobValue]) extends JobValue {
  override def value: util.List[_] = values.map(_.value).asJava
}
