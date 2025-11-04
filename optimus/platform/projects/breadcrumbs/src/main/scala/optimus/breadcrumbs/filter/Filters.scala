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
package optimus.breadcrumbs.filter

import scala.collection.mutable.ArrayBuffer
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.CrumbHint
import optimus.breadcrumbs.filter.Result.Neutral
import org.slf4j.Logger
import org.slf4j.LoggerFactory

sealed trait Result
object Result {
  object Deny extends Result
  object Accept extends Result
  object Neutral extends Result
}

trait CrumbFilter {
  def filter(c: Crumb): Result
  protected[breadcrumbs] val onMatch: Result
  protected[breadcrumbs] val onMismatch: Result

  final def matches(c: Crumb): Boolean = filter(c) == onMatch
  final def mismatches(c: Crumb): Boolean = filter(c) == onMismatch
}

object CrumbFilter {
  // This is measured by the crumb's length of the compact print of the json string
  private[optimus] val DEFAULT_MAX_CRUMB_SIZE: Integer =
    Integer.getInteger("optimus.breadcrumbs.filter.maxCrumbSize", 8000)

  private val log: Logger = LoggerFactory.getLogger(CrumbFilter.getClass)

  private[optimus] val SOURCE_REGEX_ALLOW_LIST = "SourceRegexAllowList"
  private[optimus] val CRUMB_TYPE_ALLOW_LIST = "CrumbTypeAllowList"
  private[optimus] val LARGE_CRUMB_FILTER = "LargeCrumbFilter"
  private[optimus] val CRUMB_HINT_FILTER = "CrumbHintFilter"

  def createFilter(filterConfigs: Seq[Map[String, AnyRef]]): Option[CrumbFilter] = {
    filterConfigs flatMap { config =>
      config.get("name").flatMap {
        case SOURCE_REGEX_ALLOW_LIST =>
          val regex = config
            .getOrElse(
              "pattern",
              throw new RuntimeException(s"No 'pattern' configured for a $SOURCE_REGEX_ALLOW_LIST."))
            .toString
          log.info(s"CrumbFilter - $SOURCE_REGEX_ALLOW_LIST(regex = '$regex')")
          Some(new SourceRegexAllowList(regex))
        case LARGE_CRUMB_FILTER =>
          val maxCrumbSize =
            config.get("threshold").map(t => Integer.valueOf(t.toString)).getOrElse(DEFAULT_MAX_CRUMB_SIZE)
          log.info(s"CrumbFilter - $LARGE_CRUMB_FILTER(maxSize = $maxCrumbSize)")
          Some(new LargeCrumbFilter(maxCrumbSize))
        case CRUMB_HINT_FILTER =>
          val hint = config
            .getOrElse("hint", throw new RuntimeException(s"No 'hint' configured for a $CRUMB_HINT_FILTER."))
            .toString
          log.info(s"CrumbFilter - $CRUMB_HINT_FILTER(hint = '$hint')")
          Some(new CrumbHintFilter(CrumbHint.fromString(hint)))
        case CRUMB_TYPE_ALLOW_LIST =>
          val types = config
            .getOrElse("types", throw new RuntimeException(s"No 'types' specified for $CRUMB_TYPE_ALLOW_LIST"))
            .toString
            .split(",")
            .map(_.trim)
            .toSet
          log.info(s"CrumbFilter - $CRUMB_TYPE_ALLOW_LIST($types)")
          Some(new CrumbTypeAllowList(types))
        case n =>
          log.error(s"Unsupported crumb filter $n, with config: $config")
          None
      }
    } match {
      case Seq(f) => Some(f)
      case filters @ Seq(_, _, _*) =>
        val res = new CompositeFilter
        filters.foreach(res.addFilter(_))
        Some(res)
      case _ => None
    }
  }
}

abstract class AbstractFilter(
    protected[breadcrumbs] override val onMatch: Result,
    protected[breadcrumbs] override val onMismatch: Result)
    extends CrumbFilter {
  def filter(crumb: Crumb): Result = Neutral
}

final class SourceRegexAllowList(private[breadcrumbs] val pattern: String)
    extends AbstractFilter(Result.Accept, Result.Deny) {
  private val SourceRegexPattern = pattern.r.pattern
  override def filter(crumb: Crumb): Result = {
    if (SourceRegexPattern.matcher(crumb.source.name).matches) onMatch else onMismatch
  }

  override def toString() = s"${CrumbFilter.SOURCE_REGEX_ALLOW_LIST}[pattern='${pattern}']"
}

final class CrumbTypeAllowList(private[breadcrumbs] val types: Set[String])
    extends AbstractFilter(Result.Accept, Result.Deny) {
  private[this] val processedTypes: Set[String] = types
    .map(_.replaceAll("Crumb$", ""))

  override def filter(crumb: Crumb): Result =
    if (!crumb.source.isFilterable) Result.Accept
    // so that EventPropertiesCrumb will be matched when PropertiesCrumb is included
    else if (processedTypes.exists(crumb.clazz.contains(_))) onMatch
    else onMismatch

  override def toString() = s"${CrumbFilter.CRUMB_TYPE_ALLOW_LIST}[types=${types.mkString("'", "', '", "'")}]"
}

final class LargeCrumbFilter(private[breadcrumbs] val threshold: Integer)
    extends AbstractFilter(onMatch = Result.Deny, onMismatch = Result.Accept) {
  override def filter(crumb: Crumb): Result = {
    if (!crumb.source.isFilterable) Result.Accept
    else if (crumb.asJSON.toString.length > threshold) onMatch
    else onMismatch
  }

  override def toString(): String = s"${CrumbFilter.LARGE_CRUMB_FILTER}[threshold=$threshold]"
}

final class CrumbHintFilter(private[breadcrumbs] val hint: CrumbHint)
    extends AbstractFilter(Result.Accept, Result.Deny) {
  override def filter(crumb: Crumb): Result =
    if (crumb.hints contains hint) onMatch
    else onMismatch

  override def toString(): String = s"${CrumbFilter.CRUMB_HINT_FILTER}[hint=$hint]"
}

object CompositeFilter {
  def unapply(arg: CompositeFilter): Option[CompositeFilter] = Some(arg)
}

final class CompositeFilter extends AbstractFilter(Result.Accept, Result.Deny) with Iterable[CrumbFilter] {
  private val filters = ArrayBuffer[CrumbFilter]()

  override def iterator: Iterator[CrumbFilter] = filters.toIterator

  override def filter(crumb: Crumb): Result = if (crumb.source.isFilterable) {
    // Here we want the very first onMismatch result (if any), and we also don't want to apply any filters unnecessarily
    // But still, we have to enumerate through all the filters there (without applying those after a match, though)
    // Ideally, we shouldn't even need to iterate through all filters here, but Scala doesn't seem to have a construct for that
    filters.foldLeft(Result.Neutral: Result) { case (result, filter) =>
      if (result == this.onMismatch) result
      else filter.filter(crumb)
    } match {
      case Result.Neutral => Result.Accept
      case result         => result
    }
  } else Result.Accept

  def getCurrentFilters: Iterable[CrumbFilter] = filters.toVector

  def addFilter(filter: CrumbFilter): Unit = {
    filter match {
      case cf: CompositeFilter => filters ++= cf.getCurrentFilters
      case _                   => filters += filter
    }
  }

  override def toString = s"${getClass.getSimpleName}[filters=${filters.mkString("; ")}]"
}
