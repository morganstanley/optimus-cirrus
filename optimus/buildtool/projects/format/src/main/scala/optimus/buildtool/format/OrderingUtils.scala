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
package optimus.buildtool.format

import optimus.buildtool.config.DependencyId
import optimus.buildtool.config.Id
import optimus.buildtool.config.ModuleSetId
import optimus.buildtool.config.OrderedElement

import scala.collection.compat._
import scala.collection.immutable.Seq

object OrderingUtils {
  private val shouldReportWarnings =
    sys.props.get("optimus.buildtool.format.ordering.warnings").exists(_.toBoolean)

  private val versionRegex = "\\d+(?:[_.-]\\d+)*".r
  private val separators = Array('.', '-', '_')

  final case class NestedDefinitions[T](loaded: T, groupName: String, keyName: String, line: Int)
      extends OrderedElement[DependencyId] {
    override def id: DependencyId = DependencyId(group = groupName, name = keyName)
  }

  trait NamedOrdering[A] extends Ordering[A] {
    def name(a: A): String
  }

  class PathOrdering[A <: Id] extends NamedOrdering[A] {

    def name(a: A): String = a.properPath

    def compareParts(x: String, y: String): Int =
      (x.headOption, y.headOption) match {
        case (Some(c1), Some(c2)) if (c1.isDigit && c2.isDigit) => // both are digits, we should now compare versions
          val match1 = versionRegex.findFirstIn(x).get // this is safe because both contain digits
          val match2 = versionRegex.findFirstIn(y).get

          val parts1 = match1.split(separators)
          val parts2 = match2.split(separators)

          val comparisonResult = (parts1 zip parts2).find { case (xs, ys) => xs != ys } match {
            case Some((x, y)) => x.toInt compare y.toInt
            case None         => parts1.length compare parts2.length
          }

          if (comparisonResult == 0) compareParts(x.drop(match1.length max 1), y.drop(match2.length max 1))
          else comparisonResult
        case (Some(c1), Some(c2)) =>
          val comparisonResult = c1 compare c2
          if (comparisonResult == 0) compareParts(x.tail, y.tail) else comparisonResult
        case _ => x compare y
      }

    override def compare(x: A, y: A): Int = {
      val common = x.elements.zip(y.elements)
      common.find { case (xe, ye) => xe.toLowerCase != ye.toLowerCase } match {
        case Some((xe, ye)) =>
          compareParts(xe.toLowerCase, ye.toLowerCase)
        case None =>
          x.elements.length.compare(y.elements.length)
      }
    }
  }

  implicit def pathOrdering[A <: Id]: PathOrdering[A] = new PathOrdering[A]()

  implicit object ModuleSetOrdering extends NamedOrdering[ModuleSetId] {
    override def compare(x: ModuleSetId, y: ModuleSetId): Int = x.name.compareTo(y.name)
    override def name(a: ModuleSetId): String = a.name
  }

  private val Diagnostic: (String, ObtFile, Int) => Message =
    if (shouldReportWarnings) Warning else Error

  def checkOrderingIn[A](obtFile: ObtFile, elements: Seq[OrderedElement[A]])(implicit
      ord: NamedOrdering[A]): Seq[Message] = {
    val current = elements.sortBy(el => (el.line, el.id))
    val expected = current.sortBy(_.id)
    val firstNotSorted = current.iterator.zip(expected.iterator).filter { case (a, b) => a != b }.take(1)

    def message(expected: OrderedElement[A], actual: OrderedElement[A]): String =
      s"Wrong ordering: ${ord.name(expected.id)} (line: ${expected.line}) should be defined before: ${ord.name(
          actual.id)} (line: ${actual.line})"

    firstNotSorted
      .flatMap { case (actual, expected) =>
        // report in both places as sometimes one of them is out of the PR scope
        Seq(actual.line, expected.line).map(line => Diagnostic(message(expected, actual), obtFile, line))
      }
      .to(Seq)
  }
}
