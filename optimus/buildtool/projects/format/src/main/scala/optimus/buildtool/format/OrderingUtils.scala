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

import optimus.buildtool.config.Id
import optimus.buildtool.config.OrderedElement

import scala.collection.compat._
import scala.collection.immutable.Seq

object OrderingUtils {

  implicit object PathOrdering extends Ordering[Id] {
    override def compare(x: Id, y: Id): Int = {
      val common = x.elements.zip(y.elements)
      common.find { case (xe, ye) => xe.toLowerCase != ye.toLowerCase } match {
        case Some((xe, ye)) =>
          xe.toLowerCase.compare(ye.toLowerCase)
        case None =>
          x.elements.length.compare(y.elements.length)
      }
    }
  }

  def checkOrderingIn(obtFile: ObtFile, elements: Seq[OrderedElement]): Seq[Message] = {
    val current = elements.sortBy(el => (el.line, el.id))
    val expected = current.sortBy(_.id)
    val firstNotSorted = current.iterator.zip(expected.iterator).filter { case (a, b) => a != b }.take(1)

    def message(expected: OrderedElement, actual: OrderedElement): String =
      s"Wrong ordering: ${expected.id} (line: ${expected.line}) should be defined before: ${actual.id} (line: ${actual.line})"

    firstNotSorted
      .flatMap { case (actual, expected) =>
        // report in both places as sometimes one of them is out of the PR scope
        Seq(actual.line, expected.line).map(line => Error(message(expected, actual), obtFile, line))
      }
      .to(Seq)
  }

  def checkForbiddenDependencies(
      obtFile: ObtFile,
      bundles: Seq[Bundle],
      globalForbiddenDependencies: Seq[ForbiddenDependency]): Seq[Message] = {
    def message(bundle: Bundle, forbiddenDependency: ForbiddenDependency): String =
      s"Cannot have Forbidden Dependency defined at both the global and bundle level: '${forbiddenDependency.name}' (line: ${bundle.line} in bundle '${bundle.id}')"

    for {
      bundle <- bundles
      dep <- bundle.forbiddenDependencies
      if globalForbiddenDependencies.map(_.name).contains(dep.name) && !globalForbiddenDependencies.contains(dep)
    } yield Error(message(bundle, dep), obtFile, bundle.line)
  }
}
