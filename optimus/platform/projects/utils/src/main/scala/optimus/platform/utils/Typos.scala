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
package optimus.platform.utils

object Typos {
  def levenshtein(s1: String, s2: String): Int = {
    import scala.math.min
    def minimum(i1: Int, i2: Int, i3: Int) = min(min(i1, i2), i3)
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) =>
      if (j == 0) i else if (i == 0) j else 0
    }

    for (j <- 1 to s2.length; i <- 1 to s1.length)
      dist(j)(i) =
        if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
        else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)

    dist(s2.length)(s1.length)
  }

  sealed trait Suggestion {
    def updated(input: String, choice: String): Suggestion
  }
  case object Exact extends Suggestion {
    def updated(input: String, choice: String): Suggestion = this
  }

  final case class Partial(values: Set[String], distance: Int) extends Suggestion {
    def updated(input: String, choice: String): Suggestion = levenshtein(choice, input) match {
      case 0                  => Exact
      case d if d > distance  => this
      case d if d == distance => copy(values = values + choice)
      case d if d < distance  => Partial(Set(choice), d)
    }
  }
  final case object NoMatch extends Suggestion {
    def updated(input: String, choice: String): Suggestion = levenshtein(choice, input) match {
      case 0                      => Exact
      case d if d >= input.length => this
      case d                      => Partial(Set(choice), d)
    }
  }

  /**
   * Returns suggestions that might match a given user input taken from a valid set of choices. In case of a partial
   * match, the returned suggestions are the elements from choices that are most similar to the input string, with
   * multiple elements returned in case there are equally dissimilar choices.
   */
  def suggest(input: String, choices: Set[String]): Suggestion = {
    choices.foldLeft[Suggestion](NoMatch)(_.updated(input, _))
  }

}
