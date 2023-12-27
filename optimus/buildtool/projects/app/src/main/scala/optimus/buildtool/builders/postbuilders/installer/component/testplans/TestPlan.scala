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
package optimus.buildtool.builders.postbuilders.installer.component.testplans

final case class TestPlan(headers: Seq[String], values: Seq[Seq[String]]) {
  require(values.forall(_.size == headers.size), "Headers and values must be the same length!")

  import TestPlan._

  def text: String =
    s"""${headers.mkString(s" $sep ")}
       |
       |${values.map(_.mkString(s" $sep ")).mkString("\n\n")}""".stripMargin
}

object TestPlan {
  val sep = "@"

  def merge(testplans: Seq[TestPlan]): TestPlan =
    testplans.reduce((a, b) => TestPlan(a.headers, a.values ++ b.values))
}
