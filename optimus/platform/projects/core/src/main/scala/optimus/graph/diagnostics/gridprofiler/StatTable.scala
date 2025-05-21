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
package optimus.graph.diagnostics.gridprofiler

final case class StatTableCol(title: String, format: String, values: Seq[Any]) {
  lazy val valuesFormatted: Seq[String] = values.map(format.format(_))
  lazy val width: Int = Integer.max(title.length, valuesFormatted.map(_.length).max)
  def titlePadded: String = title.padTo(width, ' ')
  def valuePadded(v: String): String = v.reverse.padTo(width, ' ').reverse
  def spacer: String = "".padTo(width, '-')
}

object StatTableCol {
  def single(title: String, format: String, value: Any) = StatTableCol(title, format, Seq(value))
}

class StatTable(val cols: Seq[StatTableCol]) {
  override def toString: String = {
    cols.map(_.titlePadded).mkString(" ") + "\n" +
      cols.map(_.spacer).mkString("+") + "\n" +
      cols
        .map(col => col.valuesFormatted.map(col.valuePadded(_)))
        .transpose
        .map(row => row.mkString(" "))
        .mkString("\n")
  }
}
