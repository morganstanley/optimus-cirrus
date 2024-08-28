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

import optimus.platform.util.PrettyStringBuilder

final case class Group(name: String, description: String, children: Iterable[ColGroup]) {
  def toJson(s: SummaryTable): String =
    s""""$name": { "title": "$description", ${children.map(_.toJson(s)).mkString(",\n")} }"""
}

final class ColGroup(val col: Col, val children: collection.Seq[ColGroup]) {
  private def buildJson(s: SummaryTable, sb: PrettyStringBuilder): PrettyStringBuilder = {
    sb.append(s""""${col.name}": """)
      .startBlock()
      .append(s""""result": ${col.format.format(col.value(s))}, """)
      .append(if (col.highlight(s)) s""""highlight": true, """ else "")
      .appendln(s""""title": "${col.description}"""")
    if (children.nonEmpty) sb.append(",")
    def func(colGroup: ColGroup): Unit = colGroup.buildJson(s, sb)
    sb.append(children, func _, ",")
    sb.endBlock()
    sb
  }

  def toJson(s: SummaryTable): String =
    buildJson(s, new PrettyStringBuilder().setBlockMarkers("{", "}")).toString
}

final case class Col(
    name: String,
    description: String,
    format: String,
    extractor: SummaryTable => Any,
    highlight: SummaryTable => Boolean = _ => false) {

  def value(s: SummaryTable): Any = extractor(s)
  def statCol(s: SummaryTable): StatTableCol = StatTableCol.single(name, format, value(s))
  def apply(children: ColGroup*): ColGroup = new ColGroup(this, children)
}

object Col {

  /**
   * Expects times in ns and displays them in seconds. Also enforces non-negative times by setting the time to 0 and
   * highlighting the output displayed (see json above), unless explicitly allowed (eg. under-utilized time can be
   * negative)
   */
  def time(
      name: String,
      desc: String,
      format: String,
      extractor: SummaryTable => Long,
      allowNegative: Boolean = false): Col =
    new Col(name, desc + " (s)", format, displayTime(extractor, allowNegative), s => !allowNegative && extractor(s) < 0)

  private def displayTime(extractor: SummaryTable => Long, allowNegative: Boolean)(s: SummaryTable): Double = {
    val nanoTime = extractor(s)
    if (!allowNegative && nanoTime < 0) 0L
    else nanoTime * 1e-9
  }

  def percentage(
      name: String,
      desc: String,
      format: String,
      extractor: SummaryTable => Double,
      highlight: SummaryTable => Boolean = _ => false): Col = {
    new Col(name + " %", desc, format, s => if (extractor(s) < 0) 0.0 else extractor(s) * 100.0, highlight(_))
  }
}
