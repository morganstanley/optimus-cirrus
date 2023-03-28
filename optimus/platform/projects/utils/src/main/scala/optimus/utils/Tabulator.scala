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
package optimus.utils

import org.apache.commons.text.WordUtils

object Tabulator {
  type Sizes = Seq[Int]
  type Row = Seq[Any]
  type Table = Seq[Row]
  type OutputtedLines = Seq[String]

  def formatNoTitle(table: Table, minWidth: Int = -1, margin: Int = 1): OutputtedLines =
    format("", table, minWidth = minWidth, margin = margin)

  // Compatible for csv data source where first row contains headers
  def format(
      title: String,
      table: Table,
      headersOnFirstRow: Boolean = false,
      minWidth: Int = -1,
      margin: Int = 1): OutputtedLines = {
    require(margin >= 0)
    table match {
      case Seq()      => Seq.empty
      case Seq(Seq()) => Seq.empty
      case _          =>
        // Pad table rows
        val maxColumnCount = table.map(_.length).max
        val paddedTable = table.map(_.padTo(maxColumnCount, ""))

        // Inject margins
        val marginText = " " * margin
        val titleWithMargins = if (title.nonEmpty) s"$marginText$title$marginText" else title
        val paddedWithMarginTable = paddedTable
          .map { row =>
            row
              .map { cell =>
                val cellContent = Option(cell).map(_.toString()).getOrElse("")
                if (cellContent.nonEmpty) s"$marginText$cellContent$marginText" else ""
              }
          }

        val sizes =
          for (row <- paddedWithMarginTable) yield for (cell <- row) yield cell.length
        val colSizes = adjustColSizesToMinWidth(for (col <- sizes.transpose) yield col.max, minWidth)
        val rows = for (row <- paddedWithMarginTable) yield formatRow(row, colSizes)
        val titleRows: OutputtedLines =
          if (titleWithMargins.nonEmpty)
            Seq(titleSeparator(colSizes)) ++ formatTitle(titleWithMargins, colSizes, margin)
          else Seq.empty[String]
        (titleRows ++ formatRows(rowSeparator(colSizes), rows, headersOnFirstRow)).filter(_.nonEmpty)
    }
  }

  private def widthWithSeparatorsExcludingLastColumn(colSizes: Sizes): Int =
    colSizes.dropRight(1).sum /* Sum of widths */ + colSizes.length - 1 /* inter-cell separators */

  private def widthWithSeparators(colSizes: Sizes): Int =
    colSizes.sum /* Sum of widths */ + colSizes.length - 1 /* inter-cell separators */

  // Extra space goes to the last column
  private def adjustColSizesToMinWidth(colSizes: Sizes, minWidth: Int): Sizes = {
    val contentWidth = widthWithSeparatorsExcludingLastColumn(colSizes)
    colSizes.dropRight(1) ++ Seq(colSizes.last.max(minWidth - contentWidth))
  }

  private[utils] def formatRows(
      rowSeparator: String,
      rows: OutputtedLines,
      headersOnFirstRow: Boolean): OutputtedLines = {
    val headers = if (headersOnFirstRow && rows.nonEmpty) Seq(rowSeparator, rows.head) else Seq.empty
    val contentRows = if (headersOnFirstRow) rows.tail else rows
    headers ++
      (rowSeparator ::
        contentRows.toList :::
        rowSeparator ::
        List[String]())
  }

  private def formatTitle(title: String, colSizes: Sizes, margin: Int): Seq[String] = {
    val marginText = " " * margin
    val wrapLength = (widthWithSeparators(colSizes) - 2 * margin).max(1)
    val titleFormat = if (wrapLength > 1) s"|%s%-${wrapLength}s%s|" else "|%s%s%s|"
    title
      .split("\n")
      .flatMap { line =>
        WordUtils.wrap(line, wrapLength, "\n", true).split("\n")
      }
      .map(line => titleFormat.format(marginText, line, marginText))
  }

  private def formatRow(row: Row, colSizes: Sizes): String = {
    val cells = for ((item, size) <- row.zip(colSizes)) yield if (size == 0) "" else s"%-${size}s".format(item)
    cells.mkString("|", "|", "|")
  }

  private[utils] def titleSeparator(colSizes: Sizes): String = rowSeparator(Seq(widthWithSeparators(colSizes)))

  private[utils] def rowSeparator(colSizes: Sizes): String = colSizes map { "-" * _ } mkString ("+", "+", "+")
}
