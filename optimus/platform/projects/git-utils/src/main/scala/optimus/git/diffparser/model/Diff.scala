import scala.collection.compat._
import scala.collection.mutable
final case class Line(tpe: LineType.Value, content: String) {
  def isNeutral: Boolean = tpe == LineType.Neutral
  def isFrom: Boolean = tpe == LineType.From
  def isTo: Boolean = tpe == LineType.To
}
final case class PartialHunk(fromFileRange: Range, toFileRange: Range, lines: mutable.Buffer[Line]) {
    this(fromFileRange, toFileRange, mutable.Buffer.empty)
  }

  def addLine(line: Line): PartialHunk = {
    this.lines :+ line
    this
  def toHunk(): Hunk = Hunk(fromFileRange, toFileRange, lines.to(Seq))
final case class Hunk(fromFileRange: Range, toFileRange: Range, lines: Seq[Line])

  def fromLine(currentLine: String): PartialHunk = {
      val range1Count = if (matcher.group(2) != null) { matcher.group(2) }
      else { "1" }
      val range2Count = if (matcher.group(4) != null) { matcher.group(4) }
      else { "1" }
      PartialHunk(fromRange, toRange, mutable.Buffer.empty)
  def empty: PartialDiff = PartialDiff(None, None, mutable.Buffer.empty, mutable.Buffer.empty)
    if (fileName.equals(Diff.noFile)) {
    headerLines: mutable.Buffer[String],
    hunks: mutable.Buffer[PartialHunk]) {
  def addLine(line: Line): PartialDiff = {
    hunks.last.lines += line
    this
  }
  def addHunk(hunk: PartialHunk): PartialDiff = {
    hunks += hunk
    this
  }
  def addHeaderLine(line: String): PartialDiff = {
    headerLines += line
    this
  }
      headerLines.to(Seq),
      hunks.map(_.toHunk()).to(Seq)
  def isRenamedFile: Boolean =
    Seq(renamedFileFromMsg, renamedFileToMsg).forall(line => headerLines.exists(_.startsWith(line)))
  def isRenamedIdenticalFile: Boolean = isRenamedFile && headerLines.contains(identicalFileContents)
        .map(_.split(" ").to(Seq))
    if (toFileName.equals(noFile)) fromFileName else toFileName
  val noFile: String = "/dev/null"
  val identicalFileContents: String = "similarity index 100%"
  val renamedFileFromMsg: String = "rename from "
  val renamedFileToMsg: String = "rename to "
