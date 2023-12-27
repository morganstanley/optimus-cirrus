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
package optimus.buildtool.utils.stats

import java.io.BufferedReader
import java.io.StringReader
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

/**
 * Extracts stats about the source code from the filesystem, including number of files, lines and bytes of scala and
 * java code (including and excluding blank and comment-only lines)
 */
object SourceCodeStats {
  private val interestingExtns = Set("scala", "java")

  def getFileStats(
      srcDir: Path,
      extensions: Set[String] = interestingExtns): Map[CompilationRun, Map[String, ImmutableFileStats]] = {
    val statsByRun = mutable.Map[CompilationRun, Map[String, MutableFileStats]]()

    val stream = Files.walk(srcDir)
    try {
      stream.parallel().forEach { f =>
        if (Files.isRegularFile(f)) {
          val name = f.getFileName.toString
          val extn = name.substring(name.lastIndexOf('.') + 1)
          if (interestingExtns.contains(extn)) {
            val relativePath = srcDir.relativize(f)
            CompilationRun.fromRelativeSourcePath(relativePath).foreach { run =>
              val statsByExtn = statsByRun.synchronized {
                statsByRun.getOrElseUpdate(run, extensions.map(e => e -> MutableFileStats()).toMap)
              }

              val r = Files.newBufferedReader(f)
              val s =
                try getStats(r)
                finally r.close()
              statsByExtn(extn).add(s)
            }
          }
        }
      }
    } finally stream.close()

    statsByRun.map { case (run, byExtn) =>
      (run, byExtn.map { case (extn, mutStats) => (extn, mutStats.asImmutable) })
    }.toMap
  }

  /**
   * Computes total line and byte counts, plus (slightly approximate) "real code" line and byte counts, where "real
   * code" excludes whitespace and comments.
   *
   * It's not perfect but it's fast (~10 seconds for all of codetree on a Windows Z440 box)
   */
  def getStats(ss: Iterable[String]): ImmutableFileStats =
    ss.foldLeft(ImmutableFileStats.empty)((z, s) => z.merge(getStats(s)))
  def getStats(s: String): ImmutableFileStats = {
    val r = new BufferedReader(new StringReader(s))
    try {
      getStats(r)
    } finally {
      r.close()
    }
  }
  def getStats(r: BufferedReader): ImmutableFileStats = {
    var line = r.readLine()
    var lineCounter, byteCounter, realCodeLineCounter, realCodeByteCounter = 0
    var state: ParseState = InWhitespace

    while (line ne null) {
      if (!state.survivesEol) state = InWhitespace
      var isCode = false
      var prevCh = '\n'
      var prevPrevCh = '\n'
      var i = 0
      val end = line.length
      while (i < end) {
        val ch = line.charAt(i)
        state = state.nextState(ch, prevCh, prevPrevCh)
        // slight bodge - ignore / since it might be the first / of a comment (this means we'd ignore a / if it's the
        // only char on a line, but that seems likely to be rare enough to not be worth coding around)
        if (state.isCode && ch != '/') {
          isCode = true
          realCodeByteCounter += 1
        }
        byteCounter += 1
        prevPrevCh = prevCh
        prevCh = ch
        i += 1
      }

      lineCounter += 1
      if (isCode) realCodeLineCounter += 1
      line = r.readLine()
    }

    ImmutableFileStats(
      fileCount = 1,
      lineCount = lineCounter,
      byteCount = byteCounter + lineCounter /* for line ends */,
      realCodeByteCount = realCodeByteCounter,
      realCodeLineCount = realCodeLineCounter
    )
  }

  private sealed abstract class ParseState(val isCode: Boolean, val survivesEol: Boolean = false) {
    def nextState(current: Char, previous: Char, previousPrevious: Char): ParseState = {
      if (previous == '/' && current == '/') InSingleLineComment
      else if (previous == '/' && current == '*') InMultiLineComment
      else if (current == '"' && previous == '"' && previousPrevious == '"') InMultiLineString
      else if (current == '"') InString
      else if (current == ' ' || current == '\t' || current == '\r') InWhitespace
      else InRegularCode
    }
  }
  private case object InWhitespace extends ParseState(isCode = false)
  private case object InRegularCode extends ParseState(isCode = true)
  private case object InString extends ParseState(isCode = true) {
    override def nextState(current: Char, previous: Char, previousPrevious: Char): ParseState = {
      if (current == '"' && previous != '\\') InRegularCode
      else this
    }
  }
  private case object InMultiLineString extends ParseState(isCode = true, survivesEol = true) {
    override def nextState(current: Char, previous: Char, previousPrevious: Char): ParseState = {
      if (current == '"' && previous == '"' && previousPrevious == '"') InRegularCode
      else this
    }
  }
  private case object InSingleLineComment extends ParseState(isCode = false) {
    override def nextState(current: Char, previous: Char, previousPrevious: Char): ParseState = this
  }
  private case object InMultiLineComment extends ParseState(isCode = false, survivesEol = true) {
    override def nextState(current: Char, previous: Char, previousPrevious: Char): ParseState = {
      if (previous == '*' && current == '/') InWhitespace
      else this
    }
  }

  sealed trait FileStats {
    def fileCount: Int
    def byteCount: Long
    def lineCount: Int
    def realCodeLineCount: Int
    def realCodeByteCount: Long
    override def toString: String =
      f"${fileCount}%,d file(s), ${lineCount}%,d lines (${byteCount / 1024}%,d KB) " +
        f"of which ${realCodeLineCount}%,d lines (${realCodeByteCount / 1024}%,d KB) are non-blank non-comment"

    final def merge(o: FileStats): FileStats = ImmutableFileStats(
      fileCount = fileCount + o.fileCount,
      byteCount = byteCount + o.byteCount,
      lineCount = lineCount + o.lineCount,
      realCodeLineCount = realCodeLineCount + o.realCodeLineCount,
      realCodeByteCount = realCodeByteCount + o.realCodeByteCount
    )
  }

  object FileStats {
    val zero: FileStats = ImmutableFileStats(0, 0, 0, 0, 0)

    def fieldExtractors[T](name: String, extractor: T => FileStats): Seq[(String, T => Any)] = {
      Seq(
        s"${name}FileCount" -> (extractor(_).fileCount),
        s"${name}ByteCount" -> (extractor(_).byteCount),
        s"${name}LineCount" -> (extractor(_).lineCount),
        s"${name}RealCodeLineCount" -> (extractor(_).realCodeLineCount),
        s"${name}RealCodeByteCount" -> (extractor(_).realCodeByteCount)
      )
    }

    def fieldsToFileStats(name: String, fields: Map[String, String]): FileStats = ImmutableFileStats(
      fileCount = fields(s"${name}FileCount").toInt,
      byteCount = fields(s"${name}ByteCount").toLong,
      lineCount = fields(s"${name}LineCount").toInt,
      realCodeLineCount = fields(s"${name}RealCodeLineCount").toInt,
      realCodeByteCount = fields(s"${name}RealCodeByteCount").toLong
    )
  }

  final case class ImmutableFileStats(
      fileCount: Int,
      byteCount: Long,
      lineCount: Int,
      realCodeLineCount: Int,
      realCodeByteCount: Long)
      extends FileStats {
    def merge(o: ImmutableFileStats): ImmutableFileStats = ImmutableFileStats(
      fileCount = fileCount + o.fileCount,
      byteCount = byteCount + o.byteCount,
      lineCount = lineCount + o.lineCount,
      realCodeLineCount = realCodeLineCount + o.realCodeLineCount,
      realCodeByteCount = realCodeByteCount + o.realCodeByteCount
    )
  }
  object ImmutableFileStats {
    val empty = ImmutableFileStats(0, 0L, 0, 0, 0L)
  }

  final case class MutableFileStats(
      fileCountAdder: AtomicInteger = new AtomicInteger,
      byteCountAdder: AtomicLong = new AtomicLong,
      lineCountAdder: AtomicInteger = new AtomicInteger,
      realCodeLineCountAdder: AtomicInteger = new AtomicInteger,
      realCodeByteCountAdder: AtomicLong = new AtomicLong)
      extends FileStats {
    override def fileCount: Int = fileCountAdder.get
    override def byteCount: Long = byteCountAdder.get
    override def lineCount: Int = lineCountAdder.get
    override def realCodeLineCount: Int = realCodeLineCountAdder.get
    override def realCodeByteCount: Long = realCodeByteCountAdder.get

    def add(f: FileStats): Unit = {
      fileCountAdder.getAndAdd(f.fileCount)
      byteCountAdder.getAndAdd(f.byteCount)
      lineCountAdder.getAndAdd(f.lineCount)
      realCodeLineCountAdder.getAndAdd(f.realCodeLineCount)
      realCodeByteCountAdder.getAndAdd(f.realCodeByteCount)
    }

    def asImmutable: ImmutableFileStats =
      ImmutableFileStats(
        fileCount = fileCount,
        byteCount = byteCount,
        lineCount = lineCount,
        realCodeLineCount = realCodeLineCount,
        realCodeByteCount = realCodeByteCount)
  }
}
