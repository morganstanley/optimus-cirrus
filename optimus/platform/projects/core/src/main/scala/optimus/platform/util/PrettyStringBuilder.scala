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
package optimus.platform.util

import java.util.{List => JList}

import optimus.core.CoreHelpers

// Crude way to handle indentation for pretty printing
// object contents using a stringbuilder
class PrettyStringBuilder(val underlying: java.lang.StringBuilder, val html: Boolean, val short: Boolean) {
  def this(html: Boolean) = this(new java.lang.StringBuilder, html, false)
  def this(html: Boolean, short: Boolean) = this(new java.lang.StringBuilder, html, short)
  def this() = this(new java.lang.StringBuilder, false, false)
  def this(underlying: java.lang.StringBuilder) = this(underlying, false, false)

  private val indentation = if (html) "&nbsp;" else "  "
  private var currentIndentation = ""
  private var indentationLevel = 0
  private var needIndent = false
  var useEntityType = false
  var showKeys = false
  var showNodeState = false
  var showCausalityID = false // e.g. 0-T8 (causality and thread ID)
  var simpleName = false
  var includeHint = false
  var showEntityToString = false
  var showNodeArgs = false
  var startBlockMarker = "["
  var endBlockMarker = "]"

  final def setBlockMarkers(start: String, end: String): PrettyStringBuilder = {
    startBlockMarker = start
    endBlockMarker = end
    this
  }

  final def startBlock(): PrettyStringBuilder = {
    appendln(" " + startBlockMarker); indent()
  }

  final def endBlock: PrettyStringBuilder = endBlock()
  final def endBlock(suffix: String = ""): PrettyStringBuilder = {
    unIndent(); appendln(endBlockMarker + suffix)
  }

  final def backspace(i: Int = 1): PrettyStringBuilder = {
    underlying.setLength(underlying.length() - i)
    this
  }

  final def indent(): PrettyStringBuilder = {
    indentationLevel += 1
    currentIndentation = indentation * indentationLevel
    this
  }

  final def unIndent(): PrettyStringBuilder = {
    indentationLevel -= 1
    currentIndentation = indentation * indentationLevel
    this
  }

  final def append(obj: Any): this.type = {
    if (indentationLevel != 0 && needIndent) {
      underlying.append(currentIndentation)
      needIndent = false
    }
    underlying.append(CoreHelpers.safeToString(obj))
    this
  }

  final def appendln(obj: Any): PrettyStringBuilder = {
    append(obj)
    endln()
    this
  }

  final def append[T](seq: Seq[T], func: T => Unit, separator: String): PrettyStringBuilder = {
    var afterFirst = false
    seq foreach { item =>
      if (afterFirst) append(separator)
      afterFirst = true
      func(item)
    }
    this
  }

  final def append[T](seq: JList[T], func: T => String, separator: String): PrettyStringBuilder = {
    var afterFirst = false
    val it = seq.iterator()
    while (it.hasNext) {
      if (afterFirst) append(separator)
      afterFirst = true
      append(func(it.next()))
    }
    this
  }

  final def isEmpty: Boolean = underlying.length == 0

  /*
  Aggregate Printing

  If we have several objects that have the same toString representation, then we can collapse them into one line and
  print the number of occurrences alongside.

  This is useful when printing tasks in the scheduler queue: (1) it makes reading the items on the queue easier, and
  (2) it frees up space to print other tasks when we have a maxNum of tasks that can be printed for each queue.
   */

  var lastString: Option[String] = None
  var aggregateCount: Int = 0

  final def appendlnAggregate(currentString: String): Boolean = {
    if (lastString.contains(currentString)) {
      aggregateCount += 1
      false
    } else {
      lastString match {
        case Some(string) =>
          flushAggregateString(string)
          lastString = Some(currentString)
          aggregateCount = 1
        case None =>
          lastString = Some(currentString)
          aggregateCount = 1
      }
      true
    }
  }

  final def appendlnAggregateEnd(): Unit = {
    lastString match {
      case Some(string) =>
        flushAggregateString(string)
        lastString = None
        aggregateCount = 0
      case None =>
    }
  }

  final private def flushAggregateString(string: String): Unit = {
    val suffix = if (aggregateCount > 1) s" [$aggregateCount occurrences]" else ""
    append(string + suffix)
    endln()
  }

  final def appendDivider(): PrettyStringBuilder = {
    appendln("__________________________________________________")
    this
  }

  final def endln(): PrettyStringBuilder = {
    underlying.append(if (html) "<br>" else "\n")
    needIndent = true
    this
  }

  final def ++=(obj: Any): PrettyStringBuilder = {
    if (obj.asInstanceOf[AnyRef] ne null)
      append(obj.toString)
    this
  }

  final def bold(obj: Any): PrettyStringBuilder = {
    if (html) append("<b>")
    append(obj)
    if (html) append("</b>")
    this
  }

  final def link(obj: Any, link: Any): PrettyStringBuilder = {
    if (html) append("<a href='") ++= link ++= "'>"
    append(obj)
    if (html) append("</a>")
    this
  }

  final def clear(): Unit = underlying.setLength(0)

  override def toString: String = underlying.toString
}
