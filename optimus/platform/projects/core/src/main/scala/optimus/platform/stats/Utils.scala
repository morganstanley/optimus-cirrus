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
package optimus.platform.stats

import java.util.concurrent.atomic.{AtomicLong, AtomicInteger, AtomicBoolean}
import java.io.PrintStream
import java.io.File
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.sys.SystemProperties
import org.apache.commons.lang3.StringUtils
import scala.jdk.CollectionConverters._

/**
 * Set of application-independent utilities. This is set up so that:
 * -- these are all STATELESS and just operate on the parameters to return something
 * -- DEPEND on only the base Java, Scala
 * -- try to have a PREFIX to group related methods (e.g. strXXX, strYYY are String utilities)
 */
object Utils {
  val emptyMap = new scala.collection.immutable.HashMap[String, String]()
  val emptyPath = new Array[String](0)
  val emptyArrayInt = new Array[Int](0)

  lazy val sysProperties = new SystemProperties
  // NOTE: Map is actually immutable on the Java side!!
  lazy val envVariables = System.getenv.asScala

  /**
   * ***************************************************************************************************
   */
  /*  Utilities dealing with STRINGs                                                                    */
  /**
   * ***************************************************************************************************
   */
  /** CENTER given string within the given Width */
  def strCenter(str: String, W: Int, fillWith: Char = ' '): String =
    if (str == null) strSpaces(W)
    else if (str.length >= W) str
    else {
      val half = (W - str.length) / 2
      val plus = if ((half * 2) + str.length == W) 0 else 1
      if (fillWith == ' ')
        strSpaces(half) + str + strSpaces(half + plus)
      else
        (fillWith.toString * half) + str + (fillWith.toString * (half + plus))
    }

  /** Return a string of N spaces */
  def strSpaces(N: Int): String = if (N > 0) " " * N else ""

  /**
   * Split a string on a given single character - does NOT use String.split(regex) so any char works Multiple split
   * chars in a row may optionally each generate an output entry (which will be empty string)
   */
  def strSplit(
      strIn: String,
      ch: Char,
      trimPieces: Boolean = true,
      trimOriginal: Boolean = true,
      entryPer: Boolean = true): Array[String] = {
    val bfr = new ArrayBuffer[String]
    if (strIn != null) {
      var str = if (trimOriginal) strIn.trim else strIn
      var indxLast = 0
      var indx = 0
      if (str.isEmpty) bfr += ""
      else
        while (indxLast < str.length) {
          indx = str.indexOf(ch, indxLast)
          if (indx == -1) {
            bfr += str.substring(indxLast)
            indxLast = str.length
          } else {
            if (indx == indxLast) { if (entryPer) bfr += "" }
            else {
              val pc = str.substring(indxLast, indx)
              bfr += (if (trimPieces) pc.trim else pc)
            }
            indxLast = indx + 1
          }
        }
    }
    bfr.toArray
  }

  /** Simple-minded way to get commas into a number for better display */
  def strCommas(value: Long, numDecimals: Int = 0): String = {
    if (numDecimals < 1) "%,d".format(value) // < 1 to also handle (erroneous) negative input value
    else {
      val fmt = "%,f" + numDecimals
      var div: Double = 10
      for (i <- 1 until numDecimals) div *= 10
      fmt.format(value / div)
    }
  }

  /**
   * Specialized split on a space only since we use it so heavily. Sequences of blanks treated as a single break,
   * leading/trailing blanks ignored
   */
  def strBlankSplit(str: String): Array[String] = {
    val bfr = new ArrayBuffer[String]()
    var indx = 0
    var strt = 0

    if (str != null) {
      while (indx < str.length) {
        while (indx < str.length && str.charAt(indx) == ' ') indx += 1
        if (indx < str.length) {
          strt = indx
          while (indx < str.length && str.charAt(indx) != ' ') indx += 1
          bfr += str.substring(strt, indx)
        }
      }
    }
    bfr.toArray
  }

  /** Drop all of the given trailing characters -- slow but needs no external libraries */
  def strDropTrailing(strIn: String, ch: Char, trim: Boolean = true): String = {
    val str = if (strIn == null) "" else if (trim) strIn.trim else strIn
    if (str.isEmpty) ""
    else if (str.charAt(str.length - 1) != ch)
      (if (trim) str.trim else str)
    else strDropTrailing(str.dropRight(1), ch, trim)
  }

  /**
   * Return where to break a string on Whitespace so it is <= max length If cannot be broken, returns 'max'
   */
  def strBreak(str: String, max: Int): Int = {
    if (max < 1 || str == null || str.isEmpty) 0
    else if (str.length <= max) str.length
    else {
      var rslt = -1
      for (i <- (max - 1) to 0 by -1 if rslt < 0) if (Character.isWhitespace(str.charAt(i))) rslt = i
      if (rslt < 0) max else rslt
    }
  }

  /** Break a string on whitespace so that no line is > max */
  def strBreakUp(str: String, max: Int): List[String] = {
    if (max < 1 || str == null) Nil
    if (str.isEmpty) List("")
    else {
      val N = strBreak(str, max)
      if (N == str.length)
        List(str)
      else
        List(str.substring(0, N)) ::: strBreakUp(str.substring(N), max)
    }
  }

  /**
   * Concatenate 2 strings IFF not already equal or strB not at end of strA Always returns at least "", never null
   *
   * IGNORE if strA already >= the max size limit (just returns strA)
   */
  def strConcat(strA: String, strB: String, separator: String = "/", maxSizeLimit: Int = 4096): String =
    if (strA == null || strA.isEmpty) {
      if (strB == null) "" else strB
    } else if (strB == null) strA // strA can't be null at this point
    else if (strB.isEmpty) strA
    else if (strA == strB) strA
    else if (strA.endsWith(strB)) strA
    else if (strA.length <= maxSizeLimit) (strA + separator + strB)
    else strA

  /**
   * Concatenate 2 strings IFF strB not already contained somewhere within strA.
   *
   * Otherwise all parameters and rules the same as for strConcat
   */
  def strConcatIf(strA: String, strB: String, separator: String = "/", maxSizeLimit: Int = 4096): String =
    if (strA == null || strB == null || strA.isEmpty || strB.isEmpty || strA.indexOf(strB) == -1)
      strConcat(strA, strB, separator, maxSizeLimit)
    else strA

  /**
   * Pick up the next integer in this string, return Long.MIN_VALUE if any error.
   *
   * @param allowTrailingNonDigits
   *   \- TRUE == pick out digits up to any non-digit, FALSE == if any non-digits, error
   */
  def strNextInteger(
      strIn: String,
      trimInput: Boolean = true,
      emptyIsZero: Boolean = false,
      allowTrailingNonDigits: Boolean = true): Long = {
    val str = if (strIn == null) "" else if (trimInput) strIn.trim else strIn

    if (str.isEmpty()) {
      if (emptyIsZero) 0L else Long.MinValue
    } else if (allowTrailingNonDigits) {
      var indx = -1
      for (i <- 0 until str.length) if (!str.charAt(i).isDigit && indx == -1) indx = i
      try {
        if (indx == 0) {
          if (emptyIsZero) 0L else Long.MinValue
        } else str.substring(0, if (indx > -1) indx else str.length).toLong
      } catch {
        case ex: Exception => Long.MinValue
      }
    } else
      try {
        str.toLong
      } catch {
        case ex: Exception => Long.MinValue
      }
  }

  /**
   * ***************************************************************************************************
   */
  /* Utilities for runtime ENVIRONMENT variables (may well be system dependent -- Windows, Linux, etc)  */
  /**
   * ***************************************************************************************************
   */
  lazy val envKeys = envVariables.keys.toList
  lazy val envKeysSorted = envKeys.sortWith { case (keyA, keyB) => keyA.compareTo(keyB) < 0 }

  /** Setup the given list of Environment variables as a List[List[String]] */
  def envList(
      props: Iterable[String],
      rightAdjust: Boolean = true,
      splitAllPaths: Boolean = true,
      splitThese: Array[String] = null): List[List[String]] = {
    val bfr = new ArrayBuffer[List[String]]
    if (props != null) props.foreach(key => {
      envVariables.get(key) match {
        case None => bfr += List(key, "(no value)")
        case Some(value) =>
          if (
            value
              .indexOf(File.pathSeparator) > -1 && (splitAllPaths || (splitThese != null && splitThese.contains(key)))
          ) {
            val pcs = value.split(File.pathSeparator)
            var keyval = key
            pcs.foreach { str =>
              bfr += List(keyval, str); keyval = ""
            }
          } else
            bfr += List(key, value)
      }
    })
    bfr.toList
  }

  /**
   * Setup the given list of Environment variables as an Iterable[String] whereby each String is setup with correct
   * spacing for direct output.
   */
  def envFormat(
      props: Iterable[String],
      rightAdjust: Boolean = true,
      splitAllPaths: Boolean = true,
      splitThese: Array[String] = null): Iterable[String] = {
    val lst = envList(props, rightAdjust, splitAllPaths, splitThese)
    val widths = tblWidths(lst)
    if (rightAdjust) widths(0) *= -1
    tblRows(widths, lst, trimData = true, truncateIfNeeded = false, trimResultRight = true)
  }

  /**
   * ***************************************************************************************************
   */
  /* Utilities for System level info                                                                    */
  /**
   * ***************************************************************************************************
   */
  final val sysPropClassPath = "java.class.path"
  final val sysPropLibPath = "java.library.path"

  val sysSplitClassPath = Array("java.class.path") // Default array to split just the Classpath
  // Default array to split several common "path" type entries
  val sysSplitDefault =
    Array("java.class.path", "java.library.path", "java.endorsed.dirs", "java.ext.dirs", "sun.boot.class.path")
  // Psuedo-properties for memory usage
  val sysPropsMemory = List("memorymax", "memorynow", "memoryfree", "memoryused")

  // List of all psuedo-properties
  val sysPropsPsuedo = sysPropsMemory ::: Nil

  /** Return true if a given "system property" is a psuedo property */
  def sysIsPsuedo(key: String): Boolean = sysPropsPsuedo.contains(key)

  /** Get a list of ALL existing keys for system properties, optionally sorted */
  def sysPropertyKeys(sorted: Boolean = true, includeMemory: Boolean = true): List[String] = {
    val keys = sysProperties.keys.toList ::: (if (includeMemory) sysPropsPsuedo else Nil)
    if (sorted) keys.sortWith { case (keyA, keyB) => keyA.compareTo(keyB) < 0 }
    else keys
  }

  /** Setup the given list of System properties in a table for output (w newlines). Options to split 'path' entries */
  def sysProperties(
      props: Iterable[String],
      rightAdjust: Boolean = true,
      splitAllPaths: Boolean = true,
      splitThese: Array[String] = null): String = {
    sysPropertiesFormat(props, rightAdjust, splitAllPaths, splitThese).mkString("\n")
  }

  /**
   * Setup the given list of System properties as an Iterable[String] whereby each String is setup with correct spacing
   * for direct output.
   */
  def sysPropertiesFormat(
      props: Iterable[String],
      rightAdjust: Boolean = true,
      splitAllPaths: Boolean = true,
      splitThese: Array[String] = null): Iterable[String] = {
    val lst = sysPropertiesList(props, rightAdjust, splitAllPaths, splitThese)
    val widths = tblWidths(lst)
    if (rightAdjust) widths(0) *= -1
    tblRows(widths, lst, trimData = true, truncateIfNeeded = false, trimResultRight = true)
  }

  /**
   * Setup the given list of System Properties as a List[List[String]], given delimited list WILL return Nil list if
   * given empty input
   */
  def sysPropertiesDelimited(
      sProps: String,
      sSplitOn: String = ",",
      sort: Boolean = true,
      rightAdjust: Boolean = true,
      splitAllPaths: Boolean = true,
      splitThese: Array[String] = null): List[List[String]] = {
    if (sProps == null || sProps.trim.isEmpty) Nil
    else {
      val pcs = sProps.split(sSplitOn).toList
      val lst = if (sort) pcs.sortWith((A, B) => A <= B) else pcs
      sysPropertiesList(lst, rightAdjust, splitAllPaths, splitThese)
    }
  }

  /** Setup the given list of System Properties as a List[List[String]] */
  def sysPropertiesList(
      props: Iterable[String],
      rightAdjust: Boolean = true,
      splitAllPaths: Boolean = true,
      splitThese: Array[String] = null): List[List[String]] = {
    val bfr = new ArrayBuffer[List[String]]
    if (props != null) props.foreach(key => {
      sysProperties.get(key) match {
        case None => bfr += List(key, "(no value)")
        case Some(value) =>
          if (
            value
              .indexOf(File.pathSeparator) > -1 && (splitAllPaths || (splitThese != null && splitThese.contains(key)))
          ) {
            val pcs = value.split(File.pathSeparator)
            var keyval = key
            pcs.foreach { str =>
              bfr += List(keyval, str); keyval = ""
            }
          } else
            bfr += List(key, value)
      }
    })
    bfr.toList
  }

  /**
   * Setup a given list of psuedo-properties as List[List[String]] Valid names are: \================ memorymax
   * memorynow memoryfree memoryused (now - free)
   */
  def sysPropertiesPsuedo(props: Iterable[String], rightAdjust: Boolean = true): List[List[String]] = {
    val bfr = new ArrayBuffer[List[String]]
    props.foreach { p =>
      {
        p match {
          case "memorymax"  => bfr += List(p, strCommas(Runtime.getRuntime.maxMemory / (1024 * 1024)) + " MB")
          case "memorynow"  => bfr += List(p, strCommas(Runtime.getRuntime.totalMemory / (1024 * 1024)) + " MB")
          case "memoryfree" => bfr += List(p, strCommas(Runtime.getRuntime.freeMemory / (1024 * 1024)) + " MB")
          case "memoryused" =>
            bfr += List(
              p,
              strCommas((Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory) / (1024 * 1024)) + " MB")
          case _ => bfr += List(p, "Not supported")
        }
      }
    }
    bfr.toList
  }

  /**
   * ***************************************************************************************************
   */
  /* Utilities for Maps                                                                                 */
  /**
   * ***************************************************************************************************
   */
  /**
   * Return a Map where entries in 'addOrOverride' add to or OVERRIDE(replace) entries in 'base' Note: May return the
   * original or parameter Map instance in certain situations, will NOT build a new Map instance in those cases.
   */
  def mapAddOrOverride[K, T](base: Map[K, T], addOrOverride: Map[K, T]): Map[K, T] =
    if (base == null && addOrOverride == null) new HashMap[K, T]()
    else if (base == null) addOrOverride
    else if (addOrOverride == null) base
    else addOrOverride ++ base.filter { case (key, value) => !addOrOverride.contains(key) }

  /**
   * Return a Map with all 'base' entries plus any NEW (by key) entries from addNew Note: May return the original or
   * parameter Map instance in certain situations, will NOT build a new Map instance in those cases.
   */
  def mapAddNew[K, T](base: Map[K, T], addNew: Map[K, T]): Map[K, T] =
    if (base == null && addNew == null) new HashMap[K, T]()
    else if (base == null) addNew
    else if (addNew == null) base
    else base ++ addNew.filter { case (key, value) => !base.contains(key) }

  /**
   * ***************************************************************************************************
   */
  /*  Utilities dealing with PATH-FORMAT data such as: /partA/partB/partC/ ...                          */
  /*                                                                                                    */
  /* In general, leading and trailing '/' are ignored (to get a leading/trailing empty node use //)     */
  /**
   * ***************************************************************************************************
   */
  /** (Relatively) safe to split the string without matching anything within one of the parts */
  val pathSeparator = "~@#"

  /** Most common default */
  val pathDefault = "/"

  /** Split a path into component pieces */
  def pathSplit(path: String, separator: String = pathDefault): Array[String] =
    if (path == null || path.isEmpty) emptyPath
    else if (separator == null || separator.isEmpty) Array(path)
    else (if (path.startsWith(separator)) path.substring(separator.length) else path).split(separator)

  /**
   * Extract the Nth node from a path-format string - val0/val1/val2/..., "" if does not exist NOTE: /val0/val1/val2
   * interpreted as val0/val1/val2 - i.e. ignore any leading /
   */
  def pathNode(path: String, N: Int, separator: String = pathDefault): String = {
    val pcs = pathSplit(path, separator)
    if (0 <= N && N < pcs.length) pcs(N) else ""
  }

  /** Convert a Map[String,String] to path format: key1/value1/key2/value2/.... - specify sort */
  def pathMapToPath(map: Map[String, String], sortByKey: Boolean = true, separator: String = pathDefault): String = {
    if (map == null || map.size == 0) ""
    else
      (if (sortByKey) map.keySet.toList.sortWith(_.compareTo(_) <= 0) else map.keySet.toList)
        .map(key => key + separator + map(key))
        .mkString(separator)
  }

  /** Convert a mutable  Map[String,String] to path format: key1/value1/key2/value2/.... - specify sort */
  def pathMutableMapToPath(
      map: scala.collection.mutable.Map[String, String],
      sortByKey: Boolean = true,
      separator: String = pathDefault): String = {
    if (map == null || map.size == 0) ""
    else
      (if (sortByKey) map.keySet.toList.sortWith(_.compareTo(_) <= 0) else map.keySet.toList)
        .map(key => key + separator + map(key))
        .mkString(separator)
  }
  def pathToMap(path: String, delimiter: String = pathDefault): Map[String, String] = {
    if (path == null || path.trim.isEmpty) emptyMap
    else {
      val map = new scala.collection.mutable.HashMap[String, String]
      val pcs = path.split(delimiter)
      if (pcs.length > 0) pcs.grouped(2).foreach(array => map.put(array(0), (if (array.length > 0) array(1) else "")))
      map.toMap
    }
  }

  /**
   * See if the first path MATCHEs the second path.
   *
   * The MATCH proceeds part by part. If the key path is shorter the match passes if parts match up to the key end.
   *
   * If the key path is longer (more parts) it will pass if all the excess parts are *
   *
   * @param trimParts
   *   \- trim blanks before comparing each part
   *
   * Each part may be a string (exact match), a * (matches anything), or characters* (leading characters match, then
   * anything)
   *
   * Example (NOTE: ? instead of * since this is in a comment!!!)
   *
   * Target: /partA/partB//partC Key: /partA/ -- matches Key: /?/partB/ -- matches Key: /partA/xxx/ -- fails Key:
   * /part?/ -- matches
   */
  def pathMatches(
      pathKey: String,
      pathTarget: String,
      emptyInputMatches: Boolean = false,
      emptyTargetMatches: Boolean = false,
      trimParts: Boolean = true,
      separator: String = pathDefault): Boolean = {
    if (pathKey != null && pathKey.nonEmpty && pathTarget != null && pathTarget.nonEmpty) {
      if (pathKey == pathTarget) true
      else {
        val pcsKey = pathSplit(pathKey, separator)
        val pcsTarget = pathSplit(pathTarget, separator)
        val N = if (pcsKey.length < pcsTarget.length) pcsKey.length else pcsTarget.length
        var rslt = true
        for (i <- 0 until N if rslt)
          if (!partMatches(trimIf(pcsKey(i), trimParts), trimIf(pcsTarget(i), trimParts))) rslt = false
        if (rslt && pcsKey.length > N)
          for (i <- N until pcsKey.length if rslt) if (trimIf(pcsKey(i), trimParts) != "*") rslt = false
        rslt
      }
    } else if ((pathKey == null || pathKey.isEmpty) && emptyInputMatches) true
    else if ((pathTarget == null || pathTarget.isEmpty) && emptyTargetMatches) true
    else false // both NULL or both EMPTY??
  }
  private def trimIf(part: String, trim: Boolean): String = if (!trim) part else if (part == null) "" else part.trim
  private def partMatches(key: String, target: String): Boolean =
    if (key == target || key == "*") true else if (!key.endsWith("*")) false else key.substring(0, key.length) == target

  /**
   * ***************************************************************************************************
   */
  /* Utilities dealing with setting up TABLEs (as strings with fixed width columns)                     */
  /**
   * ***************************************************************************************************
   */
  /**
   * Lay out as a Table, computing the necessary Widths (all left justified)
   */
  def tblTable(
      rows: Iterable[Iterable[String]],
      szBetween: Int = 2,
      centerTitles: Boolean = false,
      underline1stRow: Char = ' ',
      trim: Boolean = true,
      truncateIfNeeded: Boolean = true): Iterable[String] =
    tblRows(tblWidths(rows, trim), rows, szBetween, centerTitles, underline1stRow, trim, truncateIfNeeded)

  /**
   * Lay out the whole thing in a table based on the widths
   */
  def tblRows(
      widths: Array[Int],
      rows: Iterable[Iterable[String]],
      szBetween: Int = 2,
      centerTitles: Boolean = false,
      underline1stRow: Char = ' ',
      trimData: Boolean = true,
      truncateIfNeeded: Boolean = true,
      trimResultRight: Boolean = true): Iterable[String] = {
    val table = rows.map(tblRow(widths, _, szBetween, trimData, truncateIfNeeded, trimResultRight))
    if ((centerTitles || underline1stRow != ' ') && table.size > 0) {
      val unders: List[String] =
        if (underline1stRow != ' ') List(tblUnders(widths, underline1stRow, szBetween)) else Nil
      val lst = table.toList
      val row1 =
        if (centerTitles)
          tblRow(widths, tblCenter(widths, rows.head, truncateIfNeeded = truncateIfNeeded), szBetween, trimData = false)
        else lst.head
      val rslt = row1 :: unders ::: lst.tail
      rslt
    } else
      table
  }

  /** Lay out the whole table and write to an output device */
  def tblWrite(
      wtr: PrintStream,
      rows: Iterable[Iterable[String]],
      szBetween: Int = 2,
      centerTitles: Boolean = false,
      underline1stRow: Char = ' ',
      trimData: Boolean = true,
      trimResultRight: Boolean = true): Unit =
    tblTable(rows, szBetween, centerTitles, underline1stRow, trimData, trimResultRight).foreach(wtr.println)

  /** Lay out the whole table and write to an output device */
  def tblWriteW(
      wtr: PrintStream,
      widths: Array[Int],
      rows: Iterable[Iterable[String]],
      szBetween: Int = 2,
      centerTitles: Boolean = false,
      underline1stRow: Char = ' ',
      trimData: Boolean = true,
      truncateIfNeeded: Boolean = true,
      trimResultRight: Boolean = true): Unit =
    tblRows(widths, rows, szBetween, centerTitles, underline1stRow, trimData, truncateIfNeeded, trimResultRight)
      .foreach(wtr.println)

  /**
   * Get one row of a table given the Widths of each column and the data. Data is left-justified unless width is
   * Negative, then right justified. By default, blanks trimmed from right side.
   */
  def tblRow(
      widths: Array[Int],
      data: Iterable[String],
      szBetween: Int = 2,
      trimData: Boolean = true,
      truncateIfNeeded: Boolean = true,
      trimResultRight: Boolean = true): String = {
    val btwn = " " * szBetween
    val line = data.zipWithIndex.foldLeft("") {
      case (left, (strIn, indx)) => {
        val strTrim = if (trimData) strIn.trim else strIn
        val Wneg = if (indx >= widths.length) strTrim.length else widths(indx)
        val rightAlign = if (Wneg >= 0) false else true
        val W = Wneg.abs
        val str = if (truncateIfNeeded && strTrim.length > W) strTrim.substring(0, W) else strTrim

        left + (if (str.length >= W) str
                else if (rightAlign) ((" " * (W - str.length)) + str)
                else str.padTo(W, ' ')) + btwn
      }
    }
    if (trimResultRight) strDropTrailing(line, ' ', false)
    else if (btwn.nonEmpty) line.dropRight(btwn.length)
    else line
  }

  /** Center the given data within the column widths */
  def tblCenter(widths: Array[Int], data: Iterable[String], truncateIfNeeded: Boolean = true): Iterable[String] = {
    data.zipWithIndex.map {
      case (str, indx) => {
        val W = if (indx < widths.length) widths(indx).abs else str.length
        val split = W - str.length
        val half = if (W > 0) " " * (split / 2) else ""
        val odd = split > 0 && ((split & 0x01) > 0)

        if (W == str.length) str
        else if (W < str.length && truncateIfNeeded) str.substring(0, W)
        else half + str + half + (if (odd) " " else "")
      }
    }
  }

  /** Scan array of rows of data (as pieces). Return Int array with max number of data columns and max Width of each */
  def tblWidths(table: Iterable[Iterable[String]], trim: Boolean = true): Array[Int] = {
    if (table == null) emptyArrayInt
    else {
      var widths: Array[Int] = null
      table.foreach(row => widths = tblWidths(row, widths, trim))
      widths
    }
  }

  /**
   * Scan one row of data (in pieces) and existing widths array.
   * -- Update any column width if this data is wider
   * -- re-allocate and return new array of Ints if number of columns is larger (or null input)
   */
  def tblWidths(row: Iterable[String], widths: Array[Int], trim: Boolean): Array[Int] = {
    if (row == null && widths == null) emptyArrayInt
    else if (row == null || row.size == 0) widths
    else {
      val wNew = if (widths == null || row.size > widths.size) new Array[Int](row.size) else widths
      if (widths != null && !(wNew eq widths)) for (i <- 0 until widths.size) wNew(i) = widths(i)
      var indx = 0
      row.foreach { str =>
        if (str != null) { val lnth = (if (trim) str.trim else str).length; if (lnth > wNew(indx)) wNew(indx) = lnth };
        indx += 1
      }
      wNew
    }
  }

  /** Get a row of character X the full width of the table */
  def tblRowOf(widths: Array[Int], X: Char = ' ', szBetween: Int = 2): String = X.toString * tblWidth(widths, szBetween)

  /** Get a row of underlines (or other character) the width of each column */
  def tblUnders(widths: Array[Int], X: Char = '_', szBetween: Int = 2): String = {
    val xStr = X.toString
    val btwn = " " * szBetween
    if (widths == null || widths.size == 0) "" else widths.map(W => (xStr * W) + btwn).mkString.dropRight(szBetween)
  }

  /** Determine the full width of the table with the given sz between columns */
  def tblWidth(widths: Array[Int], szBetween: Int = 2) =
    if (widths == null) 0 else widths.sum + szBetween * (widths.size - 1)

  /**
   * ***************************************************************************************************
   */
  /*  Utilities dealing with JSON which do NOT require the Jackson JSON libraries                       */
  /*  These are generally OUTPUT type routines - e.g. build JSON syntax output strings.                 */
  /*  Use UtilsJSON for other methods for parsing, etc.                                                 */
  /**
   * ***************************************************************************************************
   */
  /** Convenience routine for JSON to have a quoted string value */
  def jsonQuoted(s: String): String = '"' + s + '"'

  /**
   * Break up a PATH, e.g. ABC/DEF/GHI into a named JSON segment: "name":{ "path_0":"ABC", "path_1":"DEF",
   * "path_2":"GHI" }
   */
  def jsonPathParsed(
      sItemName: String,
      sPath: String,
      delimiter: String = pathDefault,
      prefix: String = "path_"): String = {
    jsonQuoted(sItemName) + ':' + jsonBraces(pathSplit(sPath, delimiter).view.zipWithIndex.foldLeft("")((left, right) =>
      left + jsonQuoted(prefix + right._2) + ':' + jsonQuoted(right._1) + ','))
  }

  /** Put BRACES {} around well-formed piece of JSON - will DROP any TRAILING commas, ADD a comma after closing } */
  def jsonBraces(str: String): String = {
    if (str == null || str.trim.isEmpty) ""
    else "{" + strDropTrailing(str, ',', trim = true) + "},"
  }

  /**
   * Put ARRAY [] around well-formed piece of JSON - will DROP any TRAILING comma, ADD a comma after closing ] (unless
   * suppressed
   */
  def jsonArray(str: String, suppressTrailingComma: Boolean = false): String = {
    if (str == null || str.trim.isEmpty) ""
    else "[" + strDropTrailing(str, ',', trim = true) + (if (suppressTrailingComma) "]" else "],")
  }

  /**
   * Convert an Any item to correct string form for JSON -- INCOMPLETE, adding as needed Picks up some special cases
   * (e.g. AtomicInteger) and outputs as integer (no quotes)
   */
  def jsonToString(item: Any @unchecked, suppressEmpty: Boolean = true): String = {
    if (item == null) "null" // un-quoted in the final JSON
    else
      item match {
        case b: Boolean       => if (b) "true" else "false"
        case b: Byte          => b.toString
        case s: Short         => s.toString
        case c: Char          => c.toString
        case i: Int           => i.toString
        case l: Long          => l.toString
        case f: Float         => f.toString
        case d: Double        => d.toString
        case lng: AtomicLong  => lng.toString
        case i: AtomicInteger => i.toString
        case b: AtomicBoolean => if (b.get) "true" else "false"
        case map: Map[String @unchecked, Any @unchecked] => {
          if (map.isEmpty) "{}"
          else
            jsonBraces(
              map.keySet.toList
                .sortWith(_.compareTo(_) <= 0)
                .foldLeft("")((left, key) => left + jsonItem(key, map(key), suppressEmpty)))
        }
        case opt: Option[Any] => jsonQuoted(opt.getOrElse("").toString)

        case _ => jsonQuoted(item.toString)
      }
  }

  /**
   * Determine if this value is EMPTY: zero for numerics, blank/empty string, null
   */
  def jsonIsEmpty(item: Any): Boolean = {
    if (item == null) true
    else
      item match {
        case b: Boolean                               => false // Boolean can't be "empty"
        case b: Byte                                  => b == 0
        case s: Short                                 => s == 0
        case c: Char                                  => c == 0
        case i: Int                                   => i == 0
        case l: Long                                  => l == 0
        case f: Float                                 => f == 0.0f
        case d: Double                                => d == 0.0d
        case lng: AtomicLong                          => lng.get == 0
        case i: AtomicInteger                         => i.get == 0
        case b: AtomicBoolean                         => false
        case map: Map[Any @unchecked, Any @unchecked] => map.isEmpty
        // case map: Map[_, _] => map.isEmpty
        case opt: Option[Any] => opt == None
        case _                => item.toString.trim.isEmpty
      }
  }

  /**
   * Convert to standard JSON item: "label" : value,
   *
   * Note comma at end so this can be concatenated with other items
   */
  def jsonItem(
      label: String,
      value: Any,
      suppressEmpty: Boolean = false,
      noFinalComma: Boolean = false,
      valueIsArray: Boolean = false): String =
    if (suppressEmpty && jsonIsEmpty(value)) ""
    else jsonQuoted(label) + ':' + (if (valueIsArray) value else jsonToString(value)) + ','

  /**
   * Put out JSON for a list of exceptions. General format is: "labelStackDepth": N "label" : [ { "Exception": ... fully
   * qualified name of exception .... "Message": ... message from exception (if nonEmpty) ... "Stack": [ " first line of
   * stack trace ", " next line of stack trace", .... N lines as set by stackDepth ....
   *
   * ] }, { ... next exception in list }, .....
   *
   * ],
   */
  def jsonForExceptions(
      label: String,
      stackDepth: Int,
      lst: Iterable[Throwable],
      suppressIfEmpty: Boolean = true): String = {
    if (stackDepth < 1 || lst == null || lst == Nil) {
      if (suppressIfEmpty) "" else jsonItem(label, "[No exceptions]")
    } else
      jsonItem(label + "StackDepth", stackDepth) +
        jsonQuoted(label) + ':' +
        jsonArray(lst.foldLeft("")((left, ex) => left + jsonForException(stackDepth, ex, suppressIfEmpty = true)))
  }

  /**
   * Generate JSON block for a single Exception. General format:
   *
   * { "Exception": ... fully qualified name of exception .... "Message": ... message from exception (if nonEmpty) ...
   * "Stack": [ " first line of stack trace ", " next line of stack trace", .... N lines as set by stackDepth ....
   *
   * ] }
   */
  def jsonForException(stackDepth: Int, ex: Throwable, suppressIfEmpty: Boolean = true): String = {
    if (stackDepth < 1 || ex == null) {
      if (suppressIfEmpty) "" else jsonBraces(jsonItem("Exception", "None"))
    } else
      jsonBraces {
        val stack = ex.getStackTrace

        jsonItem("Exception", ex.getClass.getName) +
          jsonItem("Message", ex.getMessage, suppressEmpty = true) +
          (if (stackDepth < 2 || stack == null || stack.size == 0) ""
           else
             jsonItem(
               "Stack",
               jsonArray {
                 val N = stack.length.min(stackDepth)
                 stack.slice(0, N).foldLeft("")((left, item) => left + jsonQuoted(item.toString) + ',')
               },
               valueIsArray = true))
      }
  }

  /**
   * KLUDGE: Take a long string that is assumed to be well-formed JSON and split it into pieces.
   *
   * Simplistic (and slow) logic - does not really parse, just looks for good breakpoints. May well fail if some key
   * characters are escaped (e.g. \")
   */
  def jsonSplit(jsonIn: String, maxWidth: Int): List[String] = {
    var lst: List[String] = Nil
    var json = jsonIn
    var lnth = maxWidth
    var maxLnth = lnth
    while (json.length > 0) {
      val str = if (json.length > lnth) json.substring(0, lnth) else json
      var numQuotes = StringUtils.countMatches(str, "\"")
      var indx = str.lastIndexOf('}')
      if (indx < str.lastIndexOf(']')) indx = str.lastIndexOf(']')
      if (indx < str.lastIndexOf(',')) indx = str.lastIndexOf(',')
      if (indx < str.lastIndexOf(':')) indx = str.lastIndexOf(':')
      // If we found nothing, make it bigger. Will exceed max, but what else to do?
      if (indx == -1) {
        lnth += lnth / 8
        maxLnth = maxLnth max lnth
      } else if ((StringUtils.countMatches(str.substring(0, indx), "\"") & 0x01) > 0) {
        // we're within a quoted string, don't accept this and process shortened string
        indx = str.lastIndexOf('"')
        lnth = indx - 1
        // Aberration if very long quoted string, might still be in the middle and need to grow
        if (lnth < 1) {
          lnth = maxLnth + maxWidth / 8
          maxLnth = lnth
        }
      } else {
        lst ::= str.substring(0, indx + 1)
        json = json.substring(indx + 1)
        lnth = maxWidth
      }
    }
    lst.reverse
  }
}
