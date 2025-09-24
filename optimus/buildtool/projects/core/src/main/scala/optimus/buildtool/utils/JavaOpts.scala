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
package optimus.buildtool.utils

import scala.annotation.tailrec
import scala.collection.compat._

object JavaOpts {

  private val WithMemoryParam = """(-X.*?)\d+[gGmMkK]""".r
  private val Xlog = """-Xlog.*""".r
  private val GCSelector = """-XX:[+]Use.*GC""".r
  private val WithEqParam = """(-.*?)=.*""".r
  private val XXFlag = """-XX:[+\-](\w+)""".r
  private val OtherXXOption = """-XX:.*""".r
  private val JigsawParam = """--(add-exports|add-opens|add-modules|add-reads|patch-module)=.*""".r
  private val AppendableWithColonParam = {
    val appendableOptionNames = Set(
      "-da",
      "-ea",
      "-disableassertions",
      "-enableassertions",
      "-esa",
      "-dsa",
      "-enablesystemassertions",
      "-disablesystemassertions",
      "-verbose",
      "-agentlib",
      "-agentpath",
      "-javaagent"
    )
    s"""(?:${appendableOptionNames.mkString("|")}):.*""".r
  }
  private val WithColonParam = """(-.*?:).*""".r

  private final case class ParsedOpt(dedupKey: String, text: String, components: Seq[String])

  def normalize(
      javaOpts: Seq[String],
      javaVersion: Option[String] = None,
      logcb: String => Unit = _ => ()): Seq[String] = {
    val parsed = parseAll(javaOpts)
    val filtered = javaVersion.fold(parsed)(jv => filterJavaRuntimeOptions(javaMajorVersionInt(jv), parsed, logcb))
    val deduplicated = removeDuplicates(filtered)
    val verboseResolved = resolveVerboseFlags(deduplicated)
    val withoutEmptyProperties = removeEmptyProperties(verboseResolved)
    withoutEmptyProperties.flatMap(_.components)
  }

  private def parseAll(javaOpts: Seq[String]): List[ParsedOpt] = {
    val split = CliArgs.normalize(javaOpts).to(List)

    @tailrec def recur(opts: List[String], res: List[ParsedOpt]): List[ParsedOpt] = opts match {
      case opt :: param :: rest if opt.startsWith("-") && !opt.contains("=") && !param.startsWith("-") =>
        // no special handling for "-opt param" syntax options other than keeping the parts separately (we can't
        // combine them into a single arg else the jvm will complain)
        val concat = s"$opt $param"
        val parsed = ParsedOpt(concat, concat, opt :: param :: Nil)
        recur(rest, parsed :: res)
      case opt :: rest =>
        // single token options get parsed specially
        val parsed = parse(opt)
        recur(rest, parsed :: res)
      case Nil => res
    }

    recur(split, Nil).reverse
  }

  private def parse(text: String): ParsedOpt = {
    def parsed(dedupKey: String) = ParsedOpt(dedupKey, text, text :: Nil)
    def asIs = ParsedOpt(text, text, text :: Nil)
    // the point of this case analysis is to set the dedupKey correctly for interesting options
    text match {
      case WithMemoryParam(name)      => parsed(name)
      case Xlog()                     => asIs // java 9+ convention allows for multiple -Xlog:whatever options
      case GCSelector()               => parsed("GCSelector") // we keep only the last GC selector (they all conflict)
      case JigsawParam(_)             => asIs
      case WithEqParam(name)          => parsed(name) // system properties etc.
      case XXFlag(name)               => parsed("-XX:+/-" + name) // allow -XX:+Foo and -XX:-Foo to dedup together
      case OtherXXOption()            => asIs
      case AppendableWithColonParam() => asIs
      case WithColonParam(name)       => parsed(name)
      case _                          => asIs
    }
  }

  private def removeDuplicates(parsed: List[ParsedOpt]): List[ParsedOpt] =
    parsed.reverse.distinctBy(_.dedupKey).reverse

  private def resolveVerboseFlags(opts: List[ParsedOpt]): List[ParsedOpt] = {
    opts
      .foldRight((false, List.empty[ParsedOpt])) { case (opt, (removeVerbose, res)) =>
        if (opt.text == "-verbose:none")
          (true, res)
        else if (removeVerbose && opt.text.startsWith("-verbose"))
          (removeVerbose, res)
        else
          (removeVerbose, opt :: res)
      }
      ._2
  }

  private def removeEmptyProperties(opts: List[ParsedOpt]): List[ParsedOpt] = {
    opts.filterNot { opt =>
      opt.text.startsWith("-D") && opt.text.endsWith("=") && opt.text.count(_ == '=') == 1
    }
  }

  // Called from stratosphere runconf
  def filterJavacOptions(release: Int, opts: Seq[String], logcb: String => Unit = _ => ()): Seq[String] = {
    val modernJava = release > 8
    val pre = s"For java $release,"
    def log(msg: String) = if (verboseFiltering) logcb(msg) else ()
    @tailrec
    def filter(opts: Seq[String], acc: Seq[String]): Seq[String] =
      opts match {
        case flag +: arg +: rest if (modernOnlyWithArg(flag)) =>
          filter(
            rest,
            if (modernJava) {
              log(s"$pre keeping $flag $arg")
              arg +: flag +: acc
            } else {
              log(s"$pre dropping $flag $arg")
              acc
            })
        case flag +: rest if modernOnlyWithoutArg(flag) =>
          filter(
            rest,
            if (modernJava) {
              log(s"$pre keeping $flag")
              flag +: acc
            } else {
              log(s"$pre dropping $flag")
              acc
            })
        case flag +: rest =>
          filter(rest, flag +: acc)
        case _ =>
          acc.reverse.toVector
      }
    filter(opts, Seq.empty)
  }

  val modernOnlyWithArg: Set[String] = Set("--add-exports")
  val modernOnlyWithoutArg = Set.empty[String] // for now anyway
  val verboseFiltering: Boolean =
    Option(System.getenv("VERBOSE_JAVA_OPTION_FILTERING")).exists(_.toUpperCase == "TRUE")

  // Unified logging options
  // https://docs.oracle.com/en/java/javase/11/tools/java.html#GUID-BE93ABDC-999C-4CB5-A88B-1994AAAC74D5
  // Nice list of java 8, 8+ equivalents
  // https://docs.oracle.com/javase/9/tools/java.htm#GUID-BE93ABDC-999C-4CB5-A88B-1994AAAC74D5__CONVERTGCLOGGINGFLAGSTOXLOG-A5046BD1

  private val D = "(\\d+).*".r
  // Returns version before the decimal point, unless 1.blah or below, in which case blah before the decimal point
  def javaMajorVersionString(javaVersionString: String) =
    javaVersionString.split('.').toList match {
      case "1" :: D(v) :: _ => v
      case D(v) :: _        => v
      case _                => throw new IllegalArgumentException(s"Can't parse java version $javaVersionString")
    }
  def javaMajorVersionInt(javaVersionString: String) = javaMajorVersionString(javaVersionString).toInt

  // Arguments with direct translations
  private val ancientToModern = Map(
    "-XX:+PrintGC" -> "-Xlog:gc",
    "-XX:+PrintGCDateStamps" -> "-Xlog:gc::utc",
    "-XX:+PrintGCTaskTimeStamps" -> "-Xlog:task*=debug",
    "-XX:+PrintGCTimeStamps" -> "-Xlog:gc::time",
    "-XX:+PrintGCDetails" -> "-Xlog:gc*",
    "-XX:+PrintTenuringDistribution" -> "-Xlog:age*=trace",
    "-XX:+PrintAdaptiveSizePolicy" -> "-Xlog:ergo*=trace",
    "-XX:+PrintHeapAtGC" -> "-Xlog:gc+heap=trace",
    "-XX:+PrintStringDeduplicationStatistics" -> "-Xlog:stringdedup*=debug",
    "-XX:+PrintReferenceGC" -> "-Xlog:ref*=debug",
    "-XX:+PrintGCApplicationStoppedTime" -> "-Xlog:safepoint",
    "-XX:+PrintGCApplicationConcurrentTime" -> "-Xlog:safepoint",
    "-XX:+G1PrintHeapRegions" -> "-Xlog:gc+region=trace",
    // selected negations, chosen for their existence in codetree runconfs and similar environs
    "-XX:-PrintGCDateStamps" -> "-Xlog:gc::none",
    "-XX:-PrintGCTimeStamps" -> "-Xlog:gc::none",
    "-XX:-PrintGCDetails" -> "-Xlog:gc*=off"
  )

  private val modernToAncient = ancientToModern.map(_.swap)

  private val ddecode = """-D(\d+)(\+|\-):(-\S+)""".r
  private val doubleBoundedDecode = """-D(\d+)_(\d+):(-\S+)""".r

  private val no11Equiv = Set(
    "-XX:+PrintGCCause", // "GC cause is now always logged."
    "-XX:+PrintPromotionFailure", // oracle forgot about this one
    "-XX:+UseGCLogFileRotation" // "What was logged for PrintTenuringDistribution." (???)
  )

  private val no11EquivStarts = Set(
    "-XX:GCLogFileSize=", // "Log rotation is handled by the framework."
    "-XX:NumberOfGCLogFiles=", // "Log rotation is handled by the framework."
    "-XX:PrintFLSStatistics=" // no mention of this anywhere
  )

  private val debugAgentStart = "-agentlib:jdwp="
  private val IgnoreUnrecognized = s"-XX:+IgnoreUnrecognizedVMOptions"

  /**
   * Convert options to those appropriate for the specified versions. If javaRuntimeVersion==0, we prepend
   * -XX:+IgnoreUnrecognizedVMOptions and keep all variants up through javaTargetVersion.
   */
  private def filterJavaRuntimeOptions(
      javaVersion: Int,
      opts: List[ParsedOpt],
      logcb: String => Unit): List[ParsedOpt] = {
    // If ignore-unrecognized is already set, assume the callers knows what they're doing, and skip translations
    val ignoreUnrecognized = opts.exists(_.text.contains(IgnoreUnrecognized))

    // tolerant means we keep all versions, adding the ignore-unrecognized flag
    val tolerantOutput = javaVersion == 0;
    val pre = s"For java $javaVersion:"

    val addExports: Seq[ParsedOpt] =
      if (!tolerantOutput) Seq.empty
      else {
        val ourOpts = (Option(System.getenv("JAVA_OPTS")) orElse Option(System.getProperty("java_opts_testing")))
          .filter(_.nonEmpty)
          .getOrElse {
            throw new AssertionError("JAVA_OPTS not available")
          }
        parseAll(ourOpts.split("\\s").to(Seq).filter(_.contains("--add-export")))
      }

    // Special, stifling of filtering, because not all callers have flexible logging.
    def log(msg: => String) = if (verboseFiltering) logcb(msg) else ()
    def logTrans(from: String, to: String): Unit = { log(s"$pre converting $from to $to") }

    var tolerated = false // have we yet had to tolerate inappropriate args

    // True if actually true, or if we're just being tolerant, in which case make a note of our generosity.
    def cond(x: Boolean): Boolean = x || {
      tolerated |= tolerantOutput
      tolerantOutput
    }

    val pass1: List[ParsedOpt] = (opts ++ addExports).flatMap { opt =>
      opt.text match {
        // thou shalt not process arguments that be not command-line options
        case arg if !arg.startsWith("-") =>
          opt :: Nil

        // Decode special syntax for conditionally unmasking runtime options
        // -D11+:-blah    => include -blah for java >= 11
        // -D14-:-blah    => include blah for java <= 14
        case ddecode(vs, ss, arg) =>
          val sign = if (ss == "+") 1 else -1
          val argVersion = vs.toInt
          // apply a hard cutoff to tolerant if in tolerance mode
          if ((tolerantOutput && sign > 0) || cond(javaVersion * sign >= argVersion * sign)) {
            log(s"$pre unmasking $arg")
            parse(arg) :: Nil
          } else {
            log(s"$pre excluding $arg")
            Nil
          }

        // Decode special syntax for conditionally unmasking runtime options
        // -D11_14:-blah    => include -blah for java >= 11 && java <= 14
        case doubleBoundedDecode(startStr, endStr, arg) =>
          val start = startStr.toInt
          val end = endStr.toInt
          // apply a hard cutoff to tolerant if in tolerance mode
          if (cond(javaVersion >= start && javaVersion <= end)) {
            log(s"$pre unmasking $arg")
            parse(arg) :: Nil
          } else {
            log(s"$pre excluding $arg")
            Nil
          }
        // Remove the tolerance flag, so we can put it at the beginning.
        case IgnoreUnrecognized =>
          log(s"Moving $IgnoreUnrecognized to beginning")
          Nil

        case arg if (arg.startsWith("--add-export") || arg.startsWith("--add-opens")) && !ignoreUnrecognized =>
          if (cond(javaVersion >= 9)) {
            log(s"$pre retaining $arg")
            opt :: Nil
          } else {
            log(s"$pre discarding $arg")
            Nil
          }

        case arg if (no11Equiv.contains(arg) || no11EquivStarts.exists(arg.startsWith)) && !ignoreUnrecognized =>
          if (cond(javaVersion < 9) || ignoreUnrecognized) opt :: Nil else Nil

        case arg if !ignoreUnrecognized =>
          // Direct translation from ancient to modern
          if (ancientToModern.contains(arg) && cond(javaVersion >= 9)) {
            val newArg = ancientToModern(arg)
            logTrans(arg, newArg)
            parse(newArg) :: Nil
          }

          // Direct translation from modern to ancient
          else if (modernToAncient.contains(arg) && cond(javaVersion < 9)) {
            val newArg = modernToAncient(arg)
            logTrans(arg, newArg)
            parse(newArg) :: Nil
          }

          // New java likes colons more than old java did
          else if (arg.startsWith("-Xloggc:") && cond(javaVersion >= 9)) {
            val newArg = "-Xlog:gc:" + arg.substring("-Xloggc:".length)
            logTrans(arg, newArg)
            parse(newArg) :: Nil
          } else if (arg.startsWith("-Xlog:gc:") && cond(javaVersion < 9)) {
            val newArg = "-Xloggc:" + arg.substring("-Xlog:gc:".length)
            logTrans(arg, newArg)
            parse(newArg) :: Nil
          }

          // new java requires explicit hostname specification if you intend it not to be localhost only
          // preserve that behavior on new java
          // the sort of string we are looking for is -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5115
          else if (arg.startsWith(debugAgentStart)) {
            val addressStart = arg.indexOf("address=") + "address=".length
            val addressEnd = {
              val nextComma = arg.indexOf(',', addressStart)
              if (nextComma < 0) arg.length else nextComma
            }
            // valid formats here are 12345, foobar:12345, *:12345
            // in java 8, 12345 means *:12345 in java 9+, but the latter is not accepted by java 8
            val address = arg.substring(addressStart, addressEnd)
            val newAddress = {
              if (address.startsWith("*:") && cond(javaVersion < 9))
                address drop "*:".length
              else if (!address.contains(':') && cond(javaVersion >= 9))
                "*:" + address
              else address
            }
            val newArg = arg.patch(addressStart, newAddress, address.length)
            logTrans(arg, newArg)
            parse(newArg) :: Nil
          }

          // Nothing special, it seems.
          else
            opt :: Nil

        case _ => opt :: Nil
      }
    }

    if (tolerated || ignoreUnrecognized) parse(IgnoreUnrecognized) +: pass1 else pass1
  }

}
