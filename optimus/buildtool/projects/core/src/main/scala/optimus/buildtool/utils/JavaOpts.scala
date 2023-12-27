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
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.reflect.internal.util.Collections
import scala.util.Properties
import scala.collection.compat._

object JavaOpts {

  private val WithMemoryParam = """(-X.*?)\d+[gGmMkK]""".r
  private val GCFlags = Set("-XX:+UseSerialGC", "-XX:+UseParallelGC", "-XX:+USeParNewGC", "-XX:+UseG1GC")
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

  val jvmJavaVersion: String = Properties.javaVersion
  val jvmPurtyVersion: String = JavaOptionFiltering.javaPurtyVersionString(jvmJavaVersion)
  val jvmMajorVersion: Int = JavaOptionFiltering.javaMajorVersionInt(jvmJavaVersion)

  private final case class ParsedOpt(name: String, text: String)

  def normalize(javaOpts: Seq[String]): Seq[String] = {
    val parsed = parseAll(javaOpts)
    val deduplicated = removeDuplicates(parsed)
    val verboseResolved = resolveVerboseFlags(deduplicated)
    val withoutEmptyProperties = removeEmptyProperties(verboseResolved)
    withoutEmptyProperties.map(_.text)
  }

  private def parseAll(javaOpts: Seq[String]): List[ParsedOpt] = {
    val split = CliArgs.normalize(javaOpts).to(List)
    val grouped = groupWithParams(split)
    grouped.map(parse)
  }

  def groupWithParams(opts: List[String]): List[String] = {
    def isParam(s: String) = !s.startsWith("-")

    @tailrec def recur(opts: List[String], res: List[String]): List[String] = opts match {
      case opt :: param :: rest if isParam(param) => recur(rest, s"$opt $param" :: res)
      case opt :: rest                            => recur(rest, opt :: res)
      case Nil                                    => res
    }

    recur(opts, Nil).reverse
  }

  private def parse(text: String): ParsedOpt = {
    def parsed(name: String) = ParsedOpt(name, text)
    def asIs = ParsedOpt(text, text)
    text match {
      case WithMemoryParam(name)      => parsed(name)
      case opt if GCFlags(opt)        => parsed("GcFlag")
      case JigsawParam(_)             => asIs
      case WithEqParam(name)          => parsed(name)
      case XXFlag(name)               => parsed("-XX:+/-" + name)
      case OtherXXOption()            => asIs
      case AppendableWithColonParam() => asIs
      case WithColonParam(name)       => parsed(name)
      case _                          => asIs
    }
  }

  private def removeDuplicates(parsed: List[ParsedOpt]): List[ParsedOpt] =
    parsed.reverse.distinctBy { o =>
      // java 9+ convention allows for multiple meaningful -Xlog:whatever options
      if (o.name == "-Xlog:") o.text
      else o.name
    }.reverse

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

  def filterJavaRuntimeOptions(javaRuntimeVersion: String, opts: Seq[String], log: String => Unit): Seq[String] =
    JavaOptionFiltering.filterJavaRuntimeOptions(javaRuntimeVersion, opts, log)

  // Called from stratosphere runconf
  def filterJavacOptions(release: Int, opts: Seq[String], logcb: String => Unit): Seq[String] = {
    val modernJava = release > 8
    val pre = s"For java $release,"
    def log(msg: String) = if (JavaOptionFiltering.verboseFiltering) logcb(msg) else ()
    @tailrec
    def filter(opts: Seq[String], acc: Seq[String]): Seq[String] =
      opts match {
        case flag +: arg +: rest if (JavaOptionFiltering.modernOnlyWithArg(flag)) =>
          filter(
            rest,
            if (modernJava) {
              log(s"$pre keeping $flag $arg")
              acc ++ Seq(flag, arg)
            } else {
              log(s"$pre dropping $flag $arg")
              acc
            })
        case flag +: rest if JavaOptionFiltering.modernOnlyWithoutArg(flag) =>
          filter(
            rest,
            if (modernJava) {
              log(s"$pre keeping $flag")
              acc :+ flag
            } else {
              log(s"$pre dropping $flag")
              acc
            })
        case flag +: rest =>
          filter(rest, acc :+ flag)
        case _ =>
          acc
      }
    filter(opts, Seq.empty)
  }

  implicit class DistinctList[A](val xs: List[A]) extends AnyVal {
    def distinctBy[B](f: A => B): List[A] =
      Collections.distinctBy(xs)(f)
  }

}

object JavaOptionFiltering {
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

  // Returns version before the decimal point, unless 1.blah or below, in which case "1." + blah before the decimal point
  def javaPurtyVersionString(javaVersionString: String) =
    javaVersionString.split('.').toList match {
      case "1" :: D(v) :: _ => "1." + v
      case D(v) :: _        => v
      case _                => throw new IllegalArgumentException(s"Can't parse java version $javaVersionString")
    }

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
  private val ignoreUnrecognized = "IgnoreUnrecognizedVMOptions"
  private val doIgnoreUnrecognized = s"-XX:+$ignoreUnrecognized"

  def filterJavaRuntimeOptions(javaVersion: String, opts: Seq[String], log: String => Unit): Seq[String] =
    filterJavaRuntimeOptions(javaMajorVersionInt(javaVersion), opts, log)

  /**
   * Convert options to those appropriate for the specified versions. If javaRuntimeVersion==0, we prepend
   * -XX:+IgnoreUnrecognizedVMOptions and keep all variants up through javaTargetVersion.
   */
  private def filterJavaRuntimeOptions(javaVersion: Int, opts: Seq[String], logcb: String => Unit): Seq[String] = {
    // If ignore-unrecognized is already set, assume the callers knows what they're doing, and just remove things like
    // -D11+:something
    if (opts.contains(doIgnoreUnrecognized))
      opts.filter(ddecode.unapplySeq(_).isEmpty)
    else {
      // tolerant means we keep all versions, adding the ignore-unrecognized flag
      val tolerantOutput = javaVersion == 0;
      val pre = s"For java $javaVersion:"

      val addExports: Seq[String] =
        if (!tolerantOutput) Seq.empty
        else {
          val ourOpts = (Option(System.getenv("JAVA_OPTS")) orElse Option(System.getProperty("java_opts_testing")))
            .filter(_.nonEmpty)
            .getOrElse {
              throw new AssertionError("JAVA_OPTS not available")
            }
          ourOpts.split("\\s").to(Seq).filter(_.contains("--add-export"))
        }

      // Special, stifling of filtering, because not all callers have flexible logging.
      def log(msg: => String) = if (verboseFiltering) logcb(msg) else ()
      def logTrans(from: String, to: String): Unit = { log(s"$pre converting $from to $to") }

      val seen = mutable.HashSet.empty[Seq[String]]

      var tolerated = false // have we yet had to tolerate inappropriate args

      def withArg(opts: Seq[String], newArg: String, oldArg: String): Seq[String] =
        withArgs(opts, Seq(newArg), Seq(oldArg))

      def withArgs(opts: Seq[String], newArgs0: Seq[String], oldArgs0: Seq[String]): Seq[String] = {
        // Add the transformed args if they haven't been seen before
        val newArgs = if (!seen(newArgs0)) {
          seen += newArgs0
          newArgs0
        } else Seq.empty
        // Keep the old args if we're being tolerant (they'll already have been filtered out if seen before)
        val oldArgs = if (tolerantOutput) {
          tolerated = true
          oldArgs0
        } else Seq.empty
        opts ++ oldArgs ++ newArgs
      }

      // Don't ignore previously seen for these.  We'll pick out the last as a final step.
      def gcSelectors(arg: String) = arg.startsWith("-XX:+Use") && arg.endsWith("GC")

      // True if actually true, or if we're just being tolerant, in which case make a note of our generosity.
      def cond(x: Boolean): Boolean = x || {
        tolerated |= tolerantOutput
        tolerantOutput
      }

      @tailrec def filterPass1(opts: Seq[String], acc: Seq[String]): Seq[String] = {
        opts match {

          // thou shalt not process arguments that be not command-line options
          case arg +: rest if !arg.startsWith("-") =>
            filterPass1(rest, acc :+ arg)

          case arg +: rest if seen(Seq(arg)) && !gcSelectors(arg) =>
            log(s"Removing duplicate $arg")
            filterPass1(rest, acc)

          case arg1 +: arg2 +: rest if seen(Seq(arg1, arg2)) =>
            log(s"Removing duplicate $arg1 $arg2")
            filterPass1(rest, acc)

          // Decode special syntax for conditionally unmasking runtime options
          // -D11+:-blah    => include -blah for java >= 11
          // -D14-:-blah    => include blah for java <= 14
          case ddecode(vs, ss, arg) +: rest =>
            seen += Seq(arg)
            val sign = if (ss == "+") 1 else -1
            val argVersion = vs.toInt
            // apply a hard cutoff to tolerant if in tolerance mode
            if ((tolerantOutput && sign > 0) || cond(javaVersion * sign >= argVersion * sign)) {
              log(s"$pre unmasking $arg")
              filterPass1(rest, acc :+ arg)
            } else {
              log(s"$pre excluding $arg")
              filterPass1(rest, acc)
            }

          // Remove the tolerance flag, so we can put it at the beginning.
          case discard +: rest if tolerantOutput && discard.endsWith(ignoreUnrecognized) =>
            log(s"Removing $discard")
            filterPass1(rest, acc)

          // two-argument version of --add-export
          case arg +: arg2 +: rest
              if (arg.startsWith("--add-export") || arg.startsWith("--add-opens")) && !arg.contains("=") =>
            seen += Seq(arg, arg2)

            if (cond(javaVersion >= 9)) {
              log(s"$pre retaining $arg $arg2")
              filterPass1(rest, acc :+ arg :+ arg2)
            } else {
              log(s"$pre discarding $arg $arg2")
              filterPass1(rest, acc)
            }

          case arg +: rest if no11Equiv.contains(arg) || no11EquivStarts.exists(arg.startsWith(_)) =>
            if (cond(javaVersion < 9))
              filterPass1(rest, acc :+ arg)
            else
              filterPass1(rest, acc)

          case arg +: rest =>
            seen += Seq(arg)

            // Direct translation from ancient to modern
            if (ancientToModern.contains(arg) && cond(javaVersion >= 9)) {
              val newArg = ancientToModern(arg)
              logTrans(arg, newArg)
              filterPass1(rest, withArg(acc, newArg, arg))
            }

            // Direct translation from modern to ancient
            else if (modernToAncient.contains(arg) && cond(javaVersion < 9)) {
              val newArg = modernToAncient(arg)
              logTrans(arg, newArg)
              filterPass1(rest, withArg(acc, newArg, arg))
            }

            // one-argument version of add-export
            else if (arg.startsWith("--add-export") || arg.startsWith("--add-opens")) {
              if (cond(javaVersion >= 9)) {
                filterPass1(rest, acc :+ arg)
              } else {
                log(s"$pre discarding $arg")
                filterPass1(rest, acc)
              }
            }

            // New java likes colons more than old java did
            else if (arg.startsWith("-Xloggc:") && cond(javaVersion >= 9)) {
              val newArg = "-Xlog:gc:" + arg.substring("-Xloggc:".length)
              logTrans(arg, newArg)
              filterPass1(rest, withArg(acc, newArg, arg))
            } else if (arg.startsWith("-Xlog:gc:") && cond(javaVersion < 9)) {
              val newArg = "-Xloggc:" + arg.substring("-Xlog:gc:".length)
              logTrans(arg, newArg)
              filterPass1(rest, withArg(acc, newArg, arg))
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
              filterPass1(rest, acc :+ newArg)
            }

            // Nothing special, it seems.
            else
              filterPass1(rest, acc :+ arg)
          case _ =>
            acc

        }
      }

      val pass1 = filterPass1(opts ++ addExports, Seq.empty)

      // Keep only last GC specification
      val pass2 = pass1.reverse
        .foldLeft((Option.empty[String], List.empty[String])) {
          case ((opt @ Some(last), acc), arg) if gcSelectors(arg) =>
            log(s"$pre Removing $arg in favor of last-specified $last")
            (opt, acc)
          case ((None, acc), arg) if gcSelectors(arg) =>
            // found last specified
            (Some(arg), arg :: acc)
          case ((o, acc), arg) =>
            (o, arg :: acc)
        }
        ._2

      val pass3 = if (tolerated) s"-XX:+$ignoreUnrecognized" +: pass2 else pass2

      pass3
    }
  }
}
