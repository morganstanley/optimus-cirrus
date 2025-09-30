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
package optimus.graph.diagnostics.ap

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.common.annotations.VisibleForTesting
import com.google.common.hash.Hashing
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.ProfilerSource
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.AwaitStackManagement
import optimus.graph.DiagnosticSettings
import optimus.graph.Exceptions
import optimus.graph.diagnostics.ap.StackType._
import optimus.graph.diagnostics.sampling.AsyncProfilerSampler
import optimus.graph.diagnostics.sampling.BaseSamplers
import optimus.graph.diagnostics.sampling.ForensicSource
import optimus.graph.diagnostics.sampling.NullSampleCrumbConsumer
import optimus.graph.diagnostics.sampling.SampleCrumbConsumer
import optimus.graph.diagnostics.sampling.SamplingProfiler
import optimus.graph.diagnostics.sampling.SamplingProfilerSource
import optimus.graph.diagnostics.sampling.TimedStack
import optimus.platform.util.Log
import optimus.utils.CollectionUtils._

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import optimus.scalacompat.collection._
import optimus.utils.PropertyUtils
import optimus.utils.MiscUtils.Endoish._
import optimus.utils.OptimusStringUtils

import java.lang.reflect.Method
import java.util.Objects
import scala.annotation.tailrec
import scala.util.Random
import scala.util.control.NonFatal
import java.lang.{Integer => JInteger, Long => JLong}
import java.nio.ByteBuffer
import java.util.Base64
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.lang.{StringBuilder => JStringBuilder}
import scala.collection.mutable

object StackType {
  val Adapted_DAL = "Adapted_DAL"
  val Alloc = "Alloc"
  val AllocNative = "AllocNative"
  val CacheHit = "CacheHit"
  val Cpu = "Cpu"
  val Free = "Free"
  val FreeNative = "FreeNative"
  val FullWait_NS_None = "FullWait.NS.None"
  val Live = "Live"
  val LiveNative = "LiveNative"
  val Lock = "Lock"
  val Rev = "Rev"
  val Unfired_DAL = "Unfired_DAL"
  val Wait = "Wait"
}

object StackAnalysis extends Log {
  val AppName = "App"
  val TotalName = "total"
  val GCName = "GC"
  val CompilerName = "Compiler"

  import mutable.{LongMap => LM}

  // So we can case match on LongMap[_] and then treat it immediately as LongMap[StackNode].
  // It would have been easier to extend LongMap, but it's final.
  implicit private class CastMap(val generic: LM[_]) extends AnyVal {
    def cast: LM[StackNode] = generic.asInstanceOf[LM[StackNode]]
  }

  object PruningStrategy {
    val leavesHaveSelfTime = 1
    val leavesHaveNoChildren = 2
  }

  private val stackCrumbCount = new AtomicLong(0)
  private val unDedupedStackCrumbCount = new AtomicLong(0)
  private[diagnostics] def numStacksPublished: Long = stackCrumbCount.get()
  private[diagnostics] def numStacksNonDeduped: Long = unDedupedStackCrumbCount.get()
  // Fully qualified class names are abbreviated to a maximum of N successive lower-case letters:
  val camelHumpWidth: Int = PropertyUtils.get("optimus.async.profiler.hump.width", 2)
  val camelHumpWidthExpand: Int = PropertyUtils.get("optimus.async.profiler.hump.width", 10)
  val frameNameCacheSize: Int = PropertyUtils.get("optimus.async.profiler.frame.cache.size", 1000000)
  val doAbbreviateFrames: Boolean = PropertyUtils.get("optimus.async.profiler.abbreviate", true)
  val stackPathPublishStackSize = PropertyUtils.get("optimus.async.profiler.substack.cache.size", 1000 * 1000)
  val stackCacheExpireAfterSec = PropertyUtils.get("optimus.async.profiler.stack.cache.expire.sec", 3600)
  val maxStackPathCrumbLength = PropertyUtils.get("optimus.async.profiler.substack.max.strlen", 5000)

  private val hashing = Hashing.murmur3_128()
  private def hashString(s: String) = Math.abs(hashing.hashUnencodedChars(s).asLong())
  private def combineHashes(h1: Long, h2: Long) = Math.abs(AwaitStackManagement.combineHashes(h1, h2))

  class PublishedPathHashes(size: Int) {
    private val dummy = new Object
    private val cache = Caffeine.newBuilder().maximumSize(size).build[JLong, Object]
    def apply(x: Long): Boolean = Objects.nonNull(cache.getIfPresent(x))
    def +=(x: Long): Unit = cache.put(x, dummy)
  }

  // To record whether we've yet published the cumulative hash of a stack path to a particular rootUuid.
  // We blow away the cache for each rootUuid stackCacheExpireAfterSec seconds after creation, to limit
  // damage from crumb infrastructure being briefly down.
  private val pathCaches = Caffeine
    .newBuilder()
    .expireAfterWrite(stackCacheExpireAfterSec, TimeUnit.SECONDS)
    .maximumSize(10)
    .build[ChainedID, PublishedPathHashes]
  private def getPathCache(id: ChainedID): PublishedPathHashes = {
    pathCaches.get(id, _ => new PublishedPathHashes(stackPathPublishStackSize))
  }

  // Minimal wrapper to allow re-use of large backing Array while enabling operations on
  // a smaller number of elements.
  private[ap] class BackedArray[T](private val inner: Array[T], val length: Int) extends Iterable[T] {
    private def indices = 0 until length
    override def last: T = inner(length - 1)
    override def head: T = inner(0)
    def apply(i: Int): T = inner(i)

    def mapInto[U](f: T => U)(backing: Array[U]): BackedArray[U] = {
      indices.foreach { i =>
        backing(i) = f(inner(i))
      }
      new BackedArray(backing, length)
    }
    override def exists(f: T => Boolean): Boolean = indices.exists(i => f(inner(i)))
    override def foreach[U](f: T => U): Unit = indices.foreach(i => f(inner(i)))
    def replace(f: T => T): Unit = indices.foreach(i => inner(i) = f(inner(i)))
    override def iterator = indices.iterator.map(inner)
  }
  private[ap] object BackedArray {
    def apply[T](arr: Array[T]): BackedArray[T] = new BackedArray(arr, arr.length)
  }

  private def mapIntoArray[T](from: Iterable[String], into: Array[T], f: String => T): BackedArray[T] = {
    val max = into.length
    var i = 0
    val it = from.iterator
    while (it.hasNext && i < max) {
      into(i) = f(it.next())
      i += 1
    }
    new BackedArray(into, i)
  }

  private def splitInto[T](s: String, sep: Char, into: Array[T], len: Int, transform: String => T): BackedArray[T] = {
    val max = into.length
    var i = 0
    val it = new SplitIterator(s, sep, len, transform)
    while (it.hasNext && i < max) {
      into(i) = it.next()
      i += 1
    }
    new BackedArray(into, i)
  }

  // Because String#split forces us to allocate an entire array to hold the copied strings.
  // This iterator also allows transformation of each substring immediately, which hopefully allows
  // the JIT to elide storing substrings on heap at all.
  // Additionally, this implementation ignores repeated, leading and trailing delimiters.
  class SplitIterator[T](s: String, sep: Char, len: Int = 0, f: String => T) extends Iterator[T] {
    private var j = 0
    private var wordStart = 0
    private var prev: Char = sep
    private val l = if (len > 0) len else s.length
    private var theNext: T = _
    private var gotNext = false

    override def next(): T = {
      if (!gotNext)
        prepareNext()
      val ret = theNext
      gotNext = false
      ret
    }

    override def hasNext: Boolean = {
      gotNext || {
        prepareNext()
        gotNext
      }
    }

    private def prepareNext(): Unit = {
      while (!gotNext && j < l) {
        val curr = s.charAt(j)
        if (curr != sep) {
          if (j == l - 1) {
            gotNext = true
            theNext = f(s.substring(wordStart, l))
          }
        } else {
          if (prev != sep) {
            gotNext = true
            theNext = f(s.substring(wordStart, j))
          }
          wordStart = j + 1
        }
        prev = curr
        j += 1
      }
    }
  }

  object SplitIterator {
    def apply(s: String, sep: Char, len: Int = 0) = new SplitIterator(s, sep, len, identity)
  }

  // Split off count.
  // "foo;bar;wiz 37" => ("foo;bar;wiz", 11, 37)
  // If forceCopy is false AND there are no spaces other than that preceeding the count, then
  // return the original string and the length of the stack piece.
  @VisibleForTesting
  private[diagnostics] def separateStackAndCount(stackLine: String, forceCopy: Boolean): (String, Int, Long) = {
    var i = stackLine.length - 1
    // step backwards past trailing spaces
    while (i > 0 && stackLine(i) == ' ') { i -= 1 }
    val endNum = i + 1 // presumably end of counter bit
    // step backwards past counter
    while (i > 0 && stackLine(i) >= '0' && stackLine(i) <= '9') { i -= 1 }
    val beginNum = i + 1
    if (beginNum >= endNum || beginNum <= 3) return ("", 0, 0)
    while (i > 0 && stackLine(i) == ' ') { i -= 1 }
    val endStack = i + 1
    if (endStack <= 3) return ("", 0, 0)
    val n = stackLine.substring(beginNum, endNum).toLong

    // Maybe there are no additional spaces, so we can get away with returning
    // the original string?
    if (stackLine.indexOf(' ') >= endStack && !forceCopy)
      return (stackLine, endStack, n)

    // remove spaces
    i = 0
    val sb = new JStringBuilder()
    while (i < endStack) {
      val c = stackLine.charAt(i)
      if (c != ' ') sb.append(c)
      i += 1
    }
    val stack = sb.toString()
    (stack, stack.length, n)
  }

  private[diagnostics] def stackLineToTimedStack(stackLine: String): TimedStack = {
    val (stack, _, count) = separateStackAndCount(stackLine, forceCopy = true)
    TimedStack(stack.split(';'), count)
  }

  final case class StackData private (tpe: String, total: Long, self: Long, hashId: String, folded: String)

  final case class StacksAndTimers(
      stacks: Seq[StackData],
      timers: SampledTimers,
      rootSamples: Map[String, Long],
      dtStopped: Long = 0L)
  object StacksAndTimers { val empty = StacksAndTimers(Seq.empty, SampledTimersExtractor.newRecording, Map.empty) }

  // Java20+ likes to give lambdas these unique suffixes e.g. "$La$722.0x000000080173d0.apply"
  val lambdaRe = "[\\$\\d\\.]*0x\\w+\\.".r
  val lambdaPrefixSearch = ".0x0"
  val lambdaSubstitute = "\\$0x1a3bda" // escapes, since will be used in regex method
  val fullLambdaRe = "\\$+([A-Z]\\w)\\w*[\\$\\d\\.]*0x[\\dabcdef]{5,}\\.(\\w+)".r
  // Really long and plausibly a hex number.  These are inserted by the scala compiler, so we'll only have
  // to search for them once before the frame is memoized.
  private val longHexRe = "\\.[\\dabcdef]{10,}\\.".r

  // By contrast, java lambdas might be synthesized in their multitudes, so we want to remove them before
  // memoizing.
  def undisambiguateLambda(frame0: String): String =
    if (frame0.contains(lambdaPrefixSearch)) {
      val ret = fullLambdaRe.replaceAllIn(frame0, m => "." + m.group(1) + lambdaSubstitute + "_" + m.group(2))
      ret
    } else frame0

  private val CUSTOM_EVENT_PREFIX = "[custom="
  private val STACK_ID_PREFIX = "[sid="
  def customEventFrame(event: String) = s"[custom=$event]"

  private val ignoredPrefixes =
    Seq(STACK_ID_PREFIX, CUSTOM_EVENT_PREFIX)
  private def ignoreFrame(frame: String): Boolean = {
    ignoredPrefixes.exists(frame.startsWith(_))
  }

  val GCPredicate: Long =
    (StringPredicate.contains("GCTaskThr") or StringPredicate.contains("__clone") or StringPredicate.contains(
      "thread_native_entry")).assignMask()
  val JITPredicate: Long =
    (StringPredicate.contains("Compile:") or StringPredicate.contains("CompileBroker::")).assignMask()

  val DefaultFlameExclusions: Long =
    (StringPredicate.startsWith("je_prof") or StringPredicate.startsWith("je_malloc") or StringPredicate.startsWith(
      "Profiler::") or StringPredicate.contains(".runAndWait") or StringPredicate.startsWith("sampleHook")).assignMask()

  private def looksLikeGCStack(stack: BackedArray[CleanName]) = stack.exists(_.matches(GCPredicate))

  private def looksLikeJITStack(stack: BackedArray[CleanName]) = stack.exists(_.matches(JITPredicate))

  private class Unmemoizer() extends Function1[String, String] {
    val memo: LM[String] = mutable.LongMap.empty[String]
    // The first time "myFrameName" is encountered, it looks like
    //    123=myFrameName
    // Subsequently, it's just 123.
    def apply(name: String): String = {
      if (name.isEmpty || !name.head.isDigit) return name
      try {
        val sep = name.indexOf("=")
        if (sep < 0)
          return memo(name.toInt)
        val i = name.substring(0, sep).toInt
        val realName = name.substring(sep + 1)
        memo.put(i, realName)
        realName
      } catch {
        case _: Exception => name
      }
    }
  }

  def decryptDump(dump: String): String = {
    val unmemoizer = new Unmemoizer
    dump
      .split("\n")
      .map(_.split(" "))
      .collect { case Array(stacks, count) =>
        val stack = stacks.split(";").map(unmemoizer).mkString(";")
        s"$stack $count"
      }
      .mkString("\n")
  }

  final class CleanName private[optimus] (
      val name: String,
      val orig: String,
      val id: Long,
      val flags: Int,
      val predMask: Long,
      val hash: Long
  ) {
    import CleanName._

    def matches(mask: Long): Boolean = (predMask & mask) != 0L

    def isFree: Boolean = (flags & FREE) == FREE

    override def toString: String = s"($name, $id)"

    override def hashCode(): Int = id.hashCode()
    override def equals(obj: Any): Boolean = obj match {
      case cn: CleanName => id == cn.id
      case _             => false
    }

    def rootNode = new StackNode(this, 0, hash, NoNode)
  }

  def prettyFrameName(frame: String): String = CleanName.prettyFrameName(0, frame.size, frame, false)

  object CleanName {
    val FREE = 1
    val LIVE = 2
    val SPECIAL = 4
    private val frameNameCache =
      Caffeine.newBuilder().maximumSize(frameNameCacheSize).build[String, CleanName]
    private val localFrameIds = new AtomicLong(0)

    private val abbreviateError = CleanName("ERROR", "ERROR", flags = 0, predMask = 0L, 0L)

    private def apply(name: String, orig: String, flags: Int, predMask: Long, hash: Long): CleanName = {
      new CleanName(name, orig, localFrameIds.incrementAndGet(), flags, predMask, hash)
    }

    def cleanName(frame: String): CleanName = cleanName(frame, false, doAbbreviateFrames, 0)
    def cleanName(frame: String, flags: Int): CleanName = cleanName(frame, false, doAbbreviateFrames, flags)
    def cleanName(frame: String, hasFrameNum: Boolean): CleanName =
      cleanName(frame, hasFrameNum, doAbbreviateFrames, 0)

    def internOnly(frame: String): CleanName = {
      frameNameCache.get(frame, frame => CleanName(frame, frame, 0, 0, hashString(frame)))
    }

    def shortPackageName(pkgName: String): String = {
      if (pkgName == null || pkgName.length < 2) ""
      else {
        val prefix = new JStringBuilder
        prefix.append(pkgName.charAt(0))
        var i: Int = 1
        while (i < pkgName.length - 1) {
          if (pkgName.charAt(i) == '.') {
            prefix.append('.').append(pkgName.charAt(i + 1))
            i += 1 // eat next char
          }
          i += 1
        }
        prefix.append('.')
        prefix.toString
      }
    }

    private val trailingDollarStuff = "[\\d\\$]+$".r

    private def dedollarize(s: String): String =
      if (!s.contains('$')) s else trailingDollarStuff.replaceFirstIn(s, "")

    private def tryOrNull[T <: AnyRef](o: AnyRef, f: => T): T =
      try { f }
      catch {
        // Yes, Throwable, because Class methods sometimes, inexplicably, throw InternalError
        case t: Throwable =>
          whine(o, t)
          null.asInstanceOf[T]
      }

    private var complaintProbability = 1.0
    var numComplaints = 0
    def whine(o: Object, t: Throwable): Unit = {
      BaseSamplers.increment(Properties.numStackProblems, 1)
      // Always report the first.  Every time we report reduce chance of reporting by factor of 10.
      val (doit, n) = synchronized {
        numComplaints += 1
        val doit = Random.nextDouble() < complaintProbability
        if (doit) complaintProbability /= 10
        (doit, numComplaints)
      }
      if (doit) {
        val s =
          try { o.toString }
          catch { case _: Throwable => "???" }
        log.warn(s"Ignoring exception $numComplaints prettifying $s", t)
        Breadcrumbs.warn(
          ChainedID.root,
          PropertiesCrumb(
            _,
            ForensicSource,
            Properties.logMsg -> s"Ignoring exception prettifying $s",
            Properties.countNum -> n,
            Properties.exception -> t,
            Properties.stackTrace -> Exceptions.minimizeTrace(t, maxDepth = 20)
          )
        )
      }
    }

    private def cleanClass(clz: Class[_]): String = {
      // This can be called recursively, so we can't use Cache#get.
      val cached = clzPrettyNameCache.getIfPresent(clz)
      if (Objects.nonNull(cached)) cached
      else {
        val ret = {
          val sn = dedollarize(clz.getSimpleName)
          // These methods are supposed to return null when there is no encloser, but apparently
          // they sometimes throw.  In any case, avoid heavy Try/Option wrappers.
          val em = tryOrNull(clz, clz.getEnclosingMethod)
          if (Objects.nonNull(em)) cleanMethod(em) + "." + sn
          else {
            val ec = tryOrNull(clz, clz.getEnclosingClass)
            if (Objects.isNull(ec)) {
              val pkg = tryOrNull(clz, clz.getPackageName)
              if (Objects.isNull(pkg) || pkg.isEmpty) sn
              else shortPackageName(pkg) + sn
            } else
              cleanClass(ec) + "." + sn
          }
        }
        clzPrettyNameCache.put(clz, ret)
        ret
      }
    }
    private def cleanMethod(method: Method): String = {
      val ec = tryOrNull(method, method.getDeclaringClass)
      val m = method.getName
      if (Objects.isNull(ec)) m
      else {
        SamplingProfiler.classToFrame(ec, m).getOrElse(cleanClass(ec) + "." + m)
      }
    }

    // Every class we encounter in the stack trace should have been loaded already, so we want to avoid searching the classpath.
    private val flcm = classOf[ClassLoader].getDeclaredMethod("findLoadedClass", classOf[String])
    flcm.setAccessible(true)
    private val cl = classOf[StackAnalysis].getClassLoader
    private def findLoadedClass(name: String): Class[_] = cl.synchronized {
      flcm.invoke(cl, name).asInstanceOf[Class[_]]
    }

    private val clzCache: Cache[String, Class[_]] =
      Caffeine.newBuilder().maximumSize(100000).weakValues().build[String, Class[_]]
    private val clzPrettyNameCache = Caffeine.newBuilder().maximumSize(100000).weakKeys().build[Class[_], String]
    private val specialClzPrettyNameCache =
      Caffeine.newBuilder().maximumSize(100000).weakKeys().build[Class[_], Option[String]]

    private class NoClassClass
    private val NoClass = classOf[NoClassClass]

    private def forName(name: String): Class[_] = clzCache.get(
      name,
      name =>
        try {
          val c = findLoadedClass(name)
          if (Objects.nonNull(c)) c else NoClass
        } catch {
          case t: Throwable => NoClass
        })

    private val breakOn = System.getProperty("optimus.sampling.debug.frame.name", "")

    def prettyFrameName(start: Int, methodEnd: Int, frame: String, hadLambda: Boolean): String = {

      if (breakOn.nonEmpty && frame.contains(breakOn))
        log.info(s"Prettifying $frame")

      var i = start
      val methodStart = {
        var m = methodEnd - 1
        while (m >= i && frame(m) != '.' && frame(m) != '/') { m -= 1 }
        m
      }

      if (methodStart < i)
        return frame.substring(i, methodEnd)

      val method = {
        val m = if (methodStart + 1 >= methodEnd) "" else frame.substring(methodStart + 1, methodEnd)
        // Clean up methods like scala/reflect/runtime/SynchronizedSymbols$SynchronizedSymbol$$anon$9.scala$reflect$runtime$SynchronizedSymbols$SynchronizedSymbol$$super$info
        // by removing the part of the method that parallels the fqcn
        if (!hadLambda && m.contains('$')) {
          var methodStart = 0
          var lastDollar = -1
          var i = 0
          while (i < frame.size - start && i < m.size) {
            if (m(i) == '$') lastDollar = i
            else if (m(i) != frame(start + i)) i = m.size
            else methodStart = lastDollar + 1
            i += 1
          }
          if (methodStart > 0)
            m.substring(methodStart)
          else m
        } else m
      }

      def fallbackAbbreviation(): String = {
        // Try to extract and clean a valid class
        val stuff = if (methodStart > 0) {
          // construct op.gr.OGSchCon, e.g.
          val sb = new JStringBuilder()
          var charsToKeep = camelHumpWidth
          while (i < methodStart) {
            val c = frame(i)
            i += 1
            if (c == '.' || c == '/') {
              charsToKeep = camelHumpWidth
              sb.append('.')
            } else if (c.isUpper || c == '$' || c == '@' || (c >= '0' && c <= '9')) {
              charsToKeep = camelHumpWidthExpand
              sb.append(c)
            } else if (charsToKeep > 0) {
              charsToKeep -= 1
              sb.append(c)
            }
          }
          sb.toString
        } else ""
        s"$stuff.$method"
      }

      var pretty = {
        val possibleClassName = frame.substring(i, methodStart).replace('/', '.')
        val clz = {
          var clz = forName(possibleClassName)
          // Lambda disambiguation might eat too many dollar signs
          if ((clz eq NoClass) && hadLambda) clz = forName(possibleClassName + "$")
          clz
        }
        if (clz ne NoClass) {
          try {
            val classFrame = specialClzPrettyNameCache.get(clz, clz => SamplingProfiler.classToFrame(clz, method))
            classFrame.getOrElse {
              val clzName = cleanClass(clz)
              s"$clzName.$method"
            }
          } catch {
            case t: Throwable =>
              whine(possibleClassName, t)
              s"$possibleClassName.$method"
          }
        } else
          fallbackAbbreviation()
      }

      // If we still random looking hex strings, get rid of them.
      pretty = longHexRe.replaceAllIn(pretty, "_")

      pretty
    }

    def cleanName(frame: String, hasFrameNum: Boolean, abbreviate: Boolean, flags: Int): CleanName = {
      def clean(frame: String, hasFrameNum: Boolean, abbreviate: Boolean, hadLambda: Boolean): CleanName = {
        val predMask: Long = StringPredicate.getMaskForAllPredicates(frame)

        var i = 0

        val hash = hashString(frame)

        if (hasFrameNum) {
          val length = frame.size
          // Step forward past the frame number
          while (i < length && frame(i) != ']') { i += 1 }
          i += 1
          while (i < length && frame(i) == ' ') { i += 1 }
          if (i == length)
            return abbreviateError
        }

        val f = if (i > 0) frame.substring(i) else frame

        // async methods end with "_[a]" (see optimus.graph.AwaitStackManagment.StringPointers.getName where we add this); in any case, remove args
        val methodEnd = {
          var j = f.indexOf("_[a]")
          if (j < 0) j = f.indexOf('(')
          if (j < 0) f.size else j
        }

        if (!abbreviate || f.contains("::") || f.startsWith("[") || f.startsWith("@")) {
          var cleaned = frame.substring(i, methodEnd).replace('/', '.')
          cleaned = longHexRe.replaceAllIn(cleaned, "_")
          CleanName(cleaned, f, flags, predMask, hash)
        } else {
          val pretty = prettyFrameName(0, methodEnd, f, hadLambda)
          CleanName(pretty, f, flags, predMask, hash)
        }
      }

      val lambdaLess = undisambiguateLambda(frame)
      frameNameCache.get(lambdaLess, f => clean(f, hasFrameNum, abbreviate, (frame ne lambdaLess)))
    }

  }

  def rootStackNode(name: String, flags: Int = 0) = {
    val cn = CleanName.cleanName(name)
    new StackNode(cn, flags, cn.hash, NoNode)
  }

  private final case class BackupStackNodeState(kids: AnyRef, var total: Long, var self: Long)

  // Mixed into leaf node to designate a complete published stack (as opposed to the cumulative hash
  // of a possibly partial stack).  The value here is irrelevant but should be shared across jvms.
  private val stackHashTerminator: Long = hashString("this is the way the stack ends")

  val NoNode: StackNode = null

  // Each node is identified by an id unique to the frame name, plus a link to its
  // parent on the tree.  This is a conventional approach for representing flame graphs.
  final class StackNode(val cn: CleanName, val depth: Int, val pathHash: Long, val parent: StackNode) {
    def nameId: Long = cn.id
    def name: String = cn.name
    def getTotal: Long = total
    def getSelf: Long = self

    def nonEmpty: Boolean = this ne NoNode

    // be used by Profire tree builder
    private[optimus] def setTotal(samples: Long): Unit = total = samples
    private[optimus] def addTotal(samples: Long): Unit = total += samples
    private[optimus] def addSelf(samples: Long): Unit = self += samples
    private[optimus] def merge(other: StackNode): Int = {
      this.total += other.total
      this.self += other.self
      var childCount = 0
      for (otherChild <- other.kiderator) {
        val child = this.getOrCreateChildNode(otherChild.cn, () => childCount += 1)
        childCount += child.merge(otherChild)
      }
      childCount
    }

    def terminalHash: Long = combineHashes(pathHash, stackHashTerminator)

    private[StackAnalysis] var kids: AnyRef = null // (StackNode | LongMap[StackNode])
    private[StackAnalysis] var total = 0L
    private[StackAnalysis] var self = 0L

    private var _backup: BackupStackNodeState = null
    private def backup(): Unit = {
      val kidClone = kids match {
        case m: LM[_] =>
          m.cast.clone()
        case x =>
          x
      }
      _backup = BackupStackNodeState(kidClone, total, self)
    }
    private def restore(): Unit = {
      kids = _backup.kids
      total = _backup.total
      self = _backup.self
      _backup = null
    }

    // All encountered apsids for a particular leaf node when aggregating allocations and frees. If/when the allocation
    // for that leaf node drops to zero, we will remove the node and remove all these apsids from the global dictionary
    // as well.
    private[StackAnalysis] val apsids = mutable.TreeSet.empty[Long]

    override def toString: String = s"[$nameId, $name, $total, $self]"

    def isLeaf: Boolean = kids match {
      case null                      => true
      case map: LM[_] if map.isEmpty => true
      case _                         => false
    }

    def kiderator: Iterator[StackNode] =
      kids match {
        case null            => Iterator.empty
        case only: StackNode => Iterator(only)
        case map: LM[_]      => map.cast.valuesIterator
      }

    def sortedKids[B](sort: StackNode => B)(implicit ord: Ordering[B]): Seq[StackNode] =
      kids match {
        case null            => Seq.empty
        case only: StackNode => Seq(only)
        case map: LM[_]      => map.cast.values.toSeq.sortBy(sort)
      }

    private def updateChild(childId: Long, birth: () => StackNode): StackNode = {
      kids match {
        case null =>
          val ret = birth()
          kids = ret
          ret
        case only: StackNode =>
          if (only.nameId == childId) only
          else {
            val ret = birth()
            kids = LM(only.nameId -> only, ret.nameId -> ret)
            ret
          }
        case map: LM[_] =>
          map.cast.getOrElseUpdate(childId, birth())
      }
    }

    def getOrCreateChildNode(childName: CleanName): StackNode = {
      def newChildNode(): StackNode =
        new StackNode(childName, depth + 1, combineHashes(this.pathHash, childName.hash), this)
      updateChild(childName.id, newChildNode)
    }

    def getOrCreateChildNode(childName: CleanName, onAdd: () => Unit): StackNode = {
      def newChildNode(): StackNode = {
        onAdd()
        new StackNode(childName, depth + 1, combineHashes(this.pathHash, childName.hash), this)
      }
      updateChild(childName.id, newChildNode)
    }

    def dropKid(childId: Long): StackNode = kids match {
      case null =>
        null
      case only: StackNode =>
        if (only.nameId == childId) kids = null
        only
      case map: LM[_] =>
        val ret = map.remove(childId).getOrElse(null).asInstanceOf[StackNode]
        map -= childId
        if (map.isEmpty) kids = null
        ret
    }

    def addPath(names: Iterable[CleanName], count: Long): StackNode = {
      var node = this
      names.foreach { name =>
        node = node.getOrCreateChildNode(name)
      }
      node.add(count)
      node
    }

    def addFolded(folded: String, backer: Array[CleanName], count: Long): StackNode = {
      val names = splitInto(folded, ';', backer, 0, CleanName.internOnly(_))
      addPath(names, count)
    }

    // Add self time to this node, and total time to all nodes back to root
    def add(t: Long, apsid: Long): StackNode = {
      self += t
      if (apsid != 0) this.apsids += apsid
      var s = this
      do {
        s.total += t
        s = s.parent
      } while (s ne null)
      this
    }
    def add(t: Long): StackNode = add(t, 0L)

    // Returns non-null if parent is now a leaf
    def pruneFromParent(): StackNode = {
      // parent total weight *remains the same* because we simply moved our total from parent.kids to parent.total
      parent.self += total
      parent.kids match {
        case only: StackNode =>
          assert(only eq this)
          parent.kids = null
          parent
        case map: LM[_] =>
          map -= nameId
          if (map.size == 1) {
            parent.kids = map.cast.values.head
            null
          } else if (map.isEmpty) {
            parent.kids = null
            parent
          } else null
      }
    }

    def pushSelfTimesToLeaves(): Unit = {
      // Consider the following stacks,
      //
      // A -> B -> C1 (8)
      // A -> B -> C2 (4)
      // A -> B (3)
      // A -> D (15)
      //
      // We want to prune the stacks by not reporting the A -> B stack, but only the deeper leaves.
      //
      // If we simply A->B, then A -> B will now have an overall weight of 12 instead of 15, and so we will
      // overestimate the contribution of A -> D. To avoid that, we redistribute the self time of B to its children.
      // We distribute the weight to the children in proportion to their total weight, so as to not bias their
      // relative proportions. So in our example, after pruning we will have B -> C1 with weight 10 and B -> C2 with
      // weight 5. We maintained the 2:1 proportion between C2 and C1 and the equal proportion between A -> D and
      // A -> B.

      if (kids eq null) return // a leaf

      kids match {
        case only: StackNode =>
          only.self += self
          only.total += self
          self = 0L
          only.pushSelfTimesToLeaves()
        case lm: LM[_] =>
          // We sort children by self time in ascending order so that redistribution always starts with the smallest leaves.
          // This ensures that redistribution preserves the relative proportions of the heavier nodes, and also removes
          // the non-deterministic order issue of Map.values.
          // Otherwise, with the same input the redistribution results may vary.
          val kidsArr = lm.cast.valuesIterator.toArray.sortInPlaceBy(_.self)
          if (self > 0L) {
            val sum = kidsArr.map(_.total).sum
            val toAdd = self

            for (k <- kidsArr) {
              val incr =
                if (sum > 0) {
                  // The conversion to double is there to avoid overflow in the total * toAdd multiplication. Due to
                  // numerical imprecision, we have to make sure that self time is == 0 by the end of the pruning.
                  k.total.toDouble * toAdd.toDouble / sum.toDouble
                }.toLong
                else {
                  // This branch should never happen... but this is going into prod, and we care about prod not randomly dying
                  // due to errors in instrumentation code.
                  if (DiagnosticSettings.samplingAsserts) assert(false, s"total weight of children was ${sum}")
                  toAdd / kidsArr.length
                }
              k.self += incr
              k.total += incr
              self -= incr
            }

            if (DiagnosticSettings.samplingAsserts)
              assert(self >= -toAdd, s"self count $self should remain (mostly) positive")

            // Adjust the time of the last child to account for division errors
            kidsArr.head.self += self
            kidsArr.head.total += self
            self = 0L
          }
          if (DiagnosticSettings.samplingAsserts) assertConsistentTimes()
          kidsArr.foreach(_.pushSelfTimesToLeaves())
      }
    }

    def assertConsistentTimes(): Unit = {
      def times =
        s"\n$cn TIMES: self: ${self}, total ${total}, children: [${kiderator.map(_.total).mkString(", ")}]"
      assert(total == self + kiderator.map(_.total).sum, s"total time inconsistent ${times}")
      assert(self >= 0L, s"negative self time ${times}")
      assert(total >= 0L, s"negative total time ${times}")
    }

    def traverse[P](p: P, f: (P, StackNode) => P): Unit = {
      @tailrec def traverseImpl(stack: List[(P, StackNode)], f: (P, StackNode) => P): Unit = stack match {
        case Nil =>
        case (parentContext, node) :: rest =>
          val childContext = f(parentContext, node)
          val stack = node.kids match {
            case only: StackNode =>
              (childContext, only) :: rest
            case map: LM[_] =>
              map.cast.values.toSeq.sortBy(_.name).map((childContext, _)).toList ::: rest
            case null => rest
          }
          traverseImpl(stack, f)
      }
      traverseImpl(List((p, this)), f)
    }

    def traverse(f: StackNode => Unit): Unit = {
      @tailrec def traverseImpl(stack: List[StackNode], f: StackNode => Unit): Unit = stack match {
        case Nil =>
        case node :: rest =>
          f(node)
          val stack = node.kids match {
            case only: StackNode =>
              only :: rest
            case map: LM[_] =>
              map.cast.valuesIterator.toList ::: rest
            case null =>
              rest
          }
          traverseImpl(stack, f)
      }
      traverseImpl(List(this), f)
    }

    def backupLinks(): Unit = traverse { sn => sn.backup() }

    def restoreLinks(): Unit = traverse { sn => sn.restore() }

    def treeFormat(append: String => Unit): Unit = {
      this.traverse(
        0,
        (depth: Int, n) => {
          append(" " * depth + s"${n.name} ${n.total} ${n.self}")
          depth + 2
        })
    }

    def treeString: String = {
      val sb = ArrayBuffer.empty[String]
      treeFormat(sb += _)
      sb.mkString("\n")
    }

    def dotFormat(append: String => Unit): Unit = {
      append("digraph StackTree {\n")
      append("  rankdir=TB;\n")
      append("  node [shape=box];\n\n")
      var id = 0

      traverse(
        0,
        (parentId: Int, node: StackNode) => {
          id += 1
          if (parentId > 0) append(s"  node${parentId} -> node${id};\n")
          append(s"""  node${id} [label="${node.name} total=${node.total} self=${node.self}"];\n""")
          id
        }
      )
      append("}\n")
    }

    def dotString: String = {
      val sb = new JStringBuilder()
      dotFormat(sb.append(_))
      sb.toString
    }

    def csvFormat(append: String => Unit): Unit = {
      append("Frame,Caller,Method,Total,Self\n")
      var id = 0
      def traverse(parentId: Int, node: StackNode): Unit = {
        id += 1
        append(s"$id,$parentId,\"${node.name}\",${node.total},${node.self}\n")
        node.kiderator.foreach(traverse(id, _))
      }
      traverse(0, this)
      toString
    }

    def csvString: String = {
      val sb = new JStringBuilder()
      csvFormat(sb.append(_))
      sb.toString
    }

    def foldedFormat(append: String => Unit): Unit = {
      this.traverse { node =>
        if (node.isLeaf)
          append(node.frames.mkString(";") + " " + node.total + "/n")
      }
    }

    def foldedString: String = {
      val sb = new JStringBuilder()
      foldedFormat(sb.append(_))
      sb.toString
    }

    // for debugging mostly
    def root: StackNode = {
      var node = this
      while (node.parent ne NoNode)
        node = node.parent
      node
    }

    def frames: Seq[String] = {
      var fromRoot: List[StackNode] = Nil
      var node = this
      while (node.parent ne NoNode) { // i.e. don't print root itself
        if (node.name.nonEmpty)
          fromRoot = node :: fromRoot
        node = node.parent
      }
      fromRoot.map(_.name)
    }

    def pathMemoizedStackString(abbrevCache: PublishedPathHashes): Seq[(Long, String)] = {
      var sb = new JStringBuilder
      var paths: Seq[(Long, String)] = Seq.empty
      // Work backwards from leaf
      var node = this
      var fromRoot: List[StackNode] = Nil
      var known = false
      while (!known && Objects.nonNull(node.parent)) {
        if (node.name.nonEmpty) {
          fromRoot = node :: fromRoot
          known = abbrevCache(node.pathHash)
        }
        node = node.parent
      }
      if (fromRoot.isEmpty)
        return Seq.empty
      val hash = fromRoot.head.pathHash
      if (known) {
        sb.append(longAbbrev(hash, "=="))
      } else {
        abbrevCache += hash
        sb.append(s"${longAbbrev(hash, "=")}=${fromRoot.head.cn.name}")
      }

      val it = fromRoot.tail.iterator
      while (it.hasNext) {
        node = it.next()
        abbrevCache += node.pathHash
        val w = s";${longAbbrev(node.pathHash, "=")}=${node.cn.name}"
        sb.append(w)
        if (sb.length() > maxStackPathCrumbLength && it.hasNext) {
          // This shouldn't happen often, but if it does, we'll break up the stacks.
          paths = paths :+ (node.terminalHash, sb.toString())
          sb = new JStringBuilder
          // Next segment starts with the frame we just published
          sb.append(longAbbrev(node.pathHash, "=="))
        }
      }
      paths = paths :+ ((node.terminalHash, sb.toString))
      paths
    }

    def stackString = frames.mkString(";") + s" $total $self"
  }

  val encoder64 = Base64.getUrlEncoder
  def longAbbrev(v: Long, prefix: String): String = {
    val b64 = encoder64.encodeToString(ByteBuffer.allocate(8).putLong(v).array)
    // drop terminal = signs
    val i = b64.indexOf('=')
    prefix + (if (i > 0) b64.substring(0, i) else b64)
  }

}

class StackAnalysis(
    crumbConsumer: SampleCrumbConsumer = NullSampleCrumbConsumer,
    properties: Map[String, String] = Map.empty)
    extends Log
    with OptimusStringUtils {
  import StackAnalysis._

  val propertyUtils = new PropertyUtils(properties)

  val stackCacheSize = propertyUtils.get("optimus.async.profiler.stack.cache.size", 100000)
  val thumbprintDimensionBits = propertyUtils.get("optimus.async.profiler.thumbprint", 0)
  val thumbprintDimension = 1 << thumbprintDimensionBits
  val maxFrames = propertyUtils.get("optimus.async.profiler.stack.maxframes", 250)
  val pruneStrategy = propertyUtils.get("optimus.async.profiler.prune.strategy", PruningStrategy.leavesHaveNoChildren)
  val trackLiveMemory = propertyUtils.get("optimus.async.profiler.track.live.memory", true)
  val publishFullStacks = propertyUtils.get("optimus.sampling.stacks.full", true)
  val publishStackPaths = propertyUtils.get("optimus.sampling.stacks.paths", false)
  val aggressiveElision = propertyUtils.get("optimus.sampling.elide.frames", false)

  private val stackPublicationTime =
    Caffeine
      .newBuilder()
      .maximumSize(stackCacheSize)
      .build[(ChainedID, String), JLong]()

  import CleanName.cleanName

  /*
   *  Return abbreviated frame and the (original) frame's hash
   *    fully.qualified.ClassName.method(possibly, args)
   * becomes
   *    ("f.q.ClNa.method", 12345)
   */
  @VisibleForTesting
  private[diagnostics] def cleanFrameNameForTestingOnly(
      frame0: String,
      hasFrameNum: Boolean, // i.e. prepended with something like [31] by async-profiler
      abbreviate: Boolean): CleanName = cleanName(frame0, hasFrameNum, abbreviate, 0)

  private val dims = (0 until thumbprintDimension).map(d => s"d$d")

  // Return a token representing an entire stack
  private def stackMemoize(rootName: String, rootUuids: Seq[ChainedID], leaf: StackNode): String = {
    unDedupedStackCrumbCount.addAndGet(rootUuids.size)
    val tSec = System.currentTimeMillis() / 1000
    val terminalHash = leaf.terminalHash
    val sid = longAbbrev(terminalHash, "S_")
    if (AsyncProfilerSampler.stacksToCrumbs) {
      if (publishFullStacks) {
        // Filter out venues to which we've already published this stack within the last stackCacheExpireAfterSec
        val publishTo = rootUuids.filter { uuid =>
          tSec - stackPublicationTime.get((uuid, sid), _ => 1L) > stackCacheExpireAfterSec
        }
        if (publishTo.nonEmpty) {
          val frames = leaf.frames
          val thumbPrint = if (thumbprintDimensionBits > 0) {
            val printvec = new Array[Int](thumbprintDimension)
            frames.foreach { f =>
              printvec(f.hashCode & (thumbprintDimension - 1)) += 1
            }
            Some(dims.zip(printvec).toMap)
          } else None

          publishTo.foreach { uuid =>
            // deduplication key assures that we publish for each relevant root uuid and not more infrequently
            // than once ever stackCacheExpirySec
            val dedup = s"$sid:${tSec / stackCacheExpireAfterSec}:${uuid.base}"

            stackPublicationTime.put((uuid, sid), tSec)
            stackCrumbCount.incrementAndGet()
            val elems = Properties.engineId -> ChainedID.root ::
              Properties.pSID -> sid ::
              Properties.profCollapsed -> frames.mkString(";") ::
              Properties.stackThumb.maybe(thumbPrint) ::
              Properties.dedupKey -> dedup :: Elems.Nil
            crumbConsumer.consume(uuid, SamplingProfilerSource, elems)
          }
        }
      }

      if (publishStackPaths) {
        rootUuids.foreach { id =>
          val published = getPathCache(id) // This will have been blown away every stackCacheExpireAfterSec
          if (!published(terminalHash) || !published(leaf.pathHash)) {
            val memoizedPaths =
              leaf.pathMemoizedStackString(published) // might have divided a long stack into two publications
            memoizedPaths.foreach { case (terminalHash, memoizedPath) =>
              val sid = longAbbrev(terminalHash, "S_")
              val dedup = s"$sid:memo:${tSec / stackCacheExpireAfterSec}:${id.base}"
              published += terminalHash
              val elems =
                Properties.pSID -> sid :: Properties.profMS -> memoizedPath :: Properties.dedupKey -> dedup :: Elems.Nil
              crumbConsumer.consume(id, SamplingProfilerSource, elems)
            }
          }
        }
      }

    }
    sid
  }

  private[diagnostics] def assertTreesAreConsistent(root: StackNode): Unit = {
    @tailrec def walk(done: Set[StackNode], todo: List[StackNode]): Unit = {
      todo match {
        case Nil =>
        case s :: rest =>
          assert(!done.contains(s), "Tree is not actually a tree, but a graph")
          s.assertConsistentTimes()
          walk(done + s, rest ++ s.kiderator)
      }
    }

    walk(Set.empty, List(root))
  }

  // Skip over "boring" frames
  private def firstUserFrame(frames: BackedArray[CleanName]): Int = {
    if (aggressiveElision)
      Math.max(0, frames.indexOf(_.matches(StringPredicate.rootMask)))
    else 0
  }

  private val cleanNameBacker = new Array[CleanName](1000)

  // Convert a list of stacks as semi-colon-delimited frame names into tree form, with the root frame
  // at the base, and leaf nodes representing innermost frames. This is essentially what every flamegraph
  // visualizer does.  Since async-profiler doesn't natively have a nice way to differentiate different kinds of
  // stacks, we look for the custom event prefix that we injected ourselves.  Thus we'll return a map of stacks
  // type to the root frame of that flame graph.
  def buildStackTrees(
      dump: String,
      presplit: Iterable[TimedStack] = Iterable.empty): (Map[String, StackNode], SampledTimers) = {
    val unmemoize = new Unmemoizer()
    val stackTypeToRoot = mutable.HashMap.empty[String, StackNode]
    val execRoot = rootStackNode("root")
    stackTypeToRoot += Cpu -> execRoot
    // Combine everything that looks like GC or JIT, so we don't fill up splunk with these stacks.
    val gc = execRoot.getOrCreateChildNode(cleanName(GCName))
    val compiler = execRoot.getOrCreateChildNode(cleanName(CompilerName))
    val javaRoot = execRoot.getOrCreateChildNode(cleanName(AppName))
    stackTypeToRoot += Free -> cleanName(Free, CleanName.FREE).rootNode
    stackTypeToRoot += FreeNative -> cleanName(FreeNative, CleanName.FREE).rootNode

    val rec = SampledTimersExtractor.newRecording

    def getRoot(frame: CleanName): StackNode =
      if (!frame.orig.startsWith(CUSTOM_EVENT_PREFIX)) javaRoot
      else {
        val rootNameEnd = frame.orig.indexOf(']')
        if (rootNameEnd < 0) execRoot
        else {
          val rootName = frame.orig.substring(CUSTOM_EVENT_PREFIX.size, rootNameEnd)
          stackTypeToRoot.getOrElseUpdate(rootName, rootStackNode(rootName))
        }
      }

    def getSID(frame: CleanName): Long = if (frame.orig.startsWith(STACK_ID_PREFIX)) {
      val b = frame.orig.indexOf("]")
      if (b > STACK_ID_PREFIX.size) try {
        frame.orig.substring(STACK_ID_PREFIX.size, b).toLong
      } catch {
        case _: Exception => 0L
      }
      else 0L
    } else 0L

    def incorporateFrames(frames: BackedArray[CleanName], count: Long): Unit = {

      // Update sampled event counts (but only for normal everyday frames!)
      if (frames.length > 0 && !frames(0).orig.startsWith(CUSTOM_EVENT_PREFIX))
        SampledTimersExtractor.analyse(rec, frames, count)

      // climb from root, looking for name match
      val i0 = firstUserFrame(frames)
      if (frames.length > i0) {

        val last = frames.last
        val apsid = getSID(last) // If last (inner-most) frame is a stack id, extract it.
        val maxDepth = Math.min(frames.length - i0, maxFrames)

        // If first (outer-most) frame is a stack id, then look for a saved stack to graft
        // ourselves onto.
        val graftSID = getSID(frames.head)

        // If the first frame specified a stack id, then the root will be determined by the next
        // frame.
        var s = getRoot(frames(if (graftSID == 0) 0 else 1))

        // Insert the saved stack
        if (graftSID > 0) {
          // Saved stacks are inner-most first:
          val graftStack = AwaitStackManagement.awaitStack(graftSID)
          if (Objects.nonNull(graftStack)) {
            var j = graftStack.size() - 1
            while (j >= 0) {
              val f = graftStack.get(j)
              j -= 1
              if (!ignoreFrame(f))
                s = s.getOrCreateChildNode(cleanName(f))
            }
          }
        }

        // Below, we walk each collapsed frame and add it to the tree. The free events have frames that look like
        //   Free -> [sid=...]  bytes free-ed
        // We generally want to strip out the frames that are [sid=...]... except that we need them in the case of
        // "Free" so that we can remove the free-ed bytes from the right spot. Hence the beautiful logic below.
        val isCustomFree = s.cn.isFree

        var i = i0
        while (i < maxDepth) {
          val f = frames(i)
          if (isCustomFree || !ignoreFrame(f.orig))
            s = s.getOrCreateChildNode(f)
          i += 1
        }
        s.add(count, apsid)
      }
    }

    val lines = SplitIterator(dump, '\n')
    lines.foreach { stackLine =>
      val (stack, len, count) = separateStackAndCount(stackLine, forceCopy = false)
      if (count > 0) {
        val frames = splitInto(stack, ';', cleanNameBacker, len, s => cleanName(unmemoize(s)))
        // Aggregate all JIT frames into a single leaf to avoid unnecessary noise in result (we're not trying to
        // analyse the JIT)
        if (looksLikeJITStack(frames)) {
          compiler.add(count)
        } else if (looksLikeGCStack(frames))
          gc.add(count)
        else
          incorporateFrames(frames, count)
      }
    }
    presplit.foreach { case TimedStack(frames, count) =>
      val backed = mapIntoArray(frames, cleanNameBacker, f => cleanName(unmemoize(f)))
      incorporateFrames(backed, count)
    }

    if (DiagnosticSettings.samplingAsserts) stackTypeToRoot.valuesIterator.foreach(assertTreesAreConsistent)
    (stackTypeToRoot.toMap, rec)
  }

  val leafQueue = new mutable.PriorityQueue[StackNode]()((x: StackNode, y: StackNode) => {
    // Smallest total at front of queue (avoid boxing from Long#compareTo)
    var ret = -JLong.compare(x.total, y.total)
    // Resolve ambiguity in favor of pruning longer stack
    if (ret == 0) ret = JInteger.compare(x.depth, y.depth)
    // If still equivalent compare the name - this is really only useful for test stability.
    if (ret == 0) ret = JLong.compare(x.pathHash, y.pathHash)
    ret
  })

  private[diagnostics] def pruneLeaves(root: StackNode, n: Int): Seq[StackNode] = {
    // Enheap all leaves of the tree, with lowest total counts at the top.  Break ties by privileging
    // leaves that are further from root, and finally compare the name to make this more deterministic for testing.
    leafQueue.clear()

    pruneStrategy match {
      case PruningStrategy.`leavesHaveSelfTime` =>
        root.traverse { sn =>
          if (sn.self > 0)
            leafQueue.enqueue(sn)
        }

        // Pluck off leaves one by one.  If the parent newly has self-time, it will need to be enqueued
        while (leafQueue.size > n) {
          val leaf = leafQueue.dequeue()
          val parent = leaf.parent
          if (leaf.self > 0 && parent.self == 0)
            leafQueue.enqueue(parent)
          leaf.pruneFromParent()
        }

      case PruningStrategy.`leavesHaveNoChildren` =>
        root.traverse { sn =>
          if (sn.kids eq null)
            leafQueue.enqueue(sn)
        }
        // Pluck off leaves one by one.  If this turns parent into a leaf, enqueue the newly leafy parent.
        // We redistribute the self time of non-leaf nodes into the branches, because we only report leaves.
        while (leafQueue.size > n) {
          val leaf = leafQueue.dequeue()
          val parent = leaf.pruneFromParent()
          if (parent ne NoNode)
            leafQueue.enqueue(parent)
        }
        root.pushSelfTimesToLeaves()
    }

    leafQueue.toList.sortBy(-_.self)
  }

  private final class LiveTracking(val name: String) {
    val nodes = mutable.HashMap.empty[Long, StackNode]
    val root = rootStackNode(name, CleanName.LIVE)
    nodes += root.nameId -> root

    def resetLiveForTestingOnly(): Unit = {
      nodes.clear()
      root.kids = null
      root.apsids.clear()
      root.total = 0
      root.self = 0
    }

    def backup(): Unit = {
      root.backupLinks()
    }
    def restore(): Unit = {
      root.restoreLinks()
    }

    def elem: (String, StackNode) = name -> root
  }

  private val heapTracking = new LiveTracking(Live)
  private val nativeTracking = new LiveTracking(LiveNative)

  private[diagnostics] def resetLiveTestingOnly(): Unit = {
    heapTracking.resetLiveForTestingOnly()
    nativeTracking.resetLiveForTestingOnly()
  }

  private val mergeWarningCounter = new AtomicInteger(1)
  private val MAX_MERGE_DEPTH = 500

  private def mergeAllocationsIntoLive(liveTracking: LiveTracking, allocRoot: StackNode): Unit = {
    val liveRoot = liveTracking.root
    val liveNodes = liveTracking.nodes
    var merged = 0
    var truncated = 0
    var added = 0
    var alloc = 0L
    val errors = ArrayBuffer.empty[String]
    // Merge allocations into live, depth first.
    var mergeStack: List[(StackNode, StackNode, Int)] = (liveRoot, allocRoot, 0) :: Nil
    while (mergeStack.nonEmpty) {
      val (liveNode, allocNode, depth) :: rest = mergeStack
      mergeStack = rest
      if (depth > MAX_MERGE_DEPTH) {
        truncated += 1
        if (mergeWarningCounter.get > 0)
          SamplingProfiler.warn(
            s"Maximum merge depth exceeded, node=${allocNode.name}",
            new StackOverflowError(),
            mergeWarningCounter)
      } else {
        merged += 1
        liveNode.self += allocNode.self
        liveNode.total += allocNode.total
        if (allocNode.kids eq null) {
          alloc += liveNode.total
          if (allocNode.apsids.isEmpty && errors.size < 10) errors += s"Zero apsid for $allocNode"
          allocNode.apsids.foreach { apsid => liveNodes += apsid -> liveNode }
          liveNode.apsids ++= allocNode.apsids
        } else {
          allocNode.kiderator.foreach { allocChild =>
            val liveChild = liveNode.getOrCreateChildNode(allocChild.cn, () => added += 1)
            mergeStack = (liveChild, allocChild, depth + 1) :: mergeStack
          }
        }
      }
    }
    log.debug(
      s"Merged $merged allocations, truncated $truncated, added=$added, alloc=$alloc, apsids=${liveNodes.size}, errors=$errors")
  }

  private def mergeFreesIntoLive(liveTracking: LiveTracking, freeRoot: StackNode): Unit = {
    val liveNodes = liveTracking.nodes
    var merged = 0
    var missing = 0
    var free = 0L
    freeRoot.traverse { freeNode =>
      freeNode.apsids.foreach { apsid =>
        liveNodes.get(apsid) match {
          case Some(liveNode) =>
            liveNode.add(-freeNode.self)
            free += freeNode.self
            merged += 1
          case None =>
            missing += 1
        }
      }
    }
    log.debug(s"Merged $merged frees, freed=$free, missing=$missing")
  }

  private val memEpsilon = 100
  private def cleanLive(liveTracking: LiveTracking): Unit = {
    val liveNodes = liveTracking.nodes
    val liveRoot = liveTracking.root
    var dropped = 0
    def cleanDfs(liveNode: StackNode): Long = {
      // toDrop elements will either be 0 or a kid id to drop
      val toDrop: Iterator[Long] = liveNode.kiderator.map(cleanDfs)
      toDrop.foreach { id =>
        if (id != 0) {
          val dropped = liveNode.dropKid(id)
          if (Objects.nonNull(dropped)) liveNode.add(-dropped.total)
        }
      }
      if (liveNode.total < memEpsilon) {
        dropped += 1
        liveNode.apsids.foreach(liveNodes -= _)
        liveNode.nameId
      } else 0L
    }
    cleanDfs(liveRoot)
    log.debug(s"Dropped $dropped live nodes, apsids=${liveNodes.size}")
  }

  private val leafPred: StackNode => Boolean = pruneStrategy match {
    case PruningStrategy.`leavesHaveNoChildren` => (sn: StackNode) => sn.kids eq null
    case PruningStrategy.`leavesHaveSelfTime`   => (sn: StackNode) => sn.self > 0
  }

  /**
   * Build a reversed tree so that flamegraphs after pruneLeaves retain more detailed callee paths.
   *
   * - In the forward tree, pruneLeaves cuts away callees first, discarding callee detail.
   * - In the reverse tree, pruneLeaves removes callers first, preserving the deepest callee distributions.
   *
   * Therefore, when pruneLeaves is applied, these two views are complementary rather than redundant.
   * By publishing them, we preserve detail at the top and bottom, while dropping some middle layers.
   */
  private[diagnostics] def reverseTree(oldRoot: StackNode, name: String): StackNode = {
    val newReverseRoot = rootStackNode(name)
    // memory & GC friendly, avoid passing the stack at every reversing recursion
    val nodesToProcess = new mutable.ArrayDeque[(StackNode, StackNode)]()
    val stopAt = oldRoot.kiderator.find(_.cn.name == AppName) match {
      // for Cpu samples, we shouldn't prune callees under App then redistribute their self time to GC and compiler.
      case Some(appRoot) =>
        // Step 1: create reversed App tree
        val revAppRoot = newReverseRoot.getOrCreateChildNode(cleanName(AppName))
        nodesToProcess.append((appRoot, revAppRoot))
        // Step 2: attach helper roots: Compiler, GC directly under the new reversed root.
        // helper roots are siblings of App in the original tree
        val helperRoots = oldRoot.kiderator
        helperRoots.foreach { helperRoot =>
          // append rest helper roots to newReverseRoot: GC, Compiler
          if (helperRoot != appRoot && helperRoot.getTotal != 0) {
            val newHelperNode = newReverseRoot.getOrCreateChildNode(helperRoot.cn)
            newHelperNode.addTotal(helperRoot.getTotal)
            newHelperNode.addSelf(helperRoot.getSelf)
            newReverseRoot.addTotal(helperRoot.getTotal)
          }
        }
        appRoot
      case None =>
        nodesToProcess.append((oldRoot, newReverseRoot))
        oldRoot
    }

    while (nodesToProcess.nonEmpty) {
      val (currentOldNode, currentNewNode) = nodesToProcess.removeHead()
      if (currentOldNode.self > 0L) {
        var oldN = currentOldNode
        var newN = currentNewNode
        // walk up until we hit the given oldRoot
        while (oldN ne stopAt) {
          newN = newN.getOrCreateChildNode(oldN.cn)
          oldN = oldN.parent
        }
        newN.add(currentOldNode.self)
      }
      currentOldNode.kiderator.foreach(kid => nodesToProcess.append((kid, currentNewNode)))
    }

    newReverseRoot
  }

  private[diagnostics] def addReversed(
      typeName: String,
      roots: Map[String, StackNode],
      newName: String): Option[StackNode] = {
    roots
      .get(typeName)
      .map({ oldRoot =>
        reverseTree(oldRoot, newName)
      })
  }

  /**
   * Reduce a list of stack samples to a manageable set by pruning inner frames and then combining the now shorter
   * stacks.
   */
  private[diagnostics] def extractInterestingStacks(
      rootIds: Seq[ChainedID],
      collapsedFormatDump: String,
      preSplitStacks: Iterable[TimedStack],
      nmax: Int,
      generateFolded: Boolean = false
  ): StacksAndTimers = try {
    if (collapsedFormatDump.isEmpty && preSplitStacks.isEmpty) StacksAndTimers.empty
    else {
      val (roots0: Map[String, StackNode], sampledTimers) = buildStackTrees(collapsedFormatDump, preSplitStacks)

      // Update live view
      if (trackLiveMemory) {
        roots0.get(Live).foreach(mergeAllocationsIntoLive(heapTracking, _))
        roots0.get(LiveNative).foreach(mergeAllocationsIntoLive(nativeTracking, _))
        roots0.get(Free).foreach(mergeFreesIntoLive(heapTracking, _))
        roots0.get(FreeNative).foreach(mergeFreesIntoLive(nativeTracking, _))
        cleanLive(heapTracking)
        cleanLive(nativeTracking)
        heapTracking.backup()
        nativeTracking.backup()
      }

      // Add live view to roots, and remove Free stub
      val origNames =
        Seq(Cpu, Alloc, AllocNative, Adapted_DAL, Lock, Wait, Unfired_DAL, FullWait_NS_None)
      val revTrees: Seq[(String, StackNode)] = for {
        origName <- origNames
        reversed <- addReversed(origName, roots0, s"$Rev$origName")
      } yield s"$Rev$origName" -> reversed

      val roots =
        roots0.applyIf(trackLiveMemory)(_ + heapTracking.elem + nativeTracking.elem - Free - FreeNative) ++ revTrees

      if (DiagnosticSettings.samplingAsserts) roots.valuesIterator.foreach(assertTreesAreConsistent)

      // Post process trees, prune leaves etc
      val leaves: Map[String, Seq[StackNode]] = roots.mapValuesNow(pruneLeaves(_, nmax))

      // Make sure everything is still consistent
      if (DiagnosticSettings.samplingAsserts) roots.valuesIterator.foreach(assertTreesAreConsistent)

      val stackData = leaves.flatMap { case (rootName, stacks) =>
        stacks.collect {
          case s if s.total > 0 => // drop empty nodes
            if (DiagnosticSettings.samplingAsserts) {
              s.assertConsistentTimes()
              assert(leafPred(s), "stacks should have been a leaf")
            }
            val hashId = stackMemoize(rootName, rootIds, s)
            StackData(rootName, s.total, s.self, hashId, if (generateFolded) folded(s.frames, s.self) else "")
        }
      }.toSeq

      heapTracking.restore()
      nativeTracking.restore()

      val smplTotals = roots.mapValuesNow(_.total)

      StacksAndTimers(stackData, sampledTimers, smplTotals)
    }
  } catch {
    case NonFatal(e) =>
      val wrapped = new RuntimeException(
        s"""|An error occurred in StackAnalysis while parsing this collapsed dump:
            |${collapsedFormatDump.abbrev(1000)}""".stripMargin,
        e
      ).fillInStackTrace()
      wrapped.printStackTrace()
      throw wrapped
  }

  private def folded(frames: Iterable[String], weight: Long): String = frames.mkString(";") + " " + weight

  private val rng = new Random(0)
  def extraTreeAssertions[T](m: Map[T, StackNode]): Unit = {
    for ((_, n) <- m) pruneLeaves(n, rng.nextInt(100))
    m.valuesIterator.foreach(assertTreesAreConsistent)
  }

}
