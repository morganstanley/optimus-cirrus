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

import com.github.benmanes.caffeine.cache.Caffeine
import com.google.common.annotations.VisibleForTesting
import com.google.common.hash.Hashing
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.SamplingProfilerSource
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.crumbs.Properties.Key
import optimus.graph.AwaitStackManagement
import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.sampling.AsyncProfilerSampler
import optimus.graph.diagnostics.sampling.FlameTreatment
import optimus.graph.diagnostics.sampling.NullSampleCrumbConsumer
import optimus.graph.diagnostics.sampling.SampleCrumbConsumer
import optimus.graph.diagnostics.sampling.TimedStack
import optimus.platform.util.Log

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import optimus.scalacompat.collection._
import optimus.utils.PropertyUtils
import optimus.utils.MiscUtils.Endoish._
import optimus.utils.OptimusStringUtils

import java.util.Objects
import scala.annotation.tailrec
import scala.util.Random
import scala.util.control.NonFatal
import java.lang.{Integer => JInteger, Long => JLong}
import java.nio.ByteBuffer
import java.util.Base64
import java.util.concurrent.TimeUnit
import scala.collection.immutable

object StackAnalysis {

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
  val frameNameCacheSize: Int = PropertyUtils.get("optimus.async.profiler.frame.cache.size", 1000000)
  val doAbbreviateFrames: Boolean = PropertyUtils.get("optimus.async.profiler.abbreviate", true)
  val stackPathPublishStackSize = PropertyUtils.get("optimus.async.profiler.substack.cache.size", 1000 * 1000)
  val stackCacheExpireAfterSec = PropertyUtils.get("optimus.async.profiler.stack.cache.expire.sec", 3600)
  val maxStackPathCrumbLength = PropertyUtils.get("optimus.async.profiler.substack.max.strlen", 5000)

  private val frameStringInterner = Caffeine.newBuilder().maximumSize(frameNameCacheSize).build[String, String]()
  private def intern(s: String): String = frameStringInterner.get(s, _ => s)

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
    val sb = new StringBuilder()
    while (i < endStack) {
      val c = stackLine.charAt(i)
      if (c != ' ') sb += c
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

  // Java20 likes to give lambdas these unique suffixes e.g. "$La$722.0x000000080173d0.apply"
  val lambdaRe = "[\\$\\d\\.]*0x\\w+\\.".r
  val lambdaPrefixSearch = ".0x"
  val lambdaSubstitute = "\\$0x1a3bda\\." // escapes, since will be used in regex method
  def undisambiguateLambda(frame0: String): String =
    if (frame0.contains(lambdaPrefixSearch))
      lambdaRe.replaceAllIn(frame0, lambdaSubstitute)
    else frame0

  private val CUSTOM_EVENT_PREFIX = "[custom="
  private val STACK_ID_PREFIX = "[sid="
  def customEventFrame(event: String) = s"[custom=$event]"

  private val ignoredPrefixes =
    Seq(STACK_ID_PREFIX, CUSTOM_EVENT_PREFIX)
  private def ignoreFrame(frame: String): Boolean = {
    ignoredPrefixes.exists(frame.startsWith(_))
  }

  val GCPredicate =
    (StringPredicate.contains("GCTaskThr") or StringPredicate.contains("__clone") or StringPredicate.contains(
      "thread_native_entry")).assignMask()
  val JITPredicate = (StringPredicate.contains("Compile:") or StringPredicate.contains("CompileBroker::")).assignMask()

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

  final class CleanName private (
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

    def rootNode = new StackNode(this, 0, hash)
  }

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

    def cleanName(frame: String, hasFrameNum: Boolean, abbreviate: Boolean, flags: Int): CleanName = {
      def clean(frame0: String, hasFrameNum: Boolean, abbreviate: Boolean): CleanName = {
        val predMask: Long = StringPredicate.getMaskForAllPredicates(frame0)

        var i = 0
        val frame = undisambiguateLambda(frame0)
        val hash = hashString(frame)

        val length = frame.size
        if (hasFrameNum) {
          // Step forward past the frame number
          while (i < length && frame(i) != ']') { i += 1 }
          i += 1
          while (i < length && frame(i) == ' ') { i += 1 }
          if (i == length)
            return abbreviateError
        }

        // async methods end with "_[a]" (see optimus.graph.AwaitStackManagment.StringPointers.getName where we add this); in any case, remove args
        val methodEnd = {
          var j = frame.indexOf("_[a]")
          if (j < 0) j = frame.indexOf('(')
          if (j < 0) length else j
        }

        if (!abbreviate)
          CleanName(frame.substring(i, methodEnd).replaceAllLiterally("/", "."), frame, flags, predMask, hash)
        else {
          // method starts at the last separator
          var methodStart = methodEnd - 1
          while (methodStart >= i && frame(methodStart) != '.' && frame(methodStart) != '/') { methodStart -= 1 }
          if (methodStart < i)
            return CleanName(frame.substring(i, methodEnd), frame, flags, predMask, hash)

          val method = {
            if (methodStart == methodEnd - 1) ""
            else frame.substring(methodStart, methodEnd)
          }

          val pack = if (methodStart > 0) {
            // construct op.gr.OGSchCon, e.g.
            val sb = new StringBuilder()
            var charsToKeep = camelHumpWidth
            while (i < methodStart) {
              val c = frame(i)
              i += 1
              if (c == '.' || c == '/') {
                charsToKeep = camelHumpWidth
                sb += '.'
              } else if (c.isUpper || c == '$' || c == '@' || (c >= '0' && c <= '9')) {
                charsToKeep = camelHumpWidth
                sb += c
              } else if (charsToKeep > 0) {
                charsToKeep -= 1
                sb += c
              }
            }
            sb.toString
          } else ""
          val shortened = pack + method
          CleanName(shortened, frame, flags, predMask, hash)
        }
      }

      frameNameCache.get(frame, frame => clean(frame, hasFrameNum, abbreviate))
    }

  }

  def rootStackNode(name: String, flags: Int = 0) = {
    val cn = CleanName.cleanName(name)
    new StackNode(cn, flags, cn.hash)
  }

  private final case class BackupStackNodeState(kids: AnyRef, var total: Long, var self: Long)

  // Mixed into leaf node to designate a complete published stack (as opposed to the cumulative hash
  // of a possibly partial stack).  The value here is irrelevant but should be shared across jvms.
  private val stackHashTerminator: Long = hashString("this is the way the stack ends")

  // Each node is identified by an id unique to the frame name, plus a link to its
  // parent on the tree.  This is a conventional approach for representing flame graphs.
  final class StackNode(val cn: CleanName, val depth: Int, val pathHash: Long) {
    def nameId: Long = cn.id
    def name: String = cn.name
    def getTotal: Long = total
    def getSelf: Long = self

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

    private[StackAnalysis] var parent: StackNode = null
    override def toString: String = s"[$nameId, $name, $total, $self]"

    def kiderator: Iterator[StackNode] =
      kids match {
        case null            => Iterator.empty
        case only: StackNode => Iterator(only)
        case map: LM[_]      => map.cast.valuesIterator
      }

    def sortedKiderator: Iterator[StackNode] =
      kids match {
        case null            => Iterator.empty
        case only: StackNode => Iterator(only)
        case map: LM[_]      => map.cast.values.toSeq.sortBy(_.name).iterator
      }

    def getOrUpdateChildNode(childId: Long, newChildNode: () => StackNode): StackNode = {
      kids match {
        case null =>
          val ret = newChildNode()
          kids = ret
          ret
        case only: StackNode =>
          if (only.nameId == childId) only
          else {
            val ret = newChildNode()
            kids = LM(only.nameId -> only, ret.nameId -> ret)
            ret
          }
        case map: LM[_] =>
          map.cast.getOrElseUpdate(childId, newChildNode())
      }
    }

    def dropKid(childId: Long): Unit = kids match {
      case null =>
      case only: StackNode =>
        if (only.nameId == childId) kids = null
      case map: LM[_] =>
        map -= childId
        if (map.isEmpty) kids = null
    }

    private[StackAnalysis] def getOrCreateChildNode(childName: CleanName): StackNode = {
      def newChildNode(): StackNode = {
        val kidHash = combineHashes(this.pathHash, childName.hash)
        val kid = new StackNode(childName, depth + 1, kidHash)
        kid.parent = this
        kid
      }
      getOrUpdateChildNode(childName.id, newChildNode)
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
          val kidsArr = lm.cast.valuesIterator.toArray
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
      assert(self >= 0L, s"negative self time ${times}")
      assert(total >= 0L, s"negative total time ${times}")
      assert(total == self + kiderator.map(_.total).sum, s"total time inconsistent ${times}")
    }

    def traverse(f: StackNode => Unit): Unit = {
      @tailrec def traverseImpl(stack: mutable.ArrayStack[StackNode], f: StackNode => Unit): Unit = {
        if (stack.isEmpty) return
        val n = stack.pop()
        f(n)
        n.kids match {
          case only: StackNode =>
            stack += only
          case map: LM[_] =>
            stack ++= map.cast.valuesIterator
          case null =>
        }
        traverseImpl(stack, f)
      }
      traverseImpl(mutable.ArrayStack(this), f)
    }

    def backupLinks(): Unit = traverse { sn => sn.backup() }

    def restoreLinks(): Unit = traverse { sn => sn.restore() }

    def toFlameGraphRow: Seq[NestedSetModelRow] = {
      val seqb = Seq.newBuilder[NestedSetModelRow]
      traverse { sn =>
        seqb += NestedSetModelRow(sn.name, sn.depth, sn.total, sn.self)
      }
      seqb.result()
    }

    def treeString: String = {
      val sb = ArrayBuffer.empty[String]
      def traverse(n: StackNode, depth: Int): Unit = {
        sb += " " * depth + s"${n.name} ${n.total} ${n.self}"
        n.kids match {
          case null            =>
          case only: StackNode => traverse(only, depth + 2)
          case map: LM[_] =>
            map.cast.toSeq.sortBy(_._2.name).foreach(k => traverse(k._2, depth + 2))
        }
      }
      traverse(this, 0)
      sb.mkString("\n")
    }

    // for debugging most
    def root: StackNode = {
      var node = this
      while (node.parent ne null)
        node = node.parent
      node
    }

    def frames: Seq[String] = {
      var fromRoot: List[StackNode] = Nil
      var node = this
      while (node.parent ne null) { // i.e. don't print root itself
        if (node.name.nonEmpty)
          fromRoot = node :: fromRoot
        node = node.parent
      }
      fromRoot.map(_.name)
    }

    def pathMemoizedStackString(abbrevCache: PublishedPathHashes): Seq[(Long, String)] = {
      var sb = new StringBuilder
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
        sb ++= longAbbrev(hash, "==")
      } else {
        abbrevCache += hash
        sb ++= s"${longAbbrev(hash, "=")}=${fromRoot.head.cn.name}"
      }

      val it = fromRoot.tail.iterator
      while (it.hasNext) {
        node = it.next()
        abbrevCache += node.pathHash
        val w = s";${longAbbrev(node.pathHash, "=")}=${node.cn.name}"
        sb ++= w
        if (sb.size > maxStackPathCrumbLength && it.hasNext) {
          // This shouldn't happen often, but if it does, we'll break up the stacks.
          paths = paths :+ (node.terminalHash, sb.toString())
          sb = new StringBuilder
          // Next segment starts with the frame we just published
          sb ++= longAbbrev(node.pathHash, "==")
        }
      }
      paths = paths :+ ((node.terminalHash, sb.toString))
      paths
    }

    def stackString = frames.mkString(";")
  }

  val encoder64 = Base64.getUrlEncoder
  def longAbbrev(v: Long, prefix: String): String = {
    val b64 = encoder64.encodeToString(ByteBuffer.allocate(8).putLong(v).array)
    // drop terminal = signs
    val i = b64.indexOf('=')
    prefix + (if (i > 0) b64.substring(0, i) else b64)
  }

}

final case class NestedSetModelRow(label: String, level: Int, value: Long, self: Long, stackFrames: Seq[String] = Nil) {
  override def toString: String = s"$label, $level, $value $self"
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

  // Skip over a series of OGScheduler threads ending in a .run
  private def firstUserFrame(frames: BackedArray[CleanName]): Int = {
    val length = frames.length
    var i = 0
    while (i < length && frames(i).orig.startsWith("optimus/graph")) { i += 1 }
    if (i == 0 || !frames(i - 1).orig.contains(".run")) 0
    else i
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
    stackTypeToRoot += "cpu" -> execRoot
    // Combine everything that looks like GC or JIT, so we don't fill up splunk with these stacks.
    val gc = execRoot.getOrCreateChildNode(cleanName("GC"))
    val compiler = execRoot.getOrCreateChildNode(cleanName("Compiler"))
    val javaRoot = execRoot.getOrCreateChildNode(cleanName("App"))
    stackTypeToRoot += "Free" -> cleanName("Free", CleanName.FREE).rootNode
    stackTypeToRoot += "FreeNative" -> cleanName("FreeNative", CleanName.FREE).rootNode

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
    if (ret == 0) ret = x.name.compareTo(y.name)
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
          if (parent ne null)
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

  private val heapTracking = new LiveTracking("Live")
  private val nativeTracking = new LiveTracking("LiveNative")

  private[diagnostics] def resetLiveTestingOnly(): Unit = {
    heapTracking.resetLiveForTestingOnly()
    nativeTracking.resetLiveForTestingOnly()
  }

  private def mergeAllocationsIntoLive(liveTracking: LiveTracking, allocRoot: StackNode): Unit = {
    val liveRoot = liveTracking.root
    val liveNodes = liveTracking.nodes
    var added = 0
    var merged = 0
    var alloc = 0L
    val errors = ArrayBuffer.empty[String]
    def mergeDfs(liveNode: StackNode, allocNode: StackNode): Unit = {
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
          val liveChild = liveNode.getOrUpdateChildNode(
            allocChild.nameId,
            () => {
              val kidHash = combineHashes(liveNode.pathHash, allocChild.cn.hash)
              val kid = new StackNode(allocChild.cn, liveNode.depth + 1, kidHash)
              added += 1
              kid.parent = liveNode
              kid
            }
          )
          mergeDfs(liveChild, allocChild)
        }
      }
    }
    mergeDfs(liveRoot, allocRoot)
    log.debug(s"Merged $merged allocations, added $added, alloc=$alloc, apsids=${liveNodes.size}, errors=$errors")
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
      toDrop.foreach { id => if (id != 0) liveNode.dropKid(id) }
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
   * Reduce a list of stack samples to a manageable set by pruning inner frames and then combining the now shorter
   * stacks.
   */
  private[diagnostics] def extractInterestingStacks(
      rootIds: Seq[ChainedID],
      collapsedFormatDump: String,
      preSplitStacks: Iterable[TimedStack],
      nmax: Int,
      generateFolded: Boolean
  ): StacksAndTimers = try {
    if (collapsedFormatDump.isEmpty && preSplitStacks.isEmpty) StacksAndTimers.empty
    else {
      val (roots0: Map[String, StackNode], sampledTimers) = buildStackTrees(collapsedFormatDump, preSplitStacks)

      // Update live view
      if (trackLiveMemory) {
        roots0.get("Alloc").foreach(mergeAllocationsIntoLive(heapTracking, _))
        roots0.get("AllocNative").foreach(mergeAllocationsIntoLive(nativeTracking, _))
        roots0.get("Free").foreach(mergeFreesIntoLive(heapTracking, _))
        roots0.get("FreeNative").foreach(mergeFreesIntoLive(nativeTracking, _))
        cleanLive(heapTracking)
        cleanLive(nativeTracking)
        heapTracking.backup()
        nativeTracking.backup()
      }

      // Add live view to roots, and remove Free stub
      val roots = roots0.applyIf(trackLiveMemory)(_ + heapTracking.elem + nativeTracking.elem - "Free" - "FreeNative")
      if (DiagnosticSettings.samplingAsserts) roots.valuesIterator.foreach(assertTreesAreConsistent)

      // Post process trees, prune leaves etc
      val leaves: Map[String, Seq[StackNode]] = roots.mapValuesNow(pruneLeaves(_, nmax))

      // Make sure everything is still consistent
      if (DiagnosticSettings.samplingAsserts) roots.valuesIterator.foreach(assertTreesAreConsistent)

      val stackData = leaves.flatMap { case (rootName, stacks) =>
        stacks.map { s =>
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
