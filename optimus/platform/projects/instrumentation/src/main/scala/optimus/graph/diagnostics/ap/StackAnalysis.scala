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
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Crumb.SamplingProfilerSource
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.AwaitStackManagement
import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.sampling.AsyncProfilerSampler
import optimus.graph.diagnostics.sampling.TimedStack
import optimus.platform.util.Version
import optimus.platform.util.Log

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import optimus.scalacompat.collection._
import optimus.utils.PropertyUtils
import optimus.utils.MiscUtils.Endoish._

import java.util.Objects
import scala.annotation.tailrec
import scala.util.Random
import scala.util.control.NonFatal

object StackAnalysis {

  final case class SharedStack(weight: Double, hashId: String, frames: Seq[String])

  private val stackCrumbCount = new AtomicLong(0)
  private[diagnostics] def numStacksPublished: Long = stackCrumbCount.get()
  // Fully qualified class names are abbreviated to a maximum of N successive lower-case letters:
  val camelHumpWidth: Int = PropertyUtils.get("optimus.async.profiler.hump.width", 2)
  val frameNameCacheSize: Int = PropertyUtils.get("optimus.async.profiler.frame.cache.size", 1000000)
  val doAbbreviateFrames: Boolean = PropertyUtils.get("optimus.async.profiler.abbreviate", true)

  // Split off count bit
  // "foo;bar;wiz 37" => ("foo;bar;wiz", 37)
  @VisibleForTesting
  private[diagnostics] def splitStackLine(stackLine: String): (String, Long) = {
    var i = stackLine.length - 1
    // step backwards past trailing spaces
    while (i > 0 && stackLine(i) == ' ') { i -= 1 }
    val endNum = i + 1 // presumably end of counter bit
    // step backwards past counter
    while (i > 0 && stackLine(i) >= '0' && stackLine(i) <= '9') { i -= 1 }
    val beginNum = i + 1
    if (beginNum >= endNum || beginNum <= 3) return ("", 0)
    while (i > 0 && stackLine(i) == ' ') { i -= 1 }
    val endStack = i + 1
    if (endStack <= 3) return ("", 0)
    val n = stackLine.substring(beginNum, endNum).toLong
    // remove spaces
    i = 0
    val sb = new StringBuilder()
    while (i < endStack) {
      val c = stackLine.charAt(i)
      if (c != ' ') sb += c
      i += 1
    }
    val stack = sb.toString()
    (stack, n)
  }

  private[diagnostics] def stackLineToTimedStack(stackLine: String): TimedStack = {
    val (stack, count) = splitStackLine(stackLine)
    TimedStack(stack.split(';'), count)
  }

  final case class StackData private (
      tpe: String,
      total: Long,
      self: Long,
      hashId: String,
      last: String,
      folded: String)

  final case class StacksAndTimers(stacks: Seq[StackData], timers: SampledTimers, dtStopped: Long = 0L)
  object StacksAndTimers { val empty = StacksAndTimers(Seq.empty, SampledTimersExtractor.newRecording) }

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
    Seq(
      STACK_ID_PREFIX,
      CUSTOM_EVENT_PREFIX,
      "Profiler::",
      "ObjectSampler::",
      "MemAllocator::",
      "optimus.graph.AsyncProfilerIntegration",
      "Jvmti")
  private def ignoreFrame(frame: String): Boolean = {
    ignoredPrefixes.exists(frame.startsWith(_))
  }

  private def looksLikeGCStack(stack: Array[String]) = stack.exists { frame =>
    frame.contains("GCTaskThr") || frame.contains("__clone") || frame.contains("thread_native_entry")
  }

  private def looksLikeJITStack(stack: Array[String]) = stack.exists { frame =>
    frame.contains("Compile:") || frame.contains("CompileBroker::")
  }
  private class Unmemoizer() extends Function1[String, String] {
    val memo: mutable.HashMap[Int, String] = mutable.HashMap.empty
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

  private[diagnostics] final class CleanName private (
      val name: String,
      val id: Long,
      val flags: Int,
      val predMask: Long) {
    import CleanName._

    def isFree: Boolean = (flags & FREE) == FREE

    override def toString: String = s"($name, $id)"

    def rootNode = new StackNode(this, 0)
  }

  private object CleanName {
    val FREE = 1
    val LIVE = 2
    private val frameNameCache =
      Caffeine.newBuilder().maximumSize(frameNameCacheSize).build[String, CleanName]
    private val localFrameIds = new AtomicLong(0)

    private val abbreviateError = CleanName("ERROR", flags = 0, predMask = 0L)

    private def apply(name: String, flags: Int, predMask: Long): CleanName = {
      new CleanName(name, localFrameIds.incrementAndGet(), flags, predMask)
    }

    def cleanName(frame: String): CleanName = cleanName(frame, false, doAbbreviateFrames, 0)
    def cleanName(frame: String, flags: Int): CleanName = cleanName(frame, false, doAbbreviateFrames, flags)
    def cleanName(frame: String, hasFrameNum: Boolean): CleanName =
      cleanName(frame, hasFrameNum, doAbbreviateFrames, 0)

    def cleanName(frame: String, hasFrameNum: Boolean, abbreviate: Boolean, flags: Int): CleanName = {
      def clean(frame0: String, hasFrameNum: Boolean, abbreviate: Boolean): CleanName = {
        val predMask: Long = StringPredicate.getMaskForAllPredicates(frame0)

        var i = 0
        val frame = undisambiguateLambda(frame0)

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
          CleanName(frame.substring(i, methodEnd).replaceAllLiterally("/", "."), flags, predMask)
        else {
          // method starts at the last separator
          var methodStart = methodEnd - 1
          while (methodStart >= i && frame(methodStart) != '.' && frame(methodStart) != '/') { methodStart -= 1 }
          if (methodStart < i)
            return CleanName(frame.substring(i, methodEnd), flags, predMask)

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
          CleanName(shortened, flags, predMask)
        }
      }

      frameNameCache.get(frame, frame => clean(frame, hasFrameNum, abbreviate))
    }

  }

  private final case class StackNodeState(kids: mutable.LongMap[StackNode], total: Long, self: Long)

  // Each node is identified by an id unique to the frame name, plus a link to its
  // parent on the tree.  This is a more conventional approach for representing flame graphs.
  private[diagnostics] final class StackNode(val cn: CleanName, val depth: Int) {
    def id: Long = cn.id
    def name: String = cn.name

    private[StackAnalysis] var kids = mutable.LongMap.empty[StackNode]
    private[StackAnalysis] var total = 0L
    private[StackAnalysis] var self = 0L

    private var _backup: StackNodeState = null
    private def backup(): Unit = _backup = StackNodeState(kids.clone(), total, self)
    private def restore(): Unit = {
      kids = _backup.kids
      total = _backup.total
      self = _backup.self
      _backup = null
    }

    def this(cn: CleanName) = this(cn, 0)

    // All encountered apsids for a particular leaf node when aggregating allocations and frees. If/when the allocation
    // for that leaf node drops to zero, we will remove the node and remove all these apsids from the global dictionary
    // as well.
    private[StackAnalysis] val apsids = mutable.TreeSet.empty[Long]

    private[StackAnalysis] var parent: StackNode = null
    override def toString: String = s"[$id, $name, $total, $self]"

    private[StackAnalysis] def getOrCreateChildNode(cn: CleanName): StackNode = kids.getOrElseUpdate(
      cn.id, {
        val kid = new StackNode(cn, depth + 1)
        kid.parent = this
        kid
      })

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

    def pruneFromParent(): StackNode = {
      parent.kids -= id
      // parent total weight *remains the same* because we simply moved our total from parent.kids to parent.total
      parent.self += total
      if (parent.kids.isEmpty) parent
      else null
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

      if (kids.isEmpty) return // am leaf, do nothing
      else if (self != 0L) {
        val kidsArr = kids.valuesIterator.toArray
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
      // This recursion is bounded by maxFrames so it shouldn't ever overflow
      for ((_, k) <- kids) k.pushSelfTimesToLeaves()
    }

    def assertConsistentTimes(): Unit = {
      def times = s"\n$cn TIMES: self: ${self}, total ${total}, children: [${kids.values.map(_.total).mkString(", ")}]"
      assert(self >= 0L, s"negative self time ${times}")
      assert(total >= 0L, s"negative total time ${times}")
      assert(total == self + kids.valuesIterator.map(_.total).sum, s"total time inconsistent ${times}")
    }

    def traverse(f: StackNode => Unit): Unit = {
      @tailrec def traverseImpl(stack: mutable.ArrayStack[StackNode], f: StackNode => Unit): Unit = {
        if (stack.isEmpty) return
        val n = stack.pop()
        f(n)
        stack ++= n.kids.valuesIterator
        traverseImpl(stack, f)
      }

      traverseImpl(mutable.ArrayStack(this), f)
    }

    def leaves: Seq[StackNode] = {
      val ret = ArrayBuffer.empty[StackNode]
      traverse { sn => if (sn.kids.isEmpty) ret += sn }
      ret
    }

    def backupLinks(): Unit = traverse { sn => sn.backup() }

    def restoreLinks(): Unit = traverse { sn => sn.restore() }

    def treeString: String = {
      val sb = ArrayBuffer.empty[String]
      def traverse(n: StackNode, depth: Int): Unit = {
        sb += " " * depth + s"${n.name} ${n.total} ${n.self}"
        n.kids.toSeq.sortBy(_._2.name).foreach(k => traverse(k._2, depth + 2))
      }
      traverse(this, 0)
      sb.mkString("\n")
    }

    def frames: Seq[String] = {
      var fromRoot: List[StackNode] = Nil
      var s = this
      while (s.parent ne null) { // i.e. don't print root itself
        fromRoot = s :: fromRoot
        s = s.parent
      }
      fromRoot.map(_.name)
    }

    def stackString = frames.mkString(";")

  }

}

class StackAnalysis(crumbConsumer: Option[Crumb => Unit], properties: Map[String, String] = Map.empty) extends Log {
  import StackAnalysis._

  val propertyUtils = new PropertyUtils(properties)

  val stackCacheSize = propertyUtils.get("optimus.async.profiler.stack.cache.size", 100000)
  val stackCacheExpireAfterSec =
    propertyUtils.get("optimus.async.profiler.stack.cache.expire.sec", 3600)
  val thumbprintDimensionBits = propertyUtils.get("optimus.async.profiler.thumbprint", 3)
  val thumbprintDimension = 1 << thumbprintDimensionBits
  val maxFrames = propertyUtils.get("optimus.async.profiler.stack.maxframes", 250)

  val trackLiveMemory = propertyUtils.get("optimus.async.profiler.track.live.memory", true)

  private val stackToHashCache =
    Caffeine
      .newBuilder()
      .maximumSize(stackCacheSize)
      .build[Seq[String], String]()

  private val apSidToStack: Cache[java.lang.Long, SharedStack] =
    Caffeine
      .newBuilder()
      .maximumSize(stackCacheSize)
      .build[java.lang.Long, SharedStack]()

  private val hashing = Hashing.murmur3_128()
  private def stackToHash(stack: Seq[String]): String = stackToHashCache.get(
    stack,
    stack => {
      val hasher = hashing.newHasher()
      stack.foreach(s => {
        hasher.putString(s, StandardCharsets.UTF_8)
        hasher.putChar(';') // to disambiguate frame1frame2 from frame1;frame2
      })
      "S_" + hasher.hash().toString
    }
  )

  import java.lang.{Long => JLong}
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
  private def stackMemoize(rootUuids: Seq[ChainedID], frames: Seq[String]): String = {
    if (frames.size == 0)
      return "EMPTY"

    val tSec = System.currentTimeMillis() / 1000
    val sid = stackToHash(frames)

    val publishTo = rootUuids.filter { uuid =>
      tSec - stackPublicationTime.get((uuid, sid), _ => 1L) > stackCacheExpireAfterSec
    }

    if (AsyncProfilerSampler.stacksToSplunk && publishTo.nonEmpty) {
      val printvec = new Array[Int](thumbprintDimension)
      frames.foreach { f =>
        printvec(f.hashCode & (thumbprintDimension - 1)) += 1
      }
      val thumbPrint = dims.zip(printvec).toMap

      publishTo.foreach { uuid =>
        // deduplication key assures that we publish for each relevant root uuid and not more infrequently
        // than once ever stackCacheExpirySec
        val dedup = s"$sid:${tSec / stackCacheExpireAfterSec}:${uuid.base}"

        stackPublicationTime.put((uuid, sid), tSec)
        stackCrumbCount.incrementAndGet()
        val elems = Properties.engineId -> ChainedID.root ::
          Properties.pSID -> sid ::
          // Re-assemble collapsed stack for ease of splunking
          Properties.profCollapsed -> frames.filter(_.nonEmpty).mkString(";") ::
          Properties.stackThumb -> thumbPrint ::
          Properties.dedupKey -> dedup :: Version.properties
        crumbConsumer match {
          case Some(f) => f(PropertiesCrumb(uuid, SamplingProfilerSource, elems))
          case None    => Breadcrumbs(uuid, PropertiesCrumb(_, SamplingProfilerSource, elems))
        }
      }
    }
    sid
  }

  // Get snapshot of top n stacks, fixing up A-P's rather verbose formatting
  private val Samples: Regex = "---.*, (\\d+) sample.*".r
  def splitTraces(dump: String): Seq[(Double, String)] = {
    dump.split("\n\n").map(_.split("\n").toSeq).toSeq.collect {
      case Samples(n) +: frames if frames.nonEmpty =>
        n.toDouble -> stackMemoize(Seq(ChainedID.root), frames.map(cleanName(_, hasFrameNum = true).name))
    }
  }

  private[diagnostics] def assertTreesAreConsistent(root: StackNode): Unit = {
    @tailrec def walk(done: Set[StackNode], todo: List[StackNode]): Unit = {
      todo match {
        case Nil =>
        case s :: rest =>
          assert(!done.contains(s), "Tree is not actually a tree, but a graph")
          s.assertConsistentTimes()
          walk(done + s, rest ++ s.kids.valuesIterator)
      }
    }

    walk(Set.empty, List(root))
  }

  // Skip over a series of OGScheduler threads ending in a .run
  private def firstUserFrame(frames: Array[String]): Int = {
    val length = frames.size
    var i = 0
    while (i < length && frames(i).startsWith("optimus/graph")) { i += 1 }
    if (i == 0 || !frames(i - 1).contains(".run")) 0
    else i
  }

  // Convert a list of stacks as semi-colon-delimited frame names into tree form, with the root frame
  // at the base, and leaf nodes representing innermost frames. This is essentially what every flamegraph
  // visualizer does.  Since async-profiler doesn't natively have a nice way to differentiate different kinds of
  // stacks, we look for the custom event prefix that we injected ourselves.  Thus we'll return a map of stacks
  // type to the root frame of that flame graph.
  private[diagnostics] def buildStackTrees(
      dump: String,
      presplit: Iterable[TimedStack] = Iterable.empty): (Map[String, StackNode], SampledTimers) = {
    val unmemoize = new Unmemoizer()
    val stackTypeToRoot = mutable.HashMap.empty[String, StackNode]
    val execRoot = new StackNode(cleanName("root"))
    stackTypeToRoot += "cpu" -> execRoot
    // Combine everything that looks like GC or JIT, so we don't fill up splunk with these stacks.
    val gc = execRoot.getOrCreateChildNode(cleanName("GC"))
    val compiler = execRoot.getOrCreateChildNode(cleanName("Compiler"))
    stackTypeToRoot += "Free" -> cleanName("Free", CleanName.FREE).rootNode
    stackTypeToRoot += "FreeNative" -> cleanName("FreeNative", CleanName.FREE).rootNode
    val javaRoot = execRoot.getOrCreateChildNode(cleanName("App"))

    val rec = SampledTimersExtractor.newRecording

    def getRoot(frame: String): StackNode =
      if (!frame.startsWith(CUSTOM_EVENT_PREFIX)) javaRoot
      else {
        val rootNameEnd = frame.indexOf(']')
        if (rootNameEnd < 0) execRoot
        else {
          val rootName = frame.substring(CUSTOM_EVENT_PREFIX.size, rootNameEnd)
          stackTypeToRoot.getOrElseUpdate(rootName, new StackNode(cleanName(rootName)))
        }
      }

    def getSID(frame: String): Long = if (frame.startsWith(STACK_ID_PREFIX)) {
      val b = frame.indexOf("]")
      if (b > STACK_ID_PREFIX.size) try {
        frame.substring(STACK_ID_PREFIX.size, b).toLong
      } catch {
        case _: Exception => 0L
      }
      else 0L
    } else 0L

    def incorporateFrames(frames: Array[String], count: Long): Unit = {

      val cleanNames = frames.map(cleanName(_))

      // Update sampled event counts (but only for normal everyday frames!)
      if (frames.length > 0 && !frames(0).startsWith(CUSTOM_EVENT_PREFIX))
        SampledTimersExtractor.analyse(rec, cleanNames, count)

      // climb from root, looking for name match
      val i0 = firstUserFrame(frames)
      if (frames.size > i0) {

        val last = frames.last
        val apsid = getSID(last) // If last (inner-most) frame is a stack id, extract it.
        val maxDepth = Math.min(frames.size - i0, maxFrames)

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
          if (isCustomFree || !ignoreFrame(f))
            s = s.getOrCreateChildNode(cleanNames(i))
          i += 1
        }
        s.add(count, apsid)
      }
    }

    dump.split('\n').foreach { stackLine =>
      {
        val (stack, count) = splitStackLine(stackLine)
        if (count > 0) {
          val frames = stack.split(';').map(unmemoize)
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
    }
    presplit.foreach { case TimedStack(frames, count) =>
      incorporateFrames(frames.map(unmemoize), count)
    }

    if (DiagnosticSettings.samplingAsserts) stackTypeToRoot.valuesIterator.foreach(assertTreesAreConsistent)
    (stackTypeToRoot.toMap, rec)
  }

  private[diagnostics] def pruneLeaves(root: StackNode, n: Int): Seq[StackNode] = {
    // Enheap all leaves of the tree, with lowest total counts at the top.  Break ties by privileging
    // leaves that are further from root, and finally compare the name to make this more deterministic for testing.
    val leaves = new mutable.PriorityQueue[StackNode]()((x: StackNode, y: StackNode) => {
      var ret = -x.total.compareTo(y.total)
      if (ret == 0) ret = x.depth.compareTo(y.depth)
      if (ret == 0) ret = x.name.compareTo(y.name)
      ret
    })
    root.leaves.foreach(leaves.enqueue(_))
    // Pluck off leaves one by one.  If this turns parent into a leaf, enqueue the newly leafy parent.
    while (leaves.size > n) {
      val leaf = leaves.dequeue()
      val parent = leaf.pruneFromParent()
      if (parent ne null)
        leaves.enqueue(parent)
    }

    // We redistribute the self time of non-leaf nodes into the branches, because we only report leaves.
    root.pushSelfTimesToLeaves()

    leaves.toList.sortBy(-_.total)
  }

  private final class LiveTracking(val name: String) {
    val nodes = mutable.HashMap.empty[Long, StackNode]
    val root = new StackNode(cleanName(name, CleanName.LIVE))
    nodes += root.id -> root

    def resetLiveForTestingOnly(): Unit = {
      nodes.clear()
      root.kids.clear()
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
      if (allocNode.kids.isEmpty) {
        alloc += liveNode.total
        if (allocNode.apsids.isEmpty && errors.size < 10) errors += s"Zero apsid for $allocNode"
        allocNode.apsids.foreach { apsid => liveNodes += apsid -> liveNode }
        liveNode.apsids ++= allocNode.apsids
      } else {
        allocNode.kids.values.foreach { allocChild =>
          val liveChild = liveNode.kids.getOrElseUpdate(
            allocChild.id, {
              val kid = new StackNode(allocChild.cn, liveNode.depth + 1)
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
      val toDrop: Iterable[Long] = liveNode.kids.values.map(cleanDfs)
      toDrop.foreach(liveNode.kids -= _)
      if (liveNode.total < memEpsilon) {
        dropped += 1
        liveNode.apsids.foreach(liveNodes -= _)
        liveNode.id
      } else 0L
    }
    cleanDfs(liveRoot)
    log.debug(s"Dropped $dropped live nodes, apsids=${liveNodes.size}")
  }

  /**
   * Reduce a list of stack samples to a manageable set by pruning inner frames and then combining the now shorter
   * stacks.
   */
  private[diagnostics] def extractInterestingStacks(
      rootIds: Seq[ChainedID],
      collapsedFormatDump: String,
      preSplitStacks: Iterable[TimedStack],
      nmax: Int): StacksAndTimers = try {
    if (collapsedFormatDump.isEmpty && preSplitStacks.isEmpty) StacksAndTimers.empty
    else {
      val (roots0: Map[String, StackNode], events) = buildStackTrees(collapsedFormatDump, preSplitStacks)

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
        stacks.map(sn => (sn, sn.frames)).collect {
          case (s, frames) if frames.nonEmpty =>
            if (DiagnosticSettings.samplingAsserts) {
              s.assertConsistentTimes()
              assert(s.kids.isEmpty, "stacks should have been a leaf")
            }
            val hashId = stackMemoize(rootIds, frames)
            StackData(rootName, s.total, s.self, hashId, frames.last, folded(frames, s.total))
        }
      }.toSeq

      heapTracking.restore()
      nativeTracking.restore()
      StacksAndTimers(stackData, events)
    }
  } catch {
    case NonFatal(e) =>
      val wrapped = new RuntimeException(
        s"""|An error occurred in StackAnalysis while parsing this collapsed dump:
            |${collapsedFormatDump}""".stripMargin,
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
