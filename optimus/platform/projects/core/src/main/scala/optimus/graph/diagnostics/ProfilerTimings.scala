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
package optimus.graph.diagnostics

////////////////////////////////////////////////////////////////////////////////
// EXPERIMENTAL - this whole section may be deleted in the future             //
////////////////////////////////////////////////////////////////////////////////
import java.lang.{System => S}
import java.lang.management._

object ProfilerTimings {

  /**
   * For all of the time... methods, these points apply:
   *
   * <ul>
   *   <li>All just invoke the target within a loop and record the overall time.
   *   <li>The loop is unrolled to amortize the overhead of the loop itself <li>The 'direct' method just does the invoke
   *   N times
   *   <li>The 'direct==false' method attempts to invoke it N times but also calculate the time taken by the loop itself
   *   and back that out
   *   <li>In all cases, realize that this is an approximation. In particular:
   *   <ul>
   *     <li>The JIT compiler may decide to compile one/all of these into machine code in the middle of our calculation.
   *     We attempt to "warm up" the JIT with a few initial calls, but there is no guarantee.
   *     <li>Since these calls are all grouped the code will (probably) be promoted to any available on-chip cache. This
   *     executes faster than a random call embedded in a real codebase, which must be fetched from RAM.
   *   </ul>
   *   <li>So take this as an estimate and lower bound.
   * </ul>
   */
  // Hotspot compiler needs 15,000 on a server per http://java.dzone.com/articles/just-time-compiler-jit-hotspot
  val warmUpCycles = 16000 // Warmup cycles to try to force JIT compilation of timing code
  val maxAttempts = 25 // # of attempts with JIT or GC interference before giving up
  val unRoll = 100
  val N = 10000
  val nCycles = N * unRoll

  lazy val loopOverhead = timeLoopOverhead

  /**
   * Summary timings calculated once only. Returns a tuple with: <pre>
   * -- number of calls used to do the calculations
   * -- time for 1 call on System.currentTimeMillis (backing out loop overhead)
   * -- time for 1 call on System.currentTimeMillis (no attempt to account for loop overhead)
   * -- time for 1 call on System.nanoTime (backing out loop overhead)
   * -- time for 1 call on System.nanoTime (no attempt to account for loop overhead)
   * -- actual precision of nanoTime in the form:
   *
   * mmm|uuu|nnn (mmm=milliseconds, uuu=microseconds, nnn=nanoseconds)
   *
   * where a period, ., appears if the precision is NOT provided. For example,
   *
   * mmm|uuu|...
   *
   * would indicate precision only to the microsecond level. </pre>
   */
  lazy val timings =
    (nCycles, timeCurrentMillis(false), timeCurrentMillis(true), timeNanoTime(false), timeNanoTime(true), precision)

  lazy val (
    iterations,
    currentMillisNoOverhead,
    currentMillisDirect,
    nanoTimeNoOverhead,
    nanoTimeDirect,
    nanoTimePrecision) = timings

  /** Returns: (Number of GCs, Number of GC executions, Total milliseconds) */
  final def getGCStatus: (Int, Long, Long) = synchronized {
    // Note: Do we need to actually store the list in order to detect that we
    //       still have N GCs, but not the same N as the last time???
    // Note: See if there is a fast way to get the total GCs count. If we can do this
    //       then save the result from this call. If the next call still has the
    //       same total GCs, then the previously computed result is still good.
    val lst = ManagementFactory.getGarbageCollectorMXBeans
    val sz = lst.size
    var cntCollections = 0L
    var ttlTime = 0L
    var indx = 0
    while (indx < sz) {
      val gc = lst.get(indx)
      cntCollections += gc.getCollectionCount
      ttlTime += gc.getCollectionTime
      indx += 1
    }
    (sz, cntCollections, ttlTime)
  }

  /** Return accumulated JIT compilation time (milliseconds) or -1 if not supported */
  final def getJITTime: Long = {
    val comp = ManagementFactory.getCompilationMXBean
    if ((comp ne null) && comp.isCompilationTimeMonitoringSupported) comp.getTotalCompilationTime
    else -1
  }

  /**
   * Calculate estimated time for a single call on System.currentTimeMillis Returns -1 if cannot do a clean calculation.
   */
  def timeCurrentMillis(direct: Boolean): Long = {
    def notDirect: Long = {
      var cntr = 0
      var val0 = 0L
      // Concerned about overhead of -- for( i <- 0 until N)
      while (cntr < N) {
        val0 = val0 +
          S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis +
          S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis +
          S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis +
          S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis +
          S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis +
          S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis +
          S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis +
          S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis +
          S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis +
          S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis + S.currentTimeMillis
        cntr += 1
      }
      val0 // RETURN it just so compiler doesn't optimize all the code away!
    }

    def runDirect = {
      var cntr = 0
      while (cntr < N) {
        cntr += 1
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis;
        S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis; S.currentTimeMillis
      }
      cntr
    }

    // Executed code
    // First, try to convince JIT compiler to compile to machine code.
    var value = 0L
    for (i <- 0 until warmUpCycles) value += runDirect + notDirect + timeLoopOverhead
    // Dummy to make compiler think we are using 'value'
    if (value == 0) println("Profiler - junk " + value)

    // Record GC and JIT at start. If either is triggered, re-run the calculation
    val max = maxAttempts // how many times to attempt the calc
    var cntr = 0
    var rslt = -1L

    while (cntr < max) {
      val gc = getGCStatus
      val jit = getJITTime
      val calc = if (direct) {
        val start = System.nanoTime
        val rslt = notDirect
        val elapsed = System.nanoTime - start
        // Fake use of rslt so system does not eliminate the code
        if (start == -1 && rslt == 0) println("Profiler.timeNanotime " + rslt)
        elapsed / nCycles
      } else {
        val start = System.nanoTime
        val rslt = runDirect
        val elapsed = System.nanoTime - start
        // Fake use of rslt so system does not eliminate the code
        if (start == -1 && rslt == 0) println("Profiler.timeNanotime " + rslt)
        (elapsed - loopOverhead) / nCycles
      }
      if (jit == getJITTime && gc == getGCStatus) {
        rslt = calc
        cntr += max
      } else
        cntr += 1
    }
    rslt
  }

  /** Calculate estimated time for a single call on System.nanoTime */
  def timeNanoTime(direct: Boolean): Long = {
    def notDirect: Long = {
      var cntr = 0
      var val0 = 0L
      // Concerned about overhead of -- for( i <- 0 until N)
      while (cntr < N) {
        val0 = val0 +
          S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime +
          S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime +
          S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime +
          S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime +
          S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime +
          S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime +
          S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime +
          S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime +
          S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime +
          S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime + S.nanoTime
        cntr += 1
      }
      val0 // RETURN it just so compiler doesn't optimize all the code away!
    }

    def runDirect = {
      var cntr = 0
      while (cntr < N) {
        cntr += 1
        S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime;
        S.nanoTime;
        S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime;
        S.nanoTime;
        S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime;
        S.nanoTime;
        S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime;
        S.nanoTime;
        S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime;
        S.nanoTime;
        S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime;
        S.nanoTime;
        S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime;
        S.nanoTime;
        S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime;
        S.nanoTime;
        S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime;
        S.nanoTime;
        S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime; S.nanoTime;
        S.nanoTime
      }
      cntr
    }

    // Executed code
    // First, try to convince JIT compiler to compile to machine code.
    var value = 0L
    for (i <- 0 until warmUpCycles) value += runDirect + notDirect + timeLoopOverhead
    // Dummy to make compiler think we are using 'value'
    if (value == 0) println("Profiler - junk " + value)

    // Record GC and JIT at start. If either is triggered, re-run the calculation
    val max = maxAttempts // how many times to attempt the calc
    var cntr = 0
    var rslt = -1L

    while (cntr < max) {
      val gc = getGCStatus
      val jit = getJITTime
      val calc = if (direct) {
        val start = System.nanoTime
        val rslt = notDirect
        val elapsed = System.nanoTime - start
        // Fake use of rslt so system does not eliminate the code
        if (start == -1 && rslt == 0) println("Profiler.timeNanotime " + rslt)
        elapsed / nCycles
      } else {
        val start = System.nanoTime
        val rslt = runDirect
        val elapsed = System.nanoTime - start
        // Fake use of rslt so system does not eliminate the code
        if (start == -1 && rslt == 0) println("Profiler.timeNanotime " + rslt)
        (elapsed - loopOverhead) / nCycles
      }
      if (jit == getJITTime && gc == getGCStatus) {
        rslt = calc
        cntr += max
      } else
        cntr += 1
    }
    rslt
  }
  def timeLoopOverhead: Long = {
    // Make VARs else the compiler might optimize and do the additions at compile time
    var (x00, x01, x02, x03, x04, x05, x06, x07, x08, x09) = (0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
    var (x10, x11, x12, x13, x14, x15, x16, x17, x18, x19) = (0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
    var (x20, x21, x22, x23, x24, x25, x26, x27, x28, x29) = (0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
    var (x30, x31, x32, x33, x34, x35, x36, x37, x38, x39) = (0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
    var (x40, x41, x42, x43, x44, x45, x46, x47, x48, x49) = (0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
    var (x50, x51, x52, x53, x54, x55, x56, x57, x58, x59) = (0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
    var (x60, x61, x62, x63, x64, x65, x66, x67, x68, x69) = (0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
    var (x70, x71, x72, x73, x74, x75, x76, x77, x78, x79) = (0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
    var (x80, x81, x82, x83, x84, x85, x86, x87, x88, x89) = (0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
    var (x90, x91, x92, x93, x94, x95, x96, x97, x98, x99) = (0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)

    var cntr = 0
    var val0 = 0L
    val start = System.nanoTime
    // Concerned about overhead of -- for( i <- 0 until N)
    while (cntr < N) {
      val0 = 0 +
        x00 + x01 + x02 + x03 + x04 + x05 + x06 + x07 + x08 + x09 +
        x10 + x11 + x12 + x13 + x14 + x15 + x16 + x17 + x18 + x19 +
        x20 + x21 + x22 + x23 + x24 + x25 + x26 + x27 + x28 + x29 +
        x30 + x31 + x32 + x33 + x34 + x35 + x36 + x37 + x38 + x39 +
        x40 + x41 + x42 + x43 + x44 + x45 + x46 + x47 + x48 + x49 +
        x50 + x51 + x52 + x53 + x54 + x55 + x56 + x57 + x58 + x59 +
        x60 + x61 + x62 + x63 + x64 + x65 + x66 + x67 + x68 + x69 +
        x70 + x71 + x72 + x73 + x74 + x75 + x76 + x77 + x78 + x79 +
        x80 + x81 + x82 + x83 + x84 + x85 + x86 + x87 + x88 + x89 +
        x90 + x91 + x92 + x93 + x94 + x95 + x96 + x97 + x98 + x99
      cntr += 1
    }
    val end = System.nanoTime
    // Fake a reference to 'val0' so compiler doesn't eliminate the code!
    if (end == -1 && start == -1) println("Profiler.timeLoopOverhead: val0=" + val0)
    end - start
  }

  /**
   * Attempt to figure out if the precision is really down to nanosecond level or not! Returns a String showing
   * milliseconds, microseconds, nanoseconds of the form:
   *
   * mmm|uuu|nnn (mmm=milliseconds, uuu=microseconds, nnn=nanoseconds
   *
   * where a period, ., indicates that precision was NOT actually detected!
   */
  lazy val precision = detectPrecision

  // Shouldn't change, so make this private
  private def detectPrecision: String = {
    var str = "........." // Commas plugged in on exit
    // ASSUME that at least 1-second precision! (below that we're hosed anyway)
    // Cycle until all precisions are seen or > 2 seconds
    val end = System.currentTimeMillis + 2000
    val nano = System.nanoTime
    while (str.indexOf('.') > -1 && System.currentTimeMillis < end) {
      val diff = (System.nanoTime - nano).toString
      val sz = diff.length
      // Just see if a specific digit changed
      for (i <- 0 until 9) {
        if (str.charAt(i) == '.' && (sz - 9 + i) > -1 && diff.charAt(sz - 9 + i) != '0') {
          val ch = if (i < 3) 'm' else if (i < 6) 'u' else 'n'
          str = str.substring(0, i) + ch + str.substring(i + 1)
        }
      }
    }
    str.substring(0, 3) + '|' + str.substring(3, 6) + '|' + str.substring(6)
  }

  @deprecated("Do Not Use In Production", "Forever")
  /**
   * Attempt to force a GC. Not guaranteed to be successful. Even if successful, the GC executed may be a lightweight,
   * not full, GC (e.g. only collecting from the "young" generation).
   *
   * @param maxWaitMillis
   *   \- <= 0 == default
   * @return
   *   TRUE if a GC actually executed (may not be a full GC)
   */
  def forceGC(maxWaitMillis: Int = 0): Boolean = {
    val waitDflt = 100
    val waitCycle = 10
    val (gcCnt, gcRuns, gcTime) = getGCStatus
    val exitTime = System.currentTimeMillis + (if (maxWaitMillis <= 0) waitDflt else maxWaitMillis)

    while (System.currentTimeMillis < exitTime) {
      System.gc
      val (nowCnt, nowRuns, nowTime) = getGCStatus
      if (nowRuns != gcRuns) return true
      Thread.sleep(waitCycle)
    }
    false
  }
}
