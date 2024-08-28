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

import optimus.utils.SystemFinalization

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.ConcurrentHashMap
import java.time.ZonedDateTime
import scala.jdk.CollectionConverters._

/**
 * AppStats - a framework for collecting statistics for any application.
 *
 * These are the <b>runtime</b> classes which actually gather performance metrics while the application is executing.
 * Other classes will then extract the JSON version of this data and persist it to the filesystem, a database, or some
 * other repository.
 *
 * The goal is to provide a framework which:
 *
 *   - Is general and extensible so it can be used by any application.
 *   - Is lightweight so that it can be active in production if desired/necessary
 *   - Allows drill down to more specific detail as required
 *
 * The general concept is to:
 *
 *   - provide a *base class* with some basic statistics and utility methods
 *   - provide plug-in *traits* to capture statistics for other system or application areas
 *   - allow an application to define its own trait to capture relevant statistics for that application
 *   - use a simple probeStart...probeEnd set of calls to trigger the actual collection of information
 *   - deliver all of the results in a JSON format, which provides for easy extensibility
 *
 * This sub-system is designed to allow multiple independent "jobs" to execute within the same JVM, and provide a
 * separate "context" for each such job - see AppStatsControl and AppStatsRun
 *
 * The overall structure, and some key classes, consists of:
 *
 * AppStatsControl - companion object with system-wide constants, control structures (profileRun, profileWith)
 *   - also keeps the MAP of all existing statistics "job" contexts (AppStatsRun)
 *
 * AppStatsControl - external control information to tell the application which specific statistics are desired for this
 * "run"
 *   - in the Driver Controller, should acquire parameters from an external source - command line params, properties
 *     file, the DAL, etc.
 *   - when a computation is started on an engine server, this will be serialized and passed to the engine to provide it
 *     the control parameters and a back-link to a particular "run" on the Controller (AppStatsRun)
 *
 * AppStatsRun - defines one independent "job", "run", or "context" for a coherent set of statistics (based on an
 * AppStatsControl)
 *
 *   - in most cases, only a single AppStatsRun will be created for any given AppStatsControl. But this structure also
 *     supports multiple Control contexts, and "runs", within a single JVM if necessary
 *
 * AppStatsPool - interpret an application component identifier vs the AppStatsControl for a given AppStatsRun. If the
 * AppStatsControl specifies that stats should be gathered, then maintains a pool of AppStats instances which should be
 * used.
 *
 * AppStats - basic class (with traits) which collect statistics
 *
 * AppStatsPersist - utility routine to persist all statistics to the DAL or other targets. (NOT defined in Optimus
 * since it uses DAL facilities)
 *
 * Traits are pre-defined in this package to support many common statistics for an application:
 *
 * AppStats - the base class. Counts probeStart..probeEnd sequences, wall-clock and CPU time used by AppStats itself
 * AppStatsSynchronized - Marker trait will synchronize all public methods AppStatsBasics - total & user CPU time,
 * wall-clock time, compile time, max memory AppStatsEnvironment - version of the OS, Java, JVM, etc AppStatsMemory -
 * Number of GC executions, max dead objects, max storage (heap, non-heap, OS) AppStatsThreadBasic - Number current
 * threads, max live threads, total threads started AppStatsThreadDetail - Detailed per-thread information - ID, Name,
 * State, times blocked, etc
 *
 * WARNING: Be aware that gathering some performance statistics can require the allocation of objects and some CPU
 * utilization. If a lot of statistics are accumulated, especially some of the "heavy" statistics, then the measurement
 * of some <i>application</i> statistics may be distorted. The basic AppStats class does capture the CPU time used by
 * the probeStart and probeEnd methods themselves, so this can be backed out.
 */
/**
 * ***************************************************************************** The basic runtime class which gathers
 * statistics.
 *
 * This class itself gathers a basic start/end time, and accumulates wall time and CPU time used by the probeStart &
 * probeEnd methods themselves. [Mainly to make sure that the monitoring code itself is not impacting the application.]
 *
 * See AppStatsControl for the definition of most parameters. Parameters that differ:
 *
 * @param plusProperties
 *   \- additional properties to add to and OVERRIDE properties from AppStatsRun
 * @param unique
 *   \- to distinguish in-memory node if 'equals()' is used (vs using A eq B) and serves as a unique ID (within a host)
 *   for use with partOf
 * @param partOf
 *   \- link to a parent AppStats instance if this covers a portion of an overall process covered by the parent (e.g. to
 *   avoid double-counting CPU time) ****************************************************************************
 */
class AppStats(
    val thisRun: AppStatsRun,
    val component: String,
    val plusProperties: Map[String, String] = null,
    val exceptionCapture: Int = 0,
    val register: Boolean = true,
    val deep: Boolean = false,
    val suppressEmpty: Boolean = true,
    val disabled: Boolean = false,
    val partOf: Long = 0) {

  val u = Utils // Convenient access

  /**
   * Provides a unique ID within a given Host. Also makes in-memory instances distinct (since all of the other params
   * may well be equal)
   */
  val unique: Long = AppStatsControl.getUnique()

  val environment = thisRun.control.environment
  val runAt = thisRun.control.runAt
  val appID = thisRun.control.appID
  lazy val appProperties = u.mapAddOrOverride(thisRun.appProperties, plusProperties)
  val location = AppStatsControl.thisLocation
  val runDate = runAt.toLocalDate.toString

  /** Whether AppStatsSynchronized trait is mixed in */
  val hasSynchronized = this.isInstanceOf[AppStatsSynchronized]

  /** Whether AppStatsDisabled trait is mixed in */
  val hasDisabled = this.isInstanceOf[AppStatsDisabled]

  private var bMarkDisabled = false // To dynamically flag as disabled
  private var bIsFinished = false // To mark as finished

  // List to capture any Exceptions thrown relative to this AppStats
  private var lstExceptions: List[Throwable] = Nil

  // Using Atomic... handles any multi-threading/concurrency issues
  //
  val startTime = new AtomicLong // Milliseconds when Start or last Reset
  val endTime = new AtomicLong // Milliseconds of end time when data retrieved
  val cntProbes = new AtomicInteger // How many times the general 'probe' routine was executed
  val cntProbesBad = new AtomicInteger // Bad probeStart...probeEnd sequences (i.e. 2 probeStarts)
  val cntExceptions = new AtomicInteger // Count if Exceptions are thrown - assuming detail is in a log
  val cntInternalErrors = new AtomicInteger // Count of errors within AppStats itself!
  val ttlStatsWall = new AtomicLong // Wall clock time spent within this sub-system itself
  val ttlStatsCPU = new AtomicLong // CPU time spent within this sub-system itself
  val ttlStatsCPUThread = new AtomicLong // CPU measured on this thread

  // Do not incur allocation overhead unless at least one entry is declared
  @volatile
  private var userCounters: scala.collection.concurrent.Map[String, AtomicLong] = null
  @volatile
  private var userStrings: scala.collection.concurrent.Map[String, StringBuilder] = null

  private lazy val localList: List[(Any, String, Boolean, String, String)] =
    List(
      (environment, "valEnvironment", false, "Base", "The basic Environment - should be DEV, QA, or PROD"),
      (appID, "valApplication", true, "Base", "The overall Application identification invariant"),
      (
        runAt,
        "valRunAt",
        true,
        "Base",
        "The identifying Run Time (start time) for the entire application - invariant - YYYY-MM-DD-HH-MM-SS"),
      (runDate, "valRunDate", false, "Base", "Date part of valRunAt as 'YYYY-MM-DD'"),
      (
        location,
        "valLocation",
        true,
        "Base",
        "Physical location where THIS instance was executed - e.g. NA/Virginia/irg331/userX"),
      (
        appProperties,
        "valProperties",
        false,
        "Base",
        "Application properties to associate with this AppStats information"),
      (
        component,
        "valComponent",
        true,
        "Base",
        "The software component which reported these statistics - e.g. DAL/total"),
      (deep, "valDeep", false, "Base", "Whether deep statistics are requested"),
      (unique, "valUnique", false, "Base", "Unique ID of this AppStats within valLocation"),
      (partOf, "valPartOf", false, "Base", "Link to parent AppStats if this covers a portion of that overall process"),
      (
        suppressEmpty,
        "valSuppressEmpty",
        false,
        "Base",
        "Suppress JSON entries which have an empty value (null, 0, 0.0, ''"),
      (startTime, "valTimeStart", false, "Base", "Wall clock time when the FIRST collection occurred (millis)"),
      (endTime, "nowTimeEnd", false, "Base", "Wall clock time when the LAST collection occurred (millis)"),
      (cntProbes, "cntProbes", false, "Base", "The number of probeStart....probeEnd pairs executed"),
      (
        cntProbesBad,
        "cntProbesBad",
        false,
        "Base",
        "The number of unbalanced probeStart...probeEnd calls (e.g. probeStart...probeStart...probeEnd)"),
      (cntExceptions, "cntExceptions", false, "Base", "The number of Exceptions recorded - see the logs for details"),
      (
        cntInternalErrors,
        "cntInternalErrors",
        false,
        "Base",
        "Internal AppStats errors - invalid MATCH, lookup of variable name not found, etc"),
      (ttlStatsWall, "ttlStatsWall", false, "Base", "Total wall-clock time spent collecting these statistics (nanos)"),
      (
        ttlStatsCPU,
        "ttlStatsCPU",
        false,
        "Base",
        "Total CPU (process) time spent collecting these statistics (nanos) (0 if Suns OS Bean not found)"),
      (
        ttlStatsCPUThread,
        "ttlStatsThreadCPU",
        false,
        "Base",
        "Total CPU (this Thread) time spent collecting these statistics (nanos).")
    )

  // List of just the entries from the primary constructor parameters
  private lazy val paramList =
    localList(0) :: localList(1) :: localList(2) :: localList(4) :: localList(5) :: localList(6) :: Nil

  /**
   * Definition of the statistics captured by this object and associated traits.
   *
   * Format: List[(data:AnyRef, name:String, subParse:Boolean, trait:String, documentation:String)]
   *
   * where: <ul> <li>data - the actual data/counter. Currently supported: AtomicLong, AtomicInteger, StringBuilder,
   * String (immutable), Boolean (immutable), AtomicBoolean <li>name - The caller-visible name of this data variable.
   * Should be UNIQUE! Used in CSV and JSON output, and for some internal lookup operations (e.g. setTo(...)) By
   * convention, standard PREFIXes are used for both 'data' and 'name': <ul> <li>no prefix - the name should be
   * self-explanatory <li>cnt - a count of how often an event has occurred (e.g. cntExceptions). Some counts may be
   * independent of any probeStart...probeEnd calls. E.g. cntExceptions is probably bumped any time an exception occurs,
   * whether we are now within a probeStart..End pair <li>ttl - a sum of multiple probes (e.g. ttlCPU is the sum of CPU
   * between all pairs of probeStart...probeEnd calls) <li>max - the max value encountered during all pairs of
   * probeStart...probeEnd calls (e.g. memory used) <li>now - the latest reading of an increasing factor. E.g. nowCPU
   * would be the total CPU time for this application (actually, this Thread) to this point. Independent of any
   * probeStart...End calls. Contrasted to ttlCPU above. <li>val - an immutable value - e.g. OS Name </ul>
   *
   * NOTE: Best not to include spaces or other special characters - some utility somewhere down the line always chokes!
   *
   * <li>subParse - TRUE == in JSON format, subparse this field <li>trait - which Trait defines the variable
   * <li>documentation - description of the variable (for a user) </ul>
   *
   * Note: Easy way to handle within a 'foreach' is as:
   *
   * statsList.foreach{ case (data, label, subParse, trait, documentation) => ..... }
   *
   * (braces for open/close, not parens)
   */
  def statsList = fullList

  def statsListSorted = statsList.sortWith(AppStatsControl.lessByLabel(_, _))

  /**
   * PRIMARY CONSTRUCTOR CODE executed upon construction
   */
  if (register) thisRun.register(this)

  /**
   * MUST be overridden in any non-empty Trait - model:
   *
   * super.fullList ::: localList ... plus any other entries required
   */
  protected def fullList: List[(Any, String, Boolean, String, String)] = localList ::: userVariables

  private def userVariables: List[(Any, String, Boolean, String, String)] =
    if (userCounters == null && userStrings == null) Nil
    else {
      var lst: List[(Any, String, Boolean, String, String)] = Nil
      if (userCounters != null) userCounters.map { case (name, value) =>
        lst ::= (value, "userCounter" + name, false, "Base", "A user-defined integer (long) value")
      }
      if (userStrings != null) userStrings.map { case (name, value) =>
        lst ::= (value, "userString" + name, false, "Base", "A user-defined string value")
      }
      lst.sortWith { (A, B) =>
        A._2.compareTo(B._2) < 1
      }
    }

  /**
   * Setup as needed to gather statistics. Some stats (e.g. ramJVM) will be evaluated on both a probeStart and a
   * probeEnd. Some will establish a starting checkpoint and then update statistics at the probeEnd call (e.g. wallTime,
   * cpu times).
   *
   * Other statistics are not affected by probeStart/probeEnd (e.g. numExceptions) and must be updated individually as a
   * situation dictates.
   *
   * 'probeStart' and 'probeEnd' calls should be paired. Unpaired calls will be ignored (but increment cntProbesBad).
   *
   * Traits should override the 'doTraitProbe' method to execute their own logic
   *
   * If 'lastProbe' is set on a probeEnd(...) call, this is marked as Finished.
   *
   * 'final' needed here so that the accumulation of time spent within the probe... methods themselves is correct.
   *
   * @param lastProbe
   *   \- TRUE if this is the last probe before collecting and resetting (or ending the app)
   * @param target
   *   \- Pass a list of target objects which may be queried by some Traits, frequently null, not used by any
   *   pre-defined Traits
   */
  final def probeStart(lastProbe: Boolean, target: AnyRef*): Unit =
    if (hasSynchronized) synchronized { probeStartImpl(lastProbe, target) }
    else probeStartImpl(lastProbe, target)

  final def probeStart(lastProbe: Boolean = false): Unit = probeStart(lastProbe, null)

  private def probeStartImpl(lastProbe: Boolean, target: AnyRef*): Unit = {
    if (isActive) {
      entered()
      if (startTime.get == 0) startTime.set(System.currentTimeMillis)
      if (inProbeStart) cntProbesBad.incrementAndGet
      else {
        inProbeStart = true
        doTraitProbe(true, lastProbe, target)
      }
      exited()
    }
  }

  final def probeEnd(lastProbe: Boolean, target: AnyRef*): Unit =
    if (hasSynchronized) synchronized { probeEndImpl(lastProbe, target) }
    else probeEndImpl(lastProbe, target)

  final def probeEnd(lastProbe: Boolean = false): Unit = probeEnd(lastProbe, null)

  private def probeEndImpl(lastProbe: Boolean, target: AnyRef): Unit = {
    if (isActive) {
      entered()
      if (!inProbeStart) cntProbesBad.incrementAndGet
      else {
        cntProbes.incrementAndGet
        inProbeStart = false
        endTime.set(System.currentTimeMillis)
        doTraitProbe(false, lastProbe, null)
        if (lastProbe) markAsFinished()
      }
      exited()
    }
  }

  /**
   * Optional OVERRIDE point for a Trait so it can do its own probeStart or probeEnd logic.
   */
  protected def doTraitProbe(isStart: Boolean, lastProbe: Boolean, target: AnyRef*) = {
    /* NO-OP in this base class */
  }

  /**
   * UN-REGISTER this instance from the global list. Usually indicates that some severe problem has occurred, and any
   * internal resources should be cleaned up. Default is to also DISABLE the instance so it NO-OPs any future calls.
   */
  final def unRegister(disable: Boolean = true): Unit = unRegisterImpl(disable)

  protected def unRegisterImpl(disable: Boolean): Unit = {
    thisRun.unRegister(this)
    if (disable) markDisabled()
  }

  def isProbing: Boolean = if (hasSynchronized) synchronized { inProbeStart }
  else inProbeStart

  /**
   * ACCUMULATE information from the given AppStats into THIS AppStats.
   *
   * Numeric values are added, mutable strings are appended (with separator), AtomicBoolean's are OR'd
   */
  final def accumulate(from: AppStats): Unit = accumulateImpl(from)

  // This logic will handle all standard variables.
  // Traits may override accumulateFrom to handle special cases
  private def accumulateImpl(from: AppStats): Unit = {
    if (isActive) {
      entered()
      if (startTime.get == 0) startTime.set(System.currentTimeMillis)
      // Note that the 'from' list may have more/fewer variables than 'this' list
      from.statsList.foreach { case (data, label, subParse, fromTrait, doc) =>
        if (!data.isInstanceOf[Boolean]) {
          val thatItem = findByName(label)
          val thatData = thatItem match {
            case Some(value) => value._1
            case None        => null
          }
          if (thatData != null) data match {
            case lng: AtomicLong => {
              if (thatData.isInstanceOf[AtomicLong])
                thatData.asInstanceOf[AtomicLong].addAndGet(lng.get)
              else cntInternalErrors.incrementAndGet()
            }
            case i: AtomicInteger => {
              if (thatData.isInstanceOf[AtomicInteger])
                thatData.asInstanceOf[AtomicInteger].addAndGet(i.get)
              else cntInternalErrors.incrementAndGet
            }
            case sb: StringBuilder => {
              if (thatData.isInstanceOf[String] || thatData.isInstanceOf[StringBuilder]) {
                val str = u.strConcat(sb.toString, thatData.toString, AppStatsControl.Separator)
                sb.setLength(0)
                sb.append(str)
              }
            }
            case str: String => { /* immutable */ }

            case ab: AtomicBoolean =>
              if (thatData.isInstanceOf[AtomicBoolean]) {
                val thisAB = thatData.asInstanceOf[AtomicBoolean]
                thisAB.set(thisAB.get || ab.get)
              } else cntInternalErrors.incrementAndGet

            case b: Boolean => { /* immutable */ }
            case _          => cntInternalErrors.incrementAndGet
          }
        }
      }
      accumulateFrom(from) // For Trait overrides
      exited()
    }
  }

  protected def accumulateFrom(from: AppStats): Unit = {
    // NO-OP - this is an OVERRIDE point for other Traits with special needs
    // WARNING: If overridden, be aware that the 'from' instance may not even
    //          support the same Traits as this instance.
  }

  /** Set the named variable to the given numeric value */
  final def setTo(varName: String, value: Long): Unit =
    if (hasSynchronized) synchronized { setToImpl(varName, value) }
    else setToImpl(varName, value)

  protected def setToImpl(varName: String, value: Long): Unit = {
    if (isActive) {
      entered()
      val item = findByName(varName)
      item match {
        case Some((data, label, subParse, fromTrait, documentation)) =>
          data match {
            case ai: AtomicInteger =>
              ai.set(value.asInstanceOf[Int])
            case al: AtomicLong =>
              al.set(value)
            case _ => cntInternalErrors.incrementAndGet
          }
        case _ => {}
      }
      exited()
    }
  }

  /** Bump and return the number of Exceptions */
  def bumpExceptions() = if (isActive) cntExceptions.incrementAndGet

  /**
   * Record an Exception IFF exceptionCapture > 0 & not disabled (also does a BumpException) NOTE: Will record an
   * exception even if this AppStats is 'finished'
   */
  def recordException(ex: Throwable): Unit =
    if (exceptionCapture > 0 && !isDisabled && ex != null)
      synchronized { bumpExceptions(); lstExceptions ::= ex }

  /** Reset all counters/timers to initial values */
  final def reset: Unit = if (hasSynchronized) synchronized { resetImpl() }
  else resetImpl()

  protected def resetImpl(): Unit = {
    if (isActive) {
      entered()
      statsList.foreach { // Boolean is AnyVal so can't use in the match, but would be immutable anyway
        case (data, label, subParse, fromTrait, doc) =>
          if (!data.isInstanceOf[Boolean]) data match {
            case lng: AtomicLong   => lng.set(0)
            case i: AtomicInteger  => i.set(0)
            case sb: StringBuilder => sb.setLength(0)
            case str: String       => { /* immutable */ }
            case ba: AtomicBoolean => ba.set(false)
            case b: Boolean        => { /* immutable */ }
            case _                 => cntInternalErrors.set(1) // Clear, but 1 for this error itself
          }
      }
      exited()
    }
  }

  /** Determine if a user-defined integer with this name exists */
  def userIntExists(name: String): Boolean = if (userCounters == null) false else userCounters.contains(name)

  /** Get a user-defined AtomicLong for set/increment - creates if needed */
  final def userInt(name: String): AtomicLong = {
    var rslt = dummyAtomicLong
    if (isActive) {
      entered()
      rslt = userIntImpl(name)
      exited()
    } else if (rslt == null) { // never return a null
      dummyAtomicLong = new AtomicLong
      rslt = dummyAtomicLong
    }
    if (rslt eq dummyAtomicLong) rslt.set(0L)
    rslt
  }
  // Traits can override if necessary
  protected def userIntImpl(name: String): AtomicLong = {
    if (userCounters == null) synchronized {
      if (userCounters == null) userCounters = (new ConcurrentHashMap[String, AtomicLong]).asScala
    }
    userCounters.putIfAbsent(name, new AtomicLong)
    userCounters(name)
  }
  // If this is INACTIVE, return this one (so never return null)
  private var dummyAtomicLong: AtomicLong = _

  /** Determine if a user-defined String (Builder) exists */
  def userStringExists(name: String): Boolean = if (userStrings == null) false else userStrings.contains(name)

  /** Get a user-defined String(Builder) for setting or appending */
  final def userString(name: String): StringBuilder = {
    var rslt = dummyStringBuilder
    if (isActive) {
      entered()
      rslt = userStringImpl(name)
      exited()
    } else if (rslt == null) { // Never return a null
      if (dummyStringBuilder == null) {
        dummyStringBuilder = new StringBuilder
        rslt = dummyStringBuilder
      }
    }
    if (rslt eq dummyStringBuilder) rslt.setLength(0)
    rslt
  }
  // Traits can override if necessary
  protected def userStringImpl(name: String): StringBuilder = {
    if (userStrings == null) synchronized {
      if (userStrings == null) userStrings = (new ConcurrentHashMap[String, StringBuilder]).asScala
    }
    userStrings.putIfAbsent(name, new StringBuilder)
    userStrings(name)
  }
  private var dummyStringBuilder: StringBuilder = _

  /** Get the contents in JSON format - natural variable order */
  final def json: String = json(false, null)

  /** Get the contents in JSON format, natural order, provide optional data */
  final def json(optionalData: AnyRef*): String = json(false, optionalData: _*)

  /** Get the contents in JSON format - optionally sort by variable name */
  final def json(sortByVariableName: Boolean, optionalData: AnyRef*): String =
    if (hasSynchronized) synchronized { jsonImpl(sortByVariableName, optionalData: _*) }
    else
      jsonImpl(sortByVariableName, optionalData: _*)

  private def jsonImpl(byVariable: Boolean, optionalData: AnyRef*): String = {
    entered()
    val sb = new StringBuilder(2048)
    val lst = if (isDisabled) paramList else if (byVariable) statsListSorted else statsList
    sb.append('{')
    lst.foreach {
      case (data, label, subParse, fromTrait, documentation) => {
        jsonItem(sb, label, data, suppressEmpty)
        if (subParse && data != null)
          sb.append(
            parsed(
              label + "Parsed",
              if (data.isInstanceOf[ZonedDateTime]) parsed(data.asInstanceOf[ZonedDateTime]) else data.toString))
      }
    }
    val seg = if (isDisabled) "" else jsonSegment(optionalData: _*)
    if (seg != null && seg.length > 0)
      sb.append(seg)
    if (isDisabled) sb.append(quoted("STATUS")).append(':').append(quoted("DISABLED"))
    closingBrace(sb)
    val strOut = sb.toString
    exited()
    strOut
  }

  /**
   * MAY be overridden by descendents and traits to insert a segment into the final JSON
   *
   * optionalData - may provide a series of objects which the jsonSegment can use to form the resulting JSON segment.
   * Allows a given 'trait' to pick up its data from one or more of these optionalData instances, and not retain any
   * internal information at all from the probeStart and probeEnd calls.
   *
   * SHOULD have a trailing comma, ',', so that multiple segments appended together are still legal JSON <p> If
   * overridden, must return one or a series of valid JSON entries: for example <pre> "segmentname": value, - string,
   * numeric, null, sub-object, array, true/false
   *
   * or "segment1" : value, "segment2" : value, "segmentN" : value, </pre>
   *
   * WARNING: Any trait which overrides this method must be careful to preserver the AnyRef* structure when calling
   * 'super'. The standard mechanism of an override is to return this at the bottom of the method:
   *
   * super.jsonSegment(optionalData:_*) + [my json] + ,
   *
   * where the funny little :_* construct is necessary! On the CALL to the override, the AnyRef* params are grouped up
   * into an Array[AnyRef], if you just do this:
   *
   * super.jsonSegment(optionalData) + [my json] + ','
   *
   * then this Array[AnyRef] will be wrapped into ANOTHER array, so 'super' will receive an array of arrays - and
   * probably not be able to figure it out!
   */
  protected def jsonSegment(optionalData: AnyRef*): String = {
    u.jsonForExceptions("Exceptions", exceptionCapture, lstExceptions)
  }

  /**
   * Return this instance or the system-wide 'disabledInstance' if disabled. Un-registers this instance if disabled.
   * Primarily used to reduce memory footprint if gblENABLED is false or there are a lot of individual disabled
   * instances.
   */
  def ifEnabled: AppStats = if (isDisabled) { unRegister(true); AppStatsControl.disabledInstance }
  else this

  /** If TRUE, then this instance is DISABLED from stats gathering */
  def isDisabled: Boolean = disabled || hasDisabled || !thisRun.gblENABLE || bMarkDisabled

  /** If TRUE, then all stats gathering has been completed */
  def isFinished: Boolean = bIsFinished

  /** TRUE if this AppStats is still gathering stats - NOT finished or disabled */
  def isActive: Boolean = !(isDisabled || isFinished)

  /** TRUE if this AppStatis is NOT gathering stats - either finished or disabled */
  def notActive: Boolean = !isActive

  /** Mark this instance as DISABLED - usually some error has occurred */
  def markDisabled(): Unit = bMarkDisabled = true

  /**
   * Mark that all stats gathering has been completed - irreversible NOTE: Cannot be BOTH Disabled and Finished!
   * @return
   *   TRUE if status was changed to Finished
   */
  def markAsFinished(): Boolean = {
    if (!isDisabled && !isFinished) {
      if (isProbing) probeEnd(true)
      bIsFinished = true
      true
    } else
      false
  }

  val W = 20

  /** Return a \n delimited string of the basic ID information for this instance (table format) */
  def toStringID: String = {
    val sb = new StringBuilder(1024)
    ln(sb, "Environment", environment)
    ln(sb, "AppID", appID)
    ln(sb, "RunAtTime", runAt.toString)
    ln(sb, "Location", location)
    ln(sb, "Component", component)
    ln(sb, "Deep", deep)
    ln(sb, "SuppressEmpty", suppressEmpty)
    ln(sb, "Disabled", isDisabled)
    ln(sb, "Finished", isFinished)
    ln(sb, "Active", isActive)
    sb.toString
  }
  private def ln(sb: StringBuilder, lbl: String, value: String): Unit =
    sb.append(lbl.padTo(W, ' ')).append(':').append(value).append('\n')

  private def ln(sb: StringBuilder, lbl: String, b: Boolean): Unit = ln(sb, lbl, if (b) "Yes" else "No")

  /**
   * *******************************************************************************************
   */
  /* Private/protected methods here to reduce the clutter above                                 */
  /**
   * *******************************************************************************************
   */
  private var startWall = 0L
  private var startCPU = 0L
  private var startCPUThread = 0L

  private def entered(): Unit = {
    startWall = System.nanoTime
    startCPU = if (AppStatsControl.systemAsSun == null) 0 else AppStatsControl.systemAsSun.getProcessCpuTime
    startCPUThread =
      if (AppStatsControl.threadThreadCPUTimeSupported) AppStatsControl.threadBean.getCurrentThreadCpuTime else 0
  }
  private def exited(): Unit = {
    if (startWall > 0) ttlStatsWall.addAndGet(System.nanoTime - startWall)
    if (AppStatsControl.systemAsSun != null && startCPU > 0)
      ttlStatsCPU.addAndGet(AppStatsControl.systemAsSun.getProcessCpuTime - startCPU)
    if (AppStatsControl.threadThreadCPUTimeSupported && startCPUThread > 0)
      ttlStatsCPUThread.addAndGet(AppStatsControl.threadBean.getCurrentThreadCpuTime - startCPUThread)
    startWall = 0
    startCPU = 0
    startCPUThread = 0
  }
  protected def findByName(varName: String): Option[(Any, String, Boolean, String, String)] =
    statsList.find { case (data, label, subParse, fromTrait, documentation) => label == varName }

  //  Convenience method - trim trailing whitespace
  // (Can't believe this isn't supplied by StringBuilder!)
  protected def trimEnd(sb: StringBuilder): StringBuilder = {
    while (sb.length > 0 && Character.isWhitespace(sb.charAt(sb.length - 1))) sb.length_=(sb.length - 1)
    sb
  }
  // Trim trailing whitespace, and any trailing 'ch' found (multiple)
  protected def trimEnd(sb: StringBuilder, ch: Char): StringBuilder = {
    trimEnd(sb)
    while (sb.length > 0 && sb.charAt(sb.length - 1) == ch) sb.length_=(sb.length - 1)
    sb
  }

  /**
   * Convenience method for JSON - make sure there is no trailing comma before closing brace
   *
   * In JSON, after a series of name:value pairs there should not be a dangling comma before the final closing }
   *
   * this method trims all trailing whitespace, drops any dangling comma, then adds a closing }
   */
  protected def closingBrace(sb: StringBuilder): StringBuilder = {
    trimEnd(sb, ',').append('}')
    sb
  }

  // Convert an Any item to correct string form for JSON -- INCOMPLETE, adding as needed
  // Picks up some special cases (e.g. AtomicInteger) and outputs as integer (no quotes)
  protected def jsonToString(item: Any): String = u.jsonToString(item)
  // Determine if this value is empty: zero for numerics, blank/empty string, null
  protected def jsonIsEmpty(item: Any): Boolean = u.jsonIsEmpty(item)

  // Create simple JSON item - "label" : 123,  - use trimEnd to drop comma if needed
  protected def jsonItem(label: String, value: Any, suppressEmpty: Boolean): String =
    u.jsonItem(label, value, suppressEmpty)
  // if (suppressEmpty && jsonIsEmpty(value)) "" else quoted(label) + ':' + jsonToString(value) + ','
  // Create simple JSON item & append to StringBuilder
  protected def jsonItem(sb: StringBuilder, label: String, value: Any, suppressEmpty: Boolean): StringBuilder =
    sb.append(jsonItem(label, value, suppressEmpty))

  // Create simple JSON item - "label" : 123, - but SUPPRESS if empty (0, null, empty string)
  protected def jsonItem(label: String, value: Any): String = jsonItem(label, value, true)

  // Create simple JSON item and append to StringBuilder, suppress if value is empty
  protected def jsonItem(sb: StringBuilder, label: String, value: Any): StringBuilder = jsonItem(sb, label, value, true)

  // Convenience methods to set a MAX into a variable
  protected def setMax(newVal: Long, to: AtomicLong): Unit = if (newVal > to.get) to.set(newVal)
  protected def setMax(newVal: Int, to: AtomicInteger): Unit = if (newVal > to.get) to.set(newVal)

  // Work variables
  protected var inProbeStart = false

  protected def quoted(s: String): String = u.jsonQuoted(s)

  // JSON parse a path-type string - e.g.  aaa/bbb/ccc
  protected def parsed(itemName: String, path: String): String =
    u.jsonPathParsed(itemName, path, delimiter = "/", prefix = "path_")

  /**
   * Parse a ZonedDateTime into path format - e.g. 2013-12-02T16:40:13.198-05:00[America/New_York] to
   * 2013-12-02/16:40:13/198/-05:00/America/New_York
   */
  def parsed(zDate: ZonedDateTime): String = {
    if (zDate == null) ""
    else {
      val yyyy = zDate.getYear
      val MM = zDate.getMonthValue
      val dd = zDate.getDayOfMonth

      val HH = zDate.getHour
      val mm = zDate.getMinute
      val ss = zDate.getSecond

      val mmm = zDate.getNano / 1000000

      val off = zDate.getOffset
      val zone = zDate.getZone.toString.replace('/', '-')

      val rslt =
        "" + yyyy + '/' + MM + '/' + dd + '/' + HH + '/' + mm + '/' + ss + '/' + mmm + '/' + (off.toString) + '/' + zone
      rslt
    }
  }
}

/**
 * *********************************************************************************************
 */
/* Some pre-packaged AppStats with sets of Traits.                                              */
/*                                                                                              */
/* You can instantiate one of these as needed or add with ... to mixin additional traits.       */
/**
 * *********************************************************************************************
 */
/**
 * DISABLED AppStats. Provide the environment, AppID, etc just for the record If the environment, etc is not important
 * use the common AppStats.disabledInstance to reduce memory footprint.
 */
class AppStatsDisabled(
    thisRun: AppStatsRun,
    component: String,
    plusProperties: Map[String, String] = null,
    exceptionCapture: Int = 0,
    register: Boolean = true,
    deep: Boolean = false,
    suppressEmpty: Boolean = true,
    partOf: Long = 0)
    extends AppStats(
      thisRun,
      component,
      disabled = true,
      exceptionCapture = exceptionCapture,
      plusProperties = plusProperties,
      register = register,
      deep = deep,
      suppressEmpty = suppressEmpty,
      partOf = partOf
    ) {}

/** Basic AppStats - mixes in the AppStatsBasics trait */
class AppStatsBasic(
    thisRun: AppStatsRun,
    component: String,
    plusProperties: Map[String, String] = null,
    exceptionCapture: Int = 0,
    register: Boolean = true,
    deep: Boolean = false,
    suppressEmpty: Boolean = true,
    disabled: Boolean = false,
    partOf: Long = 0)
    extends AppStats(
      thisRun,
      component,
      exceptionCapture = exceptionCapture,
      plusProperties = plusProperties,
      register = register,
      deep = deep,
      suppressEmpty = suppressEmpty,
      disabled = disabled,
      partOf = partOf
    )
    with AppStatsBasics {}

/**
 * The "standard" set of AppStats includes all of the "lightweight" traits (Basics, Environment, Memory, ThreadBasic)
 */
class AppStatsStandard(
    thisRun: AppStatsRun,
    component: String,
    plusProperties: Map[String, String] = null,
    exceptionCapture: Int = 0,
    register: Boolean = true,
    deep: Boolean = false,
    suppressEmpty: Boolean = true,
    disabled: Boolean = false,
    partOf: Long = 0)
    extends AppStats(
      thisRun,
      component,
      exceptionCapture = exceptionCapture,
      plusProperties = plusProperties,
      register = register,
      deep = deep,
      suppressEmpty = suppressEmpty,
      disabled = disabled,
      partOf = partOf
    )
    with AppStatsBasics
    with AppStatsEnvironment
    with AppStatsMemory
    with AppStatsThreadBasic {}

/**
 * *********************************************************************************************
 */
/* Traits may be defined in order to add additional statistics buckets to the base set          */
/*                                                                                              */
/* WARNING: It is usually required that SUPER be called in any override method!                 */
/*                                                                                              */
/* Any trait MUST override the following:                                                       */
/*  - fullList                                                                                  */
/*                                                                                              */
/* A Trait MAY override these:                                                                  */
/*  - doTraitProbe - if overridden, usually you should NOT override probeStart or probeEnd      */
/*  - jsonSegment - to add a trait-specific piece to the JSON                                   */
/*                                                                                              */
/* See any of the following pre-defined Traits for the model of what each should do.            */
/**
 * *********************************************************************************************
 */
/**
 * TODO (OPTIMUS-0000): all instantiations of AppStats MUST have AT LEAST one Trait mixed in
 *
 * NO IDEA why this is necessary, but if you do this:
 *
 * <pre> val xxx = new AppStats(0, "a", "b", "c", "d") with ApplicationBasics
 *
 * val another = Class.loadFor(xxx.getClass.getName).newInstance </pre>
 *
 * will WORK. But if you do:
 *
 * <pre> val xxx = new AppStats(0, "a", "b", "c", "d") // with NO traits added
 *
 * val another = Class.loadFor(xxx.getClass.getName).newInstance </pre>
 *
 * then the 'newInstance' method throws an exception.
 *
 * Hopefully, someone with deeper knowledge of Scala can determine why this happens and remove this note.
 *
 * TODO (OPTIMUS-0000): should really use the new Reflection logic in Scala 2.10, but this works for now!
 */
/** Marker trait. If present, then all PUBLIC methods will be SYNCHRONIZED */
trait AppStatsSynchronized {}

/** Trait for basic start/stop, wall-clock, CPU & memory */
trait AppStatsBasics extends AppStats {
  val ttlWallTime = new AtomicLong // Accumulated wall-clock time
  val nowCPU = new AtomicLong
  val nowCPUUser = new AtomicLong
  val ttlCPU = new AtomicLong // Accumulated total CPU time
  val ttlCPUUser = new AtomicLong // Accumulated user-space CPU time
  val ttlCPUThread = new AtomicLong
  val ttlCPUUserThread = new AtomicLong
  val nowCompileTime = new AtomicLong
  val ttlCompileTime = new AtomicLong
  val maxJVMMemory = new AtomicLong // Memory used (peak) within the JVM (bytes) - simplified
  val cntInvocations = new AtomicLong // Number of times 'component' was invoked, if applicable

  private lazy val localList: List[(AnyRef, String, Boolean, String, String)] =
    List(
      (ttlWallTime, "ttlWallTime", false, "Basics", "Wall clock time between all probeStart...probeEnd pairs (nanos)"),
      (nowCPU, "nowCPU", false, "Basics", "Total CPU time used by the JVM to this point (nanos)"),
      (
        nowCPUUser,
        "nowCPUUser",
        false,
        "Basics",
        "Total User CPU time used by the JVM to this point(nanos)  (0 if using low cost mechanism for nowCPU)"),
      (
        ttlCPU,
        "ttlCPU",
        false,
        "Basics",
        "Accumulated process CPU time between all probeStart...probeEnd pair (nanos)"),
      (
        ttlCPUUser,
        "ttlCPUUser",
        false,
        "Basics",
        "Accumulated process USER CPU time between all probeStart...probeEnd pair (nanos) (0 if using low cost mechanism)"),
      (
        ttlCPUThread,
        "ttlCPUThread",
        false,
        "Basics",
        "Accumulated CPU time on this Thread between all probeStart...probeEnd pairs (on same Thread) (if enabled)"),
      (
        ttlCPUUserThread,
        "ttlCPUUserThread",
        false,
        "Basics",
        "Accumulated USER CPU time between all probeStart...probeEnd paris (on same Thread) (nanos)"),
      (nowCompileTime, "nowCompileTime", false, "Basics", "Total amount of compilation (JIT) time (millis)"),
      (
        ttlCompileTime,
        "ttlCompileTime",
        false,
        "Basics",
        "Accumulated compile (JIT) time between all probeStart...probeEnd pairs (millis)"),
      (
        maxJVMMemory,
        "maxJVMMemory",
        false,
        "Basics",
        "Maximum memory use detected within the JVM (simplistic total mem - free mem)"),
      (cntInvocations, "cntInvocations", false, "Basics", "Caller-recorded invocation count")
    )

  override def fullList = super.fullList ::: localList

  /** Bump and return the number of invocations */
  def bumpInvocations(comp: String) = cntInvocations.incrementAndGet

  // TODO (OPTIMUS-0000): there is no standard way (that I could find as of Oct 2013, Java 1.7, Scala 2.10) to get the overall CPU
  //         time of the entire application. If the management bean is the Sun version, use it (and get zero user-time)
  //         If not, sum all the Threads - but this will not pick up any time from a thread that started and ended
  //         existence between our probes!
  override protected def doTraitProbe(isStart: Boolean, lastProbe: Boolean, target: AnyRef*) = {
    super.doTraitProbe(isStart, lastProbe, target: _*)
    checkJVMRam
    val tmpCompile =
      if (AppStatsControl.compilationTimeMonitoringSupported) AppStatsControl.compileBean.getTotalCompilationTime
      else 0L
    val thrd = AppStatsControl.threadBean
    val ids = thrd.getAllThreadIds
    var tmpCPUTotal = 0L
    var tmpCPUUser = 0L
    if (!deep && AppStatsControl.systemAsSun != null)
      tmpCPUTotal = AppStatsControl.systemAsSun.getProcessCpuTime
    else if (AppStatsControl.threadThreadCPUTimeSupported) for (id <- ids) {
      val cpu = thrd.getThreadCpuTime(id) // -1 if not supported or 'id' no longer alive
      val usr = thrd.getThreadUserTime(id)
      if (cpu > 0) tmpCPUTotal += cpu
      if (usr > 0) tmpCPUUser += usr
    }
    nowCPU.set(tmpCPUTotal)
    nowCPUUser.set(tmpCPUUser)
    nowCompileTime.set(tmpCompile)
    if (isStart) {
      startWall = System.nanoTime
      startCPUUser = tmpCPUUser
      startCPUTotal = tmpCPUTotal
      startCompile = tmpCompile
      if (AppStatsControl.threadThreadCPUTimeSupported) {
        startThreadID = Thread.currentThread.threadId
        startCPUThread = thrd.getCurrentThreadCpuTime
        startCPUUserThread = thrd.getCurrentThreadUserTime
      }
    } else {
      ttlWallTime.addAndGet(System.nanoTime - startWall)
      ttlCPUUser.addAndGet(tmpCPUUser - startCPUUser)
      ttlCPU.addAndGet(tmpCPUTotal - startCPUTotal)
      ttlCompileTime.addAndGet(tmpCompile - startCompile)
      if (AppStatsControl.threadThreadCPUTimeSupported && startThreadID == Thread.currentThread.threadId) {
        startThreadID = Thread.currentThread.threadId
        ttlCPUThread.addAndGet(thrd.getCurrentThreadCpuTime - startCPUThread)
        ttlCPUUserThread.addAndGet(thrd.getCurrentThreadUserTime - startCPUUserThread)
        startThreadID = 0
      }
    }
  }
  private def checkJVMRam = {
    val rt = java.lang.Runtime.getRuntime
    val sz = rt.totalMemory - rt.freeMemory
    if (sz > maxJVMMemory.get) maxJVMMemory.set(sz)
  }
  private var startWall = 0L
  private var startCPUUser = 0L
  private var startCPUTotal = 0L
  private var startThreadID = 0L
  private var startCPUThread = 0L
  private var startCPUUserThread = 0L
  private var startCompile = 0L
}

/** A trait to display some basic Properties - immutable values */
trait AppStatsEnvironment extends AppStats {
  val valOS = someOrNA(AppStatsControl.sysProperties.get("os.name")) + '/' +
    someOrNA(AppStatsControl.sysProperties.get("os.version")) + '/' +
    someOrNA(AppStatsControl.sysProperties.get("os.arch"))

  val valScalaVersion = scala.util.Properties.versionNumberString
  val valJavaVersion = someOrNA(AppStatsControl.sysProperties.get("java.version"))
  val valJVMVersion = someOrNA(AppStatsControl.sysProperties.get("java.vm.version"))

  private lazy val localList =
    List(
      (valOS, "valOS", true, "Environment", "Current OS information"),
      (valScalaVersion, "valScala", false, "Environment", "Scala Version"),
      (valJavaVersion, "valJava", false, "Environment", "Java Version"),
      (valJVMVersion, "valJVM", false, "Environment", "JVM Version")
    )

  override def fullList = super.fullList ::: localList

  def someOrNA(value: Option[String]): String = value match {
    case Some(str) => str
    case _         => "n/a"
  }
}

/** A trait to capture more detailed Memory statistics */
trait AppStatsMemory extends AppStats {
  val nowGCs = new AtomicInteger
  val nowGCRuns = new AtomicLong
  val nowGCTime = new AtomicLong
  val ttlGCRuns = new AtomicLong
  val ttlGCTime = new AtomicLong
  val maxDeadObjects = new AtomicLong // Objects awaiting finalization
  val maxHeapMemory = new AtomicLong
  val maxNonHeapMemory = new AtomicLong
  val maxSystemMemory = new AtomicLong

  private lazy val localList =
    List(
      (nowGCs, "nowCntGCs", false, "Memory", "Number of Garbage Collectors present within the JVM"),
      (
        nowGCRuns,
        "nowGCRuns",
        false,
        "Memory",
        "How many times the GC has executed (since initialization of this JVM)"),
      (
        nowGCTime,
        "nowGCTime",
        false,
        "Memory",
        "Total CPU time used by the GC since initialization of this JVM (millis)"),
      (
        ttlGCRuns,
        "ttlGCRuns",
        false,
        "Memory",
        "How many times the GC has executed between probeStart...probeEnd sequences"),
      (ttlGCTime, "ttlGCTime", false, "Memory", "CPU used by the GC between probeStart...probeEnd sequences (millis)"),
      (
        maxDeadObjects,
        "maxDeadObjects",
        false,
        "Memory",
        "Maximum observed count of dead objects (estimate from the JVM)"),
      (maxHeapMemory, "maxHeapMemory", false, "Memory", "Maximum observed amount of Heap memory allocated"),
      (maxNonHeapMemory, "maxNonHeapMemory", false, "Memory", "Maximum observed amount of non-heap memory allocated"),
      (
        "NYI" /* maxSystemMemory */,
        "maxSystemMemory",
        false,
        "Memory",
        "Maximum observed amount of System memory used (as reported by the OS)")
    )

  override def fullList = super.fullList ::: localList

  private var cntStart = 0L
  private var timeStart = 0L

  override protected def doTraitProbe(isStart: Boolean, lastProbe: Boolean, target: AnyRef*) = {
    super.doTraitProbe(isStart, lastProbe, target: _*)
    val sz = AppStatsControl.gcBeans.size
    nowGCs.set(sz)
    var cnt = 0L
    var time = 0L
    for (i <- 0 until sz) {
      val gc = AppStatsControl.gcBeans.get(i)
      cnt += gc.getCollectionCount
      time += gc.getCollectionTime
    }
    nowGCRuns.set(cnt)
    nowGCTime.set(time)
    setMax(SystemFinalization.getObjectPendingFinalizationCount(), maxDeadObjects)
    setMax(AppStatsControl.memoryBean.getHeapMemoryUsage.getUsed, maxHeapMemory)
    setMax(AppStatsControl.memoryBean.getNonHeapMemoryUsage.getUsed, maxNonHeapMemory)
    if (isStart) {
      cntStart = cnt
      timeStart = time
    } else {
      ttlGCRuns.addAndGet(cnt - cntStart)
      ttlGCTime.addAndGet(time - timeStart)
    }
  }
}

/**
 * A trait for algorithms which must iterate to find a solution.
 *
 * This just stores the count of iterations. Usually it will be combined with other traits (e.g. ApplicationBasics) in
 * order to record the CPU time, memory, etc.
 */
trait AppStatsIterations extends AppStats {
  val ttlIterations = new AtomicLong

  private lazy val localList = List((ttlIterations, "ttlIterations", false, "Iterations", "No documentation"))

  override def fullList = super.fullList ::: localList

  override protected def doTraitProbe(isStart: Boolean, lastProbe: Boolean, target: AnyRef*) = {
    super.doTraitProbe(isStart, lastProbe, target: _*)
  }

  /** Increase the accumulated count of iterations. Return the new value. Thread-safe */
  def bumpIterations(numIterations: Long): Long = ttlIterations.addAndGet(numIterations)

  /** Increase the count of iterations by 1. Thread-safe */
  def bumpIterations(): Long = bumpIterations(1)
}

/**
 * A trait to capture summary Thread information (peak, number started, etc).
 *
 * This is relatively inexpensive to capture. For per-thread detail information see ApplicationThreadDetail.
 *
 * Note that ttlThreads may not be valid if this instance is updated by multiple threads.
 */
trait AppStatsThreadBasic extends AppStats {
  val nowThreads = new AtomicInteger
  val nowDaemons = new AtomicInteger
  val nowPeak = new AtomicInteger
  val nowStarted = new AtomicLong
  val ttlThreads = new AtomicLong

  private lazy val localList =
    List(
      (nowThreads, "nowThreads", false, "Threads", "Number of threads currently alive (daemon + non-daemon)"),
      (nowDaemons, "nowDaemons", false, "Threads", "Number of daemon threads currently alive"),
      (nowPeak, "nowThreadsPeak", false, "Threads", "Peak number of live threads since start of this JVM"),
      (nowStarted, "nowThreadsStarted", false, "Threads", "Total number of Threads started since start of this JVM"),
      (
        ttlThreads,
        "ttlThreads",
        false,
        "Threads",
        "Accumulated number of Threads detected during probeStart..probeEnd sequences")
    )

  override def fullList = super.fullList ::: localList

  private var cntStart = 0L

  override protected def doTraitProbe(isStart: Boolean, lastProbe: Boolean, target: AnyRef*) = {
    super.doTraitProbe(isStart, lastProbe, target: _*)
    val bean = AppStatsControl.threadBean
    val cnt = bean.getThreadCount
    val cntStarted = bean.getTotalStartedThreadCount
    nowThreads.set(cnt)
    nowDaemons.set(bean.getDaemonThreadCount)
    nowPeak.set(bean.getPeakThreadCount)
    nowStarted.set(cntStarted)
    if (isStart) cntStart = cntStarted
    else ttlThreads.addAndGet(cntStarted - cntStart)
  }
}

/**
 * A trait for per-Thread information. If 'deep' is True this includes some additional information which is more
 * expensive to capture.
 *
 * If 'deep' is True, some options should be SUPPORTED and ENABLED within the JVM. Specifically, see
 * java.lang.management.ThreadMXBean. isThreadCpuTimeSupported & isObjectMonitorUsageSupported must be true (and
 * enabled) in order to gather the information. If these are not supported/enabled, then instead of numeric values in
 * the final JSON, a text note will be included with the status.
 *
 * This trait does not provide a standard list of variables. Instead, a special JSON segment is constructed and returned
 * of the form: <pre>
 *
 * "threadInfo": [ {thread1info}, {thread2info}, ..., {threadNinfo} ] </pre> NOTICE: This operates off Thread IDs as
 * assigned by the JVM. In theory, these IDs may be re-used during the lifetime of the JVM. This would lead us to
 * accumulate some invalid statistics since data from more than one thread might be co-mingled.
 *
 * As a practical matter, this should never happen. Existing JVMs assign IDs starting from 1 as a Long, so the number of
 * IDs available is extremely large.
 */
trait AppStatsThreadDetail extends AppStats {
  val thrdMap = scala.collection.mutable.Map.empty[Long, perThreadInfo]

  // Placeholder no-op override since no local list exists for this Trait
  override def fullList = super.fullList

  override protected def doTraitProbe(isStart: Boolean, lastProbe: Boolean, target: AnyRef*) = {
    super.doTraitProbe(isStart, lastProbe, target: _*)
    perThread(isStart, lastProbe)
  }
  override def resetImpl(): Unit = {
    super.reset
    thrdMap.clear()
  }

  /** Construct a special JSON segment with per-thread information */
  override def jsonSegment(optionalData: AnyRef*): String = {
    super.jsonSegment(optionalData: _*) + perJSON + ','
  }

  /**
   * ***********************************************************************************
   */
  /* TODO (OPTIMUS-0000): currently, the ThreadInfo is just grabbed and the data included in the JSON */
  /*        output. It would be possible to get a start/end ThreadInfo for each set     */
  /*        of probeStart...probeEnd calls and accumulate some stats within the probes  */
  /*                                                                                    */
  /* TODO (OPTIMUS-0000): for really deep info, can get:                                              */
  /*    - stack information                                                             */
  /*    - locking info                                                                  */
  /*    - ... etc                                                                       */
  /*                                                                                    */
  /*    This can get expensive to obtain, and may not be supported by the JVM           */
  /**
   * ***********************************************************************************
   */
  var lastNowMillis = 0L

  private def perThread(isStart: Boolean, lastProbe: Boolean): Unit = {
    val bean = AppStatsControl.threadBean
    val ids = bean.getAllThreadIds
    val info = bean.getThreadInfo(ids)
    val nowMillis = System.currentTimeMillis

    lastNowMillis = nowMillis

    for (thrd <- info) {
      val map = thrdMap.get(thrd.getThreadId)
      map match {
        case Some(perT) => {
          perT.lastNoticed = nowMillis
          perT.thrdInfo = thrd
        }
        case None => {
          val perT = new perThreadInfo(thrd.getThreadId, nowMillis)
          perT.thrdInfo = thrd
          thrdMap(thrd.getThreadId) = perT
        }
      }
    }
  }

  // Build a JSON segment of the form:
  //   "threadInfo": [ {"ID":id, "CPU": ..}, ...]
  private def perJSON: String = {
    val sb = new StringBuilder(2048)
    // The following are declared here since in theory they could be enabled/disabled
    // several times during the lifetime of the JVM
    val contentionEnabled =
      AppStatsControl.threadContentionMonitoringSupported && AppStatsControl.threadBean.isThreadContentionMonitoringEnabled
    val cpuEnabled = AppStatsControl.threadThreadCPUTimeSupported && AppStatsControl.threadBean.isThreadCpuTimeEnabled

    sb.append(quoted("threadInfo")).append(':').append('[')
    thrdMap.foreach(info => jsonInfo(sb, info._2, contentionEnabled, cpuEnabled))
    trimEnd(sb, ',').append(']')
    sb.toString
  }
  // Append one array entry for this info. If 'deep' add some additional info
  // Some methods if not supported/enabled will throw exception if we try to access, so must check first
  private def jsonInfo(
      sb: StringBuilder,
      info: perThreadInfo,
      contentionEnabled: Boolean,
      cpuEnabled: Boolean): StringBuilder = {
    sb.append('{')
    jsonItem(sb, "ID", info.id)
    jsonItem(sb, "PID", "NYI") // TODO (OPTIMUS-0000): get the OS process ID for this Thread
    jsonItem(sb, "Name", info.thrdInfo.getThreadName)
    jsonItem(sb, "FirstNoticed", AppStatsControl.formatTimeWMillis.format(new java.util.Date(info.firstNoticed)))
    jsonItem(sb, "LastNoticed", AppStatsControl.formatTimeWMillis.format(new java.util.Date(info.lastNoticed)))
    if (info.lastNoticed != lastNowMillis)
      jsonItem(sb, "IsGONE", true)
    jsonItem(sb, "State", info.thrdInfo.getThreadState)
    jsonItem(sb, "InJNI", info.thrdInfo.isInNative)
    jsonItem(sb, "BlockedCount", info.thrdInfo.getBlockedCount)
    jsonItem(sb, "WaitedCount", info.thrdInfo.getWaitedCount)
    jsonItem(sb, "LockInfo", info.thrdInfo.getLockName)
    jsonItem(sb, "LockOwner", info.thrdInfo.getLockOwnerName)
    if (deep) {
      if (jsonItemIf(sb, "CPUTime", AppStatsControl.threadThreadCPUTimeSupported, cpuEnabled))
        jsonItem(sb, "CPUTime", AppStatsControl.threadBean.getThreadCpuTime(info.id))
      if (jsonItemIf(sb, "CPUTimeUser", AppStatsControl.threadThreadCPUTimeSupported, cpuEnabled))
        jsonItem(sb, "CPUTimeUser", AppStatsControl.threadBean.getThreadUserTime(info.id))
      if (jsonItemIf(sb, "BlockedTime", AppStatsControl.threadContentionMonitoringSupported, contentionEnabled))
        jsonItem(sb, "BlockedTime", info.thrdInfo.getBlockedTime)
      if (jsonItemIf(sb, "WaitedTime", AppStatsControl.threadContentionMonitoringSupported, contentionEnabled))
        jsonItem(sb, "WaitedTime", info.thrdInfo.getWaitedTime)
    }
    trimEnd(sb, ',')
    sb.append("},")
  }
  // Determine if item is Supported and Enabled - if NOT put out JSON to say so & return False
  private def jsonItemIf(sb: StringBuilder, label: String, isSupported: Boolean, isEnabled: Boolean): Boolean = {
    if (isSupported && isEnabled) true
    else {
      if (isSupported) jsonItem(sb, label, "Disabled")
      else jsonItem(sb, label, "NotSupported")
      false
    }
  }
  class perThreadInfo(val id: Long, val firstNoticed: Long) {
    var lastNoticed = firstNoticed
    var thrdInfo: java.lang.management.ThreadInfo = _
  }
}

/**
 * *********************************************************************************************
 */
/* Trait for a class which actually SUPPORTs the gathering of statistics                        */
/**
 * *********************************************************************************************
 */
/**
 * These parameters define the statistics which should be collected.
 *
 * 'component' in AppStatsControl is key - it is interpreted by each sub-system to determine which statistics should be
 * gathered.
 *
 * The COMPONENT is intended to be specific to a particular section of software. For example, the DAL might recognize.
 * <ul> <li>DAL/total - accumulate total statistics for all calls to the DAL <li>DAL/query - accumulate statistics for
 * all Query operations <li>DAL/insert - ditto for Inserts <li>DAL/update - ditto for Updates <li>DAL/delete - ditto for
 * Deletes <li>DAL/total/[FQN of an Entity] - accumulate statistics for a particular Entity <li>DAL/total/[FQN of an
 * Entity]/[name of a node] - accumulate for a Node within an Entity <li>DAL/query/[FQN ....] - etc for
 * query/insert/update/delete </ul>
 *
 * The intention is to provide an extensible framework. Initially, the DAL might recognize nothing but the "DAL/total"
 * component (which would also be very lightweight if it was decided to enable statistics gathering during production).
 *
 * Over time, if warranted, then the DAL can be enhanced to recognize additional component definitions.
 *
 * Some conventions may be adopted - e.g. componentname/total is always defined and represents the overall total for
 * componentname
 *
 * Also note that the intent is to allow for the collection of very SPECIFIC information. For example, if there is a
 * problem with the processing of one specific Entity - or even a Node within an Entity - then the collection of
 * statistics for that one item could be activated. This might even be sufficiently lightweight to be activated in
 * production if troubleshooting a problem.
 *
 * TODO (OPTIMUS-0000): how to handle definition of the same appID/location/component more than once? Throw an exception (would really
 * hate to do that if we're in production), silently ignore the call, silently replace the existing definition, or log a
 * message and either ignore or replace. ???
 */
trait AppStatsSource {

  /** Return TRUE if this sub-system recognizes and supports the given 'component' definition */
  def statsSupports(component: String): Boolean

  /**
   * Define a set of statistics which are desired. The sub-system may setup stats for 0..N of these.
   */
  def statsDefine(control: AppStatsControl): List[AppStats]

  /**
   * Reset all statistics for this sub-system to initial state
   */
  def statsReset(): Unit
}
