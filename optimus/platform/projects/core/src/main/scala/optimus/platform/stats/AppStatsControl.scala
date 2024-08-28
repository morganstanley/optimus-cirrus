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

import optimus.graph.Settings

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer
import scala.sys.SystemProperties
import scala.collection.immutable.HashMap
import java.net.InetAddress
import java.time.ZonedDateTime
import optimus.utils.datetime.ZonedDateTimeOps

import java.util.concurrent.ConcurrentHashMap
import java.lang.management.ManagementFactory

import scala.jdk.CollectionConverters._

/**
 * Enumeration of some of the common types of AppStats
 */
object AppStatsType extends Enumeration {
  val Disabled = Value
  val Basic = Value
  val Standard = Value
}

object AppStatsControl {
  val u = Utils
  val formatTimeWMillis = new java.text.SimpleDateFormat("HH:mm:ss.SSS")
  val sysProperties = new SystemProperties

  /**
   * Generate unique IDs within a single Host (location) Used for both AppStats IDs and also some other spots where
   * unique needed
   */
  private val nextUnique = new AtomicLong

  /** Component string which matches ANYTHING */
  val sMatchAll = "*/*"

  def getUnique(): Long = nextUnique.incrementAndGet

  /**
   * String that can be used as a Separator when appending strings.
   *
   * Intent is to have a string with is safe for RegEx (when headers/data is 'split') and also extremely unlikely to
   * actually appear within the data itself.
   *
   * MUST be different from the one used in JSONMerge to avoid ambiguity.
   */
  val Separator = "#@~"

  // Needed by the Profiler trait to match JSONMerge
  val AppStatsArrayMarker = "_$_"

  /**
   * *************************************************************************
   */
  /* Instantiate an AppStatsControl from command-line system properties       */
  /*                                                                          */
  /* All of these should be of the form -Dxxx=yyy on the command line. This   */
  /* will set the system property 'xxx' to have the value 'yyy'               */
  /*                                                                          */
  /* If there is NO stats.app.id given then NO AppStats at all will be done.  */
  /**
   * *************************************************************************
   */
  val propAppID = "optimus.stats.app.id"
  val propEnv = "optimus.stats.env"
  val propRunAt = "optimus.stats.run.at"
  val propProperties = "optimus.stats.properties" // Format: key1/value1;key2/value2;...
  val propComponents = "optimus.stats.components" // Format: comp1@type1;comp2@type2;... where typeN may be omitted
  //   Note: each 'compN' itself is usually a path: pathA/pathB/...

  val dfltEnv = "NoEnvSpecified"
  var properties = new HashMap[String, String]
  val dfltComponents: List[(String, AppStatsType.Value)] = List((sMatchAll, AppStatsType.Basic)) // Matches anything!

  def controlFromParams: AppStatsControl = {
    var appID = sysProperties.getOrElse(propAppID, "")
    val env = sysProperties.getOrElse(propEnv, dfltEnv)
    val runAtStr = sysProperties.getOrElse(propRunAt, "")
    val runAt =
      if (runAtStr.isEmpty) ZonedDateTime.now
      else
        try {
          ZonedDateTimeOps.parseTreatingOffsetAndZoneIdLikeJSR310(runAtStr)
        } catch {
          case ex: Exception => {
            appID += " - RunAt parse error"
            ZonedDateTime.now
          }
        }
    //
    val propsStr = sysProperties.getOrElse(propProperties, "")
    val compsStr = sysProperties.getOrElse(propComponents, "")

    // Set up any properties
    if (propsStr.nonEmpty) {
      val props = propsStr.split(";")
      for (prop <- props) {
        val pcs = prop.split("/")
        if (pcs.length > 0) {
          val key = pcs(0)
          val value = if (pcs.length > 1) pcs(1) else ""
          properties += key -> value
        }
      }
    }
    // Set up the component list - may specify a specific AppStatsType name or default to Standard
    var lst: List[(String, AppStatsType.Value)] = Nil
    if (compsStr.isEmpty) lst = dfltComponents
    else {
      val comps = compsStr.split(";")
      for (c <- comps) {
        val pcs = c.split("@")
        if (pcs.length > 0) {
          val key = pcs(0)
          val stats =
            if (pcs.length == 1) lst ::= (key, AppStatsType.Basic)
            else {
              val name = pcs(1)
              var tp = AppStatsType.Basic
              AppStatsType.values.exists(v =>
                if (v.toString.equalsIgnoreCase(name)) { tp = v; true }
                else false)
              lst ::= (key, tp)
            }
        }
      }
    }
    // IF no APP ID was specified, setup an EMPTY control! Will do NO statistics
    if (appID.isEmpty)
      new AppStatsControl("NoEnv", "NoAppID", runAt, properties, Nil)
    else
      new AppStatsControl(env, appID, runAt, properties, lst)
  }

  /**
   * *************************************************************************
   */
  /* References to system Beans needed for many of the pre-defined Traits     */
  /**
   * *************************************************************************
   */
  val systemBean = ManagementFactory.getOperatingSystemMXBean
  val compileBean = ManagementFactory.getCompilationMXBean
  val threadBean = ManagementFactory.getThreadMXBean
  val memoryBean = ManagementFactory.getMemoryMXBean
  val gcBeans = ManagementFactory.getGarbageCollectorMXBeans

  /** Cheating - the Sun implementation of the OS Bean has some extra functionality */
  val systemAsSun =
    if (systemBean.isInstanceOf[com.sun.management.OperatingSystemMXBean])
      systemBean.asInstanceOf[com.sun.management.OperatingSystemMXBean]
    else null

  // Setup some flags to avoid a zillion method calls since these are either on/off at JVM level
  val compilationTimeMonitoringSupported =
    if (compileBean eq null) false else compileBean.isCompilationTimeMonitoringSupported
  val threadObjectMonitorUsageSupported = threadBean.isObjectMonitorUsageSupported
  val threadSynchronizerUsageSupported = threadBean.isSynchronizerUsageSupported
  val threadContentionMonitoringSupported = threadBean.isThreadContentionMonitoringSupported
  val threadThreadCPUTimeSupported = threadBean.isThreadCpuTimeSupported && !Settings.useVirtualThreads

  lazy val thisHost =
    try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case _: Throwable => "HostUnknown"
    }
  lazy val thisUser = sysProperties.get("user.name") match {
    case Some(name) => name
    case _          => "UserUnknown"
  }

  lazy val thisLocation = thisHost + '/' + thisUser

  /**
   * Map of all active RUNs which may be executing within this JVM A run-time only unique key is used as the reference.
   */
  private val mapActiveRuns = (new ConcurrentHashMap[String, AppStatsRun]()).asScala

  def register(run: AppStatsRun): Unit = mapActiveRuns.put(run.runUniqueKey, run)

  def getAppRun(key: String): Option[AppStatsRun] = {
    mapActiveRuns.get(key)
  }

  /** If there is only ONE AppStatsRun, return it, else None */
  def getUniqueAppRun(): Option[AppStatsRun] = {
    if (uniqueRun == null) {
      synchronized {
        if (uniqueRun == null) {
          val keys = mapActiveRuns.keySet
          if (keys.size != 1) uniqueRun = None
          else uniqueRun = mapActiveRuns.get(keys.iterator.next())
        }
      }
    }
    uniqueRun
  }
  @volatile // 'null' == not yet computed
  private var uniqueRun: Option[AppStatsRun] = null

  /** Artificial key used at run time only to identify one "run" in the hash map */
  def genUniqueKey(runID: String): String = runID + AppStatsControl.getUnique()

  /** Generic DISABLED instance can be used to avoid instantiating new instances */
  lazy val disabledInstance = {
    val appID = "Generic/DISABLED/AppStats"
    val dummyControl = new AppStatsControl("", appID)
    val dummyRun = new AppStatsRun(dummyControl)
    val disabled = new AppStatsDisabled(dummyRun, "Generic DISABLED AppStats", register = false)
    mapActiveRuns.remove(dummyRun.runUniqueKey)
    disabled
  }

  /**
   * Return an AppStatsEngineTo IFF we have a registered Control and unique AppStatsRun else None
   */
  def getEngineTo(): AppStatsEngineTo =
    if (getControl() != null) {
      val unqAppRun = getUniqueAppRun()
      getUniqueAppRun() match {
        case Some(run) => AppStatsEngineTo(run.runUniqueKey, getControl())
        case None      => null
      }
    } else null

  /**
   * Given an AppStatsEngineTo, register the Control and the ID of the AppStatsRun from the client.
   */
  def register(appTo: AppStatsEngineTo): Unit = {
    if (appTo != null) {
      engTo = appTo
      register(engTo.control)
    }
  }
  private var engTo: AppStatsEngineTo = _

  /**
   * REGISTER the single AppStatsControl which really controls this JVM. Throws exception if there is any attempt to
   * register > 1 AppStatsControl TODO (OPTIMUS-0000): what do we do if the Driver engine is running multiple threads that are really
   * totally different and independent processes!!!
   */
  def register(appCtl: AppStatsControl): Unit =
    if (appCtl == null) {
      // no-op if given a NULL
    } else if (theAppControl == null || (theAppControl eq appCtl))
      theAppControl = appCtl
    else
      throw new IllegalStateException("AppStats: Attempt to Register a 2nd AppstatsControl")

  def getControl(): AppStatsControl = theAppControl

  private var theAppControl: AppStatsControl = _

  /**
   * IFF we are remote, and have registered an AppStatsEngineTo, then create an AppStatsEngineFrom - else throws
   * IllegalStateException
   */
  def getEngineFrom(): AppStatsEngineFrom =
    if (engTo == null) throw new IllegalStateException("getEngineFrom called but no AppStatsEngineTo ever provided")
    else {
      val run = AppStatsControl.getUniqueAppRun()
      val json = run match {
        case Some(appRun) => appRun.jsonAll(endProbes = true, includeDisabled = false)
        case None         => Nil
      }
      AppStatsEngineFrom(engTo.keyForRunTime, engTo.createdAt, json)
    }

  /**
   * Convenience - convert from simple component list to tuples with AppStatsStandard (or other) specified
   */
  def components(
      comps: Iterable[String],
      appType: AppStatsType.Value = AppStatsType.Standard): List[(String, AppStatsType.Value)] =
    comps.map(str => (str, appType)).toList

  /**
   * Return given instance or the system-wide 'disabledInstance' if disabled. Un-registers this instance if disabled.
   * Primarily used to reduce memory footprint if gblENABLED is false or there are a lot of individual disabled
   * instances.
   */
  def ifEnabled(thisOne: AppStats): AppStats =
    if (thisOne == null) disabledInstance
    else if (thisOne.isDisabled) { thisOne.unRegister(true); disabledInstance }
    else thisOne

  /** Empty (null) start method for 'profileRun' construct */
  def onRunStartEmpty(): Unit = {}

  /** Empty (null) finish method for 'profileRun' construct */
  def onRunFinishEmpty(ignored: AppStatsRun): Unit = {}

  /** Empty (null) finish method for 'profileWith' construct */
  def onProfileDoneEmpty(thisStats: AppStats): Unit = {}

  /**
   * Main control structure to control collection of statistics over the entire application. On exit will clean up and
   * remove all instances.
   *
   * See AppStatsControl for definition of most parameters.
   *
   * @param keyFromDriverServer
   *   \- if on a grid engine, passes the key of the associated 'run' on the controller server with which stats from
   *   this run should be accumulated
   * @param fnOnStart
   *   \- Executes on start, defaults to null function
   * @param fnOnFinish
   *   \- should persist all desired AppStats instances to DAL/file/storage/log
   */
  def profileRun[T](
      environment: String,
      appID: String,
      runAt: ZonedDateTime = ZonedDateTime.now,
      appProperties: Map[String, String] = new HashMap[String, String](),
      components: List[(String, AppStatsType.Value)] = List(("n/a", AppStatsType.Disabled)),
      keyFromDriverServer: String = "None, THIS is the Driver Server",
      exceptionCapture: Int = 0,
      register: Boolean = true,
      deep: Boolean = false,
      suppressEmpty: Boolean = true,
      disabled: Boolean = false,
      fnOnStart: => Unit = onRunStartEmpty(),
      fnOnFinish: AppStatsRun => Unit = onRunFinishEmpty)(fn: => T): T = {

    // Instantiate the overall "control" and "run" controller
    val control = new AppStatsControl(
      environment,
      appID,
      runAt,
      appProperties,
      components,
      exceptionCapture,
      register,
      deep,
      suppressEmpty,
      disabled)
    val run = control.createRun()
    fnOnStart
    try {
      fn
    } catch {
      case ex: Throwable => throw ex
    } finally {
      fnOnFinish(run)
      run.clearAllAppStats()
      AppStatsControl.mapActiveRuns.remove(run.runUniqueKey)
    }
  }

  /**
   * Control construct to profile a section of code. On any Exception, will unRegister the AppStats and re-throw the
   * Exception. WARNING: This serializes the execution of 'fn'
   */
  def profileWith[T](
      profiler: AppStats,
      onComplete: AppStats => Unit = onProfileDoneEmpty,
      finish: Boolean = true,
      unRegister: Boolean = false,
      lastProbe: Boolean = false,
      onExceptionUnRegister: Boolean = false)(fn: => T): T = {
    try {
      profiler.probeStart(lastProbe)
      val rslt = fn
      profiler.probeEnd(lastProbe)
      if (finish) profiler.markAsFinished()
      onComplete(profiler)
      if (unRegister) profiler.unRegister()
      rslt
    } catch {
      case ex: Throwable => {
        profiler.recordException(ex)
        if (onExceptionUnRegister) profiler.unRegister()
        throw ex
      }
    }
  }

  /**
   * Returns a formatted table with the documentation for the given AppStats. Includes only the standard variables - not
   * anything produced by creating a special JSON segment.
   *
   * General format:
   *
   * trait1 variable1.1 description1.1 variable1.2 description1.2
   *
   * trait2 variable2.1 description2.1 ...
   *
   * Newlines are included within the string.
   */
  def documentation(stats: AppStats, sorted: Boolean = true, maxLine: Int = 128): String = {
    val lst =
      if (sorted) stats.statsList.sortWith(byTraitByVar(_, _))
      else stats.statsList
    // now form into a List[List[String]] to use the table utilities
    var lastTrait = ""
    var lstOfLists: List[List[String]] = Nil

    lst.foreach {
      case (data, label, subParse, trt, documentation) => {
        val desc = u.strBreakUp(documentation, maxLine)
        var first = true
        desc.foreach { str =>
          {
            val rslt = List(if (trt == lastTrait) "" else trt, if (first) label else "", str); lastTrait = trt;
            first = false; lstOfLists ::= rslt
          }
        }
      }
    }
    u.tblTable(lstOfLists.reverse, 2).foldLeft("")((left, right) => left + right + '\n')
  }

  def lessByLabel(var1: (Any, String, Boolean, String, String), var2: (Any, String, Boolean, String, String)): Boolean =
    var1._2.compareToIgnoreCase(var2._2) < 0

  def byTraitByVar(
      var1: (Any, String, Boolean, String, String),
      var2: (Any, String, Boolean, String, String)): Boolean =
    if (var1._4 == var2._4) var1._2.compareToIgnoreCase(var2._2) < 0
    else var1._4.compareToIgnoreCase(var2._4) < 0
}

/**
 * CONTROL object to specify which AppStats should be actually instantiated and used during a run.
 *
 * The concept is that:
 *
 * -- the CONTROLLER server will read a list of 'components', 'appProperties', and possibly other parameters (e.g. deep,
 * suppressEmpty, etc) from some properties file, the DAL, command line variables, or somewhere else
 *
 * -- the CONTROLLER will also establish the 'runAt' and 'appID' values which will be invariant for all AppStats
 * instantiated in this "run" (will be serialized and sent to any engine servers used during the "run")
 *
 * -- One of these classes will be instantiated and form the control information for the entire "run"
 *
 * -- The key information is the list of COMPONENTs provided. The FIRST element in each entry should be a SUB-SYSTEM,
 * for example:
 *
 * DAL/total -- tells DAL to get overall (total) statistics DAL/entity/XYZ/total -- tells DAL to get overall stats on
 * the 'XYZ' entity IRD/EOD/total -- tells the IRD process to gather stats if this is an EOD run (assumes that IRD knows
 * how to tell this) Graph/total -- tells Graph to get stats on sending nodes to the grid ......
 *
 * The second (AppStatsType) may be used by some sub-systems to indicate how much data to collect - e.g.
 * AppStatsType.Basic vs AppStatsType.Standard
 *
 * Note however that any particular sub-system is free to ignore this parameter and instantiate any AppStats as needed.
 *
 * -- At the start of execution, this control instance will be passed to each sub-system (e.g. Graph, DAL, etc, etc)
 *
 * -- Each sub-system will examine the list of components and determine if they should instantiate any AppStats
 * instances to collect information - or just assign a DISABLED instance (so that all the probeStart....probeEnd methods
 * can remain in place within the code).
 *
 * -- The first node in the path is the only one with any external meaning - beyond that, each sub-system can specify
 * additional nodes (or other syntax) which it supports.
 *
 * -- The Graph process will (hopefully soon) be updated so that when a process is farmed out to a server on the grid,
 * this AppStatsControl will also be serialized and sent to the target process. That process in turn will de-serialize
 * and pass it to all sub-systems.
 *
 * When the grid process completes, if any non-disabled AppStats have been instantiated then the JSON from these will be
 * extracted and returned to the controller server (and persisted from there).
 *
 * @param environment
 *   \- DEV, QA, PROD
 * @param appID
 *   \- name of this overall application run - e.g. IRD/TOUSD/Gamma
 * @param runAt
 *   \- date/time when this overall "run" started
 * @param appProperties
 *   \- properties associated with this run
 * @param components
 *   \- list of which sub-systems should accumulate statistics (see above)
 * @param exceptionCapture
 *   \- 0 == do not capture any exceptions, 1 == just capture exception name & message, >1 == number of stack elements
 *   to capture
 * @param register
 *   \- TRUE==normal registration of all AppStats instances with parent AppStatsRun
 * @param deep
 *   \- if TRUE, some Traits will accumulate stats that are more expensive to obtain
 * @param suppressEmpty
 *   \- if TRUE, do not output JSON items if the value is empty (null, 0, 0.0, or empty/all blank String)
 * @param disabled
 *   \- if TRUE, then ALL accumulation is DISABLED, all public methods become no-ops
 */
class AppStatsControl(
    val environment: String,
    val appID: String,
    val runAt: ZonedDateTime = ZonedDateTime.now,
    val appProperties: Map[String, String] = new HashMap[String, String](),
    val components: List[(String, AppStatsType.Value)] = Nil,
    val exceptionCapture: Int =
      0, // 0 == none, > 0 == how many stack elements to capture (1==just exception name & message)
    val register: Boolean = true,
    val deep: Boolean = false,
    val suppressEmpty: Boolean = true,
    val disabled: Boolean = false)
    extends Serializable {

  /** Determine if * / * (extra spaces needed in comment) exists in the components - so everything passes */
  lazy val containsStarStar: Boolean =
    components.contains((comp: String, tp: AppStatsType.Value) => comp == AppStatsControl.sMatchAll)

  def u = Utils

  /** Return any components for the given subSystem - e.g. componentsFor("DAL") */
  def componentsFor(subsys: String): List[(String, AppStatsType.Value)] = components.filter { case (path, tp) =>
    u.pathNode(path, 0, "/") == subsys
  }

  // NOTE: List of AppStatsRun instances which are children of this Control.
  //       NOT serialized to any grid server since it would be meaningless
  //           and huge!
  @transient private var lstRuns: List[AppStatsRun] = Nil

  def register(thisRun: AppStatsRun): Unit = synchronized { lstRuns ::= thisRun }

  def getRegisteredRuns: List[AppStatsRun] = lstRuns

  /** Create an AppStatsRun, possible override some parameter settings */
  def createRun(
      plusProperties: Map[String, String] = null,
      exceptionCapture: Int = this.exceptionCapture,
      register: Boolean = this.register,
      deep: Boolean = this.deep,
      suppressEmpty: Boolean = this.suppressEmpty,
      disabled: Boolean = this.disabled) = {

    new AppStatsRun(
      this,
      u.mapAddOrOverride(appProperties, plusProperties),
      exceptionCapture,
      register,
      deep,
      suppressEmpty,
      disabled)
  }

  /** Get ALL of the JSON for all Runs associated with this Control */
  def json(endProbes: Boolean = true, includeDisabled: Boolean = false): List[String] = {
    var json: List[String] = Nil
    getRegisteredRuns.foreach(run => json = run.jsonAll(endProbes, includeDisabled) ::: json)
    json
  }
}

/**
 * One RUN of statistics. Note that within one Server Driver there may be multiple logically distinct Runs executing
 * within the same JVM.
 *
 * Hence the AppStatsControl Object contains a Map[key, AppStatsRun] to manage these.
 *
 * This object will hold a list of all AppStats instances which are in play, and from which JSON can be generated as
 * output, for this particular Run.
 *
 * It also holds a list of JSON generated externally. For example, if we send a computation out to a grid server, that
 * server may well run some AppStats collection, extract the JSON when done, and transport this JSON back to the Server
 * Driver. This JSON will then be added to the "external" JSON list.
 */
class AppStatsRun(
    val control: AppStatsControl,
    val appProperties: Map[String, String] = null,
    val exceptionCapture: Int =
      0, // 0 == none, > 0 == how many stack elements to capture (1==just exception name & message)
    val register: Boolean = true,
    val deep: Boolean = false,
    val suppressEmpty: Boolean = true,
    val disabled: Boolean = false) {

  val u = Utils // Convenient access

  /** The pool of AppStats to use for Components defined in the parent AppStatsControl */
  val pool = new AppStatsPool(this)

  /**
   * Unique Key for internal use only. Unique for this JVM only, never persisted. Allows information from grid engines
   * to be associated with this Run.
   */
  lazy val runUniqueKey = AppStatsControl.genUniqueKey(control.appID)

  //
  // CONSTRUCTOR CODE
  //
  AppStatsControl.register(this) // The system-wide list of AppStatsRun
  control.register(this) // register to a particular 'control'

  /**
   * Create an AppStats associated with this AppStatsRun with optional param overrides.
   *
   * NOTE: If gblENABLE is False, will return the (single) default disabled instance
   *
   * Convenience routine for creating the basic types. If a different mix of Traits is needed then do a direct
   * instantiation, e.g.
   *
   * ( new AppStats(....) with Trait1 with Trait2 with ...).ifEnabled
   *
   * or more efficiently:
   *
   * if(gblENABLE) new AppStats(....) with Trait1 with Trait2 with ... else AppStatsControl.disabledInstance
   *
   * to use the single global disabled instance instead of instantiatine a bunch of AppStats which will be disabled
   * anyway.
   *
   * The only difference is if the caller wants to record the appID, time, component, etc of the AppStats and the fact
   * that it was Disabled.
   */
  def createAppStats(
      flavor: AppStatsType.Value = AppStatsType.Standard,
      component: String = "None",
      plusProperties: Map[String, String] = null,
      exceptionCapture: Int = this.exceptionCapture,
      register: Boolean = this.register,
      deep: Boolean = this.deep,
      suppressEmpty: Boolean = this.suppressEmpty,
      disabled: Boolean = this.disabled): AppStats =
    if (gblENABLE) flavor match {
      case AppStatsType.Disabled =>
        new AppStatsDisabled(this, component, plusProperties, exceptionCapture, register, deep, suppressEmpty)
      case AppStatsType.Basic =>
        new AppStatsBasic(this, component, plusProperties, exceptionCapture, register, deep, suppressEmpty, disabled)
      // TODO (OPTIMUS-0000): if NULL or not recognized default to 'Standard'???
      case _ =>
        new AppStatsStandard(this, component, plusProperties, exceptionCapture, register, deep, suppressEmpty, disabled)
    }
    else
      AppStatsControl.disabledInstance

  /**
   * ***************************************************************************
   */
  /*  CRITICAL - global controller to ENABLE/DISABLE ALL AppStats                   */
  /*           - initialized from system property: optimus.stats.enabled        */
  /*           - may be dynamically turned on/off                               */
  /**
   * ***************************************************************************
   */
  private var gblEnable = 0 // 0 == not yet initialized, < 0 == OFF, > 0 == ENABLED

  private def qrySysProp: Boolean = {
    val str = System.getProperty("optimus.stats.enabled")
    str != null && (str == "1" || str.equalsIgnoreCase("true") || str.equalsIgnoreCase("t"))
  }

  /** See if AppStats is ENABLED on a global basis */
  def gblENABLE = {
    if (gblEnable == 0) gblEnable = if (qrySysProp) +1 else -1
    gblEnable > 0
  }

  /**
   * Manually force gblENABLE to TRUE or FALSE WARNING: This may result in anomalous behavior if used indiscriminately.
   * For example, if on some instance a 'probeStart(...)' has been called, then resetENABLE(false), and the
   * 'probeEnd(...)' called.
   *
   * The 'probeEnd' will be ignored since the global ENABLE is false.
   *
   * If resetENABLE(true) is then called at a later time, any future probeStart will be considered an error (unmatched).
   * If a probeEnd(..) arrives it will be processed, but the accumulated statistics will not cover the time period
   * expected.
   *
   * In general, resetENABLE should be called only at known "boundaries" within the application.
   */
  def resetENABLE(to: Boolean): Unit = gblEnable = if (to) +1 else -1

  /** Re-query the "optimus.stats.enabled" system property & reset gblENABLE */
  def reQuerySysProp: Unit = resetENABLE(qrySysProp)

  /**
   * Where we are executing & the context - e.g. datacenter/host/user
   *
   * If a 'run' is distributed across the grid, then different statistics collections will have different values here.
   * On any one given server this should remain constant.
   *
   * DEFAULT: host/user - prefer to be able to set this to region/datacenter/host/user/taskID or similar
   */
  lazy val thisLocation = AppStatsControl.thisLocation

  /**
   * AppStats objects automatically call this 'register' so that there is a system-wide list of all active AppStats
   * objects. This allows the top-level logic to pull the output from everyone if needed.
   *
   * If gblENABLE == FALSE, this becomes a NO-OP
   */
  def register(appStat: AppStats): Unit = {
    if (gblENABLE) synchronized { lstAll ::= appStat }
  }

  /** Un-register an instance from the global list */
  def unRegister(appStat: AppStats): Unit = {
    if (lstAll != Nil) synchronized { if (lstAll.contains(appStat)) lstAll = lstAll diff List(appStat) }
  }
  // List of all Registered AppStats instances
  private var lstAll: List[AppStats] = Nil
  // List of external JSON entries (e.g. returned from an execution on another server)
  private var lstJSONExternal: List[String] = Nil

  /** Retrieve the list of all Registered AppStats instances */
  def allAppStats: List[AppStats] = lstAll

  /** CLEAR the list of ALL Registered AppStats instances -- BE CAREFUL! */
  def clearAllAppStats(): Unit = synchronized {
    lstAll = Nil
    ()
  }

  /** CLEAR the list of ALL external JSON */
  def clearJSONExternal(): Unit = synchronized {
    lstJSONExternal = Nil
    ()
  }

  /** ADD an externalized JSON string to the list */
  def addJSONExternal(json: String): Unit = if (json != null && json.nonEmpty) synchronized(lstJSONExternal ::= json)

  /** GET the list of external JSON entries */
  def allJSONExternal: List[String] = lstJSONExternal

  /**
   * Retrieve a SUBSET of the list of all Registered AppStats - optionally PURGE from allAppStats list MAY return
   * allAppStats if no difference. WARNING: someAppStats(true, true, true, true) will wind up setting allAppStats to Nil
   */
  def someAppStats(
      includeActive: Boolean,
      includeFinished: Boolean,
      includeDisabled: Boolean,
      purge: Boolean): List[AppStats] = {
    val lst =
      if (includeActive == false && includeFinished == false && includeDisabled == false) Nil
      else if (includeActive && includeFinished && includeDisabled) allAppStats
      else
        allAppStats.filter(stat =>
          (includeActive && stat.isActive) || (includeFinished && stat.isFinished) || (includeDisabled && stat.isDisabled))

    if (purge && lst.size != allAppStats.size) purgeAppStats(lst)
    lst
  }

  /**
   * PURGE any entry in the given list from the allAppStats in-memory list
   *
   * NOTE: These will still remain in memory if anyone else holds a reference.
   */
  def purgeAppStats(these: List[AppStats]): Unit = {
    purgeAppStats(these, (stat: AppStats) => true)
  }

  /**
   * PURGE any entry in the given list from the allAppStats in-memory list IFF fn() is true
   *
   * NOTE: These will still remain in memory if anyone else holds a reference.
   */
  def purgeAppStats(these: List[AppStats], fn: AppStats => Boolean): Unit = {
    if (these != null && these != Nil) synchronized {
      lstAll = lstAll.filterNot(stat => these.contains(stat) && fn(stat))
    }
  }

  /** Return a List of FINISHED AppStats, optionally purge them from the allAppStats list */
  def listFinished(purge: Boolean = false): List[AppStats] = someAppStats(false, true, false, purge)

  /** Return a List of all DISABLED AppStats, optionall purge them from the allAppStats list */
  def listDisabled(purge: Boolean = false): List[AppStats] = someAppStats(false, false, true, purge)

  /** Get the count of ALL existing AppStats */
  def cntAllAppStats: Int = synchronized { allAppStats.size }

  /** Get the count of all DISABLED AppStats */
  def cntDisabled: Int = synchronized { allAppStats.count(_.isDisabled) }

  /** Get the count of all FINISHED appStats */
  def cntFinished: Int = synchronized { allAppStats.count(_.isFinished) }

  /** Get the count of all ACTIVE AppStats */
  def cntActive: Int = synchronized { allAppStats.count(_.isActive) }

  /** Get the count of all INACTIVE (finished or disabled) AppStats */
  def cntInActive: Int = synchronized { allAppStats.count(_.notActive) }

  /** Get the count of external JSON entries */
  def cntJSONExternal: Int = synchronized { allJSONExternal.size }

  /** Determine if there are any DISABLED AppStats */
  def anyDisabled: Boolean = synchronized { allAppStats.exists(_.isDisabled) }

  /** Determine if there are any FINISHED AppStats */
  def anyFinished: Boolean = synchronized { allAppStats.exists(_.isFinished) }

  /** Determine if there are any ACTIVE AppStats */
  def anyActive: Boolean = synchronized { allAppStats.exists(_.isActive) }

  private val jsonAnyRefs = new ArrayBuffer[AnyRef]

  /**
   * Provide an object which may be used by one or more Traits in order to generate a jsonSegment.
   *
   * These will be accumulated and passed when the jsonAll method is invoked.
   *
   * NOTE: If gblENABLE==FALSE this becomes a NO-OP
   */
  def jsonAdd(obj: AnyRef): Unit = if (gblENABLE && obj != null) synchronized(jsonAnyRefs.append(obj))

  /** Clear the list of JSON additional objects created by jsonAdd(...) calls */
  def clearJSONAdd: Unit = synchronized { jsonAnyRefs.clear() }

  /**
   * Collect the JSON output for the given List of AppStat instances Will pass all objects previously provided by
   * jsonAdd to json(optionalData: AnyRef*) calls
   */
  def jsonFor(
      lst: List[AppStats],
      endProbes: Boolean = true,
      lastProbe: Boolean = true,
      includeDisabled: Boolean = false): List[String] = {
    lst
      .filter(stat => {
        if (endProbes && stat.isProbing) stat.probeEnd(lastProbe); includeDisabled || !stat.isDisabled
      })
      .map(_.json(jsonAnyRefs.toSeq: _*))
  }

  /**
   * Collect the JSON output from ALL registered Application Statistics objects PLUS any external JSON strings which
   * exist.
   *
   * Will pass all objects previously provided by jsonAdd to json(optionalData: AnyRef*) calls
   */
  def jsonAll(endProbes: Boolean = true, includeDisabled: Boolean = false): List[String] = {
    jsonFor(allAppStats, endProbes, includeDisabled) ::: allJSONExternal
  }

  //
  // TODO (OPTIMUS-0000): provide a STACK whereby an AppStats can add itself when the probeStart(...) method is
  //        called and remove itself (if it is the top of the stack - which it should be) when
  //        the probeEnd(....) method is finished
  //
  //        Will allow a given AppStats to determine any "parent" AppStats which is already
  //        gathering statistics. In such a case, this AppStats may want to record the ID of
  //        the parent since many of it's stats are already included in the parent. For example,
  //        the wall-clock time for this AppStats should be less than or equal the wall-clock
  //        time of the parent.
  //
  //        This parent/child relationship is not used at runtime, but may well be used by
  //        external analysis routines when presenting or analyzing the data.
}

/**
 * ***************************************************************************
 */
/*  Objects intended to be serialized and passed to/from a grid engine.       */
/*                                                                            */
/*  The TO object passes the basic AppStatsControl with all the information   */
/*  about what AppStats should be collected on the grid engine.               */
/*                                                                            */
/*  The FROM object returns any JSON from the grid engine to the Driver       */
/*  controller 'keyForRunTime' allows the FROM object to be linked to the     */
/*  current AppStatsRun to handle any returned JSON information.              */
/**
 * ***************************************************************************
 */
/** Common base class for ...EngineTo & ...EngineFrom */
class AppStatsEngine() extends Serializable {
  val createdAt = System.currentTimeMillis
}
final case class AppStatsEngineTo(keyForRunTime: String, control: AppStatsControl) extends AppStatsEngine

final case class AppStatsEngineFrom(
    keyForRunTime: String, // Copy from ..EngineTo instance
    createdAtEngineTo: Long, // Copy of 'createdAt' from the ..EngineTo instance
    json: List[String])
    extends AppStatsEngine {

  def isEmpty: Boolean = json == null || json.isEmpty
  def nonEmpty: Boolean = !isEmpty
}
