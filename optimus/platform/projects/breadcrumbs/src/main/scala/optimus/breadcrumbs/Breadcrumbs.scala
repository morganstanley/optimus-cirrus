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
package optimus.breadcrumbs

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.{ ArrayList => jArrayList }
import java.util.{ List => jList }
import java.util.{ Map => jMap }
import com.google.common.cache.CacheBuilder
import msjava.base.util.internal.SystemPropertyUtils
import msjava.zkapi.internal.ZkaContext
import optimus.breadcrumbs.BreadcrumbLevel.Level
import optimus.breadcrumbs.Breadcrumbs.SetupFlags
import optimus.cloud.CloudUtil
import optimus.breadcrumbs.crumbs.Crumb.CrumbFlag
import optimus.breadcrumbs.crumbs._
import optimus.breadcrumbs.filter._
import optimus.breadcrumbs.kafka.BreadcrumbsKafkaTopicMapper
import optimus.breadcrumbs.kafka.BreadcrumbsKafkaTopicMapperT
import optimus.breadcrumbs.kafka.KafkaTopicMapping
import optimus.breadcrumbs.routing.CrumbRoutingRule
import optimus.breadcrumbs.zookeeper.BreadcrumbsPropertyConfigurer
import optimus.logging.ThrottledWarnOrDebug
import optimus.utils.PropertyUtils
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

object BreadcrumbConsts {
  val CrumbBundle = "_crumbs" // compressed, encoded stream of serialized crumbs
  val BreadcrumbVersion = "0.6"
}

trait BreadcrumbConfig {
  def tag: String
}

object DefaultBreadcrumbConfig extends BreadcrumbConfig {
  override def tag: String = "crumbs"
}

class BreadcrumbConfigFromMap(m: Map[String, String]) extends BreadcrumbConfig {
  val tag: String = m.getOrElse("tag", DefaultBreadcrumbConfig.tag)
}

class BreadcrumbConfigFromEnv extends BreadcrumbConfig {
  val tag: String = PropertyUtils.get("breadcrumb.tag", DefaultBreadcrumbConfig.tag)
}

object BreadcrumbLevel {
  sealed trait Level extends Ordered[Level] {
    private[breadcrumbs] val value: Int
    final def compare(that: Level): Int = this.value.compareTo(that.value)
  }

  implicit def levelToInt(level: Level): Int = level.value

  private[breadcrumbs] case object All extends Level { final val value = Int.MaxValue }
  case object Error extends Level { private[breadcrumbs] final val value = 40000 }
  case object Warn extends Level { private[breadcrumbs] final val value = 30000 }
  case object Info extends Level { private[breadcrumbs] final val value = 20000 }
  case object Debug extends Level { private[breadcrumbs] final val value = 10000 }
  case object Trace extends Level { private[breadcrumbs] final val value = 5000 }

  def asString(value: Int) = value match {
    case Error.value => Error.toString
    case Warn.value  => Warn.toString
    case Info.value  => Info.toString
    case Debug.value => Debug.toString
    case Trace.value => Trace.toString
    case _           => "N/A"
  }

  final val Default = Info

  // this method is solely for parsing levels specified externally, say, as a JVM property
  private[breadcrumbs] final def parse(level: String): Level = level.toUpperCase match {
    case "ERROR"   => Error
    case "WARN"    => Warn
    case "INFO"    => Info
    case "DEBUG"   => Debug
    case "TRACE"   => Trace
    case "DEFAULT" => Default
    case _         => throw new IllegalArgumentException(s"Illegal breadcrumb level '$level'")
  }
}

// TODO (OPTIMUS-20756): Remove the whole object and the additional logging provided by
// it once the issue with uuid and duplicated crumbs is tracked down and fixed
private object BreadcrumbsVerifier {

  private val log = LoggerFactory.getLogger(BreadcrumbsVerifier.getClass)

  private[breadcrumbs] def withUuidVerifiedInDalCrumbs(c: Crumb)(body: => Boolean): Boolean = {
    val repr = c.uuid.repr
    val wrongCrumbFromDal = c.source.name == "DAL" && (repr == null || repr == "")
    if (wrongCrumbFromDal) {
      val stackTrace = Thread.currentThread().getStackTrace.mkString("\n")
      val chainedIdRepr = s"ChainedID(repr = $repr, depth = ${c.uuid.depth}, level = ${c.uuid.crumbLevel}, " +
        s"vertexId = ${c.uuid.vertexId})"
      log.debug(s"Crumb $c won't be sent due to empty repr in uuid $chainedIdRepr. Stack trace: $stackTrace")
      false
    } else body
  }
}

object Breadcrumbs {
  val log: Logger = LoggerFactory.getLogger("Breadcrumbs")
  private val resourceName = "breadcrumb.resource"
  private val queueLengthName = "breadcrumb.queue.length"
  private val queueDrainTime = "breadcrumb.queue.drain.ms"

  private val defaultResources = PropertyUtils.get(resourceName, "deferred")

  val queueLength: Int =
    if (defaultResources == "none" || defaultResources == "off") 1 else PropertyUtils.get(queueLengthName, 10000)
  val queue = new ArrayBlockingQueue[Crumb](queueLength)
  val drainTime: Int = PropertyUtils.get(queueDrainTime, 2000)

  private val registrationCallbacks = new mutable.HashMap[String, (Registration, Boolean) => Unit]
  private[breadcrumbs] val interests = new mutable.HashMap[String, Map[String, (Int, ChainedID)]]
  private val interestsLock = new ReentrantReadWriteLock()

  // For custom handling of registration
  def registerRegistrationCallback(id: String, cb: (Registration, Boolean) => Unit): Unit =  {
    interestsLock.writeLock.lock()
    try {
      registrationCallbacks.put(id, cb)
    } finally {
      interestsLock.writeLock().unlock()
    }
  }

  final case class Registration private[Breadcrumbs] (interestedIn: ChainedID, task: ChainedID, scopeTag: Option[String]) {
    private var deregistered: Boolean = false
    def deregister(): Unit = this.synchronized {
      if (!deregistered) {
        deregisterInterest(this)
        deregistered = true
      }
    }
    def deregister[A](a: A): A = {
      deregister()
      a
    }
  }

  /**
   * Register interest in crumbs published for the same VM but under a different ChainedID. While registration is
   * active, all crumbs with the uuid interestedIn will be published as well with the uuid of task.
   *
   * For example, suppose a dist engine is publishing to a ChainedID root, which by astonishing coincidence happens to
   * have acquired a random MSUuid of ROOT. Then we get a task with the ID TASK-A#2#4. Interest of the latter in the
   * former will be registered in PhenotypeExcecutor.executeNodeFromBytes. Going forward, any crumbs published with a
   * root ID of ROOT will also be published to TASK-A#2#4. Now suppose that we get, in the same process, a recursive
   * task TASK-A#2#4#2, while #2#4 is still active of course and thus not yet deregistered. In this case, we do not
   * publish engine events to all three possible IDs but just continue to send only to the engine root and to the first
   * ID with root TASK-A we received, viz TASK-A#2#4, and we'll do so until all IDs with root TASK-A have been
   * deregistered.
   *
   * You might also want to dual publish within an engine, e.g. to associate GC events with a current calculation that
   * is scoped by a chainID. This is permitted, but we will never publish an id to its ancestors, e.g. If you
   * {{{
   * registerInterest(AAAAA#1, AAAAA#2)
   * registerInterest(AAAAA#1, AAAAA#1#5)
   * }}}
   * then crumbs sent to AAAAA#1 will also go to #2 and #1#5, but not vice versa.
   */
  def registerInterest(interestedIn: ChainedID, task: ChainedID): Registration = registerInterest(interestedIn, task, None)
  def registerInterest(task: ChainedID): Registration = registerInterest(ChainedID.root, task, None)
  def registerInterest(interestedIn: ChainedID, task: ChainedID, tag: Option[String]): Registration = {
    val rs = interestedIn.base
    val ts = task.repr
    val reg = Registration(interestedIn, task, tag)
    interestsLock.writeLock.lock()
    try {
      registrationCallbacks.values.foreach(_ (reg, true))
      if (!interests.contains(rs)) {
        interests.put(rs, Map(ts -> (1, task)))
      } else if (!interests(rs).contains(ts)) {
        val m = interests(rs) + (ts -> (1, task))
        interests.put(rs, m)
      } else {
        val (i, c) = interests(rs)(ts)
        val m = interests(rs) + (ts -> (i + 1, c))
        interests.put(rs, m)
      }
    } finally {
      log.debug(s"registerInterest($rs, $ts) --> $interests")
      interestsLock.writeLock.unlock()
    }
    reg
  }

  private def deregisterInterest(reg: Registration): Unit = {
    val rs = reg.interestedIn.base
    val ts = reg.task.repr
    interestsLock.writeLock.lock()
    try {
      registrationCallbacks.values.foreach(_ (reg, false))
      if (interests.contains(rs) && interests(rs).contains(ts)) {
        val (i, c) = interests(rs)(ts)
        if (i <= 1) {
          interests(rs) = interests(rs) - ts
          if (interests(rs).isEmpty)
            interests -= rs
        } else
          interests.put(rs, interests(rs) + (ts -> (i - 1, c)))
      }
    } finally {
      log.debug(s"de-registerInterest($rs, $ts) --> $interests")
      interestsLock.writeLock.unlock()
    }
  }

  def currentRegisteredInterests: Map[String, Map[String, (Int, ChainedID)]] = interests.toMap

  private[breadcrumbs] def replicate(c: Crumb): Iterable[Crumb] = {
    if (c.flags.contains(CrumbFlag.DoNotReplicateOrAnnotate)) {
      List(c)
    } else {
      interestsLock.readLock.lock()
      try {
        val doNotReplicate = c.flags.contains(CrumbFlag.DoNotReplicate)
        interests.get(c.uuid.base).fold[Iterable[Crumb]](List(c)) { regs: Map[String, (Int, ChainedID)] =>
          var outOfProcessReplicas: List[Crumb] = Nil
          var listeners: List[ChainedID] = Nil
          regs.valuesIterator.foreach { i: (Int, ChainedID) =>
            val listener = i._2
            if (!doNotReplicate && c.uuid.base != listener.base)
              outOfProcessReplicas = new WithReplicaFrom(listener, c) :: outOfProcessReplicas
            if (!c.uuid.repr.startsWith(listener.repr))
              listeners = listener :: listeners
          }
          val cc = if (listeners.nonEmpty) new WithCurrentlyRunning(c, listeners) else c
          cc :: outOfProcessReplicas
        }
      } finally {
        interestsLock.readLock.unlock()
      }
    }
  }

  @volatile private var impl: BreadcrumbsPublisher = genImpl(defaultResources)

  private[breadcrumbs] def getImpl: BreadcrumbsPublisher = impl

  private def genImpl(resources: String): BreadcrumbsPublisher = {
    def genPublisher(resource: String): BreadcrumbsPublisher = {
      if ((resource eq null) || resource == "" || resource == "off" || resource == "none")
        new BreadcrumbsIgnorer
      else if (resource.startsWith("deferred") || resource.startsWith("kafka"))
        new DeferredConfigurationBreadcrumbsPublisher()
      else if (resource == "log")
        new BreadcrumbsLoggingPublisher(new BreadcrumbConfigFromEnv)
      else
        throw new RuntimeException(s"Unknown breadcrumb resource: $resource")
    }
    log.debug(s"Loading breadcrumbs from $resources")
    val resourceArray = resources.split(",")
    if (resourceArray.length > 1)
      new BreadcrumbsCompositePublisher(resourceArray.toSet map genPublisher)
    else
      genPublisher(resources)
  }

  private[breadcrumbs] def setImpl(resource: String): Unit = {
    setImpl(genImpl(resource))
  }

  private[optimus] def setImpl(newImpl: BreadcrumbsPublisher): Unit = {
    val oldImpl = this.synchronized {
      val oldImpl = impl
      Breadcrumbs.log.info(s"Setting breadcrumbs implementation from $oldImpl to $newImpl")
      newImpl.init()
      impl = newImpl
      oldImpl
    }
    oldImpl.shutdown()
  }

  def disable() = setImpl(new BreadcrumbsIgnorer)

  def isBackendPublishingEnabled(): Boolean = getImpl match {
    // although we might find a backend publisher among routing rules, that's not enough for us
    case router: BreadcrumbsRouter    => router.defaultPublisher.isInstanceOf[BreadcrumbsKafkaPublisher]
    case _: BreadcrumbsKafkaPublisher => true
    case _: BreadcrumbsIgnorer        => false
    case _                            => false
  }

  sealed trait SetupFlag
  type SetupFlags = Set[SetupFlag]
  object SetupFlags {
    // Throw if the supplied config would not enable any crumbs to be sent
    private[optimus] case object StrictInit extends SetupFlag
    private[optimus] case object VerboseInit extends SetupFlag

    val None = Set.empty[SetupFlag]
  }

  // Customize a publisher.  This can only be done once - subsequent attempts will be
  // relatively cheaply ignored.
  private[optimus] def customizedInit(keys: => Map[String, String], zkc: => ZkaContext, setupFlags: SetupFlags = SetupFlags.None): Unit = this.synchronized {
    val newImpl = impl.customize(keys, zkc, setupFlags)
    if (newImpl ne impl)
      setImpl(newImpl)
  }

  // Load a new publisher based on the resource string.
  // Re-run customized init if necessary.
  def reload(): Unit = reload(PropertyUtils.get(resourceName) getOrElse defaultResources)
  def reload(resource: String): Unit = {
    var newImpl = genImpl(resource)
    for ((keys, zkc) <- impl.savedCustomization)
      newImpl = newImpl.customize(keys, zkc)
    setImpl(newImpl)
  }

  def collecting: Boolean = impl.collecting

  def isTraceEnabled: Boolean = impl.isEnabledForTrace
  def isDebugEnabled: Boolean = impl.isEnabledForDebug
  def isInfoEnabled: Boolean = impl.isEnabledForInfo
  def isTraceEnabled(id: ChainedID): Boolean = impl.isEnabledForTrace || BreadcrumbLevel.Trace >= id.crumbLevel
  def isDebugEnabled(id: ChainedID): Boolean = impl.isEnabledForDebug || BreadcrumbLevel.Debug >= id.crumbLevel
  def isInfoEnabled(id: ChainedID): Boolean = impl.isEnabledForInfo || BreadcrumbLevel.Info >= id.crumbLevel

  def send(c: Crumb): Boolean = impl.send(c)

  // Instantiates and sends crumb if and only if publisher or uuid level is trace/debug/info/warn/error enabled.
  def trace(uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.trace(uuid, cf)
  def debug(uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.debug(uuid, cf)
  def info(uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.info(uuid, cf)
  def warn(uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.warn(uuid, cf)
  def error(uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.error(uuid, cf)

  // Instantiates crumb if publisher or uuid level is trace/debug/info/warn/error enabled, and then
  // sends it unless it's still in our cache of crumbs already sent.
  def traceOnce(uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.traceOnce(uuid, cf)
  def debugOnce(uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.debugOnce(uuid, cf)
  def infoOnce(uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.infoOnce(uuid, cf)
  def warnOnce(uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.warnOnce(uuid, cf)
  def errorOnce(uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.errorOnce(uuid, cf)

  // Instantiates and sends the crumb if the publisher or uuid level is trace/debug/info/warn/error enabled and
  // the specified key is not in our already-sent cache.
  def traceOnce(key: AnyRef, uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.traceOnce(key, uuid, cf)
  def debugOnce(key: AnyRef, uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.debugOnce(key, uuid, cf)
  def infoOnce(key: AnyRef, uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.infoOnce(key, uuid, cf)
  def warnOnce(key: AnyRef, uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.warnOnce(key, uuid, cf)
  def errorOnce(key: AnyRef, uuid: ChainedID, cf: ChainedID => Crumb): Boolean = impl.errorOnce(key, uuid, cf)

  // Instantiates and sends the crumb if the publisher or uuid level is enabled for the specified log level.
  def apply(uuid: ChainedID, cf: => ChainedID => Crumb, level: BreadcrumbLevel.Level = BreadcrumbLevel.All): Boolean =
    impl.send(uuid, cf, level)

  def sendOnce(uuid: ChainedID, cf: ChainedID => Crumb, level: BreadcrumbLevel.Level = BreadcrumbLevel.Default): Unit =
    impl.sendOnce(null, uuid, cf, level)
  def sendOnce(key: AnyRef, uuid: ChainedID, cf: ChainedID => Crumb, level: BreadcrumbLevel.Level): Boolean =
    impl.sendOnce(key, uuid, cf, level)

  def flush(): Unit = {
    log.debug("Breadcrumbs about to be flushed")
    impl.flush()
    log.info("Breadcrumbs flushed")
  }

  object Flush {
    def &&:[T](t: T): T = { // cute!
      flush()
      t
    }
  }

  def shutdown(): Unit = {
    impl.shutdown()
    log.info("Breadcrumbs shutdown completed")
  }

  protected[breadcrumbs] final def runProtected[T](f: () => T): Unit = {
    try {
      f()
    } catch {
      case NonFatal(ex) =>
        log.error(s"An error occurred", ex)
    }
  }

}

abstract class BreadcrumbsPublisher extends Filterable {
  Breadcrumbs.log.debug(s"Initializing ${this.getClass}")

  private[breadcrumbs] lazy val level: Level = BreadcrumbLevel.parse(PropertyUtils.get("breadcrumb.level", "DEFAULT"))
  private val scv = new StandardCrumbValidator
  def savedCustomization: Option[(Map[String, String], ZkaContext)] = None
  def customize(keys: => Map[String, String], zkc: => ZkaContext, setupFlags: SetupFlags = SetupFlags.None): BreadcrumbsPublisher = this
  def collecting: Boolean
  def init(): Unit

  def isEnabledForTrace: Boolean = BreadcrumbLevel.Trace >= this.level
  def isEnabledForDebug: Boolean = BreadcrumbLevel.Debug >= this.level
  def isEnabledForInfo: Boolean = BreadcrumbLevel.Info >= this.level

  protected[breadcrumbs] final def trace(uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    send(uuid, cf, BreadcrumbLevel.Trace)
  protected[breadcrumbs] final def debug(uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    send(uuid, cf, BreadcrumbLevel.Debug)
  protected[breadcrumbs] final def info(uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    send(uuid, cf, BreadcrumbLevel.Info)
  protected[breadcrumbs] final def warn(uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    send(uuid, cf, BreadcrumbLevel.Warn)
  protected[breadcrumbs] final def error(uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    send(uuid, cf, BreadcrumbLevel.Error)

  protected[breadcrumbs] final def traceOnce(uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    sendOnce(null, uuid, cf, BreadcrumbLevel.Trace)
  protected[breadcrumbs] final def debugOnce(uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    sendOnce(null, uuid, cf, BreadcrumbLevel.Debug)
  protected[breadcrumbs] final def infoOnce(uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    sendOnce(null, uuid, cf, BreadcrumbLevel.Info)
  protected[breadcrumbs] final def warnOnce(uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    sendOnce(null, uuid, cf, BreadcrumbLevel.Warn)
  protected[breadcrumbs] final def errorOnce(uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    sendOnce(null, uuid, cf, BreadcrumbLevel.Error)

  protected[breadcrumbs] final def traceOnce(key: AnyRef, uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    sendOnce(key, uuid, cf, BreadcrumbLevel.Trace)
  protected[breadcrumbs] final def debugOnce(key: AnyRef, uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    sendOnce(key, uuid, cf, BreadcrumbLevel.Debug)
  protected[breadcrumbs] final def infoOnce(key: AnyRef, uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    sendOnce(key, uuid, cf, BreadcrumbLevel.Info)
  protected[breadcrumbs] final def warnOnce(key: AnyRef, uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    sendOnce(key, uuid, cf, BreadcrumbLevel.Warn)

  protected[breadcrumbs] final def errorOnce(key: AnyRef, uuid: ChainedID, cf: ChainedID => Crumb): Boolean =
    sendOnce(key, uuid, cf, BreadcrumbLevel.Error)

  protected[breadcrumbs] def sendInternal(c: Crumb): Boolean

  protected def warning: Option[ThrottledWarnOrDebug] = None

  final protected[breadcrumbs] def send(c: Crumb): Boolean = {
    if (c.uuid.base.length > 0) {
      val cs = Breadcrumbs.replicate(c)
      cs.forall(sendInternal)
    } else {
      Breadcrumbs.log
        .warn(s"Not sending crumb with empty uuid base: $c", new IllegalArgumentException("Empty UUID base"))
      false
    }
  }

  final protected[breadcrumbs] def send(
      uuid: ChainedID,
      cf: ChainedID => Crumb,
      level: BreadcrumbLevel.Level): Boolean = {
    var crumbSent = false
    if (uuid == null)
      Breadcrumbs.log.debug(
        s"null chain ID received from:\n${Thread.currentThread.getStackTrace.toSeq.mkString("\n ")}")
    else if (collecting && level >= Math.min(this.level, uuid.crumbLevel)) {
      val c: Crumb = cf(uuid)
      scv.validate(c)
      val filtered =
        try {
          isFiltered(c)
        } catch {
          case t: Throwable =>
            val msg = s"Unable to filter $c due to $t, discarded..."
            Breadcrumbs.log.debug(msg)
            warning.foreach(_.fail(msg))
            true
        }

      if (filtered) {
        Breadcrumbs.log.debug(s"Crumb $c was filtered")
      } else {
        crumbSent = send(c)
      }
    }
    crumbSent
  }

  // Try to avoid sending the same crumb multiple times
  private val CacheMax = PropertyUtils.get("breadcrumb.dedup.cache.size", 10000)
  private val dummy = new Object
  private val sent = CacheBuilder.newBuilder().maximumSize(CacheMax).build[AnyRef, Object]

  private[breadcrumbs] final def sendOnce(
      key: AnyRef,
      uuid: ChainedID,
      cf: ChainedID => Crumb,
      level: BreadcrumbLevel.Level): Boolean = {
    var crumbSent = false
    if (collecting && level >= Math.min(this.level, uuid.crumbLevel)) {
      // Note: can't use get(,Callable), because we could end up being called recursively via logging;
      // an erroneous cache miss will just result in extra breadcrumbs being sent.
      if (key ne null) {
        // Don't bother instantiating crumb if key is present
        if (sent.getIfPresent(key) eq null) {
          sent.put(key, dummy)
          crumbSent = send(cf(uuid))
        }
      } else {
        // We're using the crumb as the key, so we need to instantiate it to check.
        val c = cf(uuid)
        scv.validate(c)
        if (sent.getIfPresent(c) eq null) {
          sent.put(c, dummy)
          crumbSent = send(c)
        }
      }
    }
    crumbSent
  }

  def flush(): Unit
  def shutdown(): Unit
}

private[optimus] class BreadcrumbsRouter(
    val rules: Seq[CrumbRoutingRule],
    private[breadcrumbs] val defaultPublisher: BreadcrumbsPublisher)
    extends BreadcrumbsPublisher {
  import BreadcrumbsRouter.log
  import Breadcrumbs.runProtected

  require(rules != null, "Rules cannot be null")
  require(defaultPublisher != null, "Default publisher cannot be null")

  private lazy val publishers = rules.map(_.publisher) ++ Seq(defaultPublisher)

  override def collecting: Boolean = true

  private var initCompleted = new AtomicBoolean(false)

  override def init(): Unit = {
    if (initCompleted.compareAndSet(false, true)) {
      if (rules.isEmpty) log.info(s"Initializing with empty rule set")
      else log.info(s"Initializing with default publisher $defaultPublisher and rule set ${rules.mkString("; ")}")
      publishers.toSet foreach { publisher: BreadcrumbsPublisher =>
        runProtected(publisher.init)
      }
      log.info(s"Initialization complete")
    }
  }

  override val getFilter: Option[CrumbFilter] = {
    if (rules.isEmpty)
      defaultPublisher.getFilter
    else
      None
  }

  // [SEE_BREADCRUMB_FILTERING]
  private def route(c: Crumb): BreadcrumbsPublisher = {
    val targetPublisher = rules
      .find { _.matcher matches c }
      .map { _.publisher }
    targetPublisher getOrElse defaultPublisher
  }

  protected[breadcrumbs] override def sendInternal(c: Crumb): Boolean = {
    val r = route(c)
    r.sendInternal(c)
  }

  override def flush(): Unit = publishers foreach { publisher =>
    runProtected(publisher.flush)
  }

  override def shutdown(): Unit = publishers foreach { publisher =>
    runProtected(publisher.shutdown)
  }
}

object BreadcrumbsRouter {
  private val log: Logger = LoggerFactory.getLogger(classOf[BreadcrumbsRouter])
}

class BreadcrumbsIgnorer extends BreadcrumbsPublisher {
  override val collecting = false
  override def init(): Unit = {}
  override def flush(): Unit = {}
  override def sendInternal(c: Crumb): Boolean = true
  override def shutdown(): Unit = {}
}

class DeferredConfigurationBreadcrumbsPublisher() extends BreadcrumbsPublisher {
  override def customize(keys: => Map[String, String], zkc: => ZkaContext, setupFlags: SetupFlags = SetupFlags.None): BreadcrumbsPublisher = {
    Breadcrumbs.log.info(s"${this.getClass} reconfiguring with $keys")
    BreadcrumbsPropertyConfigurer.implFromConfig(zkc, keys, setupFlags)
  }
  override def collecting: Boolean = true
  override def init(): Unit = {
    Breadcrumbs.log.info(s"${this.getClass} dummy initialization, pending customization")
  }
  // Enqueue crumbs pending eventual configuration.
  override def sendInternal(c: Crumb): Boolean = BreadcrumbsVerifier.withUuidVerifiedInDalCrumbs(c) {
    Breadcrumbs.queue.offer(c)
  }
  override def flush(): Unit = {}
  override def shutdown(): Unit = {}
}

class BreadcrumbsLoggingPublisher(cfg: BreadcrumbConfig = new BreadcrumbConfigFromEnv) extends BreadcrumbsPublisher {
  private val log = Breadcrumbs.log
  override def collecting: Boolean = true
  override def init(): Unit = {
    // One-time drain, since we won't be using queue any more
    val buffer = new jArrayList[Crumb]()
    Breadcrumbs.queue.drainTo(buffer)
    buffer.asScala.foreach(send)
  }
  override def flush(): Unit = {}
  override def shutdown(): Unit = {}
  override def sendInternal(c: Crumb): Boolean = {
    log.info(s"crumb: ${c.asJSON.toString}")
    true
  }
}

object BreadcrumbsKafkaPublisher {
  private[breadcrumbs] val TopicMapKey: String = "topicMap"
}
class BreadcrumbsKafkaPublisher private[breadcrumbs] (props: jMap[String, Object], topicProps: jMap[String, Object])
    extends BreadcrumbsPublisher {
  import Breadcrumbs.log
  import Breadcrumbs.runProtected
  import BreadcrumbsKafkaPublisher.TopicMapKey
  private var producer: Option[KafkaProducer[String, String]] = None
  @volatile private var initialized: Boolean = false
  private[this] val topicMap: Seq[KafkaTopicMapping] = if (topicProps.containsKey(TopicMapKey)) {
    topicProps
      .get(TopicMapKey)
      .asInstanceOf[jList[jMap[String, String]]]
      .asScala
      .map { jm: jMap[String, String] =>
        KafkaTopicMapping.fromJava(jm)
      }
      .toSeq
  } else {
    Seq.empty[KafkaTopicMapping]
  }
  protected[breadcrumbs] val topicMapper: BreadcrumbsKafkaTopicMapperT = new BreadcrumbsKafkaTopicMapper(topicMap)
  // These properties are coming in as parsed YAML and could be practically any type.
  private val targetWarningInterval = Option(props.get("warning.interval.sec")).map(_.toString.toDouble).getOrElse(60.0)
  private val warningAveragingTime =
    Option(props.get("warning.averaging.sec")).map(_.toString.toDouble).getOrElse(300.0)
  val encryptPayload: Boolean = {
    SystemPropertyUtils.getBoolean("optimus.breadcrumbs.encrypt_payload", false, log) || CloudUtil.isCloud()
  }
  override val warning = Some(new ThrottledWarnOrDebug(log, targetWarningInterval, warningAveragingTime))
  override def collecting: Boolean = producer.isDefined
  override def init(): Unit = synchronized {
    if (!initialized) {
      try {
        auth.CrumbsAuthProvider.initAuth(props)
        if (encryptPayload) {
          props.put("value.serializer", "optimus.breadcrumbs.kafka.EncryptionStringSerializer")
          log.info(
            "optimus.breadcrumbs.encrypt_payload property is set to true or in cloud environment. Enabling " +
              "EncryptionStringSerializer and encrypting crumbs.")

        }
        producer = Some(new KafkaProducer[String, String](props))
        initialized = true
        drainThread.setDaemon(true)
        drainThread.start()
      } catch {
        case t: Throwable =>
          Breadcrumbs.log.warn(s"Unable to create KafkaProducer: $t $props")
          producer = None
      }
    }
  }

  override def sendInternal(c: Crumb): Boolean = BreadcrumbsVerifier.withUuidVerifiedInDalCrumbs(c) {
    Breadcrumbs.log.trace(s"Sending $c")
    Breadcrumbs.queue.offer(c) || {
      warning.foreach(_.fail("Failed to send kafka message due to full queue."))
      false
    }
  }

  private var sentCount = 0L
  private var thresholdCount = 1000L
  private val isRunning = new AtomicBoolean(true)

  private val drainThread = new Thread {

    override def run(): Unit = {

      def countSent(n: Int): Boolean = {
        sentCount += n
        if (sentCount >= thresholdCount) {
          sentCount += 1
          sendKafka(LogPropertiesCrumb(ChainedID.root, Properties.breadcrumbsSentSoFar -> sentCount))
          log.info(s"Published $sentCount crumbs to kafka so far.")

          while (sentCount >= thresholdCount) thresholdCount *= 2
          true
        } else false
      }

      try {
        while (isRunning.get()) {
          Breadcrumbs.queue.take() match {
            case f: FlushMarker =>
              val buf = new jArrayList[Crumb]
              Breadcrumbs.queue.drainTo(buf)
              buf.asScala.foreach(sendKafka)
              countSent(buf.size)
              f.flushed()
            case c: Crumb =>
              sendKafka(c)
              countSent(1)
          }
        }
      } finally {
        sendKafka(LogPropertiesCrumb(ChainedID.root, Properties.breadcrumbsSentSoFar -> sentCount))
        producer.foreach { p =>
          p.close()
        }
        producer = None
      }
    }
  }

  private object CompletionCallback extends Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (metadata ne null)
        warning.foreach(_.succeed(s"Successfully sent kafka message to partition ${metadata.partition()}"))
      else
        warning.foreach(_.fail(s"Failed to send kafka message: $exception"))
    }
  }

  def sendKafka(c: Crumb): Boolean = producer.isDefined && {
    try {
      val record = new ProducerRecord[String, String](topicMapper.topicForCrumb(c).topic, c.asJSON.toString())
      producer.foreach(_.send(record, CompletionCallback))
      true
    } catch {
      case t: Throwable =>
        warning.foreach(_.fail(s"Unable to send $c via Kafka due to $t"))
        false
    }
  }

  override def flush(): Unit = {
    val f = new FlushMarker
    if (Breadcrumbs.queue.offer(f))
      if (!f.await(Breadcrumbs.drainTime)) {
        val unsentCrumbCount = Breadcrumbs.queue.size - 1
        log.warn(
          s"BreadcrumbsKafkaPublisher timed out after ${Breadcrumbs.drainTime} ms waiting for crumb queue to drain to the flush marker")
        if (unsentCrumbCount > 0) {
          log.warn(s"The last ${unsentCrumbCount} crumbs will not be published")
        }
      }
    producer.foreach(_.flush())
  }

  override def shutdown(): Unit = {
    flush()
    Breadcrumbs.queue.clear()
    isRunning.set(false)
    runProtected(() => drainThread.interrupt())
    log.info(s"Shutting down BreadcrumbsKafkaPublisher after $sentCount crumbs")
  }
}

object BreadcrumbsCompositePublisher {
  val log: Logger = LoggerFactory.getLogger(classOf[BreadcrumbsCompositePublisher])
}

class BreadcrumbsCompositePublisher(private[breadcrumbs] val publishers: Set[BreadcrumbsPublisher])
    extends BreadcrumbsPublisher {
  import Breadcrumbs.runProtected
  import BreadcrumbsCompositePublisher.log

  override def customize(keys: => Map[String, String], zkc: => ZkaContext, setupFlags: SetupFlags = SetupFlags.None): BreadcrumbsPublisher = {
    new BreadcrumbsCompositePublisher(publishers.map(_.customize(keys, zkc)))
  }

  override def collecting: Boolean = publishers forall (_.collecting)

  override def init(): Unit = publishers foreach { publisher =>
    log.info(s"Initializing $publisher")
    runProtected(publisher.init)
  }

  override val getFilter: Option[CrumbFilter] = {
    val filters = publishers.foldLeft(Seq.empty[CrumbFilter])((acc, p) => acc ++ p.getFilter)
    if (filters.nonEmpty) {
      val result = new CompositeFilter()
      filters.foreach(result.addFilter(_))
      Some(result)
    } else
      None
  }

  protected[breadcrumbs] override def sendInternal(c: Crumb): Boolean = {
    publishers.foldLeft(true) { (acc, publisher) =>
      try {
        log.debug(s"Running sendInternal for $publisher")
        acc && publisher.sendInternal(c)
      } catch {
        case NonFatal(ex) =>
          log.info("An error has occurred", ex)
          true
      }
    }
  }

  override def flush(): Unit = publishers foreach { publisher =>
    log.info(s"Executing flush for $publisher")
    runProtected(publisher.flush)
  }

  override def shutdown(): Unit = publishers foreach { publisher =>
    log.info(s"Shutting down $publisher")
    runProtected(publisher.shutdown)
  }
}

trait BreadcrumbRegistration {
  import scala.language.experimental.macros
  def withRegistration[T](interestedIn: ChainedID)(block: T): T = macro BreadcrumbRegistration.withRegistrationImpl[T]
  def withRootRegistration[T](block: T): T = macro BreadcrumbRegistration.withRootRegistrationImpl[T]
}

object BreadcrumbRegistration extends BreadcrumbRegistration {
  import scala.reflect.macros.blackbox.Context

  def withRegistrationImpl[T: c.WeakTypeTag](c: Context)(interestedIn: c.Expr[ChainedID])(
      block: c.Expr[T]): c.Expr[T] = {
    import c.universe._
    val reg = c.internal.reificationSupport.freshTermName("reg")
    val ret = q"""{
       val $reg = _root_.optimus.breadcrumbs.Breadcrumbs.registerInterest($interestedIn, _root_.optimus.platform.EvaluationContext.scenarioStack.getTrackingNodeID)
       ${reg}.deregister($block)
    }"""
    c.Expr[T](ret)
  }

  def withRootRegistrationImpl[T: c.WeakTypeTag](c: Context)(block: c.Expr[T]): c.Expr[T] = {
    import c.universe._
    val reg = c.internal.reificationSupport.freshTermName("reg")
    val ret = q"""{
       val $reg = _root_.optimus.breadcrumbs.Breadcrumbs.registerInterest(_root_.optimus.platform.EvaluationContext.scenarioStack.getTrackingNodeID)
       ${reg}.deregister($block)
    }"""
    c.Expr[T](ret)
  }
}
