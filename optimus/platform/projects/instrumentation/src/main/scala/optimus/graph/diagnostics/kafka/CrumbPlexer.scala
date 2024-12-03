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
package optimus.graph.diagnostics.kafka

import com.github.benmanes.caffeine.cache.Caffeine
import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import kafka.zk.KafkaZkClient
import msjava.kerberos.auth.MSKerberosConfiguration
import msjava.zkapi.ZkaAttr
import msjava.zkapi.ZkaConfig
import msjava.zkapi.internal.ZkaContext
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.ProfilerSource
import optimus.breadcrumbs.crumbs.{Properties => P}
import optimus.breadcrumbs.crumbs.Events
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.breadcrumbs.util.LimitedSizeConcurrentHashMap
import optimus.graph.diagnostics.sampling.BaseSamplers
import optimus.graph.diagnostics.sampling.SamplingProfiler
import optimus.utils.Args4JOptionHandlers.StringHandler
import optimus.utils.MiscUtils.retry
import optimus.utils.OptimusStringUtils
import optimus.utils.PropertyUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.client.ZKClientConfig
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.{Option => ArgOption}
import org.slf4j.LoggerFactory
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.io.BufferedOutputStream
import java.io.FileDescriptor
import java.io.FileOutputStream
import java.io.FileWriter
import java.io.PrintStream
import java.io.PrintWriter
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util
import java.util.Properties
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.TimeUnit
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Objects
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Try
import scala.util.control.NonFatal

class KafkaListenerArgs {
  val topicsDefault = "crumbs,dist"
  @ArgOption(name = "--topics", usage = "List of Kafka topics to subscribe to (comma-separated) default: crumbs,dist")
  val topics: String = topicsDefault

  @ArgOption(name = "--zookeeper", usage = "Zookeeper host list (comma-separated)")
  val zkHosts = ""

  @ArgOption(name = "--bootstrap", usage = "Bootstrap Kafka hosts (comma-separated)")
  val kServers = ""

  @ArgOption(name = "--userjaas", usage = "If set, add user jaas configuration ")
  val userjaas = false

  @ArgOption(
    name = "--publish",
    usage =
      "Crumbplexer's own publication config, in form \"zkenv/node\", where zkenv is qa or prod, and node is the starting point under optimus/breadcrumbs"
  )
  val crumbPublication = ""

  val securityProtocolDefault = "PLAINTEXT"
  @ArgOption(
    name = "--protocol",
    usage = "Kafka security protocol, valid values: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL")
  val kSecurityProtocol: String = securityProtocolDefault

  @ArgOption(name = "--serviceName", usage = "Kafka service name")
  val kServiceName = ""

  @ArgOption(
    name = "--start",
    aliases = Array("-s"),
    usage =
      "Starting crumb publication time as YYYY-MM-DDT00:00 (UTC) or ms since epoch, or negative ms offset from now \"beginning\", default = -3600000 (1 hour ago)"
  )
  val startTimeArg: String = "-3600000"

  @ArgOption(
    name = "--end",
    aliases = Array("-e"),
    usage =
      "Ending crumb publication time as \"YYYY-MM-DDT00:00\" (UTC), or ms since epoch, or negative ms offset from now, or \"never\" (which is the default)"
  )
  val endTimeArg: String = "never"

  @ArgOption(
    name = "--endslop",
    usage =
      "Continue reading until kafka receipt time exceeds crumb publication end time by this amount, default 1000ms")
  val endSlop = 1000

  @ArgOption(name = "--prefix", usage = "UUID starts with...  Default blank.", aliases = Array("-f"))
  val prefix: String = ""

  @ArgOption(
    name = "--validuuidregex",
    aliases = Array("-u"),
    usage = "Deem uuids as valid if they match this regex, default '.+'")
  val uuidRegexString: String = "\\S+"

  @ArgOption(name = "--waitAfterComplete", usage = "If set, exit ms after observing AppCompleted event")
  val waitAfterComplete: Int = -1

  @ArgOption(name = "--wait", usage = "Exit if no crumbs for this many msec, default Long.MaxValue")
  var quiescenceTimeout: Long = Long.MaxValue // This one does get overwritten

  @ArgOption(
    name = "--mergequeue",
    usage =
      "Crumbs are read in parallel from each partition and merged by publication time in a priority queue, preferably at least 100 (default) deep"
  )
  val mergeQueueSize = 100

  @ArgOption(name = "--mergewait", usage = "If queue is not at desired length, wait at most 50 ms (default).")
  val mergeWait = 50

  @ArgOption(name = "--json", usage = "Print as JSON as opposed to Scala Map (default true")
  val asJson = true

  @ArgOption(name = "--quiet", usage = "Don't print anything except progress updates.")
  val quiet = false

  @ArgOption(name = "--status", usage = "Print update to stderr every N seconds.")
  val status = 0

  @ArgOption(
    name = "--hashpoolsize",
    usage = "If greater than zero, filter into buckets by hash of root uuid modulo the argument.")
  val hashPoolSize = 0
  @ArgOption(name = "--hashpoolid", usage = "Print only this hash bucket.")
  val hashPoolId = 0

  @ArgOption(
    name = "--killpastevent",
    usage = "If set, cut of output after an event crumb with event matching this regex.")
  val killPastEventRegexString = "NONONONO"

  @ArgOption(name = "--enrich", usage = "If set, prepend output lines with potentially useful information.")
  val enrich = false

  @ArgOption(name = "--buffer", usage = "Default true.  If set to false, flush output after each line.")
  val bufferOut = true

  @ArgOption(name = "--truncate", usage = "If set, truncate output to this many lines")
  val truncateLines: Int = -1

  @ArgOption(
    name = "--importanteventregex",
    usage =
      "Regex for 'important' events after which to flush output if buffering, default:AppStarted|AppCompleted|RuntimeShutDown"
  )
  val importantEventRegexString = "AppStarted|AppCompleted|RuntimeShutDown"

  @ArgOption(
    name = "--plexdir",
    usage = "If set, crumbs with a given root ChainedID will be written to files of that name in this directory.",
    handler = classOf[StringHandler]
  )
  val plexDirName: Option[String] = None

  val flushAfterMsDefault = 5000
  @ArgOption(name = "--flushafter", usage = "If writing plex files, flush after ms of inactivity, default 5000")
  val flushAfterMs: Int = flushAfterMsDefault

  val closeAfterMsDefault = 60000
  @ArgOption(
    name = "--closeafter",
    usage = "If writing plex files, close (potentially to be reopened later) after ms of inactivity, default 60000")
  val closeAfterMs: Int = closeAfterMsDefault

  @ArgOption(
    name = "--dedupCacheSize",
    usage = "Number of dedup keys to retain in memory"
  )
  val nDedupKeys = 100 * 1000 * 1000

  @ArgOption(name = "--help", aliases = Array("-h"), usage = "Help.")
  val help = false

}

trait CrumbRecordParser {
  protected val logger = LoggerFactory.getLogger(this.getClass)
  protected def error(msg: String, id: String = "", t: Throwable): Unit
  // For ms increments of
  // 100, 200, 400, 800, 1600, .. maxLastFailedParseWaitMs
  // If at least 2x the max wait has elapsed, then reset to 100ms.
  protected val maxFailedParseExceptionCount: Int =
    Integer.parseInt(System.getProperty("optimus.breadcrumbs.maxFailedParseExceptionCount", "4"))
  protected val lastFailedParseTimeAndWaitMs: LimitedSizeConcurrentHashMap[Class[_], (Instant, Long)] =
    new LimitedSizeConcurrentHashMap[Class[_], (Instant, Long)](maxFailedParseExceptionCount)
  protected val maxLastFailedParseWaitMs: Long =
    Integer.parseInt(System.getProperty("optimus.breadcrumbs.maxLastFailedParseWaitMs", "6400")).toLong

  protected val recordMaxLength = PropertyUtils.get("optimus.crumbplexer.max.record.length", 100000)

  protected val parseErrors = new AtomicLong(0)
  protected def parseRecord(s: ConsumerRecord[String, String]): Try[Map[String, JsValue]] = {
    val value = s.value()
    if (value.length > recordMaxLength)
      Failure(new IllegalArgumentException(s"Record size ${value.length} > $recordMaxLength"))
    else
      Try(value.parseJson.convertTo[Map[String, JsValue]]).recoverWith { case t: Throwable =>
        parseErrors.incrementAndGet()
        val (lastFailedParseTime, waitMs): (Instant, Long) =
          lastFailedParseTimeAndWaitMs.getOrElse(t.getClass, (Instant.EPOCH, -1))
        val nowMs: Instant = patch.MilliInstant.now
        val elapsedMs: Long = nowMs.toEpochMilli - lastFailedParseTime.toEpochMilli
        val msg: String = s"Failed to parse ${s.value.take(1000)} ${t.getMessage.take(1000)}"
        val didLogError: Boolean = if (elapsedMs > waitMs) {
          error(msg, "", t)
          true
        } else {
          logger.debug(msg)
          false
        }
        if (elapsedMs > maxLastFailedParseWaitMs * 2) {
          // Reset if we went long enough without failing.
          lastFailedParseTimeAndWaitMs.put(t.getClass, (nowMs, 100))
        } else if (didLogError && waitMs < maxLastFailedParseWaitMs) {
          // Double the previous time between failure logging.
          lastFailedParseTimeAndWaitMs.put(t.getClass, (nowMs, waitMs * 2))
        }
        Failure(t)
      }
  }
}

object CrumbPlexer extends App with CrumbRecordParser with OptimusStringUtils {

  val cli = new KafkaListenerArgs
  val parser = new CmdLineParser(cli)
  def usage(): Unit = parser.printUsage(System.err)
  try {
    parser.parseArgument(args: _*)
  } catch {
    case e: CmdLineException =>
      logger.error(e.getMessage)
      usage()
      System.exit(1)
  }
  import cli._
  if (help) {
    usage()
    System.exit(0)
  }

  private val publishingCrumbsOurself =
    if (crumbPublication.contains("/")) {
      val Array(env, configNode) = crumbPublication.split("/")
      Breadcrumbs.customizedInit(
        Map("breadcrumb.config" -> configNode),
        new ZkaContext(ZkaConfig.fromURI(s"zpm://$env.na/optimus").attr(ZkaAttr.KERBEROS, false))
      )
      SamplingProfiler.applicationSetup("CrumbPlexer")
      true
    } else false

  import optimus.platform.util.Version
  private val stdProps = P.appId -> "CrumbPlexer" :: Version.properties

  private val publishSource = ProfilerSource

  private def warn(msg: String, id: String = "") = {
    logger.warn(msg)
    if (publishingCrumbsOurself)
      Breadcrumbs.warn(
        ChainedID.root,
        PropertiesCrumb(
          _,
          publishSource,
          P.logMsg -> msg :: P.logLevel -> "WARN" :: id.emptyOrSome.map(P.publisherId -> _) :: stdProps))
  }

  private def info(msg: String, id: String = "") = {
    logger.info(msg)
    if (publishingCrumbsOurself)
      Breadcrumbs.info(
        ChainedID.root,
        PropertiesCrumb(
          _,
          publishSource,
          P.logMsg -> msg :: P.logLevel -> "INFO" ::
            id.emptyOrSome.map(P.publisherId -> _) :: stdProps))
  }

  override protected def error(msg: String, id: String = "", t: Throwable) = {
    logger.error(msg)
    if (publishingCrumbsOurself)
      Breadcrumbs.error(
        ChainedID.root,
        PropertiesCrumb(
          _,
          publishSource,
          P.logLevel -> "ERROR" ::
            P.logMsg -> msg :: id.emptyOrSome.map(P.publisherId -> _) :: P.exception -> t :: stdProps))
  }

  private val poolId = if (hashPoolSize > 0) s"poolid=$hashPoolId/$hashPoolSize" else "1/1"

  if (bufferOut)
    System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream(FileDescriptor.out)), false))

  private val uuidRegex = s"($uuidRegexString)".r
  private val killPastEventRegex = killPastEventRegexString.r
  private val importantEventRegex = importantEventRegexString.r

  private val badSeeds = CacheBuilder.newBuilder.maximumSize(10000).build[String, String]
  private val counts: Cache[String, java.lang.Long] =
    CacheBuilder.newBuilder.maximumSize(10000).build[String, java.lang.Long]

  private final case class FileEntry(uuid: String, f: PrintWriter, var tPrinted: Long, var tFlushed: Long)
  private val files = new java.util.HashMap[String, FileEntry] // so we can iterate and delete

  private def isBadSeed(uuid: String) = {
    val ret = uuid.split("#").scan("")(_ + _ + "#").tail.exists(badSeeds.getIfPresent(_) ne null)
    ret
  }

  private val df = DateTimeFormatter.ISO_ZONED_DATE_TIME
  private def tUTC(t: Long): String = df.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(t), ZoneId.of("UTC")))

  private def parseTimeArg(timeArg: String) =
    if (timeArg == "beginning")
      0L
    else if (timeArg == "never")
      Long.MaxValue
    else
      (Try[Long] {
        timeArg.toLong
      } orElse Try[Long] {
        LocalDateTime.parse(timeArg).atZone(ZoneId.of("UTC")).toEpochSecond * 1000L
      }).map { targ =>
        if (targ <= 0)
          System.currentTimeMillis() + targ
        else
          targ
      }.recover { case e: Throwable =>
        logger.error(s"$poolId Unable to parse $timeArg: $e")
        System.exit(-1)
        0L
      }.get

  private val t1: Long = parseTimeArg(startTimeArg)
  private var tFinal = parseTimeArg(endTimeArg)

  private val props = new Properties()
  props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kSecurityProtocol)
  props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, kServiceName)
  props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kServers)
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "4096")
  props.put(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, "1000000")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  private val zkClientName = "[ZooKeeperClient] "
  private val zkClientConfig = new ZKClientConfig
  private val zkClient =
    KafkaZkClient(
      zkHosts,
      JaasUtils.isZkSaslEnabled,
      30000,
      30000,
      Int.MaxValue,
      Time.SYSTEM,
      zkClientName,
      zkClientConfig)
  if (userjaas)
    props.put(
      SaslConfigs.SASL_JAAS_CONFIG,
      s"""com.sun.security.auth.module.Krb5LoginModule required principal=\"${MSKerberosConfiguration.getUserPrincipal}\" useTicketCache=true;"""
    )

  private val partitions = zkClient.getReplicaAssignmentForTopics(topics.split(",").toSet).keys.toSeq

  private val consumers: Seq[KafkaConsumer[String, String]] = retry(3, 60000L, logger) { () =>
    partitions.map { p =>
      logger.info(s"Creating consumer for partition $p")
      val consumer = new KafkaConsumer[String, String](props)
      consumer.assign(Seq(p).asJava)
      try {
        consumer.offsetsForTimes(Map[TopicPartition, java.lang.Long](p -> t1).asJava).asScala.values.headOption match {
          case None => System.err.println(s"Invalid start time $t1, will attempt to automatically reset the offset")
          case Some(x) if Objects.isNull(x) =>
            logger.warn(s"$poolId No offset timestamp available for partition $p")
          case Some(offsetAndTimestamp) =>
            logger.warn(
              s"$poolId Seeking to offset ${offsetAndTimestamp.offset} on partition $p based on timestamp $t1")
            consumer.seek(p, offsetAndTimestamp.offset)
        }
      } catch {
        case NonFatal(e) =>
          logger.warn(s"Ignoring exception trying to set offsets for $p -> $consumer", e)
      }
      consumer
    }
  } { case _: NullPointerException =>
    true
  }

  private trait QElem {
    def tEnqueued: Long
  }
  private final case class QCrumb(
      uuid: String,
      override val tEnqueued: Long,
      tCrumb: Long,
      tKafka: Long,
      crumb: Map[String, JsValue],
      partition: Int)
      extends QElem
  private class QPing extends QElem { def tEnqueued: Long = -1 }

  private val queue =
    new java.util.concurrent.PriorityBlockingQueue[QElem](
      11,
      (o1: QElem, o2: QElem) => o1.tEnqueued.compare(o2.tEnqueued))

  private def getAs[T: JsonReader](m: Map[String, JsValue], k: String): Option[T] =
    m.get(k).flatMap(x => Try(x.convertTo[T]).toOption)

  private val totalDropped = new AtomicLong(0)
  private val ignored = new AtomicLong(0)
  private val deduplicated = new AtomicLong(0)
  private var printed = 0L
  private var continue = true
  private var badUuids = 0L

  private val dedupCache = Caffeine.newBuilder().maximumSize(nDedupKeys).build[String, Object]()
  private val dummy = new Object
  def isDuplicate(string2JsValue: Map[String, JsValue]): Boolean = {
    getAs[String](string2JsValue, "dedupKey") match {
      case None => false // if there isn't a dedupKey at all, assume not a duplicate
      case Some(dedupKey) =>
        var ret = true;
        dedupCache.get(
          dedupKey,
          { _ =>
            // This closure only runs if the key wasn't found.
            ret = false
            dummy
          })
        if (ret)
          deduplicated.incrementAndGet()
        ret
    }
  }

  private val threads = consumers.map { consumer =>
    new Thread {
      override def run(): Unit = {
        var skipping = false
        var nDropped = 0
        while (continue) {
          try {
            val consumerRecords = consumer.poll(Duration.ofMillis(1000L))
            if (queue.size() > 50000) {
              if (!skipping) {
                warn(s"$poolId $consumer dropping records queue size ${queue.size}")
              }
              skipping = true
              val n = consumerRecords.count()
              nDropped += n
              totalDropped.addAndGet(n)
            } else {
              if (skipping) {
                warn(s"$poolId $consumer dropped $nDropped records due to  queue size")
                skipping = false
                nDropped = 0
              }
              consumerRecords.iterator().asScala.foreach { s: ConsumerRecord[String, String] =>
                for (
                  stringToJsValue <- parseRecord(s);
                  tKafka = s.timestamp();
                  uuidFull <- getAs[String](stringToJsValue, "uuid")
                    .flatMap(uuidRegex.unapplySeq)
                    .flatMap(_.headOption);
                  tCrumb <- getAs[Long](stringToJsValue, "t") if !isDuplicate(stringToJsValue)
                ) {
                  val rootUuid = uuidFull.split("#").head
                  if (rootUuid.length == 0) {
                    badUuids += 1
                  } else if (
                    rootUuid.startsWith(prefix) &&
                    !stringToJsValue.contains(P.crumbplexerIgnore.name)
                    && (hashPoolSize == 0 || (Math.abs(rootUuid.hashCode) % hashPoolSize) == hashPoolId)
                    && !isBadSeed(rootUuid)
                  ) {
                    getAs[String](stringToJsValue, "event").filter(killPastEventRegex.unapplySeq(_).isDefined).foreach {
                      event =>
                        warn(s"$poolId Truncating $rootUuid due to $event event", rootUuid)
                        badSeeds.put(uuidFull, event)
                    }
                    if (truncateLines > 0) {
                      val count = counts.get(rootUuid, () => Long.box(0))
                      if (count > truncateLines) {
                        warn(s"$poolId Truncating $rootUuid at $count", rootUuid)
                        badSeeds.put(rootUuid, "truncated")
                      }
                    }
                    val tEnqeued = System.currentTimeMillis()
                    queue.offer(QCrumb(rootUuid, tEnqeued, tCrumb, tKafka, stringToJsValue, s.partition()))
                  } else {
                    ignored.incrementAndGet()
                  }
                  if (
                    (tCrumb > tFinal && tKafka > tFinal) && (tCrumb > (tFinal + endSlop) && tKafka > (tFinal + endSlop))
                  )
                    continue = false
                }
              }
            }
          } catch {
            case e: Exception =>
              error(s"$poolId $consumer poll failed", "", e)
              Thread.sleep(10000)
          }
        }
      }
    }
  }

  threads.foreach { t =>
    t.setDaemon(true)
    t.start()
  }

  val plexDirOpt = plexDirName.map(Paths.get(_))
  plexDirOpt.foreach { dir =>
    Files.createDirectories(dir)
    val timer = new Timer
    timer.schedule(
      new TimerTask {
        override def run(): Unit = {
          queue.offer(new QPing)
        }
      },
      0L,
      flushAfterMs)
  }

  private def cleanUp(): Unit = {
    val t = System.currentTimeMillis()
    val s0 = files.size
    var nFlushed = 0
    var nClosed = 0
    val i = files.entrySet().iterator()
    while (i.hasNext) {
      val fe @ FileEntry(uuid, f, tPrinted, tFlushed) = i.next().getValue
      val dt = t - tPrinted
      if (dt > closeAfterMs) {
        logger.debug(s"$poolId Closing $uuid dt=$dt")
        f.flush()
        f.close()
        nClosed += 1
        i.remove()
      } else if (dt > flushAfterMs && tFlushed < tPrinted) {
        logger.trace(s"$poolId Flushing $uuid dt=$dt")
        f.flush()
        fe.tFlushed = t
        nFlushed += 1
      }
    }
    val t1 = System.currentTimeMillis()
    val s1 = files.size
    logger.debug(s"$poolId Closed $nClosed, flushed $nFlushed, $s0 $s1 in ${t1 - t}")
  }

  private var tNextLog = 0L
  private val rootUuids = new ConcurrentHashMap[String, Boolean];
  private var tLastDrained = System.currentTimeMillis()
  private val es = new util.ArrayList[QElem](mergeQueueSize * 10)
  // Give the queue some time to fill up, for better sorting.
  if (queue.size < mergeQueueSize)
    Thread.sleep(mergeWait)
  while (continue) {
    val tNow = System.currentTimeMillis()
    val e1 = queue.poll(mergeWait, TimeUnit.MILLISECONDS)
    if (e1 eq null) {
      if ((tNow - tLastDrained) > quiescenceTimeout)
        continue = false
    } else {
      es.clear()
      queue.drainTo(es, mergeQueueSize * 10)
      tLastDrained = tNow
      if (es.size < mergeQueueSize / 2)
        Thread.sleep(mergeWait)
      val esl: List[QElem] =
        e1 :: es.asScala.toList
      for (e <- esl) e match {
        case QCrumb(uuid, tEnqueued, tCrumb, tKafka, m, partition) =>
          val uuidPath = Paths.get(uuid)
          // In theory, a UUID can be anything.  We just need to make sure it can be treated as a simple file name.
          if (uuidPath.getNameCount != 1 || uuidPath.getName(0).startsWith(".")) {
            badSeeds.put(uuid, "badcount")
            logger.warn(s"Ignoring improper file name $uuid")
          } else if (tCrumb <= tFinal) {
            val t = System.currentTimeMillis()
            rootUuids.put(uuid, true)
            plexDirOpt match {
              case Some(plexDir) =>
                try {
                  val fe @ FileEntry(_, f, _, _) = files.computeIfAbsent(
                    uuid,
                    { _ =>
                      logger.debug(s"$poolId $partition $uuid")
                      FileEntry(uuid, new PrintWriter(new FileWriter(plexDir.resolve(uuidPath).toFile, true)), t, t - 1)
                    })
                  if (asJson) f.println /* deliberately printing to file */ (prefix + m.toJson.toString)
                  else f.println /* deliberately printing to file */ (prefix + m.toString)
                  fe.tPrinted = t
                  printed += 1
                } catch {
                  case t: Throwable =>
                    error(s"Error opening file for $e", uuid, t)
                    Thread.sleep(60000)
                }
              case None =>
                if (!quiet) {
                  val prefix = if (enrich) {
                    val flags: Int =
                      getAs[String](m, "event").flatMap(importantEventRegex.unapplySeq(_)).fold(0)(_ => 1)
                    s"CRUMB $uuid ${tUTC(tCrumb)} $poolId $flags "
                  } else ""
                  printed += 1
                  if (asJson) {
                    val json = m.toJson
                    println /* deliberately printing to stdout in CLI app */ (prefix + json.toString)
                  } else
                    println /* deliberately printing to stdout in CLI app */ (prefix + m.toString)
                }
            }
            val tnow = System.currentTimeMillis()
            if (status > 0 && tNow > tNextLog) {
              tNextLog = tNow + status * 1000L
              val liveRoots = rootUuids.size
              rootUuids.clear()
              val dtCrumb = tnow - tCrumb
              val dtKafka = tnow - tKafka
              val dtQueue = tnow - tEnqueued
              BaseSamplers.setCounter(P.plexerCountPrinted, printed)
              BaseSamplers.setCounter(P.plexerCountDropped, totalDropped.get)
              BaseSamplers.setCounter(P.plexerCountDeduped, deduplicated.get)
              BaseSamplers.setCounter(P.plexerCountParseError, parseErrors.get)
              BaseSamplers.setGauge(P.plexerSnapRootCount, liveRoots)
              BaseSamplers.setGauge(P.plexerSnapQueueSize, queue.size)
              BaseSamplers.setGauge(P.plexerSnapLagClient, dtCrumb)
              BaseSamplers.setGauge(P.plexerSnapLagKafka, dtKafka)
              BaseSamplers.setGauge(P.plexerSnapLagQueue, dtQueue)
              info(
                s"$poolId printed=$printed dropped=$totalDropped ignored=$ignored deduplicated=$deduplicated misparsed=$parseErrors badUuids=$badUuids qs=${queue.size} dtCrumb=$dtCrumb dtKafka=$dtKafka dtQueue=$dtQueue roots=$liveRoots")
            }
          }
          if (waitAfterComplete > 0 && getAs[String](m, "event").contains(Events.AppCompleted.name)) {
            tFinal = tCrumb + waitAfterComplete
            quiescenceTimeout = waitAfterComplete
          }

        case _: QPing =>
          cleanUp()
      }
    }
  }
  if (Objects.nonNull(plexDirName ne null)) cleanUp()

  if (bufferOut)
    System.out.flush()

  System.exit(0)

}
