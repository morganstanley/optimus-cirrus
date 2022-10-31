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
package optimus.breadcrumbs.kafka

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

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import kafka.zk.KafkaZkClient
import optimus.breadcrumbs.crumbs.Events
import optimus.breadcrumbs.util.LimitedSizeConcurrentHashMap
import optimus.utils.Args4JOptionHandlers.StringHandler
import optimus.utils.MiscUtils.retry
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.{Option => ArgOption}
import org.slf4j.LoggerFactory
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Try

class KafkaListenerArgs {
  val topicsDefault = "crumbs,dist"
  @ArgOption(name = "--topics", usage = "List of Kafka topics to subscribe to (comma-separated) default: crumbs,dist")
  val topics: String = topicsDefault

  @ArgOption(name = "--zookeeper", usage = "Zookeeper host list (comma-separated)")
  val zkHosts = ""

  @ArgOption(name = "--bootstrap", usage = "Bootstrap Kafka hosts (comma-separated)")
  val kServers = ""

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

  @ArgOption(name = "--progress", usage = "Print update to stderr every N crumbs.")
  val progress = 0

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
  val plexDir: Option[String] = None

  val flushAfterMsDefault = 5000
  @ArgOption(name = "--flushafter", usage = "If writing plex files, flush after ms of inactivity, default 5000")
  val flushAfterMs: Int = flushAfterMsDefault

  val closeAfterMsDefault = 60000
  @ArgOption(
    name = "--closeafter",
    usage = "If writing plex files, close (potentially to be reopened later) after ms of inactivity, default 60000")
  val closeAfterMs: Int = closeAfterMsDefault

  @ArgOption(name = "--help", aliases = Array("-h"), usage = "Help.")
  val help = false

}

trait KafkaRecordParserT {
  protected val logger = LoggerFactory.getLogger(this.getClass)
  protected val lastFailedParseTimeAndWaitMs: LimitedSizeConcurrentHashMap[Class[_], (Instant, Long)]
  protected val maxLastFailedParseWaitMs: Long
  protected def logError(msg: String): Unit = {
    logger.error(msg)
  }
  protected def parseRecord(s: ConsumerRecord[String, String]): Try[Map[String, JsValue]] =
    Try {
      s.value.take(100000).parseJson.convertTo[Map[String, JsValue]]
    }.recoverWith { case t: Throwable =>
      val (lastFailedParseTime, waitMs): (Instant, Long) =
        lastFailedParseTimeAndWaitMs.getOrElse(t.getClass, (Instant.EPOCH, -1))
      val nowMs: Instant = patch.MilliInstant.now
      val elapsedMs: Long = nowMs.toEpochMilli - lastFailedParseTime.toEpochMilli
      val msg: String = s"Failed to parse ${s.value.take(1000)} $t"
      val didLogError: Boolean = if (elapsedMs > waitMs) {
        logError(msg)
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

object KafkaListener extends App with KafkaRecordParserT {
  val cli = new KafkaListenerArgs
  val parser = new CmdLineParser(cli)
  def usage(): Unit = parser.printUsage(System.err)
  try {
    parser.parseArgument(args: _*)
  } catch {
    case e: CmdLineException =>
      logger.info(e.getMessage)
      usage()
      System.exit(1)
  }
  import cli._
  if (help) {
    usage()
    System.exit(0)
  }

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
        logger.error(s"Unable to parse $timeArg: $e")
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

  private val zkClient = KafkaZkClient(zkHosts, JaasUtils.isZkSaslEnabled, 30000, 30000, Int.MaxValue, Time.SYSTEM)
  private val partitions = zkClient.getReplicaAssignmentForTopics(topics.split(",").toSet).keys.toSeq

  private val consumers: Seq[KafkaConsumer[String, String]] = retry(3, 60000L, logger) { () =>
    partitions.map { p =>
      val c = new KafkaConsumer[String, String](props)
      c.assign(Seq(p).asJava)
      c.offsetsForTimes(Map[TopicPartition, java.lang.Long](p -> t1).asJava).asScala.values.headOption match {
        case None => System.err.println(s"Invalid start time $t1, will attempt to automatically reset the offset")
        case Some(offsetAndTimestamp) =>
          System.err.println(s"Seeking to offset ${offsetAndTimestamp.offset} on partition $p based on timestamp $t1")
          c.seek(p, offsetAndTimestamp.offset)
      }
      c
    }
  } { case _: NullPointerException =>
    true
  }

  private trait QElem {
    def t: Long
  }
  private final case class QCrumb(
      uuid: String,
      override val t: Long,
      tKafka: Long,
      crumb: Map[String, JsValue],
      partition: Int)
      extends QElem
  private class QPing extends QElem { def t: Long = -1 }

  private val queue =
    new java.util.concurrent.PriorityBlockingQueue[QElem](11, (o1: QElem, o2: QElem) => o1.t.compare(o2.t))

  private def getAs[T: JsonReader](m: Map[String, JsValue], k: String): Option[T] =
    m.get(k).flatMap(x => Try(x.convertTo[T]).toOption)

  // For ms increments of
  // 100, 200, 400, 800, 1600, .. maxLastFailedParseWaitMs
  // If at least 2x the max wait has elapsed, then reset to 100ms.
  private[this] val maxFailedParseExceptionCount: Int =
    Integer.parseInt(System.getProperty("optimus.breadcrumbs.maxFailedParseExceptionCount", "4"))
  override protected val lastFailedParseTimeAndWaitMs: LimitedSizeConcurrentHashMap[Class[_], (Instant, Long)] =
    new LimitedSizeConcurrentHashMap[Class[_], (Instant, Long)](maxFailedParseExceptionCount)
  override protected val maxLastFailedParseWaitMs: Long =
    Integer.parseInt(System.getProperty("optimus.breadcrumbs.maxLastFailedParseWaitMs", "6400")).toLong

  private var iPrinted = 0L
  private var continue = true
  private var tLastRead = System.currentTimeMillis()
  private var tLastLogged = Long.MaxValue
  private var tLastKafka = Long.MaxValue
  private var ignored = 0L
  private var bad = 0L
  private val threads = consumers.map { c =>
    new Thread {
      override def run(): Unit = {
        while (continue) {
          while (queue.size > 50000) Thread.sleep(100)
          val crs = c.poll(Duration.ofMillis(1000L))
          crs.iterator().asScala.foreach { s =>
            for (
              m <- parseRecord(s);
              tKafka = s.timestamp();
              uuidFull <- getAs[String](m, "uuid").flatMap(uuidRegex.unapplySeq).flatMap(_.headOption);
              tLogged <- getAs[Long](m, "t")
            ) {
              val uuid = uuidFull.split("#").head
              if (uuid.length == 0) {
                bad += 1
              } else if (
                uuid.startsWith(prefix)
                && (hashPoolSize == 0 || (Math.abs(uuid.hashCode) % hashPoolSize) == hashPoolId)
                && !isBadSeed(uuid)
              ) {
                getAs[String](m, "event").filter(killPastEventRegex.unapplySeq(_).isDefined).foreach { event =>
                  logger.info(s"Truncating $uuid due to $event event")
                  badSeeds.put(uuidFull, event)
                }
                if (truncateLines > 0) {
                  val count = counts.get(uuid, () => Long.box(0))
                  if (count > truncateLines) {
                    logger.info(s"Truncating $uuid at $count")
                    badSeeds.put(uuid, "truncated")
                  }
                }
                queue.offer(QCrumb(uuid, tLogged, tKafka, m, s.partition()))
              } else {
                ignored += 1
                if (progress > 0 && (ignored % progress) == 0) {
                  val tRead = System.currentTimeMillis()
                  val tsLogged = tUTC(tLogged)
                  if (tRead > tLastRead) {
                    val rRead = progress / (tRead - tLastRead)
                    val rLogged = if (tLogged > tLastLogged) progress / (tLogged - tLastLogged) else 0
                    tLastLogged = tLogged
                    tLastRead = tRead
                    logger.info(
                      s"$hashPoolId Printed=$iPrinted Ignored=$ignored Bad=$bad age=${tRead - tLogged} $tsLogged $rLogged $rRead qs=${queue.size}")
                  }
                }
              }
              if (
                (tLogged > tFinal && tKafka > tFinal) && (tLogged > (tFinal + endSlop) && tKafka > (tFinal + endSlop))
              )
                continue = false
            }
          }
        }
      }
    }
  }

  threads.foreach { t =>
    t.setDaemon(true)
    t.start()
  }

  if (plexDir.isDefined) {
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
        logger.debug(s"Closing $uuid dt=$dt")
        f.flush()
        f.close()
        nClosed += 1
        i.remove()
      } else if (dt > flushAfterMs && tFlushed < tPrinted) {
        logger.trace(s"Flushing $uuid dt=$dt")
        f.flush()
        fe.tFlushed = t
        nFlushed += 1
      }
    }
    val t1 = System.currentTimeMillis()
    val s1 = files.size
    logger.info(s"$hashPoolId Closed $nClosed, flushed $nFlushed, $s0 $s1 in ${t1 - t}")
  }

  private var t0 = System.currentTimeMillis()
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
      val esl = e1 :: es.asScala.toList
      for (e <- esl) e match {
        case QCrumb(uuid, tLogged, tKafka, m, _) =>
          if (tLogged <= tFinal) {
            val t = System.currentTimeMillis()
            if (plexDir.isDefined) {
              try {
                val fe @ FileEntry(_, f, _, _) = files.computeIfAbsent(
                  uuid,
                  { u =>
                    logger.debug(s"Opening $u")
                    FileEntry(u, new PrintWriter(new FileWriter(s"${plexDir.get}/$u", true)), t, t - 1)
                  })
                if (asJson) f.println /* deliberately printing to stdout in CLI app */ (prefix + m.toJson.toString)
                else f.println /* deliberately printing to stdout in CLI app */ (prefix + m.toString)
                fe.tPrinted = t
              } catch {
                case t: Throwable =>
                  logger.error(s"Error opening file for $e", t)
              }
            } else if (!quiet) {
              val prefix = if (enrich) {
                val flags: Int = getAs[String](m, "event").flatMap(importantEventRegex.unapplySeq(_)).fold(0)(_ => 1)
                s"CRUMB $uuid ${tUTC(tLogged)} $hashPoolId $flags "
              } else ""
              if (asJson) {
                val json = m.toJson
                println /* deliberately printing to stdout in CLI app */ (prefix + json.toString)
              } else
                println /* deliberately printing to stdout in CLI app */ (prefix + m.toString)
            }
            iPrinted += 1
            if (progress > 0 && (iPrinted % progress) == 0) {
              val tnow = System.currentTimeMillis()
              val ts = tUTC(tLogged)
              if (tNow > t0 && tKafka > tLastKafka) {
                logger.info(
                  s"$hashPoolId Printed $ts $iPrinted ign=$ignored bad=$bad read=${progress / (tnow - t0)} write=${progress / (tKafka - tLastKafka)} qs=${queue.size}")
              }
              t0 = tnow
              tLastKafka = tKafka
            }
          }
          if (waitAfterComplete > 0 && getAs[String](m, "event").contains(Events.AppCompleted.name)) {
            tFinal = tLogged + waitAfterComplete
            quiescenceTimeout = waitAfterComplete
          }

        case _: QPing =>
          cleanUp()
      }
    }
  }
  if (plexDir ne null) cleanUp()

  if (bufferOut)
    System.out.flush()

  System.exit(0)

}
