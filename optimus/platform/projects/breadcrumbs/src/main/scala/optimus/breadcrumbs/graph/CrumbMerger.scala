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
package optimus.breadcrumbs.graph

import java.io.FileInputStream
import java.io.InputStream
import java.time.Instant
import java.time.ZoneId
import java.net.URL

import org.kohsuke.args4j.{Option => ArgOption}
import spray.json._
import DefaultJsonProtocol._

import scala.collection.mutable
import scala.io.Source
import scala.util.Try
import java.time.ZonedDateTime
import java.util.zip.GZIPInputStream

import msjava.base.util.uuid.MSUuid
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.EdgeType
import optimus.breadcrumbs.crumbs.Events
import optimus.breadcrumbs.crumbs.Crumb.Headers
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.JsonImplicits._
import org.slf4j.LoggerFactory
import spray.json.JsValue
import optimus.utils.Args4JOptionHandlers.StringHandler
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer

class CrumbMergerArgs {
  @ArgOption(name = "--aftercomplete")
  var waitAfterComplete = 1000l

  @ArgOption(name = "--completion")
  var completionRegexString = "Completed|AppCompleted|RuntimeShutDown|DalClientRespRcvd"

  @ArgOption(name = "--aftersilence")
  var waitAfterSilence: Long = 3600 * 1000l

  @ArgOption(name = "--progress")
  var progress = 0l

  @ArgOption(name = "--input", handler = classOf[StringHandler])
  var name: Option[String] = None

  @ArgOption(name = "--gzip")
  var gzip = false

  @ArgOption(name = "--pretty")
  var pretty = false

  @ArgOption(name = "--debug")
  var debug = false

  @ArgOption(name = "--cleanup")
  var cleanup = 1000

  @ArgOption(name = "--quiet")
  var quiet = false

  @ArgOption(name = "--infervuid")
  var inverVuid = false

}

class CrumbMerger(args: CrumbMergerArgs = new CrumbMergerArgs) {
  import CrumbMerger._
  import args._

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  type VertexType = Map[String, JsValue]

  private val completionRegex = completionRegexString.r

  implicit class MapStringToJsonOps(m: VertexType) {
    def getAs[T: JsonReader](k: String): Option[T] = m.get(k).flatMap(x => Try(x.convertTo[T]).toOption)
    def getOrElseAs[T: JsonReader](k: String, default: T): T =
      m.get(k).flatMap(x => Try(x.convertTo[T]).toOption).getOrElse(default)
    def mut(f: VertexType => VertexType): VertexType = f(m)
  }

  private val vertices = mutable.Map.empty[String, Vertex]
  private val vertexUpdateTimes = mutable.Map.empty[String, Long]
  private val vertexCompletionTimes = mutable.Map.empty[String, Long]
  private val vertexMaps = vertices :: vertexUpdateTimes :: vertexCompletionTimes :: Nil

  private var nVertex = 0l
  private var purged = 0l
  private var i = 0l

  // Look for a vertex that we already know about, and create a  new one if necessary.
  private def getExisting(vuid: String, uuid: String, t: Long): Vertex =
    vertices
      .get(vuid)
      .fold {
        // Create brand new vertex
        nVertex = nVertex + 1
        vertexUpdateTimes += ((vuid, t))
        val v = new Vertex(uuid, vuid, t)
        vertices += ((vuid, v))
        v
      } { vertex =>
        vertex.withTime(t)
      }

  // Note: The .toMap is necessary, and (for some resason) breakOut won't compile.
  private def toMergeable(crumb: Map[String, JsValue], src: Option[String]): Map[Properties.Key[_], JsValue] =
    (crumb - ("crumb", "uuid", "vuid", "t", "src", "tUTC", "replicaFrom"))
      .map(e => (Properties.stringToKey(e._1, src), e._2))
      .toMap

  private def lt(c1: Vertex, c2: Vertex) = c1.tFirst.isBefore(c2.tFirst)

  // Insert child edges in parent and parent edges in child.
  private def insertEdges(
      edge: String)(child: Vertex, vuid: String, uuid: String)(parent: Vertex, pvuid: String, puuid: String): Unit = {
    vertices(pvuid) = parent.copy(children = parent.children + Vertex.Edge(EdgeType.withName(edge), child.vuid))
    vertices(vuid) = child.copy(parents = child.parents + Vertex.Edge(EdgeType.withName(edge), parent.vuid))
  }

  private def process(in: InputStream, f: Vertex => Unit): Unit = {

    Source.fromInputStream(in).getLines.foreach { line =>
      for (crumb: Map[String, JsValue] <- Try(line.parseJson.convertTo[Map[String, JsValue]]);
           // Actual id deriving from the publishing app
           appVuid <- crumb.getAs[String](Headers.VertexId);
           appUuid <- crumb.getAs[String](Headers.Uuid);
           // Special treatment of replicas, which should have original ownership restored
           replica = crumb.getAs[ChainedID]("replicaFrom");
           vuid <- replica.map(_.vertexId) orElse Some(appVuid);
           uuid <- replica.map(_.repr) orElse Some(appUuid);
           t <- crumb.getAs[Long](Headers.Time);
           ctype <- crumb.getAs[String]("crumb");
           src = crumb.getAs[String]("src")) {
        val zdt = toZDT(t)
        i = i + 1
        if (progress > 0 && (i % progress) == 0)
          logger.info(s"Processed $i into ${vertices.size}, purged $purged")
        vertexUpdateTimes += ((vuid, t))
        val existingVertex = getExisting(vuid, uuid, t)

        // If this crumb is a replica, ensure a link from it to the app vertex
        if (replica.isDefined) {
          val existingAppVertex = getExisting(appVuid, appUuid, t)
          insertEdges(EdgeType.ReplicatedTo.toString)(existingAppVertex, appVuid, appUuid)(existingVertex, vuid, uuid)
        }

        if (ctype == "Event") {
          // Events get stored in the event sequence, and as properties
          crumb.getAs[String]("event").foreach { e =>
            val ev = Events.parseEvent(e, crumb.getAs[String](Headers.Source))
            vertices(vuid) = existingVertex.copy(
              events = existingVertex.events :+ (zdt, ev),
              properties = existingVertex.properties + (ev -> ev.toJson(zdt)))
            if (completionRegex.unapplySeq(e).isDefined)
              vertexCompletionTimes(vuid) = t
          }
        } else if (ctype == "Edge" || ctype == "Host") {
          for (edge <- crumb.getAs[String]("edgeType");
               pvuid <- crumb.getAs[String]("pvuid");
               puuid <- crumb.getAs[String]("parent")) {
            val existingParentVertex = getExisting(pvuid, puuid, t)
            insertEdges(edge)(existingVertex, vuid, uuid)(existingParentVertex, pvuid, puuid)
          }
        }

        // log-like crumbs simply get appended to a sequence
        else if (crumb.getAs[String]("_mappend").contains("append")) {
          vertices(vuid) = existingVertex.copy(logs = existingVertex.logs :+ (zdt, toMergeable(crumb, src)))
        }

        // while most crumbs are merged in with overwrite semantics
        else {
          vertices(vuid) = existingVertex.copy(properties = existingVertex.properties ++ toMergeable(crumb, src))
        }

        if (debug) {
          val newVertex = vertices(vuid)
          logger.info(s"$i Existing $existingVertex \n$i New $newVertex")
        }

        if ((i % cleanup) == 0) {
          val done: Seq[String] = vertexCompletionTimes
            .filter {
              case (_, tc) => tc < (t - cleanup)
            }
            .keys
            .toSeq
          if (done.nonEmpty) {
            purged += done.size
            done.map(vertices).sortWith(lt).foreach(f)
            for (vuid <- done; m <- vertexMaps) m -= vuid
          }
        }
      }
    }
    vertices.values.toSeq.sortWith(lt).foreach(f)
  }

  def merge(in: InputStream, gzip: Boolean): Iterable[Vertex] = {
    val vertices = ArrayBuffer.empty[Vertex]
    val links = mutable.Map.empty[MSUuid, Vertex]
    val orphans = mutable.Map.empty[String, Vertex]
    val leaves = mutable.Map.empty[String, Vertex]

    process(
      if (gzip) new GZIPInputStream(in) else in,
      v => {
        vertices += v
        links += ((v.vuid, v))
        if (v.parents.isEmpty)
          orphans += ((v.uuid.toString, v))
        if (v.children.isEmpty)
          leaves += ((v.uuid.toString, v))
      }
    )

    // Look for uuid matches between orphans and leaves.  This should not be necessary if vuids are correctly propagated
    if (args.inverVuid) {
      for ((uuid, vc) <- orphans; // child
           vp <- leaves.get(uuid)) {
        // parent
        val tFirst = if (vc.tFirst.isBefore(vp.tFirst)) vc.tFirst else vp.tFirst
        val tLast = if (vc.tLast.isAfter(vp.tLast)) vc.tLast else vp.tLast
        val events = (vc.events ++ vp.events).sortBy(_._1.toInstant.toEpochMilli)
        val children = vc.children ++ vp.children
        val parents = vc.children ++ vp.children
        val logs = (vc.logs ++ vp.logs).sortBy(_._1.toInstant.toEpochMilli)
        val properties = vc.properties ++ vp.properties
        val v = Vertex(vp.uuid, vp.vuid, tFirst, tLast, events, children, parents, logs, properties)
        // Both vuids now point to same vertex
        links(vc.vuid) = v
        links(vp.vuid) = v
      }
    }

    Vertex.connect(vertices)
    vertices

  }

  def merge(url: URL, gzip: Boolean = false): Iterable[Vertex] = merge(url.openStream(), gzip)

  // Attempt to handle either file names or URLs, with auto-unzipping.
  def merge(name: String): Iterable[Vertex] = {
    val gzip = name.endsWith(".gz")
    if (name.matches("\\w+:\\/\\S+$"))
      merge(new URL(name), gzip)
    else
      merge(new FileInputStream(name), gzip)
  }
}

object CrumbMerger {
  def apply() = new CrumbMerger
  def toZDT(t: Long): ZonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(t), ZoneId.of("UTC"))
}

object CrumbMergerApp extends App {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val cli = new CrumbMergerArgs
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

  val cm = new CrumbMerger(cli)
  val vs = cli.name.fold(cm.merge(System.in, cli.gzip))(cm.merge)
  val json = vs.toJson
  if (cli.pretty)
    println(json.prettyPrint)
  else
    println(json.compactPrint)

}
