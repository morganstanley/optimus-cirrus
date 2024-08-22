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
package optimus.profiler

import java.io.File
import java.io.FileWriter
import java.io.Writer
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.OGTrace
import optimus.graph.OGTraceReader
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.pgo.Profiler.autoGenerateConfig
import optimus.graph.diagnostics.pgo.AutoPGOThresholds
import optimus.graph.diagnostics.pgo.ConfigWriterSettings
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import org.kohsuke.args4j.spi.ExplicitBooleanOptionHandler
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import optimus.scalacompat.collection._

class TraceExplorerCmdLine {
  import org.kohsuke.args4j.Option

  @Option(
    name = "-s",
    aliases = Array("--stats"),
    handler = classOf[ExplicitBooleanOptionHandler],
    usage = "show statistics breakdown of ogtrace file contents")
  val doStats: Boolean = true

  @Option(name = "-h", aliases = Array("--hotspots"), usage = "reconstruct hotspots based on ogtrace")
  val doHotspots: Boolean = false

  @Option(name = "-d", aliases = Array("--dot"), usage = "generate a .dot file with the complete call graph")
  val doDot: Boolean = false

  @Option(name = "-c", aliases = Array("--csv"), usage = "generate CSV files with properties, nodes, and edges")
  val doCSV: Boolean = false

  @Option(name = "-o", aliases = Array("--config"), usage = "generate Optimus Cache Config file")
  val configName: String = ""

  @Option(
    name = "-f",
    aliases = Array("--file"),
    handler = classOf[StringArrayOptionHandler],
    required = false,
    usage = "ogtrace file(s) (e.g. from --profile-graph fulltrace)"
  )
  val fileNames: Array[String] = Array[String]()
}

private[optimus] object TraceHelper {
  def getReaderFromFile(file: String): OGTraceReader =
    if (Files.exists(Paths.get(file))) OGTrace.readingFromFile(file)
    else throw new IllegalArgumentException(s"file $file does not exist.")

  def autoGenerateConfigFromReaders(readers: Seq[OGTraceReader], configName: String, cfg: AutoPGOThresholds): Unit = {
    val settings = ConfigWriterSettings(thresholds = cfg)
    autoGenerateConfig(configName, profileDataFromTraces(readers), settings)
  }

  def getReadersFromFiles(fileNames: Seq[String]): Seq[(String, OGTraceReader)] =
    fileNames.map(file => (file, getReaderFromFile(file)))

  private def profileDataFromTraces(readers: Seq[OGTraceReader]): Seq[PNodeTaskInfo] =
    readers.flatMap(_.getHotspots.asScala)
}

/**
 * This application can load an ogtrace file saved after a --profile-graph fulltrace
 *
 * Currently it reads it and summarizes its contents.
 */
object TraceExplorer extends App {
  import TraceHelper._

  lazy val log = getLogger(this)
  lazy val cmdLine = new TraceExplorerCmdLine
  lazy val parser = new CmdLineParser(cmdLine)
  try {
    parser.parseArgument(args.toArray: _*)
  } catch {
    case x: CmdLineException =>
      log.info(x.getMessage)
      parser.printUsage(System.err)
      System.exit(1)
  }

  lazy val readers = getReadersFromFiles(cmdLine.fileNames)
  var nodeCnt = 0L
  /*
   * 1. Statistics (how many of which kind of what is in the file)
   */
  if (cmdLine.doStats) for ((file, reader) <- readers) {
    log.info(s"File $file contents:")
    log.info(s"${reader.stats.edgeTables} Edge tables holding ${reader.stats.edges} edges")
    log.info(s"${reader.stats.eventTables} Event tables holding " + reader.stats.eventCounts.sum + " events")
    for (eventID <- 1 until reader.stats.eventCounts.length)
      log.info(s"... ${reader.stats.eventCounts(eventID)} events of type ${OGTrace.eventToName(eventID)}")

    var edgesCnt = 0L
    reader.visitNodes((x: PNodeTask, _: PNodeTaskInfo) => {
      if (x ne null) {
        nodeCnt += 1
        if (x.callees ne null) edgesCnt += x.callees.size
      }
    })
    log.info(s"Replaying the trace instantiated $nodeCnt nodes, $edgesCnt outgoing edges")
  }

  /*
   * 2. Reconstructed hotspots
   */
  if (cmdLine.doHotspots) for ((file, reader) <- readers) {
    val pntis: Seq[PNodeTaskInfo] = reader.getHotspots.asScala
    val writer = new FileWriter(file + ".hotspots.csv")
    try { PNodeTaskInfo.printCSV(Map("" -> pntis).mapValuesNow(_.asJava).asJava, 0, writer) }
    finally { writer.close() }
  }

  /*
   * 3. CSV, GraphViz
   */
  def writeCsvDot(prefix: String, trace: OGTraceReader): Unit = {
    import cmdLine._

    def open(suffix: String): Writer = {
      val file = Paths.get(new File(prefix + suffix).toURI)
      Files.createDirectories(file.getParent)
      Files.newBufferedWriter(file, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)
    }
    val (edgesW, nodesW, propsW) =
      if (doCSV) (open(".edges.csv"), open(".nodes.csv"), open(".props.csv"))
      else (null, null, null)
    val dotW = if (doDot) open(".dot") else null

    try {
      val seenNodes = new mutable.LongMap[AnyRef](nodeCnt.toInt)
      val seenProps = new mutable.LongMap[AnyRef](nodeCnt.toInt)
      if (doDot) dotW.write("digraph optimus {\n")
      trace.visitNodes { (node, info) =>
        if (node ne null) {
          if (node.callees ne null) {
            if (doDot) {
              if (!seenNodes.contains(node.id)) {
                seenNodes.update(node.id, null)
                dotW.write(s"""${node.id} [label="${info.fullName}"];${'\n'}""")
              }
            }
            node.callees.forEach { child =>
              if (doCSV) edgesW.write(s"${node.id},${child.id}\n")
              if (doDot) dotW.write(s"${child.id} -> ${node.id};\n")
            }
          }
          if ((nodesW ne null) && (info ne null)) nodesW.write(s"${node.id},${info.id}\n")
        }
        if ((info ne null) && (propsW ne null)) {
          if (!seenProps.contains(info.id)) {
            seenProps.update(info.id, null)
            propsW.write(s"${info.id},${info.fullName}\n")
          }
        }
      }
      if (doDot) dotW.write("}")
    } finally {
      if (doCSV) List(edgesW, nodesW, propsW).foreach(_.close())
      if (doDot) dotW.close()
    }
  }

  readers.foreach((writeCsvDot _).tupled)

  /*
   * Generated cache config
   */
  if (cmdLine.configName.nonEmpty)
    autoGenerateConfigFromReaders(readers.map(_._2), cmdLine.configName, AutoPGOThresholds())

  /*
   * Future direction
   * 1. make output file names configurable
   * 2. neo4j insert (maybe)
   */
}
