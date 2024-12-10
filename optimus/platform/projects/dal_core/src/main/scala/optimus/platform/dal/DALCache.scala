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
package optimus.platform.dal

import java.io.BufferedInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import com.google.protobuf.CodedInputStream
import com.google.protobuf.CodedOutputStream
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.MessageLite
import optimus.dsi.serialization.json.PropertySerialization
import optimus.graph.DiagnosticSettings
import optimus.platform.dsi.bitemporal.CmdLogLevelHelper
import optimus.platform.dsi.bitemporal.Command
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import optimus.platform.dsi.bitemporal.DSIQueryTemporality.At
import optimus.platform.dsi.bitemporal.DSIQueryTemporality.TxRange
import optimus.platform.dsi.bitemporal.EntityClassQuery
import optimus.platform.dsi.bitemporal.EnumerateKeys
import optimus.platform.dsi.bitemporal.ExpressionQueryCommand
import optimus.platform.dsi.bitemporal.GetInfo
import optimus.platform.dsi.bitemporal.PartialSelectResult
import optimus.platform.dsi.bitemporal.ReadAdminCommand
import optimus.platform.dsi.bitemporal.ReadOnlyCommandWithTT
import optimus.platform.dsi.bitemporal.ReadOnlyCommandWithTTRange
import optimus.platform.dsi.bitemporal.ReadOnlyCommandWithTemporality
import optimus.platform.dsi.bitemporal.ReferenceQuery
import optimus.platform.dsi.bitemporal.Result
import optimus.platform.dsi.bitemporal.RoleMembershipQuery
import optimus.platform.dsi.bitemporal.SelResult
import optimus.platform.dsi.bitemporal.Select
import optimus.platform.dsi.bitemporal.SelectResult
import optimus.platform.dsi.bitemporal.SerializedKeyQuery
import optimus.platform.dsi.bitemporal.TemporalityQueryCommand
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.bitemporal.proto.Dsi.CommandProto
import optimus.platform.dsi.bitemporal.proto.Dsi.ResultProto
import optimus.platform.dsi.bitemporal.proto.Dsi.VersioningResultProto
import optimus.platform.relational.ExtraCallerDetail
import optimus.platform.storable.PersistentEntity
import optimus.platform.util.Log
import optimus.utils.datetime.ZoneIds
import org.apache.commons.codec.binary.Hex

import scala.util.Try

object DALCache {

  // Path must be provided in order to enable the DAL cache
  private val propFolder = System.getProperty("optimus.dal.cache.folder")

  // control which "Commands" are cacheable - some use cases want GetInfo or RoleMembershipQuery cached
  private[optimus] val shouldCacheCommandGetInfo =
    DiagnosticSettings.getBoolProperty("optimus.dal.cache.command.GetInfo", false)
  private[dal] val shouldCacheCommandRoleMembershipQuery =
    DiagnosticSettings.getBoolProperty("optimus.dal.cache.command.RoleMembershipQuery", false)

  // capture Result of any ReadOnlyCommand sent to DAL Broker as a binary file
  private val propCaptureBinary = DiagnosticSettings.getBoolProperty("optimus.dal.cache.capture.binary", true)

  // THIS IS DISABLED until JSON-B format to be used by Postgres is stable
  // capture entity data from "SelResult" in json format (this can be edited and used in replays)
  // private val propCaptureJson = DiagnosticSettings.getBoolProperty("optimus.dal.cache.capture.json", false)

  // record debug information on capture (this will cause significant performance degradation due to interaction with
  // batching of DAL requests)
  private val propCaptureDebugOnCapture =
    DiagnosticSettings.getBoolProperty("optimus.dal.cache.capture.debugOnCapture", false)

  // Record useful debug information about (first) call chain that resulted in a particular ReadOnlyCommand
  // being sent to the DAL Broker.
  // generating debug information is relatively expensive, and this minor cost is enough to seriously
  // alter the performance characteristics of the DAL request batching. It's safe to do here because we're not
  // likely to be making a real DAL request (we'll be loading from the cache).
  // It may be to do with the way the node.waitersToFullMultilineNodeStack works, but this isn't clear.
  // The main impact of generating debug information whilst caching is that there are many, many more small
  // requests sent to the DAL, rather than a few large batched ones.
  private val propCaptureDebugOnReplay =
    DiagnosticSettings.getBoolProperty("optimus.dal.cache.capture.debugOnReplay", false)

  // use captured binary Results instead of sending ReadOnlyCommands to DAL Broker
  private val propReplayBinary = DiagnosticSettings.getBoolProperty("optimus.dal.cache.replay.binary", true)

  // THIS IS DISABLED until JSON-B format to be used by Postgres is stable
  // Override the entity data within "SelResult" using contents of cache json files
  // private val propReplayJson = DiagnosticSettings.getBoolProperty("optimus.dal.cache.replay.json", false)

  val Global: DALCache = create(
    Option(propFolder),
    propCaptureBinary,
    false,
    propCaptureDebugOnCapture,
    propCaptureDebugOnReplay,
    propReplayBinary,
    false)

  /* Some processing of options to allow simpler set up from properties
   * This also makes testing the logic easier */
  def create(
      cacheFolder: Option[String],
      captureBinary: Boolean,
      captureJson: Boolean,
      captureDebugOnCapture: Boolean,
      captureDebugOnReplay: Boolean,
      replayBinary: Boolean,
      replayJson: Boolean): DALCache =
    new DALCache(
      cacheFolder,
      captureBinary || captureJson, // force binary capture if json capture enabled
      captureJson,
      captureDebugOnCapture && (captureBinary || captureJson), // warning - this will impact performance
      captureDebugOnReplay && (replayBinary || replayJson),
      replayBinary || replayJson, // force replay binary if replay json enabled
      replayJson
    )
}

trait DALCacheTrait {
  def dalCacheEnabled: Boolean
  def dalCacheDisabled = !dalCacheEnabled

  def shouldCache(command: Command): Boolean
  def load(command: Command): Option[Seq[Result]]
  def load(
      commands: Seq[Command],
      commandProtos: Seq[CommandProto],
      extraCallerDetailOpt: Option[ExtraCallerDetail]): Option[Seq[Result]]
  def save(
      command: Command,
      commandProto: MessageLite,
      results: Seq[Result],
      resultProtos: Seq[MessageLite],
      extraCallerDetailOpt: Option[ExtraCallerDetail]): Unit
}

class DALCache(
    cacheFolder: Option[String],
    val captureBinary: Boolean,
    val captureJson: Boolean,
    val captureDebugOnCapture: Boolean,
    val captureDebugOnReplay: Boolean,
    val replayBinary: Boolean,
    val replayJson: Boolean)
    extends DALCacheTrait {

  // make this lazy val to avoid initialize the CommandProtoSerializers when DAL cache disabled
  private lazy val impl = new DALCacheImpl(
    cacheFolder,
    captureBinary,
    captureJson,
    captureDebugOnCapture,
    captureDebugOnReplay,
    replayBinary,
    replayJson)

  def dalCacheEnabled: Boolean = cacheFolder.isDefined

  def shouldCache(command: Command): Boolean = impl.shouldCache(command)
  def load(command: Command): Option[Seq[Result]] = impl.load(command)
  def load(
      commands: Seq[Command],
      commandProtos: Seq[CommandProto],
      extraCallerDetailOpt: Option[ExtraCallerDetail]): Option[Seq[Result]] =
    impl.load(commands, commandProtos, extraCallerDetailOpt)
  def save(
      command: Command,
      commandProto: MessageLite,
      results: Seq[Result],
      resultProtos: Seq[MessageLite],
      extraCallerDetailOpt: Option[ExtraCallerDetail]): Unit =
    impl.save(command, commandProto, results, resultProtos, extraCallerDetailOpt)
}

/**
 * responsible for reading and writing Commands and Results in file cache
 */
class DALCacheImpl(
    cacheFolder: Option[String],
    captureBinary: Boolean,
    captureJson: Boolean,
    captureDebugOnCapture: Boolean,
    captureDebugOnReplay: Boolean,
    replayBinary: Boolean,
    replayJson: Boolean)
    extends DALCacheTrait
    with Log
    with CommandProtoSerialization {

  val dalCacheEnabled = cacheFolder.isDefined

  // workaround for complexity with caching GetInfo
  // allows initial GetInfo command to establish DAL connection
  // but GetInfoResult is replaced with cached version before DALCache.Global.save
  @volatile private var preventCacheLookupOnFirstGetInfo = DALCache.shouldCacheCommandGetInfo

  private val dalCacheFolder: File = cacheFolder
    .map { f =>
      val folder = new File(f)
      folder.mkdirs()
      folder
    }
    .getOrElse(new File("DALCache"))

  private val dalCacheFolderTemp: File = cacheFolder
    .map { f =>
      val folder = new File(f, "Temp")
      folder.mkdirs
      folder
    }
    .getOrElse(new File(s"DALCache${File.separator}Temp"))

  // CountGroupings is not cached
  private val cacheableCommands: PartialFunction[Command, _] = {
    // GetAuditInfo and GetSlots
    case _: ReadAdminCommand =>
    // Event related commands
    case _: ReadOnlyCommandWithTT =>
    // no implementations at present
    case _: ReadOnlyCommandWithTTRange =>
    // GetAuditInfo, EnumerateIndices, EnumerateKeys, GetRefsNotAtSlot, SelectSpace, TemporalityQueryCommand,
    // Select, Count
    case _: ReadOnlyCommandWithTemporality =>
    // PriQL expression support
    case _: ExpressionQueryCommand =>
    // Not supported by in-memory DAL, so needs to be cached
    case _: RoleMembershipQuery if (DALCache.shouldCacheCommandRoleMembershipQuery) =>
    // returns server time
    case _: GetInfo if (DALCache.shouldCacheCommandGetInfo) =>
  }

  def shouldCache(command: Command): Boolean = {
    val cacheable = cacheableCommands.isDefinedAt(command)
    if (!cacheable) log.debug(s"Command not cached ${command.getClass} : $command")
    dalCacheEnabled && cacheable
  }

  private def className(ref: Any): String = ref.getClass.getSimpleName
  private def uuidStr(proto: MessageLite): String = UUID.nameUUIDFromBytes(proto.toByteArray).toString
  private val dtf = DateTimeFormatter.ofPattern("yyyyMMdd.HHmmss.nnnnnnnnn")

  // note that Instant.toEpochMilli can throw ArithmeticException due to seconds * 1000 > Long.maxValue
  private def dateStr(instant: Instant): String = dtf.format(ZonedDateTime.ofInstant(instant, ZoneIds.UTC))

  private def temporalityNames(temporality: DSIQueryTemporality): Seq[String] = temporality match {
    case t: At      => Seq(s"TT.${dateStr(t.txTime)}", s"VT.${dateStr(t.validTime)}")
    case t: TxRange => Seq(s"From.${dateStr(t.range.from)}", s"To.${dateStr(t.range.to)}")
    case _          => Seq()
  }

  private def file(fileType: String, command: Command, commandProto: MessageLite): File = {
    val commandName = className(command)
    val uuid = uuidStr(commandProto)
    val uuidFileName = s"UUID.$uuid.$fileType"
    val fallback = Seq(commandName, uuidFileName)

    val parts = command match {
      case c: Select => {
        val qns: Seq[String] = c.query match {
          case q: ReferenceQuery =>
            Seq(s"Ref.${className(q.ref)}", s"Hex.${Hex.encodeHexString(q.ref.data)}.$fileType")
          case q: SerializedKeyQuery =>
            Seq(s"Key.${q.key.typeName}", uuidFileName)
          case q: EntityClassQuery =>
            Seq(s"Entity.${q.classNameStr}.$fileType")
          case _ =>
            Seq(uuidFileName)
        }
        val tns: Seq[String] = temporalityNames(c.temporality)
        Seq(s"Select.${className(c.temporality)}") ++ tns ++ Seq(s"Query.${className(c.query)}") ++ qns
      }
      case c: EnumerateKeys => {
        val tns: Seq[String] = temporalityNames(c.temporality)
        Seq(s"EnumerateKeys.${className(c.temporality)}") ++ tns ++ Seq(
          c.typeName,
          s"${c.propNames.mkString(".")}.$fileType")
      }
      case c: TemporalityQueryCommand => {
        val tns: Seq[String] = temporalityNames(c.temporality)
        Seq(s"TemporalityQueryCommand.${className(c.temporality)}") ++ tns ++ Seq(uuidFileName)
      }
      case _ => fallback
    }

    val targetFile = new File((Seq(dalCacheFolder) ++ parts).mkString(File.separator))
    targetFile.getParentFile.mkdirs
    targetFile
  }

  private def write(data: String, file: File): Unit = write(data.getBytes(StandardCharsets.UTF_8), file)

  private def write(data: Array[Byte], file: File) = {
    val tempFile = new File(dalCacheFolderTemp, s"${UUID.randomUUID()}.tmp")
    Files.write(tempFile.toPath, data)
    if (!tempFile.renameTo(file)) tempFile.delete()
  }

  def load(command: Command): Option[Seq[Result]] = {
    load(Seq(command), Seq(toProto(command)), None)
  }

  def load(
      commands: Seq[Command],
      commandProtos: Seq[CommandProto],
      extraCallerDetailOpt: Option[ExtraCallerDetail]
  ): Option[Seq[Result]] =
    if (dalCacheDisabled || !replayBinary) None
    else {
      if (preventCacheLookupOnFirstGetInfo && commands.size == 1 && commands.head.isInstanceOf[GetInfo]) {
        preventCacheLookupOnFirstGetInfo = false
        None
      } else {
        // dal cache enabled
        val allReadOnly = commands.forall(shouldCache(_))
        // simplified use of cache (only if all commands are read only)
        if (allReadOnly) {
          val files = commands.zip(commandProtos).map { case (command, commandProto) =>
            val binFile = file("data.bin", command, commandProto)
            extraCallerDetailOpt.foreach { extraCallerDetail =>
              if (captureDebugOnReplay && binFile.exists) {
                captureDebug(command, commandProto, extraCallerDetail)
              }
            }
            binFile
          }
          // only if cache files exist for all commands
          if (files.forall(_.exists)) {
            try {
              Some(files.flatMap(decodeResults))
            } catch {
              case e: InvalidProtocolBufferException => {
                log.warn("Failed cache load for " + files, e)
                None
              }
            }
          } else None
        } else None
      }
    }

  private def captureDebug(command: Command, commandProto: MessageLite, extraCallerDetail: ExtraCallerDetail): Unit = {
    val debugFile = file("debug.txt", command, commandProto)
    if (!debugFile.exists) {
      // generate detail file (this can be expensive)
      import scala.util.Properties.{lineSeparator => NewLine}
      val text = s"Command Class: ${command.getClass}$NewLine$NewLine" +
        s"Command: ${command.logDisplay(CmdLogLevelHelper.TraceLevel)}$NewLine$NewLine" +
        extraCallerDetail.generateTraces()
      write(text, debugFile)
    }
  }

  private def decodeResults(file: File): Seq[Result] = ??? /* {
    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
    val tryResult = Try {
      val size = dis.readInt()
      (0 until size).map { i =>
        val entryType = dis.readInt()
        val entryLength = dis.readInt()
        val entryBytes = Array.ofDim[Byte](entryLength)
        dis.read(entryBytes)
        val cis = CodedInputStream.newInstance(entryBytes)

        val result = entryType match {
          case 1 => fromProto(ResultProto.parseFrom(cis))
          case 2 => fromProto(VersioningResultProto.parseFrom(cis))
          case n => throw new IllegalStateException((s"result type $n not recognized in cache file $file"))
        }
        result match {
          case r: SelResult if replayJson => {
            val jsonOpt = loadResultsJson(new File(file.getPath.replaceFirst(".data.bin", ".data.json")))
            jsonOpt
              .map { bigMap =>
                val newValue = r.value.map(pe => updatePersistentEntity(pe, bigMap))
                r match {
                  case r1: SelectResult        => r1.copy(value = newValue)
                  case r2: PartialSelectResult => r2.copy(value = newValue)
                }
              }
              .getOrElse(result)
          }
          case _ => result
        }
      }
    }
    dis.close()
    tryResult.get
  } */

  private def encodePersistentEntity(pe: PersistentEntity): (String, Map[String, Any]) = {
    val se = pe.serialized
    val map = Map[String, Any]() +
      ("class_name" -> se.className) +
      ("tt_from" -> dateStr(pe.txInterval.from)) +
      ("tt_to" -> dateStr(pe.txInterval.to)) +
      ("vt_from" -> dateStr(pe.vtInterval.from)) +
      ("vt_to" -> dateStr(pe.vtInterval.to)) +
      ("props" -> se.properties)
    (pe.versionedRef.toString, map.toMap)
  }

  private def updatePersistentEntity(pe: PersistentEntity, bigMap: Map[String, Map[String, Any]]): PersistentEntity = {
    bigMap
      .get(pe.versionedRef.toString)
      .map(map => {
        val se = pe.serialized.copy(properties = map.get("props").get.asInstanceOf[Map[String, Any]])
        pe.copy(serialized = se)
      })
      .getOrElse(pe)
  }

  private def resultsJson(results: Seq[Result]): Option[String] = {
    val bigMap = results.flatMap { result =>
      result match {
        case r: SelResult => r.value.map(encodePersistentEntity)
        case _            => Seq()
      }
    }.toMap
    if (bigMap.isEmpty) None else Some(PropertySerialization.propertiesToJsonString(bigMap))
  }

  private def loadResultsJson(f: File): Option[Map[String, Map[String, Any]]] = {
    if (f.exists) {
      import scala.jdk.CollectionConverters._
      import scala.util.Properties.{lineSeparator => NewLine}
      val str = Files.readAllLines(f.toPath, StandardCharsets.UTF_8).asScala.mkString(NewLine)
      Some(PropertySerialization.fromEncodedProps(str).asInstanceOf[Map[String, Map[String, Any]]])
    } else {
      None
    }
  }

  def save(
      command: Command,
      commandProto: MessageLite,
      results: Seq[Result],
      resultProtos: Seq[MessageLite],
      extraCallerDetailOpt: Option[ExtraCallerDetail]): Unit = {
    if (captureBinary) {
      val binFile = file("data.bin", command, commandProto)
      if (!binFile.exists) write(encodeResults(results, resultProtos), binFile)
    }
    if (captureJson) {
      val jsonFile = file("data.json", command, commandProto)
      if (!jsonFile.exists) resultsJson(results).foreach(json => write(json, jsonFile))
    }
    extraCallerDetailOpt.foreach { extraCallerDetail =>
      if (captureDebugOnCapture) {
        captureDebug(command, commandProto, extraCallerDetail)
      }
    }
  }

  private def encodeResults(results: Seq[Result], resultProtos: Seq[MessageLite]) = {
    val sizes = resultProtos.map(_.getSerializedSize)

    val os = new ByteArrayOutputStream(4 + (4 * resultProtos.size) + sizes.sum)
    val dos = new DataOutputStream(os)

    dos.writeInt(resultProtos.size)

    resultProtos.foreach { rp =>
      val pType = rp match {
        case _: ResultProto           => 1
        case _: VersioningResultProto => 2
      }

      dos.writeInt(pType)

      val rpBytesOS = new ByteArrayOutputStream(rp.getSerializedSize)
      val rpCos = CodedOutputStream.newInstance(rpBytesOS)
      rp.writeTo(rpCos)
      rpCos.flush()
      val rpBytes = rpBytesOS.toByteArray
      dos.writeInt(rpBytes.length)
      dos.write(rpBytes)
    }
    dos.flush()

    os.toByteArray
  }
}
