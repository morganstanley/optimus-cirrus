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
package optimus.platform.dsi.bitemporal

import optimus.dsi.base.actions.AddBlobAction
import optimus.dsi.base.actions.PutEntityTimeSlice
import optimus.dsi.base.actions.TxnAction
import optimus.dsi.notification.NotificationEntry
import optimus.dsi.partitioning.Partition
import optimus.dsi.pubsub.Subscription
import optimus.dsi.session.ClientSessionInfo
import optimus.dsi.session.EstablishSession
import optimus.dsi.session.EstablishSessionFailure
import optimus.dsi.session.EstablishSessionResult
import optimus.dsi.session.EstablishSessionSuccess
import optimus.entity.EntityAuditInfo
import optimus.graph.DiagnosticSettings
import optimus.platform._
import optimus.platform.dal.NotificationStream
import optimus.platform.dal.NotificationStream.SubscriptionIdType
import optimus.platform.dal.config.DalClientInfo
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.messages.StreamsACLs
import optimus.platform.dal.messages.MessagesNotificationCallback
import optimus.platform.dal.messages.MessagesPayload
import optimus.platform.dal.messages.MessagesPayloadBase
import optimus.platform.dal.messages.MessagesSubscription
import optimus.platform.dal.messages.MessagesTransactionPayload
import optimus.platform.dal.prc.RedirectionReason
import optimus.platform.dal.servicediscovery.ServiceDiscoveryElement
import optimus.platform.dsi.bitemporal.CmdLogLevelHelper._
import optimus.platform.dsi.bitemporal.QueryTemporalityType.QueryTemporalityType
import optimus.platform.dsi.bitemporal.WriteBusinessEvent.PutSlots
import optimus.platform.dsi.bitemporal.proto.FeatureInfo
import optimus.platform.dsi.expressions._
import optimus.platform.dsi.expressions.{Entity => EntityExpression}
import optimus.platform.dsi.expressions.{Event => EventExpression}
import optimus.platform.dsi.expressions.{Select => SelectExpression}
import optimus.platform.dsi.metrics.WriteMetadataOpsStats
import optimus.platform.pickling.ImmutableByteArray
import optimus.platform.storable._
import optimus.scalacompat.collection._

import java.time.Instant
import java.util.Objects
import scala.collection.compat._
import scala.collection.mutable

object CmdLogLevelHelper {
  final val NoLog = ""

  final val TraceLevel = 0
  final val DebugLevel = 1
  final val InfoLevel = 2
  final val WarnLevel = 3
  final val ErrorLevel = 4
  final val NoLogLevel = -1

  @inline final def il(lvl: Int): Boolean = lvl <= InfoLevel
  @inline final def dl(lvl: Int): Boolean = lvl <= DebugLevel

  def iterDisplay[A](
      prefix: String,
      it: Seq[A],
      convFunc: (A) => String = (a: A) => a.toString,
      toShow: Int = 64): String = {
    val sz = it.size
    if (sz <= toShow) (it map convFunc).mkString(s"$prefix(", ",", ")")
    else {
      it.take(toShow).map(convFunc).mkString(s"$prefix(", ",", s" ...${(sz - toShow).toString})")
    }
  }

  def strDisplay(lvl: Int, prefix: String, str: String): String = {
    val lim = if (dl(lvl)) 256 else 64
    val output = if (lim < str.length) s"${str.substring(0, lim)}..." else str
    s"$prefix$output"
  }

}

sealed trait HasClassName {
  def cn: String
}

sealed trait Command {
  def logDisplay(level: Int): String
  def className(): Option[SerializedEntity.TypeRef] = None
}

/*
 * Commands are split into two paths: ReadOnlyCommands that are sent to a read-only broker, and LeadWriterCommands that must be executed on the lead writer
 *
 * LeadWriterCommands:
 *  - WriteCommands:
 *    - Put
 *    - InvalidateAfter
 *    - AssertValid
 *    - PutApplicationEvent
 *    - GeneratedAppEvent
 *    - WriteBusinessEvent (only within a PutApplicationEvent command; never on its own)
 *  - AdminCommands
 *    - InvalidateAllCurrent
 *    - Obliterate
 *    - SystemCommand
 *  - Others:
 *    - ResolveKeys
 *
 * ReadOnlyCommands:
 *
 *
 */

sealed trait ReadOnlyCommand extends Command {
  def logDisplay(level: Int) = if (il(level)) s"[R]${getClass.getSimpleName}" else NoLog
  // 20170914: don't provide default implementation in any base/abstract command so that
  // whenever a new read command is added, we could decide there in the concrete command
  def needStreamingExecution: Boolean
}

sealed trait UnentitledCommand {
  self: ReadOnlyCommand =>
}

sealed trait LeadWriterCommand extends Command {
  // In order to enable partitioning for LeadWriterCommands we need to provide the className implementation to all the
  // commands as much as possible. If any command is added in the future, we would need it to have classNameImpl defined
  // and corresponding logic in PartitionedDsiProxy to resolve partition for that command. Given that the
  // partitionedDsiProxy has list of WriteCommands which is collectively exhaustive so adding any new LeadWriterCommand
  // would lead to a syntax error by default.
  protected def classNameImpl: Option[SerializedEntity.TypeRef]
  final override def className(): Option[SerializedEntity.TypeRef] = classNameImpl
}

trait ValidTimeHolder {
  protected def vt: MicroPrecisionInstant
  final def validTime: Instant = vt.underlying
}

sealed trait WriteCommand extends LeadWriterCommand {
  def logDisplay(level: Int): String = if (il(level)) s"[W]${getClass.getSimpleName}" else NoLog
}

sealed trait WriteCommandWithValidTime extends WriteCommand with ValidTimeHolder

sealed trait AdminCommand extends LeadWriterCommand {
  def auditInfoString: String
  def logDisplay(level: Int): String = if (il(level)) s"[A]${getClass.getSimpleName}" else NoLog
}

sealed trait ReadAdminCommand extends ReadOnlyCommand {
  override def logDisplay(level: Int): String = if (il(level)) s"[RA]${getClass.getSimpleName}" else NoLog
}

sealed trait ReadOnlyCommandWithTemporality extends ReadOnlyCommand {
  def temporality: DSIQueryTemporality
  override def logDisplay(level: Int): String = if (il(level)) s"${super.logDisplay(level)} T:$temporality" else NoLog

  protected def checkIfStreamingQuery(q: Query): Boolean = (q, temporality) match {
    case (_: ReferenceQuery | _: EntityCmReferenceQuery, _: DSIQueryTemporality.At) => false
    case (_: EventCmReferenceQuery, _: DSIQueryTemporality.TxTime)                  => false
    case (SerializedKeyQuery(key, _, _), _: DSIQueryTemporality.At)                 => !key.unique
    case (EventSerializedKeyQuery(key, _), _)                                       => true
    case _                                                                          => true
  }
}

sealed trait ReadOnlyCommandWithTT extends ReadOnlyCommand {
  def tt: Instant
  override def logDisplay(level: Int): String = if (il(level)) s"${super.logDisplay(level)} TT:$tt" else NoLog
}

final case class IsEntitled(cmd: Command) extends ReadOnlyCommand with UnentitledCommand {
  def needStreamingExecution: Boolean = false
}

sealed trait ContainsEntityReference {
  def ref: EntityReference
}

sealed trait ContainsBusinessEventReference {
  def ref: BusinessEventReference
}

sealed trait ReadOnlyCommandWithTTRange extends ReadOnlyCommand {
  def ttRange: TimeInterval
  override def logDisplay(level: Int): String =
    if (il(level)) s"${super.logDisplay(level)} TT:[${ttRange.from},${ttRange.to})" else NoLog
}

sealed trait AccMetadataOp

object AccMetadataOp {
  case object Create extends AccMetadataOp
  case object Drop extends AccMetadataOp
  case object Update extends AccMetadataOp
  case object UpdateAccTxTime extends AccMetadataOp

  def withName(str: String): AccMetadataOp = str match {
    case "Create"          => Create
    case "Drop"            => Drop
    case "Update"          => Update
    case "UpdateAccTxTime" => UpdateAccTxTime
    case "Refresh" =>
      throw new IllegalArgumentException(s"Your accelerator client is out-of-date, please upgrade and try again")
  }
}

private[optimus] final case class AccMetadataCommand(
    clsName: String,
    slot: Int,
    action: AccMetadataOp,
    accTtOpt: Option[Instant],
    accInfoOpt: Option[SerializedEntity],
    forceCreateIndex: Boolean = false)
    extends WriteCommand {
  if (accTtOpt.isEmpty && action != AccMetadataOp.Drop)
    throw new IllegalArgumentException(s"accTxTime should not be empty when action is $action")

  override def logDisplay(level: Int): String = {
    if (il(level)) {
      s"$action table in Postgres for projected entity class $clsName slot $slot with accTt: $accTtOpt"
    } else NoLog
  }

  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = Option(clsName)
}

object Put {
  def apply(
      value: SerializedEntity,
      lockToken: Option[Long],
      validTime: Instant,
      minAssignableTtOpt: Option[Instant] = None,
      monoTemporal: Boolean = false,
      ignoreRefResolve: Boolean = false): Put =
    new Put(
      value,
      MicroPrecisionInstant.ofInstant(validTime),
      lockToken,
      minAssignableTtOpt,
      monoTemporal,
      ignoreRefResolve)
}
final case class Put private (
    value: SerializedEntity,
    vt: MicroPrecisionInstant,
    lockToken: Option[Long],
    minAssignableTtOpt: Option[Instant],
    monoTemporal: Boolean,
    ignoreRefResolve: Boolean
) extends WriteCommandWithValidTime
    with HasClassName {
  def cn = value.className
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = Option(value.className)
  override def logDisplay(level: Int): String = {
    if (il(level)) {
      s"[W]${getClass.getSimpleName} Class: ${value.className} LT:$lockToken Ref: ${value.entityRef} Linkages: ${value.linkages} VT: $validTime"
    } else NoLog
  }
}

object InvalidateAfter {
  def apply(
      entityRef: EntityReference,
      versionedRef: VersionedReference,
      lockToken: Long,
      vt: Instant,
      clazzName: String,
      monoTemporal: Boolean = false): InvalidateAfter =
    new InvalidateAfter(
      entityRef,
      versionedRef,
      MicroPrecisionInstant.ofInstant(vt),
      lockToken,
      clazzName,
      monoTemporal)
}
final case class InvalidateAfter private (
    entityRef: EntityReference,
    versionedRef: VersionedReference,
    vt: MicroPrecisionInstant,
    lockToken: Long,
    clazzName: String,
    monoTemporal: Boolean
) extends WriteCommandWithValidTime
    with HasClassName {
  def cn = clazzName
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = Option(clazzName)
  override def logDisplay(level: Int): String = s"${super.logDisplay(level)} VT: $validTime"
}

object AssertValid {
  def apply(refs: Iterable[EntityReference], vt: Instant): AssertValid = {
    new AssertValid(MicroPrecisionInstant.ofInstant(vt), refs)
  }
}
final case class AssertValid private (vt: MicroPrecisionInstant, refs: Iterable[EntityReference])
    extends WriteCommandWithValidTime {
  // Not needed to be defined as we would ignore AssertValid if the concerned entity is handled by a different partition
  // This command doesn't participate in partition resolution on the client side
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = None
}

object PutApplicationEvent {
  def fromJava(bes: Seq[WriteBusinessEvent], application: Int, contentOwner: Int): PutApplicationEvent =
    new PutApplicationEvent(bes, application, contentOwner)
}

final case class PutApplicationEvent(
    bes: Seq[WriteBusinessEvent],
    application: Int,
    contentOwner: Int,
    clientTxTime: Option[Instant] = None,
    elevatedForUser: Option[String] = None,
    appEvtId: Option[AppEventReference] = None,
    retry: Boolean = false,
    minAssignableTtOpt: Option[Instant] = None,
    ignoreListForReferenceResolution: Set[EntityReference] = Set.empty[EntityReference]
) extends WriteCommand {
  // Not needed to be defined as the classnames are resolved using WriteBusinessEvent
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = None
  override def logDisplay(level: Int): String = {
    if (il(level)) {

      val txs =
        if (clientTxTime.isDefined)
          s" clientTxTime= ${clientTxTime.get}"
        else
          ""

      s"${super.logDisplay(level)} PAE: $txs ${bes map { wbe =>
          wbe.logDisplay(level)
        } mkString ";"}"
    } else NoLog
  }

  def extractClientInfo = (application, contentOwner, clientTxTime, elevatedForUser)
}

final case class InvalidateRevertProps(
    er: EntityReference,
    lt: Long,
    cn: SerializedEntity.TypeRef,
    monoTemporal: Boolean = false)
    extends HasClassName

final case class WriteBusinessEvent(
    evt: SerializedBusinessEvent,
    state: EventStateFlag,
    asserts: Iterable[EntityReference],
    puts: Seq[WriteBusinessEvent.Put],
    putSlots: Seq[WriteBusinessEvent.PutSlots],
    invalidates: Seq[InvalidateRevertProps],
    reverts: Seq[InvalidateRevertProps]
) extends WriteCommand {
  override def logDisplay(level: Int): String = {
    if (il(level)) {
      val assertString = iterDisplay[EntityReference]("A:", asserts.toSeq, { _.toString })
      val putString = iterDisplay[WriteBusinessEvent.Put](
        "P:",
        puts,
        { case WriteBusinessEvent.Put(e, lt, _) =>
          s"(${e.entityRef},${e.className},${lt},${e.linkages})"
        })
      val putSlotsString = iterDisplay[PutSlots](
        " PS:",
        putSlots,
        { case p @ WriteBusinessEvent.PutSlots(es, lt) =>
          s"${p.numPuts},${es.entityRef},${es.className},${lt},${es.linkages})"
        })
      val invalidateString = iterDisplay[InvalidateRevertProps](
        " I:",
        invalidates,
        { case InvalidateRevertProps(ref, lt, cn, monoTemporal) =>
          val monoTempString = if (monoTemporal) ",m" else ""
          s"(${ref},${lt},${cn}${monoTempString})"
        }
      )
      val revertString = iterDisplay[InvalidateRevertProps](
        " R:",
        reverts,
        { case InvalidateRevertProps(ref, lt, cn, monoTemporal) =>
          val monoTempString = if (monoTemporal) ",m" else ""
          s"(${ref},${lt},${cn}${monoTempString})"
        }
      )
      s"${super.logDisplay(level)} E:${evt.id}-${evt.className}-S:$state-VT:${evt.validTime}-TT:${evt.tt} " +
        s"$assertString $putString $putSlotsString $invalidateString $revertString"
    } else NoLog
  }
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = Option(evt.className)
}

object WriteBusinessEvent {
  final case class Put(ent: SerializedEntity, lockToken: Option[Long], monoTemporal: Boolean = false)
      extends HasClassName {
    def cn = ent.className
  }
  final case class PutSlots(ents: MultiSlotSerializedEntity, lockToken: Option[Long]) extends HasClassName {
    lazy val numPuts = ents.entities.size
    def cn = ents.className
  }
}

final case class GeneratedAppEvent(asserts: Seq[AssertValid], puts: Seq[Put], invalidates: Seq[InvalidateAfter])
    extends WriteCommand {
  override def logDisplay(level: Int): String = {
    if (il(level)) {
      val assertString = iterDisplay[Command](
        "A:",
        asserts,
        { c =>
          c.logDisplay(level)
        })
      val putString = iterDisplay[Command](
        "P:",
        puts,
        { c =>
          c.logDisplay(level)
        })
      val invalidateString = iterDisplay[Command](
        "I:",
        invalidates,
        { c =>
          c.logDisplay(level)
        })
      s"${super.logDisplay(level)} GAE: $assertString $putString $invalidateString"
    } else NoLog
  }

  // Not needed to be defined as this is a server side construct
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = None
}

final case class CreateSlots(fqcn: SerializedEntity.TypeRef, slots: Set[Int]) extends WriteCommand {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"${super.logDisplay(level)} ASS: ${className()} $slots"
    else NoLog
  }
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = Option(fqcn)
}

final case class GetSlots(fqcn: SerializedEntity.TypeRef) extends ReadAdminCommand {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"${super.logDisplay(level)} GSS: ${className()}"
    else NoLog
  }
  override def className(): Option[String] = Option(fqcn)
  override def needStreamingExecution: Boolean = false
}

final case class FillSlot(
    // eventReference, newEventVersions
    fqcn: SerializedEntity.TypeRef, // maybe un-needed but hey
    entityReference: EntityReference,
    newEntityVersions: Seq[(VersionedReference, SerializedEntity)])
    extends WriteCommand {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"${super.logDisplay(level)} FS: $entityReference ${newEntityVersions.size}"
    else NoLog
  }
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = Option(fqcn)
}

sealed trait HasQuery {
  val query: Query
}

final case class CountGroupings(query: ClassNameQuery, readTxTime: Instant) extends ReadOnlyCommand with HasQuery {
  require(
    query.isInstanceOf[EntityClassQuery] || query.isInstanceOf[EventClassQuery],
    "query must be EntityClassQuery or EventClassQuery")
  override def logDisplay(level: Int): String = {
    if (il(level)) s"[R]${getClass.getSimpleName} Q:$query RTT:$readTxTime" else NoLog
  }
  override def className(): Option[String] = query.className
  override def needStreamingExecution: Boolean = false
}

final case class Count(query: Query, temporality: DSIQueryTemporality)
    extends ReadOnlyCommandWithTemporality
    with HasQuery {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"[R]${getClass.getSimpleName} Q:$query T:$temporality" else NoLog
  }
  override def className(): Option[String] = query.className
  override def needStreamingExecution: Boolean = false
}

final case class Select(query: Query, temporality: DSIQueryTemporality)
    extends ReadOnlyCommandWithTemporality
    with HasQuery {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"[R]${getClass.getSimpleName} Q:$query T:$temporality" else NoLog
  }
  override def className(): Option[String] = query.className

  override def needStreamingExecution: Boolean = checkIfStreamingQuery(query)

}

final case class SelectSpace(query: Query, temporality: DSIQueryTemporality)
    extends ReadOnlyCommandWithTemporality
    with HasQuery {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"[R]${getClass.getSimpleName} Q:$query T:$temporality" else NoLog
  }
  override def className(): Option[String] = query.className

  override def needStreamingExecution: Boolean = true
}

final case class GetRefsNotAtSlot(
    query: Query,
    fqcn: SerializedEntity.TypeRef,
    slot: Int,
    temporality: DSIQueryTemporality)
    extends ReadOnlyCommandWithTemporality
    with HasQuery {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"[R]${getClass.getSimpleName} NotAtSlot Q:$query S:$slot"
    else NoLog
  }
  override def className(): Option[String] = Option(fqcn)

  // GetRefsNotAtSlot has no partial result, thus couldn't be streamed back right now
  override def needStreamingExecution: Boolean = false // checkIfStreamingQuery(query)
}

// Not Read Only because it has to be executed on the lead writer
final case class ResolveKeys(keys: Seq[SerializedKey]) extends LeadWriterCommand {
  def logDisplay(level: Int): String = {
    if (il(level)) {
      s"[K]${getClass.getSimpleName} ${if (dl(level)) iterDisplay(" Keys", keys) else ""}"
    } else NoLog
  }

  // Keys for this command must be resolved using SerializedKeys contained. Therefore, marking it as None
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = None
}

sealed trait EnumerateCommand extends ReadOnlyCommandWithTemporality {
  def typeName: String
  def propNames: Seq[String]

  override def logDisplay(level: Int): String = {
    if (il(level)) {
      s"${super.logDisplay(level)} Tp:$typeName ${if (dl(level)) iterDisplay(" Props", propNames)}"
    } else NoLog
  }

  override def className(): Option[String] = Option(typeName)
}

final case class EnumerateKeys(typeName: String, propNames: Seq[String], temporality: DSIQueryTemporality)
    extends EnumerateCommand {
  // temporality is not used/checked for this command
  override def needStreamingExecution: Boolean = true
}

final case class EnumerateIndices(typeName: String, propNames: Seq[String], temporality: DSIQueryTemporality)
    extends EnumerateCommand {
  // temporality is not used/checked for this command
  override def needStreamingExecution: Boolean = true
}

/**
 * Query should be MultiRelationElement instead of bytes, need to refactoring priql to move encoder to core project
 */
sealed trait QueryCommand extends ReadOnlyCommandWithTemporality {
  def query: ImmutableByteArray
  def classNames: Seq[String]
}

object DALQueryResultType extends Enumeration {
  type DALQueryResultType = Value
  val FullData = Value
  val StorableReference = Value
}
object DALDataType extends Enumeration {
  type DALDataType = Value
  val Entity = Value
  val BusinessEvent = Value
}

final case class TemporalityQueryCommand(
    query: ImmutableByteArray,
    classNames: Seq[String],
    temporality: DSIQueryTemporality,
    dataType: DALDataType.DALDataType,
    queryResultType: DALQueryResultType.DALQueryResultType,
    treeVersion: Int = 1)(private val queryString: Option[String] = None)
    extends QueryCommand {
  override def logDisplay(level: Int): String = {
    if (il(level))
      queryString.map(x => s"[R]${getClass.getSimpleName} T:$temporality Q:$x").getOrElse(super.logDisplay(level))
    else
      NoLog
  }
  override def needStreamingExecution: Boolean = true
}

sealed trait QueryPlan

object QueryPlan {
  object Default extends QueryPlan
  object Accelerated extends QueryPlan
  object FullTextSearch extends QueryPlan
  object Sampling extends QueryPlan
}

/**
 * Command representing a Expression query. The corresponding result is QueryResult.
 */
final case class ExpressionQueryCommand(query: Expression, plan: QueryPlan, entitledOnly: Boolean)
    extends ReadOnlyCommand {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"[R]${getClass.getSimpleName} Q:${ExpressionQueryCommand.getQueryString(query, plan, true)}"
    else NoLog
  }
  override def toString: String = queryString

  lazy val classNames: Seq[String] = ExpressionQueryCommand.getClassNames(query)
  lazy val latestTT: Instant = ExpressionQueryCommand.getLatestTT(query)
  lazy val classInheritanceInfos: Map[String, Seq[String]] = ExpressionQueryCommand.getClassInheritanceInfos(query)
  lazy val queryString = ExpressionQueryCommand.getQueryString(query, plan, false)

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case c: ExpressionQueryCommand => queryString == c.queryString && entitledOnly == c.entitledOnly
      case _                         => false
    }
  }
  override def hashCode(): Int = Objects.hash(queryString, entitledOnly: java.lang.Boolean)
  // If there is 'limit 1' in acc query and the entity is accHC entity, it will be run in non-streaming way.
  private lazy val isStreamingQuery: Boolean = query match {
    case SelectExpression(e: EntityExpression, _, _, _, _, Some(Constant(1, _)), _, _, _, _)
        if plan == QueryPlan.Accelerated =>
      !ExpressionQueryCommand.accHealthCheckClassNames.contains(e.name)
    case _ => true
  }
  override def needStreamingExecution: Boolean = isStreamingQuery
}

object ExpressionQueryCommand {
  val defaultConstFormatter = new DefaultConstFormatter

  val accHealthCheckClassNames =
    Set("optimus.dsi.base.AccHealthCheck", "optimus.dal.metrics.entities.AccMetricsHealthCheck")

  def getClassNames(query: Expression): Seq[String] = {
    val collector = new NameCollector
    collector.visit(query)
    collector.names.toSeq
  }

  def getClassInheritanceInfos(query: Expression): Map[String, Seq[String]] = {
    QuerySourceCollector.collectEntities(query).iterator.map(t => (t.name, t.superClasses)).toMap
  }

  def getLatestTT(query: Expression): Instant = {
    val collector = new LatestTTCollector
    collector.visit(query)
    collector.tt
  }

  def getQueryString(query: Expression, plan: QueryPlan, abbreviated: Boolean): String = plan match {
    case QueryPlan.Default =>
      if (abbreviated) ExpressionPriQLFormatter.format(query)
      else ExpressionPriQLFormatter.format(query, ExpressionPriQLFormatter.MaxThreshold)
    case QueryPlan.Sampling =>
      if (abbreviated) ExpressionPriQLFormatter.formatSampling(query)
      else ExpressionPriQLFormatter.formatSampling(query, ExpressionPriQLFormatter.MaxThreshold)
    case QueryPlan.Accelerated =>
      if (abbreviated) ExprCommandSqlFormatter.format(query)
      else ExprCommandSqlFormatter.format(query, ExprCommandSqlFormatter.MaxThreshold)
    case QueryPlan.FullTextSearch =>
      if (abbreviated) ExpressionAtlasSearchFormatter.format(query)
      else ExpressionAtlasSearchFormatter.format(query, ExpressionAtlasSearchFormatter.MaxThreshold)
  }

  // Collect all class names in query Expression.
  private class NameCollector extends ExpressionVisitor {
    val names = mutable.Set[String]()

    override protected def visitEntity(e: EntityExpression): Expression = {
      names += e.name
      e
    }

    override protected def visitEntityBitemporalSpace(e: EntityBitemporalSpace): Expression = {
      names += e.name
      e
    }

    override protected def visitEvent(e: EventExpression): Expression = {
      names += e.name
      e
    }
  }

  // Collect latest tt in query Expression
  private class LatestTTCollector extends ExpressionVisitor {
    var tt: Instant = TimeInterval.NegInfinity

    override protected def visitEntity(e: EntityExpression): Expression = {
      if (e.when.readTxTime.isAfter(tt)) tt = e.when.readTxTime
      e
    }

    override protected def visitEntityBitemporalSpace(e: EntityBitemporalSpace): Expression = {
      if (e.when.readTxTime.isAfter(tt)) tt = e.when.readTxTime
      e
    }

    override protected def visitEvent(e: EventExpression): Expression = {
      if (e.when.readTxTime.isAfter(tt)) tt = e.when.readTxTime
      e
    }
  }

  object ExprCommandSqlFormatter {
    val MaxThreshold = AbbreviationThreshold(None, None)
    lazy val DefaultThreshold = AbbreviationThreshold(
      Option(DiagnosticSettings.getIntProperty("optimus.priql.print.expressionListAbbreviationThreshold", 10)),
      Option(DiagnosticSettings.getIntProperty("optimus.priql.print.expressionWhereAbbreviationThreshold", 500))
    )

    def format(e: Expression, threshold: AbbreviationThreshold = DefaultThreshold) = {
      val formatter = new ExprCommandSqlFormatter(defaultConstFormatter, threshold)
      formatter.visit(e)
      formatter.toString
    }
  }

  private class ExprCommandSqlFormatter(constFormatter: ConstFormatter, threshold: AbbreviationThreshold)
      extends ExpressionSqlFormatter(constFormatter, threshold) {
    override protected def writeLine(style: Indentation): Unit = {
      write(" ")
    }
    override protected def indent(style: Indentation): Unit = {}

    override protected def writeEntitySource(e: EntityExpression): Unit = {
      write(e.name)
      write("(")
      write(e.when)
      write(")")
      write(" ")
      writeAsAliasName(getAliasName(e.id))
    }

    override protected def writeEventSource(e: EventExpression): Unit = {
      write(e.name)
      write("(")
      write(e.when)
      write(")")
      write(" ")
      writeAsAliasName(getAliasName(e.id))
    }

    override protected def visitIn(i: In): Expression = {
      visit(i.e)
      write(" IN (")
      i.values match {
        case Left(s) =>
          visit(s)
        case Right(l) =>
          val size = l.size
          threshold.listThreshold.filter(th => size > th && size > 2) map { _ =>
            visit(l.head)
            write(s"... [${size - 2} elements]... ")
            visit(l.last)
          } getOrElse {
            var first = true
            for (v <- l) {
              if (!first) write(", ")
              visit(v)
              first = false
            }
          }
      }

      write(")")
      i
    }
  }
}

object Obliterate {

  sealed trait Mode {
    def value: Int
  }
  object Mode {

    /**
     * This is default mode where all metadata is removed as a part of single command.
     */
    final case object Full extends Mode { override val value = 1 }

    /**
     * This will not remove key and grouping. And rest of the metadata will be removed in chunks depending on the
     * batch-size configured on the server-side. This is useful for entities with very large number of versions,
     * which can be safely obliterated during weekdays.
     *
     * Accordingly, it will send back ObliterateResult and client may need to re-issue the same command multiple
     * times, where the last command should be with Full mode to remove key and grouping as well.
     */
    final case object Chunking extends Mode { override val value = 2 }

    val default: Mode = Full
    def fromValue(v: Int): Mode = v match {
      case 1 => Full
      case 2 => Chunking
      case _ => throw new IllegalArgumentException(s"Unknown Obliterate mode value: $v")
    }
  }

  def apply(q: Query): Obliterate = Obliterate(Seq(q))
  def lift(cmds: Seq[Obliterate]): Obliterate = Obliterate(cmds.flatMap(_.queries))
}

final case class Obliterate(
    queries: Seq[Query],
    mode: Obliterate.Mode = Obliterate.Mode.Full
) extends WriteCommand {

  require(
    mode == Obliterate.Mode.Full ||
      queries.forall(q => q.isInstanceOf[ReferenceQuery] || q.isInstanceOf[SerializedKeyQuery]),
    s"Obliterate in ${Obliterate.Mode.Chunking} mode can only be used with ReferenceQuery or SerializedKeyQuery, but got: ${queries
        .mkString(", ")}"
  )

  def this(query: Query) = this(Seq(query))

  private val qStr =
    if (queries.tail.isEmpty) s"Obliterate (mode: $mode): ${queries.headOption.getOrElse("")}"
    else s"ObliterateBatch (mode: $mode) of ${queries.size}"
  override def logDisplay(level: Int): String = if (il(level)) s"[W]$qStr" else NoLog
  // Classname for Obliterate must be resolved using the queries contained in obliterate. So marking this as None
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = None
}

final case class QueryReferenceResult(
    dataType: DALDataType.DALDataType,
    refs: Iterable[(StorableReference with TypedReference, Option[Instant], Option[VersionedReference])])
    extends FullResult[(StorableReference with TypedReference, Option[Instant], Option[VersionedReference])](refs)
    with Result {

  override def logDisplay(level: Int): String = {
    if (il(level)) s"${super.logDisplay(level)} ${if (dl(level)) s"dt=$dataType" else ""}" else NoLog
  }

  override def generatePartialInstance(
      partialResult: Iterable[(StorableReference with TypedReference, Option[Instant], Option[VersionedReference])],
      isLast: Boolean) =
    new PartialQueryReferenceResult(dataType, partialResult, isLast)
}

final case class PartialQueryReferenceResult(
    dataType: DALDataType.DALDataType,
    refs: Iterable[(StorableReference with TypedReference, Option[Instant], Option[VersionedReference])],
    isLast: Boolean)
    extends PartialResult[(StorableReference with TypedReference, Option[Instant], Option[VersionedReference])](refs)
    with Result {

  override def generateFullResult(
      partialResults: Iterable[(StorableReference with TypedReference, Option[Instant], Option[VersionedReference])])
      : FullResult[(StorableReference with TypedReference, Option[Instant], Option[VersionedReference])] =
    QueryReferenceResult(dataType, partialResults)

}

// for the moment just returns server time
final case class GetInfo() extends ReadOnlyCommand with UnentitledCommand {
  override def needStreamingExecution: Boolean = false
}

// This is stateless command which is used to create new session
final case class CreateNewSession private[optimus] (es: EstablishSession)
    extends ReadOnlyCommand
    with UnentitledCommand {
  override def needStreamingExecution: Boolean = false
}
final case class CreateNewSessionResult private[optimus] (esr: EstablishSessionResult) extends Result

final case class SessionTokenRequest(
    roles: Set[String],
    realID: Option[String],
    effectiveID: Option[String],
    appID: Option[String],
    token: Option[Array[Byte]],
    featureInfo: FeatureInfo)
    extends ReadOnlyCommand
    with UnentitledCommand {
  override def needStreamingExecution: Boolean = false
}
final case class RoleMembershipQuery(realId: String, clientSessionInfo: Option[ClientSessionInfo] = None)
    extends ReadOnlyCommand
    with UnentitledCommand {
  override val className: Option[SerializedEntity.TypeRef] =
    None // Should be BasicRole but not in the dependencies at this point
  override def needStreamingExecution: Boolean = false
}
sealed trait EntityAction
object EntityAction {
  case object Read extends EntityAction
  case object Create extends EntityAction
  case object Update extends EntityAction
  case object Invalidate extends EntityAction
}
final case class CanPerformAction(typeRef: SerializedEntity.TypeRef, action: EntityAction) extends ReadOnlyCommand {
  override def needStreamingExecution: Boolean = false
}

final case class GetBusinessEventByKey(key: SerializedKey, tt: Instant) extends ReadOnlyCommandWithTT {
  override val className: Option[SerializedEntity.TypeRef] = Some(key.typeName.asInstanceOf[SerializedEntity.TypeRef])
  override def needStreamingExecution: Boolean = false
  override def logDisplay(level: Int): String = if (il(level)) s"[R]${getClass.getSimpleName} Q:$key TT:$tt" else NoLog
}

final case class GetBusinessEvent(ref: BusinessEventReference, tt: Instant)
    extends ReadOnlyCommandWithTT
    with ContainsBusinessEventReference {
  override def needStreamingExecution: Boolean = false
  override def logDisplay(level: Int): String = if (il(level)) s"[R]${getClass.getSimpleName} Q:$ref TT:$tt" else NoLog
}

final case class GetBusinessEventsByClass(eventClassQuery: EventClassQuery, tt: Instant) extends ReadOnlyCommandWithTT {
  override val className: Option[SerializedEntity.TypeRef] = Some(eventClassQuery.classNameStr)
  override def needStreamingExecution: Boolean = true
  override def logDisplay(level: Int): String =
    if (il(level)) s"[R]${getClass.getSimpleName} Q:$eventClassQuery TT:$tt" else NoLog
}

final case class GetInitiatingEvent(ref: EntityReference, vt: Instant, tt: Instant)
    extends ReadOnlyCommandWithTT
    with ContainsEntityReference {
  override def needStreamingExecution: Boolean = false
  override def logDisplay(level: Int): String =
    if (il(level)) s"[R]${getClass.getSimpleName} Q:$ref VT:$vt TT:$tt" else NoLog
}

final case class GetEntityEventTimeline(ref: EntityReference, tt: Instant)
    extends ReadOnlyCommandWithTT
    with ContainsEntityReference {
  override def needStreamingExecution: Boolean = false
  override def logDisplay(level: Int): String = if (il(level)) s"[R]${getClass.getSimpleName} Q:$ref TT:$tt" else NoLog
}

final case class GetEntityEventValidTimeline(
    ref: EntityReference,
    tt: Instant,
    validTimeInterval: Option[ValidTimeInterval])
    extends ReadOnlyCommandWithTT
    with ContainsEntityReference {
  override def needStreamingExecution: Boolean = false
  override def logDisplay(level: Int): String = if (il(level)) s"[R]${getClass.getSimpleName} Q:$ref TT:$tt" else NoLog
}

final case class GetEventTimeline(ref: BusinessEventReference, tt: Instant)
    extends ReadOnlyCommandWithTT
    with ContainsBusinessEventReference {
  override def needStreamingExecution: Boolean = false
  override def logDisplay(level: Int): String = if (il(level)) s"[R]${getClass.getSimpleName} Q:$ref TT:$tt" else NoLog
}

// NB: Includes VT to support "fast-forward" queries where vt != id.vt
// NB: typeName denotes the name of the specific entity to be returned
final case class GetAssociatedEntities(
    ref: BusinessEventReference,
    vt: Instant,
    tt: Instant,
    typeName: Option[String] = None
) extends ReadOnlyCommandWithTT
    with ContainsBusinessEventReference {
  override def needStreamingExecution: Boolean = false
  override def logDisplay(level: Int): String =
    if (il(level)) s"[R]${getClass.getSimpleName} Q:$ref VT:$vt TT:$tt ${typeName.map(t => s"T:$t").getOrElse("")}"
    else NoLog
}

final case class GetEventTransactions(ref: BusinessEventReference, uptoTt: Instant)
    extends ReadOnlyCommandWithTT
    with ContainsBusinessEventReference {
  override def needStreamingExecution: Boolean = false
  override def logDisplay(level: Int): String =
    if (il(level)) s"[R]${getClass.getSimpleName} Q:$ref TT:$tt" else NoLog
  override def tt: Instant = uptoTt
}

// system command, DSI-specific
final case class SystemCommand(cmd: String) extends AdminCommand {
  def auditInfoString: String = logDisplay(CmdLogLevelHelper.DebugLevel)
  override def logDisplay(level: Int): String = s"[S]${getClass.getSimpleName}${strDisplay(level, " Cmd:", cmd)}"
  // System command must be executed on all partitions.
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = None
}

// command representing an error that occurred before we call the dsi execute function
final case class ErrorCommand(t: Throwable) extends ReadOnlyCommand {
  override def logDisplay(level: Int): String = s"${super.logDisplay(ErrorLevel)} Exc: $t"
  override def needStreamingExecution: Boolean = false
}

// command representing a noop
case object VoidCommand extends ReadOnlyCommand {
  override def logDisplay(level: Int): String = s"${super.logDisplay(ErrorLevel)} NOOP"
  override def needStreamingExecution: Boolean = false
}

// Is not a write command in order to disallow batch execution
final case class InvalidateAllCurrent(clazzName: String, count: Option[Long]) extends WriteCommand with HasClassName {
  def cn = clazzName
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = Option(clazzName)
  def auditInfoString: String = s"InvalidateAllCurrent($clazzName)"
  override def logDisplay(level: Int): String = if (il(level)) s"[I] ${getClass.getSimpleName}" else NoLog
}

object InvalidateAllCurrent {
  def apply(cn: String): InvalidateAllCurrent = InvalidateAllCurrent(cn, None)
}

final case class InvalidateAllCurrentByRefs(entityReferences: Seq[EntityReference], clazzName: String)
    extends WriteCommand
    with HasClassName {
  def this(ref: EntityReference, clazzName: String) = this(Seq(ref), clazzName)
  def cn = clazzName
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = Option(clazzName)
  def auditInfoString: String = logDisplay(CmdLogLevelHelper.DebugLevel)
  override def logDisplay(level: Int): String =
    if (il(level)) s"[I] ${getClass.getSimpleName}${if (dl(level)) iterDisplay(" Refs", entityReferences) else ""}"
    else NoLog
}

final case class PrepareMonoTemporal(entityReferences: Seq[EntityReference], clazzName: String)
    extends WriteCommand
    with HasClassName {
  def this(ref: EntityReference, clazzName: String) = this(Seq(ref), clazzName)
  def cn = clazzName
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = Option(clazzName)
  def auditInfoString: String = logDisplay(CmdLogLevelHelper.DebugLevel)
  override def logDisplay(level: Int): String =
    if (il(level)) s"[M] ${getClass.getSimpleName}${if (dl(level)) iterDisplay(" Refs", entityReferences) else ""}"
    else NoLog
}

final case class Heartbeat(uuid: String, sentTime: Instant) extends ReadOnlyCommand {
  override def needStreamingExecution: Boolean = false
}

final case class GetProtoFileProperties(localProtoFileVersionNum: Int, clientFeature: FeatureInfo)
    extends ReadOnlyCommand {
  override def needStreamingExecution: Boolean = false
}

abstract class AbstractRawCommand(scriptName: String) extends AdminCommand with WriteCommand {
  def partition: Option[Partition]
  def auditInfoString: String = logDisplay(CmdLogLevelHelper.DebugLevel)
  override def logDisplay(level: Int): String =
    s"[S]${getClass.getSimpleName}${strDisplay(level, " Script:", scriptName)}"
  // Not needed to be defined as this is a server side construct
  override protected def classNameImpl: Option[SerializedEntity.TypeRef] = None
}

final case class QueryEntityMetadata(entityRef: EntityReference) extends ReadOnlyCommand {
  override def needStreamingExecution: Boolean = false
}

sealed trait PubSubCommand extends Command {
  def streamId: String
  override def logDisplay(level: Int): String =
    if (il(level)) s"${this.getClass.getSimpleName}(id=$streamId)" else NoLog
}

final case class CreatePubSubStream(
    override val streamId: String,
    subs: Seq[Subscription],
    startTime: Option[Instant],
    endTime: Option[Instant],
    vtFilterInterval: Option[ValidTimeInterval] = None
) extends PubSubCommand {
  override def logDisplay(level: Int): String =
    if (il(level)) {
      val isSow = subs.exists(_.includeSow)
      s"${this.getClass.getSimpleName}(id=$streamId, startTime=$startTime, endTime=$endTime, subCount=${subs.size}, sow=$isSow, vtFilterInterval=$vtFilterInterval)"
    } else NoLog
}

final case class ChangeSubscription(
    override val streamId: String,
    changeRequestId: Int,
    newSubs: Seq[Subscription],
    removeSubs: Seq[Subscription.Id])
    extends PubSubCommand {
  override def logDisplay(level: Int): String =
    if (il(level))
      s"${this.getClass.getSimpleName}(streamId=$streamId, addSubCount=${newSubs.size}, removeSubCount=${removeSubs.size})"
    else NoLog
}

final case class ClosePubSubStream(override val streamId: String) extends PubSubCommand

sealed trait MessagesCommand extends Command {
  override def logDisplay(level: Int): String =
    if (il(level)) s"${this.getClass.getSimpleName}" else NoLog
}

/**
 * MessagesClientCommand below is introduced to avoid another hierarchy of Commands/Request. CreateMessagesClientStream
 * has attributes callback, RuntimeEnv which are only required in the client side and not on the server side and hence
 * the ServerSideCommand -> CreateMessagesStream only has streamId and subscriptions
 */
sealed trait MessagesClientCommand extends MessagesCommand

sealed trait MessagesServerCommand extends MessagesCommand

sealed trait MessagesStreamCommand extends MessagesCommand {
  def streamId: String
  override def logDisplay(level: Int): String =
    if (il(level)) s"${this.getClass.getSimpleName}(id=$streamId)" else NoLog
}

final case class CreateMessagesClientStream(
    createStream: CreateMessagesStream,
    callback: MessagesNotificationCallback,
    env: RuntimeEnvironment
) extends MessagesClientCommand
    with MessagesStreamCommand {
  override val streamId: String = createStream.streamId
  override def logDisplay(level: Int): String =
    if (il(level)) {
      s"${this.getClass.getSimpleName}(streamId=$streamId, subs=${createStream.subs})"
    } else NoLog
}

final case class CreateMessagesStream(
    override val streamId: String,
    subs: Set[MessagesSubscription],
    appSpecificConsumerId: Option[String],
    startTime: Option[Instant],
) extends MessagesServerCommand
    with MessagesStreamCommand {
  override def logDisplay(level: Int): String =
    if (il(level))
      s"${this.getClass.getSimpleName}(streamId=$streamId, subs=$subs, consumerId=$appSpecificConsumerId, startTime=$startTime)"
    else NoLog
}

final case class ChangeMessagesSubscription(
    override val streamId: String,
    changeRequestId: Int,
    newSubs: Set[MessagesSubscription],
    removeSubs: Set[MessagesSubscription.Id])
    extends MessagesServerCommand
    with MessagesStreamCommand {
  override def logDisplay(level: Int): String =
    if (il(level))
      s"${this.getClass.getSimpleName}(streamId=$streamId, changeRequestId=$changeRequestId, addSubs=$newSubs, removeSubs=$removeSubs)"
    else NoLog
}

final case class CloseMessagesStream(override val streamId: String)
    extends MessagesServerCommand
    with MessagesStreamCommand

final case class CommitMessagesStream(override val streamId: String, commitIds: Seq[Long])
    extends MessagesServerCommand
    with MessagesStreamCommand {
  override def logDisplay(level: Int): String =
    if (il(level))
      s"${this.getClass.getSimpleName}(streamId=$streamId, commitIds=${commitIds.take(10)})"
    else NoLog
}

// streamAppId is passed from kafkaStreamsInputPipeline, inform us of the identifier of inputPipeline
final case class StreamsACLsCommand(streamAppId: String, acls: Seq[StreamsACLs]) extends MessagesServerCommand {
  override def logDisplay(level: Int): String =
    if (il(level))
      s"${getClass.getSimpleName}(appId=$streamAppId, acls=$acls)"
    else NoLog
}

sealed trait MessagesPublishCommandBase extends MessagesServerCommand {

  /** Identifier for the Message sent - used in logging and crumbs. */
  def identifier: String

  /** How to publish the Message? Fire-and-forget or Guaranteed. */
  def option: MessagesPublishCommandBase.Option

  def classNames: Seq[String]

  // Each class is represented by a Seq[String] to cover the full hierarchy
  def typeRefsForEntitlementCheck: Seq[Seq[String]]
}

object MessagesPublishCommandBase {
  sealed trait Option {
    // If false, it will behave as "fire-and-forget", i.e. there will be no guarantee of message
    // reaching messaging system. And so, it's faster than the alternative.
    def waitForAck: Boolean
  }

  object Option {

    case object Ack extends Option {
      override val waitForAck: Boolean = true
    }

    case object IgnoreAck extends Option {
      override val waitForAck: Boolean = false
    }
  }
}

final case class MessagesPublishCommand(
    msg: MessagesPublishCommand.MessageType,
    option: MessagesPublishCommandBase.Option
) extends MessagesPublishCommandBase {
  override def identifier: String = clsName
  override def classNames: Seq[String] = Seq(clsName)

  // for SCE, not including full hierarchy
  override def typeRefsForEntitlementCheck: Seq[Seq[String]] = Seq(Seq(clsName))

  def clsName: String = msg.sbe.className

  override def logDisplay(level: Int): String =
    if (il(level))
      s"${this.getClass.getSimpleName}(eventType=$clsName, ref=${msg.sbe.id}, waitForAck=${option.waitForAck})"
    else NoLog
}

object MessagesPublishCommand {
  type MessageType = SerializedContainedEvent
}

final case class MessagesPublishTransactionCommand(
    msg: MessagesPublishTransactionCommand.MessageType,
    waitForAck: Boolean = true
) extends MessagesPublishCommandBase {
  // default to true in case of Transaction as we must ensure that message is delivered and persisted
  override def option: MessagesPublishCommandBase.Option =
    if (waitForAck) MessagesPublishCommandBase.Option.Ack else MessagesPublishCommandBase.Option.IgnoreAck

  override def identifier: String =
    s"Transaction (Event: $businessEventClassName Cls: [${classNames.mkString(", ")}])"

  // expected to be single event associated with Published Transaction
  def transactionContent: Map[SerializedBusinessEvent, Seq[SerializedEntity]] =
    msg.putEvent.bes.map(be => be.evt -> be.puts.map(_.ent).sortBy(_.className)).toMap

  private def serializedEvents: Seq[SerializedBusinessEvent] = msg.putEvent.bes.map(_.evt)

  // expected to be single event associated with Published Transaction
  def businessEventClassName: String = serializedEvents.map(_.className).mkString(",")

  // note: it's sorted at this point, because the Event/Put is not in the same order as definition.
  private[optimus] def serializedEntities: Seq[SerializedEntity] =
    msg.putEvent.bes.flatMap(_.puts.map(_.ent)).sortBy(_.className)

  override def classNames: Seq[String] = serializedEntities.map(_.className).distinct

  // note: types should include className already, BUT we need to put className on front so the entitlement check fail highlights it
  override def typeRefsForEntitlementCheck: Seq[Seq[String]] =
    serializedEvents.map(e => e.className +: e.types) ++ serializedEntities.map(e => e.className +: e.types)

  override def logDisplay(level: Int): String =
    if (il(level)) s"${this.getClass.getSimpleName}(identifier=$identifier, waitForAck=${option.waitForAck})"
    else NoLog
}

object MessagesPublishTransactionCommand {
  type MessageType = SerializedUpsertableTransaction
}

sealed trait Result {
  // This should ONLY be overridden in ErrorResult.
  def isSuccess: Boolean = true

  def logDisplay(level: Int): String = {
    if (il(level)) getClass.getSimpleName else NoLog
  }

  lazy val weight: Int = 1
}

final case class IsEntitledResult(res: Boolean) extends Result

sealed trait PrcResult extends Result

case object VoidResult extends Result

// Separate StreamingResultTrait is used to ensure that the 'StreamingResult' classes share base functionality
sealed trait StreamingResultTrait[T] extends Result { def values: Iterable[T] }
final case class StreamingResult[T](values: Iterable[T]) extends StreamingResultTrait[T]
final case class EnumKeysStreamingResult[T](values: Iterable[T]) extends StreamingResultTrait[T]
final case class EnumIndicesStreamingResult[T](values: Iterable[T]) extends StreamingResultTrait[T]

sealed trait TimestampedResult extends Result {
  def txTime: Instant
  override def logDisplay(level: Int): String = if (il(level)) s"${super.logDisplay(level)} txR: $txTime" else NoLog
}

sealed trait AppEventWriteResult extends TimestampedResult with HasResultStats[AppEventWriteResult] {
  def putResults: Seq[PutResult]
  def invResults: Seq[InvalidateAfterResult]
  def assertResults: Seq[AssertValidResult]
  override lazy val weight = (putResults ++ invResults ++ assertResults).map(_.weight).sum

  override def getResultStats(): Option[ResultStats] = {
    super.getResultStats() match {
      case None =>
        val entEvtPutStats =
          putResults.flatMap(r => r.permRef.getTypeId).groupBy(identity).mapValuesNow(_.length.toLong)
        val entEvtInvStats =
          invResults.flatMap(r => r.permRef.getTypeId).groupBy(identity).mapValuesNow(_.length.toLong)
        val putStats = ResultStatsByInt(entEvtPutStats)
        val invStats = ResultStatsByInt(entEvtInvStats)
        val statsOpt = Option(WriteResultStatsByInt(putStats, invStats))
        setResultStats(statsOpt)
        statsOpt
      case statsOpt => statsOpt
    }
  }
}

sealed trait VersionedResult extends TimestampedResult {
  def versionedRef: VersionedReference
  override def logDisplay(level: Int): String =
    if (il(level)) s"${super.logDisplay(level)} VR: $versionedRef" else NoLog
}

sealed trait VersionedResultWithRef extends VersionedResult {
  def permRef: EntityReference
  override def logDisplay(level: Int): String = if (il(level)) s"${super.logDisplay(level)} PR: $permRef" else NoLog
}

final case class SessionTokenResult(
    realID: Option[String],
    roles: Set[String],
    encryptedToken: Array[Byte],
    featureInfo: FeatureInfo)
    extends Result

final case class PutResult(
    permRef: EntityReference,
    versionedRef: VersionedReference,
    lockToken: Long,
    vtInterval: ValidTimeInterval,
    txTimeTo: Option[Instant],
    txTime: Instant)
    extends VersionedResultWithRef {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"${super.logDisplay(level)}VR:${versionedRef.toString} LT:$lockToken VT:${vtInterval.toString}"
    else NoLog
  }
}

final case class InvalidateAfterResult(
    permRef: EntityReference,
    types: Seq[SerializedEntity.TypeRef],
    keys: Seq[SerializedKey],
    txTime: Instant)
    extends TimestampedResult

final case class AssertValidResult() extends Result

final case class RevertResult(
    permRef: EntityReference,
    lockToken: Long,
    types: Seq[SerializedEntity.TypeRef],
    keys: Seq[SerializedKey],
    txTime: Instant
)

// WriteBusinessEventResult is only used within the broker
final case class WriteBusinessEventResult(
    putResults: Seq[PutResult],
    invResults: Seq[InvalidateAfterResult],
    assertResults: Seq[AssertValidResult],
    revertResults: Seq[RevertResult],
    txTime: Instant)
    extends TimestampedResult {
  override lazy val weight = putResults.map(_.weight).sum
}
final case class PutApplicationEventResult(appEvent: SerializedAppEvent, beResults: Seq[WriteBusinessEventResult])
    extends AppEventWriteResult {
  def txTime: Instant = appEvent.tt
  override def putResults: Seq[PutResult] = beResults flatMap (_.putResults)
  override def invResults: Seq[InvalidateAfterResult] = beResults flatMap (_.invResults)
  override def assertResults: Seq[AssertValidResult] = beResults flatMap (_.assertResults)
}
object PutApplicationEventResult {
  def empty(
      tt: Instant,
      app: Int,
      owner: Int,
      clientInfo: DalClientInfo,
      reqId: String,
      teaIds: Set[String],
      receivedAt: Option[Instant]): PutApplicationEventResult = {
    PutApplicationEventResult(
      SerializedAppEvent.empty(
        tt,
        clientInfo.realId,
        Some(clientInfo.effectiveId),
        app,
        owner,
        reqId,
        clientInfo.clientHostPort,
        clientInfo.applicationIdentifier.appId,
        clientInfo.applicationIdentifier.zoneId,
        receivedAt,
        teaIds
      ),
      Nil
    )
  }

  // NB this generates the skeleton of a PutApplicationEventResult which roughly corresponds to what would be sent to
  // a client when a PAE is first executed by the server. This skeleton does not match exactly with the result the
  // client would get, but should be roughly corresponding in terms of the AppEvent itself and the puts performed.
  // Other information like invalidations etc. is not preserved.
  def recreatePutAppEvtResultFromActions(
      pae: PutApplicationEvent,
      ae: SerializedAppEvent,
      actionsForAppEvt: Seq[TxnAction]): PutApplicationEventResult = {
    val putResults: Map[BusinessEventReference, Seq[PutResult]] = actionsForAppEvt
      .collect {
        case p: PutEntityTimeSlice
            if (p.versionedRef != VersionedReference.Nil && p.updatedAppEvent == p.seg.data.creationAppEvt) =>
          val putResult = PutResult(
            p.entityRef,
            p.versionedRef,
            DateTimeSerialization.fromInstant(ae.tt),
            p.vtInterval,
            p.ttInterval.map(_.to).orElse(Some(TimeInterval.Infinity)),
            ae.tt)
          p.seg.data match {
            case EvtTimeSliceData(_, _, _, ref, _, _, _) => (ref, putResult)
            case _                                       => (BusinessEventReference.Nil, putResult)
          }
      }
      .groupBy(_._1)
      .map { case (bevref, values) => bevref -> values.map(_._2) }
    val beRes = pae.bes.map { wbe =>
      val wbePrs = putResults.getOrElse(
        wbe.evt.id,
        throw new IllegalArgumentException(
          s"Appevent with id ${pae.appEvtId} already persisted to dal with different txn actions. Rejecting the transaction")
      )
      require(
        wbePrs.length == wbe.puts.length,
        s"Expected to find ${wbe.puts.length} PutResult(s) for event ${wbe.evt.id}, but actually found ${wbePrs.length}")
      WriteBusinessEventResult(wbePrs, Seq.empty, Seq.empty, Seq.empty, ae.tt)
    }
    PutApplicationEventResult(ae, beRes)
  }
}

final case class GeneratedAppEventResult(
    appEvent: SerializedAppEvent,
    assertResults: Seq[AssertValidResult],
    putResults: Seq[PutResult],
    invResults: Seq[InvalidateAfterResult])
    extends AppEventWriteResult {
  def txTime: Instant = appEvent.tt
  override def logDisplay(level: Int): String = {
    if (il(level)) {
      val assertString = iterDisplay[Result](
        "A:",
        assertResults,
        { c =>
          c.logDisplay(level)
        })
      val putString = iterDisplay[Result](
        "P:",
        putResults,
        { c =>
          c.logDisplay(level)
        })
      val invalidateString = iterDisplay[Result](
        "I:",
        invResults,
        { c =>
          c.logDisplay(level)
        })
      s"${super.logDisplay(level)} AE:${appEvent.id}-${appEvent.tt} $assertString $putString $invalidateString"
    } else NoLog
  }
}

sealed trait SelResult extends Result {
  def value: Iterable[PersistentEntity]
  override def logDisplay(level: Int): String = {
    if (il(level)) {
      // here we have broken iterable - can't be iterated more than once so we can't touch it!!!
      super.logDisplay(level)
    } else NoLog
  }
}

trait ResultStats {
  def attachPartitions(partitions: Set[Partition]): ResultStats = {
    if (_partitions.isEmpty) _partitions = partitions
    else _partitions ++= partitions
    this
  }

  def getPartitions: Set[Partition] = _partitions

  private[this] var _partitions: Set[Partition] = Set.empty
}

final case class ResultStatsByInt(cnIdtoCount: Map[Int, Long]) extends ResultStats
final case class ResultStatsByString(cnIdtoCount: Map[String, Long]) extends ResultStats
final case class WriteResultStatsByInt(putStats: ResultStatsByInt, invalidateStats: ResultStatsByInt)
    extends ResultStats

sealed trait HasResultStats[T <: Result] { self: T =>
  private var resStats: Option[ResultStats] = None
  def setResultStats(stats: Option[ResultStats]): Unit = { resStats = stats }
  def getResultStats(): Option[ResultStats] = resStats
  def withResultStats(stats: Option[ResultStats]): T = {
    setResultStats(stats)
    this
  }
  def withNoResultStats(): T = {
    resStats = None
    this
  }
}

object SelectResult {
  def unapply(arg: Result): Option[Iterable[PersistentEntity]] = arg match {
    case sel: SelectResult => unapply(sel)
    case _                 => None
  }
  def unapply(arg: SelectResult): Option[Iterable[PersistentEntity]] = Some(arg.value)
}

final case class SelectResult(value: Iterable[PersistentEntity])
    extends FullResult[PersistentEntity](value)
    with SelResult
    with HasResultStats[SelectResult] {

  override def generatePartialInstance(
      partialResult: Iterable[PersistentEntity],
      isLast: Boolean): PartialSelectResult =
    if (isLast) PartialSelectResult(partialResult, isLast).withResultStats(getResultStats())
    else PartialSelectResult(partialResult, isLast)
}

final case class PrcSelectResult(
    underlying: SelectResult,
    vtInterval: ValidTimeInterval,
    txInterval: TimeInterval,
    remoteLsqtMap: Map[Partition, Instant])
    extends PrcFullResult[PersistentEntity](underlying.value) {
  override def generatePartialInstance(
      partialResult: Iterable[PersistentEntity],
      isLast: Boolean): PrcPartialSelectResult =
    if (isLast)
      PrcPartialSelectResult(
        PartialSelectResult(partialResult, isLast).withResultStats(underlying.getResultStats()),
        vtInterval,
        txInterval,
        remoteLsqtMap)
    else
      PrcPartialSelectResult(PartialSelectResult(partialResult, isLast), vtInterval, txInterval, remoteLsqtMap)
}

object PartialSelectResult {
  def unapply(arg: Result): Option[(Iterable[PersistentEntity], Boolean)] = arg match {
    case parRes: PartialSelectResult => Some(parRes.value, parRes.isLast)
    case _                           => None
  }
}

final case class PartialSelectResult(value: Iterable[PersistentEntity], isLast: Boolean)
    extends PartialResult[PersistentEntity](value)
    with SelResult
    with HasResultStats[PartialSelectResult] {

  override def generateFullResult(partialResults: Iterable[PersistentEntity]): FullResult[PersistentEntity] =
    SelectResult(partialResults)
}

final case class PrcPartialSelectResult(
    underlying: PartialSelectResult,
    vtInterval: ValidTimeInterval,
    txInterval: TimeInterval,
    remoteLsqtMap: Map[Partition, Instant])
    extends PrcPartialResult[PersistentEntity](underlying.value)
    with HasResultStats[PrcPartialSelectResult] {
  override val isLast: Boolean = underlying.isLast
  override def generateFullResult(partialResults: Iterable[PersistentEntity]): PrcFullResult[PersistentEntity] =
    PrcSelectResult(SelectResult(partialResults), vtInterval, txInterval, remoteLsqtMap)

  override def withResultStats(stats: Option[ResultStats]): PrcPartialSelectResult =
    PrcPartialSelectResult(
      PartialSelectResult(underlying.value, isLast).withResultStats(getResultStats()),
      vtInterval,
      txInterval,
      remoteLsqtMap)
  override def withNoResultStats(): PrcPartialSelectResult =
    if (getResultStats().isEmpty) this
    else
      PrcPartialSelectResult(PartialSelectResult(underlying.value, isLast), vtInterval, txInterval, remoteLsqtMap)
  setResultStats(underlying.getResultStats())
}

final case class SelectSpaceResult(value: Iterable[SelectSpaceResult.Rectangle])
    extends FullResult[SelectSpaceResult.Rectangle](value)
    with HasResultStats[SelectSpaceResult]
    with Result {
  override def generatePartialInstance(
      partialResult: Iterable[SelectSpaceResult.Rectangle],
      isLast: Boolean): PartialResult[SelectSpaceResult.Rectangle] =
    if (isLast) PartialSelectSpaceResult(partialResult, isLast).withResultStats(getResultStats())
    else PartialSelectSpaceResult(partialResult, isLast)
}
object SelectSpaceResult {
  final case class Rectangle(
      txInterval: TimeInterval,
      vtInterval: ValidTimeInterval,
      vref: VersionedReference,
      eref: EntityReference,
      peOpt: Option[PersistentEntity] = None) {
    def containsPoint(vt: Instant, tt: Instant): Boolean = {
      vtInterval.contains(vt) && txInterval.contains(tt)
    }
  }
}

final case class PartialSelectSpaceResult(value: Iterable[SelectSpaceResult.Rectangle], isLast: Boolean)
    extends PartialResult[SelectSpaceResult.Rectangle](value)
    with HasResultStats[PartialSelectSpaceResult]
    with Result {
  override def generateFullResult(
      partialResults: Iterable[SelectSpaceResult.Rectangle]): FullResult[SelectSpaceResult.Rectangle] =
    new SelectSpaceResult(partialResults)
}

final case class GetRefsNotAtSlotResult(refs: Iterable[StorableReference])
    extends Result
    with HasResultStats[GetRefsNotAtSlotResult] {
  override lazy val weight = refs.size
}

object GetInitiatingEventResult {
  def unapply(arg: Result): Option[(Option[SerializedBusinessEvent], Option[PersistentEntity])] = arg match {
    case r: GetInitiatingEventResult => Some(r.event -> r.entity)
    case _                           => None
  }
  def unapply(arg: GetInitiatingEventResult): Option[(Option[SerializedBusinessEvent], Option[PersistentEntity])] =
    Some(arg.event -> arg.entity)
}

final case class GetInitiatingEventResult(event: Option[SerializedBusinessEvent], entity: Option[PersistentEntity])
    extends Result
    with HasResultStats[GetInitiatingEventResult]

final case class GetEntityEventValidTimelineResult(
    eventsEntitiesCombo: Seq[(Option[SerializedBusinessEventWithTTTo], Option[PersistentEntity])])
    extends Result
    with HasResultStats[GetEntityEventValidTimelineResult] {
  override lazy val weight = eventsEntitiesCombo.size
}

final case class GetEntityEventValidTimelineLazyLoadResult(
    brefVtfsCombo: Seq[(Option[SerializedBusinessEventWithTTTo], Instant)])
    extends Result
    with HasResultStats[GetEntityEventValidTimelineLazyLoadResult] {
  override lazy val weight = brefVtfsCombo.size
}

final case class InterimGetEntityEventValidTimelineLazyLoadResult(
    evtTscDataVtsCombo: Seq[(Option[EvtTimeSliceData], Instant)])
    extends Result
    with HasResultStats[InterimGetEntityEventValidTimelineLazyLoadResult] {}

final case class InterimGetEntityEventValidTimelineResult(
    eventsEntitiesCombo: Seq[(Option[EvtTimeSliceData], Option[PersistentEntity])])
    extends Result
    with HasResultStats[InterimGetEntityEventValidTimelineResult] {}

final case class GetEntityEventTimelineResult(
    eventsEntitiesCombo: Iterable[(SerializedBusinessEvent, Option[PersistentEntity])])
    extends FullResult[(SerializedBusinessEvent, Option[PersistentEntity])](eventsEntitiesCombo)
    with Result
    with HasResultStats[GetEntityEventTimelineResult] {
  override def generatePartialInstance(
      partialResult: Iterable[(SerializedBusinessEvent, Option[PersistentEntity])],
      isLast: Boolean) =
    if (isLast) PartialGetEntityEventTimelineResult(partialResult, isLast).withResultStats(getResultStats())
    else PartialGetEntityEventTimelineResult(partialResult, isLast)
}

final case class InterimEntityEventTimelineResult(
    eventsEntitiesCombo: Iterable[(EvtTimeSliceData, Option[PersistentEntity])])
    extends Result
    with HasResultStats[InterimEntityEventTimelineResult] {}

final case class PartialGetEntityEventTimelineResult(
    eventsEntitiesCombo: Iterable[(SerializedBusinessEvent, Option[PersistentEntity])],
    isLast: Boolean)
    extends PartialResult[(SerializedBusinessEvent, Option[PersistentEntity])](eventsEntitiesCombo)
    with Result
    with HasResultStats[PartialGetEntityEventTimelineResult] {

  override def generateFullResult(partialResults: Iterable[(SerializedBusinessEvent, Option[PersistentEntity])])
      : FullResult[(SerializedBusinessEvent, Option[PersistentEntity])] =
    new GetEntityEventTimelineResult(partialResults).withResultStats(getResultStats())

}

final case class InvalidateAllCurrentResult(count: Int, txTime: Option[Instant]) extends Result {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"${super.logDisplay(level)}Cnt: $count" else NoLog
  }
}

final case class PrepareMonoTemporalResult(count: Int, txTime: Option[Instant]) extends Result {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"${super.logDisplay(level)}Cnt: $count" else NoLog
  }
}

final case class SystemCommandResult(reply: String) extends Result {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"${super.logDisplay(level)}${strDisplay(level, "Rep: ", reply)}" else NoLog
  }
}

final case class AccTableResult(clsName: String, slot: Int) extends Result {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"${super.logDisplay(level)}${strDisplay(level, "successful: ", s"$clsName $slot")}" else NoLog
  }
}

final case class CreateSlotsResult(alreadyExistingSlots: Set[Int]) extends Result {
  override def logDisplay(level: Int): String =
    if (il(level)) s"${super.logDisplay(level)} Exists: ${alreadyExistingSlots.mkString(",")}"
    else NoLog
}
final case class GetSlotsResult(slots: Set[Int]) extends Result {
  override def logDisplay(level: Int): String =
    if (il(level)) s"${super.logDisplay(level)} Exists: ${slots.mkString(",")}"
    else NoLog
}

// what goes in one of these?
case object FillSlotResult extends Result {
  override def logDisplay(level: Int): String =
    if (il(level)) s"${super.logDisplay(level)}"
    else NoLog
}

object GetBusinessEventResult {
  def unapply(arg: Result): Option[Iterable[SerializedBusinessEvent]] = arg match {
    case res: GetBusinessEventResult => Some(res.events)
    case _                           => None
  }
}

final case class GetBusinessEventResult(events: Iterable[SerializedBusinessEvent])
    extends FullResult[SerializedBusinessEvent](events)
    with Result
    with HasResultStats[GetBusinessEventResult] {

  override def generatePartialInstance(
      partialResult: Iterable[SerializedBusinessEvent],
      isLast: Boolean): PartialGetBusinessEventResult =
    if (isLast) PartialGetBusinessEventResult(partialResult, isLast).withResultStats(getResultStats())
    else PartialGetBusinessEventResult(partialResult, isLast)
}

object PartialGetBusinessEventResult {
  def unapply(arg: Result): Option[(Iterable[SerializedBusinessEvent], Boolean)] = arg match {
    case res: PartialGetBusinessEventResult => Some(res.events -> res.isLast)
    case _                                  => None
  }
}

final case class PartialGetBusinessEventResult(events: Iterable[SerializedBusinessEvent], isLast: Boolean)
    extends PartialResult[SerializedBusinessEvent](events)
    with Result
    with HasResultStats[PartialGetBusinessEventResult] {

  override def generateFullResult(
      partialResults: Iterable[SerializedBusinessEvent]): FullResult[SerializedBusinessEvent] =
    GetBusinessEventResult(partialResults)
}

final case class GetBusinessEventWithTTToResult(events: Iterable[SerializedBusinessEventWithTTTo])
    extends Result
    with HasResultStats[GetBusinessEventWithTTToResult] {
  override lazy val weight = events.size
}

// Some errors (e.g. DuplicateKeyException) may need to ensure that subsequent reads can see
// the problem (e.g. can read the duplicate entity).
// readAfter is a transactionTime that can be used to see that data.  The Entity Resolver
// write paths set lastPersistTime to readAfter if necessary when a write command fails.
// Read paths do not need (and should not) check for this.
// See OPTIMUS-2857
final case class ErrorResult(error: Throwable, readAfter: Instant = null) extends Result {
  override def isSuccess: Boolean = false
  override def logDisplay(level: Int): String = s"${super.logDisplay(ErrorLevel)} Ex: $error"
}

final case class CountResult(number: Long) extends Result with HasResultStats[CountResult] {
  override def logDisplay(level: Int): String = if (il(level)) s"${super.logDisplay(level)} Num: $number" else NoLog
}

final case class GetInfoResult(
    serverTime: Instant,
    featureInfo: FeatureInfo,
    timeLordClocks: Map[String, Instant] = Map.empty,
    partitionLsqtMap: Map[Partition, Instant] = Map.empty)
    extends Result {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"${super.logDisplay(level)} SrvT: $serverTime BLD: ${featureInfo.build} tlC: $timeLordClocks"
    else NoLog
  }
}

final case class RoleMembershipResult(roleIds: Set[String]) extends Result
final case class BooleanResult(value: Boolean) extends Result

final case class GetAuditInfoResult(value: Iterable[EntityAuditInfo])
    extends FullResult[EntityAuditInfo](value)
    with Result
    with HasResultStats[GetAuditInfoResult] {
  override def logDisplay(level: Int): String =
    if (il(level)) s"${super.logDisplay(level)} EntityAudit Info result count: ${value.size}" else NoLog

  override def generatePartialInstance(
      partialResult: Iterable[EntityAuditInfo],
      isLast: Boolean): PartialGetAuditInfoResult =
    if (isLast) new PartialGetAuditInfoResult(partialResult, isLast).withResultStats(getResultStats())
    else PartialGetAuditInfoResult(partialResult, isLast)
}

final case class PartialGetAuditInfoResult(value: Iterable[EntityAuditInfo], isLast: Boolean)
    extends PartialResult[EntityAuditInfo](value)
    with Result
    with HasResultStats[PartialGetAuditInfoResult] {
  override def generateFullResult(partialResults: Iterable[EntityAuditInfo]): FullResult[EntityAuditInfo] =
    GetAuditInfoResult(partialResults)
}

sealed abstract class FullResult[T](values: Iterable[T]) extends AbstractFullResult[T, Result](values) with Result {
  type PartialType = PartialResult[T]
  override lazy val weight = values.size
  final def generatePartialInstance(partialResult: Iterable[T], isFirst: Boolean, isLast: Boolean): PartialType =
    generatePartialInstance(partialResult, isLast)
  def generatePartialInstance(partialResult: Iterable[T], isLast: Boolean): PartialType
}

sealed abstract class PrcFullResult[T](values: Iterable[T])
    extends AbstractFullResult[T, PrcResult](values)
    with PrcResult {
  override type PartialType = PrcPartialResult[T]
  final def generatePartialInstance(partialResult: Iterable[T], isFirst: Boolean, isLast: Boolean): PartialType =
    generatePartialInstance(partialResult, isLast)
  def generatePartialInstance(partialResult: Iterable[T], isLast: Boolean): PartialType
}

sealed abstract class PartialResult[T](values: Iterable[T])
    extends AbstractPartialResult[T, Result](values)
    with Result {
  type FullType = FullResult[T]
  override lazy val weight = values.size
}

sealed abstract class PrcPartialResult[T](values: Iterable[T])
    extends AbstractPartialResult[T, PrcResult](values)
    with PrcResult {
  type FullType = PrcFullResult[T]
  override lazy val weight = values.size
}

sealed trait HasSeqResult[T] extends Result {
  def seqRes: Seq[T]
  override def logDisplay(level: Int): String = {
    if (il(level)) {
      s"${super.logDisplay(level)} NumR: ${seqRes.size}${if (dl(level)) iterDisplay(" Res: ", seqRes) else ""}"
    } else NoLog
  }
}

sealed trait KeysResult extends HasSeqResult[SerializedKey] {
  def keys: Iterable[SerializedKey]
  def seqRes: Seq[SerializedKey] = keys.toSeq
}

final case class ResolveKeysResult(refs: Seq[Option[EntityReference]]) extends HasSeqResult[Option[EntityReference]] {
  def seqRes: Seq[Option[EntityReference]] = refs
}

final case class EnumerateKeysResult(keys: Iterable[SerializedKey])
    extends FullResult[SerializedKey](keys)
    with KeysResult
    with HasResultStats[EnumerateKeysResult] {

  override def generatePartialInstance(
      partialResult: Iterable[SerializedKey],
      isLast: Boolean): PartialEnumerateKeysResult =
    if (isLast) new PartialEnumerateKeysResult(partialResult, isLast).withResultStats(getResultStats())
    else PartialEnumerateKeysResult(partialResult, isLast)
}

final case class PartialEnumerateKeysResult(keys: Iterable[SerializedKey], isLast: Boolean)
    extends PartialResult[SerializedKey](keys)
    with KeysResult
    with HasResultStats[PartialEnumerateKeysResult] {

  override def generateFullResult(partialResults: Iterable[SerializedKey]): FullResult[SerializedKey] =
    new EnumerateKeysResult(partialResults)
}

final case class EnumerateIndicesResult(keys: Iterable[SerializedKey])
    extends FullResult[SerializedKey](keys)
    with KeysResult
    with HasResultStats[EnumerateIndicesResult] {

  override def generatePartialInstance(
      partialResult: Iterable[SerializedKey],
      isLast: Boolean): PartialEnumerateIndicesResult =
    if (isLast) PartialEnumerateIndicesResult(partialResult, isLast).withResultStats(getResultStats())
    else PartialEnumerateIndicesResult(partialResult, isLast)
}

final case class PartialEnumerateIndicesResult(keys: Iterable[SerializedKey], isLast: Boolean)
    extends PartialResult[SerializedKey](keys)
    with KeysResult
    with HasResultStats[PartialEnumerateIndicesResult] {

  override def generateFullResult(partialResults: Iterable[SerializedKey]): FullResult[SerializedKey] =
    EnumerateIndicesResult(partialResults).withResultStats(getResultStats())
}

final case class HeartbeatResult(receiveTime: Instant) extends Result

final case class GetProtoFilePropertiesResult(remoteProtoFileVersionNum: Int, featureInfo: FeatureInfo) extends Result

/** It's just a wrapper used to pass additional data to the metrics-related logic. */
protected[optimus] final case class MetricsWriteResult(
    blobWithSize: Map[AddBlobAction, BlobMetrics],
    metadataResult: Seq[Result],
    metadataOpsStats: WriteMetadataOpsStats,
    partition: Partition,
    elevatedForUser: Option[String])
    extends Result
protected[optimus] final case class MetricsReadResult(cmd: ReadOnlyCommand, res: Result, partitions: Set[Partition])
    extends Result

final case class QueryEntityMetadataResult(entityMetadata: EntityMetadata)
    extends Result
    with HasResultStats[QueryEntityMetadataResult] {}

trait VersioningResult extends Result

final case class VersioningValidResult(versionedCommands: Seq[Command]) extends VersioningResult

final case class VersioningErrorResult(error: Throwable) extends VersioningResult

final case class PutShortApplicationEvent(aref: AppEventReference)

final case class UOWNotification(appEvents: Seq[UOWApplicationEvent], tt: Instant)

final case class PartitionedUOWNotification(
    beRefs: Seq[BusinessEventReference],
    tt: Instant,
    numPartitionKeys: Option[Int])
// TODO (OPTIMUS-58124): change key signature to accomodate keys of a different data type
final case class PartitionedUOWNotificationKey(className: String, keyName: String, keyValue: String)

final case class UOWApplicationEvent(id: AppEventReference, beRefs: Seq[BusinessEventReference])

final case class GetAuditInfo(entityRef: EntityReference, clazzName: String, temporality: DSIQueryTemporality)
    extends ReadAdminCommand
    with ReadOnlyCommandWithTemporality {
  override def logDisplay(level: Int): String = {
    if (il(level)) s"${super.logDisplay(level)} T: $temporality className: $clazzName"
    else NoLog
  }
  override val className = Some(clazzName)
  override def needStreamingExecution: Boolean = temporality match {
    case _: DSIQueryTemporality.At => false
    case _                         => true
  }
}

object Query {
  def apply(k: Key[_]) = SerializedKeyQuery(k.toSerializedKey)
}

sealed trait Query {
  def className: Option[String]
}

sealed trait EntityQuery extends Query
sealed trait EventQuery extends Query
sealed trait ClassNameQuery extends Query

sealed trait AllowEntitledOnly {
  self: Query =>
  def entitledOnly: Boolean
}

// TODO (OPTIMUS-20990): This should directly take FinalTypedReference.
final case class ReferenceQuery private[optimus] (
    ref: EntityReference,
    entitledOnly: Boolean = false,
    override val className: Option[String] = None)
    extends EntityQuery
    with AllowEntitledOnly

object ReferenceQuery {
  def apply(r: EntityReference): ReferenceQuery = ReferenceQuery(ref = r, entitledOnly = false, className = None)
  def apply(r: EntityReference, className: String): ReferenceQuery =
    ReferenceQuery(ref = r, entitledOnly = false, className = Option(className))
}

final case class VersionedReferenceQuery(
    versionedRef: VersionedReference,
    validTimeInterval: ValidTimeInterval,
    txTimeInterval: TimeInterval,
    cn: String,
    entitledOnly: Boolean)
    extends EntityQuery
    with AllowEntitledOnly {
  override def className: Option[String] = Some(cn)
}

final case class SerializedKeyQuery(
    key: SerializedKey,
    refs: Set[EntityReference] = Set.empty,
    entitledOnly: Boolean = false)
    extends EntityQuery
    with AllowEntitledOnly {
  override def className = Some(key.typeName)
}

final case class EntityClassQuery(classNameStr: String, entitledOnly: Boolean = false)
    extends EntityQuery
    with ClassNameQuery
    with AllowEntitledOnly {
  override def className = Some(classNameStr)
}

final case class EntityClassAppIdUserIdQuery(classNameStr: String, appId: Option[String], userId: Option[String])
    extends EntityQuery
    with ClassNameQuery {
  override def className = Some(classNameStr)
}

final case class EventClassQuery(classNameStr: String, entitledOnly: Boolean = false)
    extends EventQuery
    with ClassNameQuery
    with AllowEntitledOnly {
  override def className = Some(classNameStr)
}

class EventReferenceQuery(val ref: BusinessEventReference) extends EventQuery {
  override def className: Option[String] = None

  override def equals(obj: Any): Boolean = obj match {
    case erq: EventReferenceQuery => erq.ref == this.ref
    case _                        => false
  }

  override def hashCode(): Int = ref.##
}

object EventReferenceQuery {
  def apply(ref: BusinessEventReference): EventReferenceQuery = new EventReferenceQuery(ref)
  def unapply(arg: EventReferenceQuery): Some[BusinessEventReference] = Some(arg.ref)
}

final case class EventSerializedKeyQuery(key: SerializedKey, entitledOnly: Boolean = false)
    extends EventQuery
    with AllowEntitledOnly {
  override def className = Some(key.typeName)
}

final case class LinkageQuery(parentTypeStr: String, parentPropertyName: String, childEntityRef: EntityReference)
    extends Query {
  override def className = Some(parentTypeStr)
}

final case class EntityCmReferenceQuery(cmid: CmReference, clazzName: String) extends EntityQuery {
  override def className: Option[String] = Some(clazzName)
}

final case class EventCmReferenceQuery(cmid: CmReference, clazzName: String) extends EventQuery {
  override def className: Option[String] = Some(clazzName)
}

sealed trait DSIQueryTemporality {
  def readTxTime: Instant
  def tempType: QueryTemporalityType
}

sealed trait HasVtTime {
  def readValidTime: Instant
}

object QueryTemporalityType extends Enumeration {
  type QueryTemporalityType = Value
  val At, ValidTime, TxTime, All, TxRange, BitempRange, OpenVtTxRange = Value
}

object DSIQueryTemporality {
  final case class At(validTime: Instant, txTime: Instant) extends DSIQueryTemporality with HasVtTime {
    def readTxTime = txTime
    def readValidTime: Instant = validTime
    def tempType = QueryTemporalityType.At
  }
  final case class ValidTime(validTime: Instant, readTxTime: Instant) extends DSIQueryTemporality with HasVtTime {
    def readValidTime: Instant = validTime
    def tempType = QueryTemporalityType.ValidTime
  }
  final case class TxTime(txTime: Instant) extends DSIQueryTemporality {
    def readTxTime = txTime
    def tempType = QueryTemporalityType.TxTime
  }
  final case class All(readTxTime: Instant) extends DSIQueryTemporality {
    def tempType = QueryTemporalityType.All
  }
  // We use a TxRange query temporality to implement a range query for replication. The idea is that we want
  // all updates for a classname or an entity within a transaction interval.
  // A query for classname cl and TxRange [t1,t2] will return all entity versions of cl where
  // TxRange.contains(r.ttInterval.from) || ttRange.contains(r.ttInterval.to)
  // to cover all possible updates to that entity class, including invalidates (the latter condition)
  final case class TxRange(range: TimeInterval) extends DSIQueryTemporality {
    def readTxTime = range.to
    def tempType = QueryTemporalityType.TxRange
  }
  class BitempRange(val vtRange: TimeInterval, val ttRange: TimeInterval, val inRange: Boolean)
      extends DSIQueryTemporality
      with HasVtTime {
    def readTxTime = ttRange.to
    def readValidTime: Instant = vtRange.to
    def tempType = QueryTemporalityType.BitempRange

    override def equals(obj: Any): Boolean = obj match {
      case btr: BitempRange => btr.vtRange == this.vtRange && btr.ttRange == this.ttRange && btr.inRange == this.inRange
      case _                => false
    }

    override def hashCode(): Int = {
      var h = vtRange.##
      h = 37 * h + ttRange.##
      h = 37 * h + inRange.##
      h
    }

    override def toString: String =
      s"BitempRange(vtRange=$vtRange, ttRange=$ttRange, inRange=$inRange)"
  }
  object BitempRange {
    def apply(vtRange: TimeInterval, ttRange: TimeInterval, inRange: Boolean): BitempRange =
      new BitempRange(vtRange, ttRange, inRange)
    def unapply(btr: BitempRange): Some[(TimeInterval, TimeInterval, Boolean)] = Some(
      (btr.vtRange, btr.ttRange, btr.inRange))
  }
  // this represents a tx range where vt timeline is open i.e. vtTo == Inf
  final case class OpenVtTxRange(range: TimeInterval) extends DSIQueryTemporality {
    def readTxTime = range.to
    def tempType = QueryTemporalityType.OpenVtTxRange
  }

  def getTemporalCoordinates(temporality: DSIQueryTemporality) = {
    temporality match {
      case All(rtt)           => (null, rtt)
      case TxTime(tt)         => (null, tt)
      case ValidTime(vt, rtt) => (vt, null)
      case At(vt, tt)         => (vt, tt)
      // This is added to support the transactionTimeline api with bounds
      case BitempRange(vtRange, ttRange, _) => (vtRange.from, ttRange.from)
      // Note that we also throw for TxRange here as this method should not be called on ranges.
      case _ => throw new IllegalArgumentException
    }
  }

}

sealed abstract class PartialQueryResult(values: Iterable[Array[Any]]) extends PartialResult(values) {
  def metaData: Option[QueryResultMetaData]
  override def generateFullResult(partialResults: Iterable[Array[Any]]): FullResult[Array[Any]] =
    throw new IllegalArgumentException("Need provide QueryResultMetaData to generate QueryResult.")
  def generateFullResult(partialResults: Iterable[Array[Any]], meta: QueryResultMetaData): FullResult[Array[Any]] =
    QueryResult(partialResults, meta)
}

final case class IntermediatePartialQueryResult(override val values: Iterable[Array[Any]])
    extends PartialQueryResult(values) {
  override def metaData: Option[QueryResultMetaData] = None
  override def isLast: Boolean = false
}

object FinalPartialQueryResult {
  def unapply(arg: Result): Option[(Iterable[Array[Any]], Some[QueryResultMetaData])] = arg match {
    case fpqr: FinalPartialQueryResult => Some(fpqr.values -> fpqr.metaData)
    case _                             => None
  }
}

final case class FinalPartialQueryResult(override val values: Iterable[Array[Any]], metaData: Some[QueryResultMetaData])
    extends PartialQueryResult(values)
    with HasResultStats[FinalPartialQueryResult] {

  override def isLast: Boolean = true
}

object QueryResult {
  def unapply(arg: Result): Option[(Iterable[Array[Any]], QueryResultMetaData)] = arg match {
    case qr: QueryResult => Some(qr.values -> qr.metaData)
    case _               => None
  }
}

final case class QueryResult(value: Iterable[Array[Any]], metaData: QueryResultMetaData)
    extends FullResult[Array[Any]](value)
    with HasResultStats[QueryResult] {

  override def generatePartialInstance(partialResult: Iterable[Array[Any]], isLast: Boolean): PartialQueryResult = {
    if (isLast) FinalPartialQueryResult(partialResult, Some(metaData)).withResultStats(getResultStats())
    else IntermediatePartialQueryResult(partialResult)
  }
}

class QueryResultHolder {
  // It is to fetch query result in non-streaming way.
  private var result: Option[QueryResult] = None
  def setResult(r: QueryResult): Unit = {
    result = Some(r)
  }
  def getResult(): QueryResult = result.getOrElse {
    throw new IllegalArgumentException(s"Expected query result but got nothing")
  }
}

class QueryResultSet(val result: Iterable[Array[Any]], val metaData: QueryResultMetaData) {
  def reader = new QueryResultSetReader(this)
}

class QueryResultSetReader(resultSet: QueryResultSet) {
  private var currentRow: Array[Any] = Array.empty
  private val metaData = resultSet.metaData
  private val iterator = resultSet.result.iterator

  // Update currentRow and moves the cursor to the next row if hasNext.
  def next(): Boolean = {
    if (iterator.hasNext) {
      currentRow = iterator.next()
      true
    } else {
      currentRow = Array.empty
      false
    }
  }

  // Check if the column is null given index.
  def isNull(index: Int): Boolean = {
    currentRow(index) == null
  }

  // Get the value of column given index and type T.
  def get[T](index: Int): T = {
    currentRow(index).asInstanceOf[T]
  }

  // Check if the column is null given fieldName.
  def isNull(fieldName: String): Boolean = {
    currentRow(metaData.fieldIndex(fieldName)) == null
  }

  // Get the value of column given fieldName and type T.
  def get[T](fieldName: String): T = {
    val index = metaData.fieldIndex(fieldName)
    currentRow(index).asInstanceOf[T]
  }
}

final case class QueryResultMetaData(val fields: Array[Field]) {
  val fieldsCount: Int = fields.length

  lazy val peFields: Seq[Int] = 0 until fieldsCount filter { i =>
    fieldType(i) == TypeCode.PersistentEntity
  }

  lazy val rectFields: Seq[Int] = 0 until fieldsCount filter { i =>
    fieldType(i) == TypeCode.Rectangle
  }

  def fieldName(index: Int): String = fields(index).name
  def fieldType(index: Int): TypeCode = fields(index).typeCode

  def fieldIndex(name: String): Int = {
    val index = fields.indexWhere(f => f.name == name)
    if (index == -1) throw new IllegalArgumentException(s"Invalid field name: $name.")
    index
  }
  def fieldType(name: String): TypeCode =
    fields
      .find(f => f.name == name)
      .map(_.typeCode)
      .getOrElse(throw new IllegalArgumentException(s"Invalid field name: $name."))
}

object QueryResultMetaData {
  import optimus.platform.dsi.expressions.PropertyLabels
  import optimus.platform.dsi.expressions.{Select => Sel}
  val StorageTxTime = PropertyLabels.StorageTxTime
  val EntityRef = PropertyLabels.EntityRef
  val VersionedRef = PropertyLabels.VersionedRef
  val Rectangle = PropertyLabels.Rectangle

  def from(s: Sel): QueryResultMetaData = {
    // we can infer more information when versioning metadata is available at server side
    val fields: Array[Field] = s.properties.iterator.map(p => Field(p.name, inferTypeCode(p.e))).toArray
    QueryResultMetaData(fields)
  }

  val SinglePersistentEntity: QueryResultMetaData = {
    QueryResultMetaData(Array(Field("*", TypeCode.PersistentEntity)))
  }
  private[optimus] def isEntityReferenceQuery(s: Sel): Boolean = {
    s.properties.exists {
      _.e match {
        case Property(PropertyType.Special, Seq(EntityRef), _) => true
        case _                                                 => false
      }
    }
  }

  private def inferTypeCode(e: Expression): TypeCode = {
    // these are the 5 forms we currently support
    e match {
      case Function(ConvertOps.ToEntity.Name, Property(PropertyType.Special, Seq(EntityRef), _) :: Nil) =>
        TypeCode.PersistentEntity
      case Property(PropertyType.Special, Seq(EntityRef), _)     => TypeCode.Reference
      case Property(PropertyType.Special, Seq(StorageTxTime), _) => TypeCode.Sequence
      case Property(PropertyType.Special, Seq(VersionedRef), _)  => TypeCode.VersionedReference
      case Property(PropertyType.Special, Seq(Rectangle), _)     => TypeCode.Rectangle
      case Aggregate(AggregateType.Count, _, _)                  => TypeCode.Long
      case _ =>
        throw new UnsupportedOperationException(s"Cannot infer TypeCode from expression: $e")
    }
  }
}

final case class Field(name: String, typeCode: TypeCode)

sealed trait PubSubResult extends Result {
  def streamId: String
  override def logDisplay(level: Int): String =
    if (il(level)) s"${this.getClass.getSimpleName}(id=$streamId)" else NoLog
}

final case class CreatePubSubStreamSuccessResult(
    override val streamId: String,
    txTime: Instant,
    postChecks: Seq[SubscriptionIdType])
    extends PubSubResult

final case class ChangeSubscriptionSuccessResult(
    override val streamId: String,
    changeRequestId: Int,
    txTime: Instant,
    postChecks: Seq[SubscriptionIdType])
    extends PubSubResult {
  override def logDisplay(level: Int): String =
    if (il(level)) s"${getClass.getSimpleName}(id=$streamId, changeRequestId=$changeRequestId)" else NoLog
}

sealed trait PubSubStreamResult extends PubSubResult

final case class PubSubSowStartResult(override val streamId: String, txTime: Instant) extends PubSubStreamResult

final case class PubSubSowEndResult(override val streamId: String, txTime: Instant) extends PubSubStreamResult

final case class PubSubCatchupStartResult(override val streamId: String, txTime: Instant) extends PubSubStreamResult

final case class PubSubCatchupCompleteResult(override val streamId: String, txTime: Instant) extends PubSubStreamResult

final case class ClosePubSubStreamSuccessResult(override val streamId: String) extends PubSubResult

sealed trait PubSubSowResult extends PubSubResult {
  def txTime: Instant
  def subId: Subscription.Id
  override def logDisplay(level: Int): String =
    if (il(level)) s"${this.getClass.getSimpleName}(id=$streamId, subId: $subId)" else NoLog
}
final case class PubSubSowFullResult(
    override val streamId: String,
    override val subId: Subscription.Id,
    override val txTime: Instant,
    result: QueryResult)
    extends PubSubSowResult
final case class PubSubSowPartialResult(
    override val streamId: String,
    override val subId: Subscription.Id,
    override val txTime: Instant,
    parRes: PartialQueryResult)
    extends PubSubSowResult
    with HasResultStats[PubSubSowResult] {}

sealed trait PubSubNotificationResultBase extends PubSubResult {
  def txTime: Instant
  def writeReqId: Option[String]
  def entries: Seq[PubSubNotificationResult.Entry]
}

sealed trait CommonDirectPubSubNotificationResult extends PubSubNotificationResultBase {
  lazy val entryMap: Map[Subscription.Id, Seq[NotificationEntry]] =
    entries
      .groupMap(_.subId)(_.ntEntry)

  override def entries: Seq[PubSubNotificationResult.SimpleEntry]

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: CommonDirectPubSubNotificationResult =>
      other.streamId == streamId && other.txTime == txTime && other.entryMap == entryMap
    case _ => false
  }

  override def hashCode(): Int = Objects.hash(streamId, txTime, entryMap)
}

object PubSubNotificationResult {
  object Entry {
    def apply(subId: Subscription.Id, ntEntry: NotificationEntry): SimpleEntry = SimpleEntry(subId, ntEntry)
  }
  sealed trait Entry {
    def subId: Subscription.Id
  }
  final case class SimpleEntry(subId: Subscription.Id, ntEntry: NotificationEntry) extends Entry
  final case class EntryReference(subId: Subscription.Id, index: Int) extends Entry
  def apply(
      streamId: String,
      writeReqId: Option[String],
      txTime: Instant,
      entries: Seq[PubSubNotificationResult.SimpleEntry]): DirectPubSubNotificationResult =
    DirectPubSubNotificationResult(streamId, writeReqId, txTime, entries)
}
sealed trait PubSubNotificationResult extends PubSubNotificationResultBase {}
final case class DirectPubSubNotificationResult(
    override val streamId: String,
    override val writeReqId: Option[String],
    override val txTime: Instant,
    override val entries: Seq[PubSubNotificationResult.SimpleEntry]
) extends FullResult[PubSubNotificationResult.SimpleEntry](entries)
    with PubSubNotificationResult
    with CommonDirectPubSubNotificationResult {

  override def logDisplay(level: Int): String =
    if (il(level)) s"${this.getClass.getSimpleName}(id=$streamId, txTime: $txTime)" else NoLog

  override def generatePartialInstance(
      partialResult: Iterable[PubSubNotificationResult.SimpleEntry],
      isLast: Boolean): DirectPubSubNotificationPartialResult =
    DirectPubSubNotificationPartialResult(streamId, writeReqId, txTime, partialResult.toSeq, isLast)
}
final case class ReferentialPubSubNotificationResult(
    override val streamId: String,
    override val writeReqId: Option[String],
    override val txTime: Instant,
    override val entries: Seq[PubSubNotificationResult.EntryReference],
    numberOfOutOfLineEntries: Int
) extends PubSubNotificationResult {}

object PubSubNotificationPartialResult {
  def apply(
      streamId: String,
      writeReqId: Option[String],
      txTime: Instant,
      entries: Seq[PubSubNotificationResult.SimpleEntry],
      isLast: Boolean): DirectPubSubNotificationPartialResult =
    DirectPubSubNotificationPartialResult(streamId, writeReqId, txTime, entries, isLast)
}
sealed trait PubSubNotificationPartialResult extends PubSubNotificationResultBase {
  val isLast: Boolean
}
final case class DirectPubSubNotificationPartialResult(
    override val streamId: String,
    override val writeReqId: Option[String],
    override val txTime: Instant,
    override val entries: Seq[PubSubNotificationResult.SimpleEntry],
    override val isLast: Boolean
) extends PartialResult[PubSubNotificationResult.SimpleEntry](entries)
    with PubSubNotificationPartialResult
    with CommonDirectPubSubNotificationResult {

  override def logDisplay(level: Int): String =
    if (il(level)) s"${this.getClass.getSimpleName}(id=$streamId, txTime: $txTime)" else NoLog

  override def generateFullResult(
      partialResults: Iterable[PubSubNotificationResult.SimpleEntry]): DirectPubSubNotificationResult =
    DirectPubSubNotificationResult(streamId, writeReqId, txTime, partialResults.toSeq)

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: DirectPubSubNotificationPartialResult => super.equals(other) && other.isLast == isLast
    case _                                            => false
  }

  override def hashCode(): Int = Objects.hash(streamId, txTime, entryMap, Boolean.box(isLast))
}
final case class ReferentialPubSubNotificationPartialResult(
    override val streamId: String,
    override val writeReqId: Option[String],
    override val txTime: Instant,
    override val entries: Seq[PubSubNotificationResult.EntryReference],
    override val isLast: Boolean,
    numberOfOutOfLineEntries: Int)
    extends PubSubNotificationPartialResult

final case class PubSubHeartbeatResult(override val streamId: String, txTime: Instant) extends PubSubResult

sealed trait PubSubGlobalResult extends PubSubResult

final case class PubSubRepLagOverlimitResult(override val streamId: String, lagTime: Long) extends PubSubGlobalResult

final case class PubSubRepLagUnderlimitResult(override val streamId: String, lagTime: Long) extends PubSubGlobalResult

final case class PubSubUpstreamChangedResult(override val streamId: String) extends PubSubGlobalResult

final case class PubSubBrokerDisconnect(override val streamId: String) extends PubSubGlobalResult

final case class PubSubBrokerConnect(override val streamId: String) extends PubSubGlobalResult

final case class PubSubTickDelay(delayedTypes: Set[String]) extends PubSubGlobalResult {
  override val streamId: String = "-1" // should not be used
}

final case class PubSubTickDelayOver(recoveredTypes: Set[String]) extends PubSubGlobalResult {
  override val streamId: String = "-1" // should not be used
}

/**
 * Response received from a PRC server when it wants to instruct us to redirect the query we sent to an alternative
 * service. Currently, the service that we use is derived purely from the RedirectionReason.
 */
final case class DalPrcRedirectionResult(redirectionReason: RedirectionReason) extends Result

private[optimus] sealed trait ServiceDiscoveryCommand extends Command

private[optimus] final case class DiscoverServices(
    establishSession: EstablishSession,
    env: DalEnv,
    sysLoc: Option[String])
    extends ServiceDiscoveryCommand {
  override def logDisplay(level: Int): String = if (il(level)) s"[SD]${getClass.getSimpleName}" else NoLog
}

// The result of a service discovery request to the DAL. We expect to get back a list of services we can go look up,
// plus some session establishment data
private[optimus] sealed trait DiscoverServicesResult extends Result

final case class DiscoverServicesSuccess(
    services: Seq[ServiceDiscoveryElement],
    establishResult: EstablishSessionSuccess)
    extends DiscoverServicesResult

private[optimus] sealed trait DiscoverServicesFailure extends DiscoverServicesResult
private[optimus] object DiscoverServicesFailure {
  // GenericFailures are just for backcompat -- used if we get an unknown FailureType enum value in a protocol message
  final case class GenericFailure(msg: String) extends DiscoverServicesFailure
  final case class CannotEstablishSession(establishResult: EstablishSessionFailure) extends DiscoverServicesFailure
  final case class CannotDiscoverServices(msg: String) extends DiscoverServicesFailure
}

sealed trait MessagesResult extends Result

/**
 * MessagesClientResult below is introduced to avoid another hierarchy of Results.
 * MessagesCreateClientStreamSuccessResult: has attribute Notification stream which will be created on the clientside
 * and is not required in the serverside result the ServerSideResult -> MessagesCreateStreamSuccessResult only would
 * have attribute streamId
 */
sealed trait MessagesClientResult extends MessagesResult

sealed trait MessagesServerResult extends MessagesResult

sealed trait MessagesStreamResult extends MessagesResult {
  def streamId: String
}

final case class MessagesPublishSuccessResult(eventClassName: String) extends MessagesServerResult

final case class StreamsACLsCommandSuccessResult(appId: String) extends MessagesServerResult

final case class MessagesCreateClientStreamSuccessResult(
    override val streamId: String,
    stream: NotificationStream[MessagesSubscription, String])
    extends MessagesClientResult
    with MessagesStreamResult
final case class MessagesCreateStreamSuccessResult(override val streamId: String)
    extends MessagesServerResult
    with MessagesStreamResult
final case class MessagesChangeSubscriptionSuccessResult(override val streamId: String, changeRequestId: Int)
    extends MessagesServerResult
    with MessagesStreamResult
final case class MessagesCloseStreamSuccessResult(override val streamId: String)
    extends MessagesServerResult
    with MessagesStreamResult

final case class MessagesErrorResult(error: Throwable) extends MessagesResult

final case class MessagesBrokerDisconnect(override val streamId: String) extends MessagesStreamResult
final case class MessagesBrokerConnect(override val streamId: String) extends MessagesStreamResult

sealed trait MessagesNotificationResult extends MessagesStreamResult {
  def publishReqId: String
  def commitId: Long
  def entries: Seq[MessagesNotificationResult.Entry]
}

object MessagesNotificationResult {
  sealed trait Entry {
    def subId: Subscription.Id
    def className: String
  }

  object Entry {
    def apply(
        subId: Subscription.Id,
        className: String,
        messagesPayload: MessagesPayloadBase
    ): Entry = messagesPayload match {
      case publishEvent: MessagesPayload                  => apply(subId, publishEvent.message)
      case publishTransaction: MessagesTransactionPayload => apply(subId, className, publishTransaction.message)
    }

    def apply(
        subId: Subscription.Id,
        serializedMsg: SerializedContainedEvent
    ): SimpleEntry = SimpleEntry(subId, serializedMsg)

    def apply(
        subId: Subscription.Id,
        className: String,
        serializedMsg: SerializedUpsertableTransaction
    ): TransactionEntry = TransactionEntry(subId, className, serializedMsg)
  }

  /** NotificationEntry used for ContainedEvent case. */
  final case class SimpleEntry(
      subId: Subscription.Id,
      serializedMsg: SerializedContainedEvent
  ) extends Entry {
    override def className: String = serializedMsg.sbe.className
  }

  final case class TransactionEntry(
      subId: Subscription.Id,
      className: String,
      serializedMsg: SerializedUpsertableTransaction
  ) extends Entry

  def apply(
      streamId: String,
      publishReqId: String,
      commitId: Long,
      entries: Seq[MessagesNotificationResult.Entry]
  ): DirectMessagesNotificationResult = DirectMessagesNotificationResult(streamId, publishReqId, commitId, entries)
}

final case class DirectMessagesNotificationResult(
    override val streamId: String,
    override val publishReqId: String,
    override val commitId: Long,
    override val entries: Seq[MessagesNotificationResult.Entry]
) extends MessagesNotificationResult {
  override def logDisplay(level: Int): String =
    if (il(level))
      s"${this.getClass.getSimpleName}(streamId=$streamId, publishReqId=$publishReqId, commitId: $commitId)"
    else NoLog
}

final case class ObliterateResult(
    appEvent: SerializedAppEvent,
    // Re-issue obliteration in "Chunking" mode.
    pendingObliterations: Set[Query],
    // Ready for last obliteration in "Full" mode.
    pendingFinalObliterations: Set[Query]
) extends TimestampedResult {
  override def txTime: Instant = appEvent.tt
  def merge(result: Result): Result = result match {
    case r: ObliterateResult =>
      ObliterateResult(
        r.appEvent,
        r.pendingObliterations ++ pendingObliterations,
        r.pendingFinalObliterations ++ pendingFinalObliterations
      )
    case VoidResult     => this
    case e: ErrorResult => e
    case o              => throw new DSISpecificError("Unexpected DSI result type " + o.getClass.getName)
  }
}
