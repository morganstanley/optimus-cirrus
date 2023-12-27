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
package optimus.tools.scalacplugins.entity.reporter

import scala.collection.mutable

object OptimusAlarms {
  private sealed trait State
  private case class RegistrationMode(alarms: mutable.Map[Int, mutable.Buffer[OptimusAlarmBuilder]]) extends State
  private case class ReadMode(alarms: Map[Int, OptimusAlarmBuilder]) extends State

  val SuppressedTag = "[SUPPRESSED]"

  @volatile private var state: State = RegistrationMode(mutable.HashMap())

  // lock used to prevent concurrent initialization / registration
  private val initLock = new Object

  def init(): Unit = {
    // force initialization of classes containing alarms. will do nothing if they were already initialized.
    // we avoid holding the lock during this so that we don't have to reason about multi-threaded class initialization
    // deadlock issues
    CopyMethodAlarms.ensureLoaded()
    DALAlarms.ensureLoaded()
    MacroUtilsAlarms.ensureLoaded()
    OptimusErrors.ensureLoaded()
    OptimusNonErrorMessages.ensureLoaded()
    PartialFunctionAlarms.ensureLoaded()
    HandlerAlarms.ensureLoaded()
    ReactiveAlarms.ensureLoaded()
    RelationalAlarms.ensureLoaded()
    StagingErrors.ensureLoaded()
    StagingNonErrorMessages.ensureLoaded()
    CodeStyleErrors.ensureLoaded()
    CodeStyleNonErrorMessages.ensureLoaded()
    Scala213MigrationMessages.ensureLoaded()
    UIAlarms.ensureLoaded()
    VersioningAlarms.ensureLoaded()
    PluginMacrosAlarms.ensureLoaded()

    initLock.synchronized {
      state match {
        case ReadMode(_) => // nothing to do
        case RegistrationMode(registeredAlarms) =>
          assert(registeredAlarms.nonEmpty, "found no alarms")

          val validatedAlarms = mutable.HashMap[Int, OptimusAlarmBuilder]()

          def initGroup(entry: (Int, mutable.Buffer[OptimusAlarmBuilder])): Unit = {
            val (_, group) = entry
            def add(oab: OptimusAlarmBuilder): Unit = {
              val sn = oab.id.sn
              validatedAlarms.get(sn) match {
                case Some(existing) =>
                  assert(
                    existing.id.sn == oab.id.sn && existing.id.tpe == oab.id.tpe && existing.template == oab.template,
                    s"Duplicated message ID $sn"
                  )
                case None => ()
              }
              validatedAlarms.put(sn, oab)
            }

            group.foreach(add)
          }

          registeredAlarms.foreach(initGroup)
          state = ReadMode(validatedAlarms.toMap)
      }
    }
  }

  def register(base: Int, alarm: OptimusAlarmBuilder): alarm.type = initLock.synchronized {
    assert(base % 10000 == 0, "illegal alarm base")
    state match {
      case ReadMode(_) => throw new IllegalStateException("already initialized")
      case RegistrationMode(registeredAlarms) =>
        val idseq: mutable.Buffer[OptimusAlarmBuilder] = registeredAlarms.getOrElse(
          base, {
            val idseq: mutable.Buffer[OptimusAlarmBuilder] = mutable.ListBuffer()
            registeredAlarms.put(base, idseq)
            idseq
          })
        idseq += alarm
        alarm
    }
  }

  // volatile read, so worst case is that we think it's in RegistrationMode but it's really in ReadMode in which case
  // init() does nothing and we try again.
  def get(id: Int): Option[OptimusAlarmBuilder] = state match {
    case ReadMode(validatedAlarms) =>
      validatedAlarms.get(id)
    case RegistrationMode(_) =>
      init()
      get(id)
  }
}

trait OptimusAlarms {
  // TODO (OPTIMUS-62197): get rid of this field entirely
  protected def base: Int
  private val idseq: mutable.Buffer[OptimusAlarmBuilder] = mutable.ListBuffer()

  // This is used as a hack to make sure alarm classes get loaded
  def ensureLoaded(): Unit = {}

  protected def register(alarm: OptimusAlarmBuilder): alarm.type = OptimusAlarms.register(base, alarm)

  final def idToText: Map[AlarmId, String] =
    idseq.map { a =>
      (a.id, a.template)
    }.toMap

  final def alarmId(sn: Int, tpe: OptimusAlarmType.Tpe): AlarmId = {
    require(sn >= 0)
    AlarmId(sn, tpe)
  }
}

trait BuilderHelpers { self: OptimusAlarms =>
  import OptimusAlarmType._
  type Info
  private type Ctor[R] = (AlarmId, Info, String) => R
  def newBuilder[R <: OptimusAlarmBuilder](builder: Ctor[R])(tpe: Tpe): (Int, Info, String) => R =
    (sn: Int, phase: Info, template: String) => register(builder(alarmId(sn, tpe), phase, template))
}

trait OptimusAlarmBuilder {
  val id: AlarmId
  val template: String
}

object OptimusAlarmType extends Enumeration {
  type Tpe = Value

  val SILENT, DEBUG, INFO, WARNING, ERROR, ABORT = Value
}

final case class AlarmId(sn: Int, tpe: OptimusAlarmType.Tpe)

trait OptimusAlarmBase {
  val id: AlarmId
  val message: String
  val template: String

  override def toString(): String = OptimusMessageRegistry.getMessage(this)

  def isError: Boolean = id.tpe == OptimusAlarmType.ERROR
  def isWarning: Boolean = id.tpe == OptimusAlarmType.WARNING
  def isInfo: Boolean = id.tpe == OptimusAlarmType.INFO
  def isDebug: Boolean = id.tpe == OptimusAlarmType.DEBUG
  def isAbort: Boolean = id.tpe == OptimusAlarmType.ABORT
}
