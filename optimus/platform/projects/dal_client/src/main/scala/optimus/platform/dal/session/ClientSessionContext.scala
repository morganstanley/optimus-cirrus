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
package optimus.platform.dal.session

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantReadWriteLock

import optimus.dsi.session.EstablishSession
import optimus.dsi.session.EstablishSessionFailure
import optimus.dsi.session.EstablishSessionResult
import optimus.dsi.session.EstablishSessionSuccess
import optimus.dsi.session.EstablishedClientSession
import optimus.platform.dal.SessionFetcher
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal.DALRetryableActionException
import optimus.platform.dsi.bitemporal.DSISpecificError
import optimus.session.utils.ClientEnvironment

object ClientSessionContext {
  final case class SessionData(
      existingSession: Option[EstablishedClientSession],
      establishSession: Option[(Int, EstablishSession)],
      closeSessions: Set[Int]) {
    def rolesetMode: Option[RolesetMode.Established] = existingSession.map(session => session.rolesetMode)
  }

  class SessionParametersChangedException extends Exception
  class UnregisteredClasspathException(message: String) extends Exception(message)
  // client=None is ok as we don't need to close the client which encounters this exception
  class UnregisteredClasspathHashException(message: String) extends DALRetryableActionException(message, None)
  class OnBehalfSessionInvalidProidException(message: String) extends Exception(message)
  class OnBehalfSessionInvalidTokenException(message: String) extends Exception(message)

  sealed abstract class SessionState
  case object Uninitialized extends SessionState
  final case class Initialized(session: EstablishedClientSession) extends SessionState
  final case class PendingEstablish(command: EstablishSession) extends SessionState
  final case class PendingReestablish(currentSession: EstablishedClientSession, command: EstablishSession)
      extends SessionState

  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  private[optimus /*platform*/ ] def handleEstablishSessionFailure(
      es: EstablishSession,
      esf: EstablishSessionFailure): (Exception, Option[EstablishSession]) = {
    val failureType = esf.failureType
    val message = esf.message
    failureType match {
      case EstablishSessionFailure.Type.Generic =>
        (new DSISpecificError(message), None)
      case EstablishSessionFailure.Type.SessionParametersChanged =>
        (new SessionParametersChangedException(), None)
      case EstablishSessionFailure.Type.UnregisteredClasspath if es.fullClasspath.isEmpty =>
        // We failed to connect by classpath hash alone.
        // Retry but this time include our full classpath in the hopes that the broker will recognize it to be
        // a subset of something registered
        log.info("Session establishment failed due to unregistered classpath hash. Will retry with full classpath.")
        val establishSession = es.copy(fullClasspath = Some(ClientEnvironment.classpath))
        (new UnregisteredClasspathHashException(message), Some(establishSession))
      case EstablishSessionFailure.Type.UnregisteredClasspath => (new UnregisteredClasspathException(message), None)
      case EstablishSessionFailure.Type.OnBehalfSessionInvalidProid =>
        (new OnBehalfSessionInvalidProidException(message), None)
      case EstablishSessionFailure.Type.OnBehalfSessionInvalidToken =>
        (new OnBehalfSessionInvalidTokenException(message), None)
      case _ => (new DSISpecificError(s"Unknown failure: $message"), None)
    }
  }

  private[optimus /*platform*/ ] def handleEstablishSessionSuccess(
      es: EstablishSession,
      ess: EstablishSessionSuccess): EstablishedClientSession = {
    val EstablishSessionSuccess(
      appId,
      appConfigFound,
      sessionId,
      serverSupportedFeatures,
      roleset,
      establishmentTime,
      encryptedSessionToken,
      entitlementsToken,
      keyHash) = ess
    EstablishedClientSession(
      appId,
      appConfigFound,
      sessionId,
      serverSupportedFeatures,
      roleset,
      establishmentTime,
      es,
      encryptedSessionToken,
      entitlementsToken,
      keyHash)
  }
}

trait ClientSessionContextT {
  def sessionData: ClientSessionContext.SessionData
}

class ClientSessionContext extends ClientSessionContextT {
  import ClientSessionContext._

  private var _sessionState: SessionState = Uninitialized
  private val _sessionFetcher = new AtomicReference[SessionFetcher]()
  // Keeping the lock mechanism as fair as we ideally want any changes to the sessionState to be acknowledged first.
  // Given that state changes are expected to be rare once connection state is Initialized this should be safe.
  private val syncLock = new ReentrantReadWriteLock(true)

  private[optimus] def bindSessionFetcher(sessionFetcher: SessionFetcher): Unit = {
    _sessionFetcher.set(sessionFetcher)
  }

  protected def nextSessionId(lastSessionId: Int) = lastSessionId + 1
  // Testing hook
  protected def notifySetEstablishTime(): Unit = {}
  protected def notifySessionData(sessionData: SessionData): Unit = {}
  protected def notifyBeforeSessionEstablished(): Unit = {}

  def consumeEstablishSessionResult(result: EstablishSessionResult): Unit = withWriteLock {
    sessionData.establishSession.foreach { case (_, command) =>
      require(!_sessionState.isInstanceOf[Initialized])
      result match {
        case ess: EstablishSessionSuccess =>
          val hookedCommand = if (SessionFetcher.enableGlobalSessionTt) {
            Option(_sessionFetcher.get())
              .map { sessionFetcher =>
                val globalTimeOpt = sessionFetcher.getGlobalSessionEstablishmentTime
                if (globalTimeOpt.isEmpty) {
                  // In the half way of EvaluationContext initialization:
                  //   - ess.establishmentTime will be set as GlobalSessionTxTime
                  //   - hook command with ess.establishmentTime(the incoming GlobalSessionTxTime) for the later compare "s.establishingCommand == command" in setEstablishSession

                  // Assumption: GetInfo() is the only one who hits this case at initialization stage
                  // Update EstablishmentTime globally
                  sessionFetcher.initGlobalSessionEstablishmentTime(ess.establishmentTime)
                  val hookedCommand = command.copy(
                    sessionInfo = command.sessionInfo.copy(establishmentTime = Some(ess.establishmentTime)))
                  notifySetEstablishTime()
                  hookedCommand
                } else {
                  // Assume there is no race condition for EvaluationContext initialization:
                  //  there is only one session establishment (GetInfo) at init stage, whose session establishment time is set as global (EntityResolver-wise)
                  //  the other session establishment command shall be hooked with this global time
                  val commandTimeOpt = command.sessionInfo.establishmentTime
                  if (commandTimeOpt != globalTimeOpt) {
                    log.warn(
                      s"session establishment command shall be hooked via global value [$globalTimeOpt], but command still uses [$commandTimeOpt]")
                  }
                  command
                }
              }
              .getOrElse {
                log.warn("GlobalSessionTt is enabled but SessionFetcher is not bound")
                command
              }
          } else {
            command
          }
          val established = handleEstablishSessionSuccess(hookedCommand, ess)
          notifyBeforeSessionEstablished()
          setState(Initialized(established))
          log.info(
            s"Client session established on server at ${ess.establishmentTime} with roleset: " +
              s"${ess.rolesetMode} and entitlements token: ${ess.entitlementsToken.toLoggableString()}")
          log.info(
            s"Client supported features are " +
              s"${SupportedFeatures.myFeatures.features.map(_.value).mkString("", ", ", "")}")
        case esf: EstablishSessionFailure =>
          val (ex, esOpt) = handleEstablishSessionFailure(command, esf)
          esOpt.foreach(es => setEstablishSession(es))
          throw ex
      }
    }
  }

  private def overwriteWithGlobalSessionTxTime(command: EstablishSession): EstablishSession = {
    if (SessionFetcher.enableGlobalSessionTt && command.sessionInfo.establishmentTime.isEmpty) {
      Option(_sessionFetcher.get())
        .flatMap(_.getGlobalSessionEstablishmentTime)
        .map(globalTime => command.copy(sessionInfo = command.sessionInfo.copy(establishmentTime = Some(globalTime))))
        .getOrElse(command)
    } else {
      // GlobalSessionTt disabled or Already has been overwritten with GlobalSessionTxTime or RuntimeProperties.DsiSessionEstablishmentTimeProperty
      command
    }
  }

  def sessionData: SessionData = {
    val sessionData = withReadLock {
      _sessionState match {
        case Uninitialized             => SessionData(None, None, Set.empty)
        case Initialized(session)      => SessionData(Some(session), None, closedSessions(None))
        case PendingEstablish(command) =>
          // not hard-coding 0 instead calling nextSessionId as subclasses can provide different id to start with
          SessionData(None, Some(nextSessionId(-1), overwriteWithGlobalSessionTxTime(command)), closedSessions(None))
        case PendingReestablish(session, command) =>
          // Since we're establishing a new session, it must have different parameters from the old one.
          // So use a new sessionId, and close the old one.
          SessionData(
            Some(session),
            Some(nextSessionId(session.sessionId), overwriteWithGlobalSessionTxTime(command)),
            closedSessions(Some(session.sessionId)))
      }
    }

    notifySessionData(sessionData)
    sessionData
  }

  def isNotInitialized: Boolean = withReadLock {
    _sessionState match {
      case _: Initialized                                              => false
      case Uninitialized | _: PendingEstablish | _: PendingReestablish => true
    }
  }

  // subclasses can override with a different behaviour like holding all closed sessions across different ClientSessionContexts
  protected def closedSessions(id: Option[Int]): Set[Int] = id map (Set(_)) getOrElse Set.empty[Int]

  private[optimus] def setEstablishSession(establishSession: => EstablishSession): Unit = withWriteLock {
    val command = overwriteWithGlobalSessionTxTime(establishSession)
    val newState = _sessionState match {
      case Uninitialized                                        => PendingEstablish(command)
      case Initialized(s) if (s.establishingCommand == command) =>
        // We only need to re-establish if the EstablishSession is different from what got us our last session
        Initialized(s)
      case Initialized(s)           => PendingReestablish(s, command)
      case PendingEstablish(_)      => PendingEstablish(command)
      case PendingReestablish(s, _) => PendingReestablish(s, command)
    }
    setState(newState)
  }

  private[optimus] def connectionClosed(): Unit = withWriteLock {
    val newState = _sessionState match {
      case Initialized(session) => PendingReestablish(session, session.reestablishCommand)
      case _                    => _sessionState
    }
    setState(newState)
  }

  private def setState(newState: SessionState): Unit = withWriteLock {
    if (newState == _sessionState) {
      log.info(s"Client session establishment state unchanged: ${_sessionState.getClass.getSimpleName}")
    } else {
      log.info(
        s"Client session establishment state changed: " +
          s"${_sessionState.getClass.getSimpleName} => ${newState.getClass.getSimpleName}")
      _sessionState = newState
    }
  }

  private[optimus] def connectionSession(): Option[EstablishedClientSession] = withReadLock {
    _sessionState match {
      case Initialized(s) => Some(s)
      case _              => None
    }
  }

  private def withReadLock[T](f: => T): T = {
    syncLock.readLock.lock()
    try {
      f
    } finally { syncLock.readLock.unlock() }
  }

  private def withWriteLock[T](f: => T): T = {
    syncLock.writeLock.lock()
    try {
      f
    } finally { syncLock.writeLock.unlock() }
  }
}
