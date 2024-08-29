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
package optimus.platform.dsi

import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Crumb._
import optimus.breadcrumbs.crumbs.Events

private[optimus] object DalClientCrumbSource extends Crumb.DalSource {
  override def name = "DALCLT"; override val flags = CrumbFlags(CrumbFlag.DoNotReplicateOrAnnotate)
}
private[optimus] object DalUpgradeCrumbSource extends Crumb.DalSource { override def name = "DALAUT" }
private[optimus] object DalZoneRequestCrumbSource extends Crumb.DalSource { override def name = "DALTZR" }
private[optimus] object DalCrumbSource extends Crumb.DalSource { override def name = "DAL" }
private[optimus] object DalTraceCrumbSource extends Crumb.DalSource { override def name = "DALTRC" }
private[optimus] object DalEntCrumbSource extends Crumb.DalSource { override def name = "DALENT" }
private[optimus] object DALHCCrumbSource extends Crumb.Source { override val name = "DALHC" }
private[optimus] object DalRequestTrackerCrumbSource extends Crumb.DalSource { override def name = "DALRQT" }
private[optimus] object DalDualBackendsCrumbSource extends Crumb.DalSource { override def name = "DALDUAL" }
private[optimus] object DalDdcCrumbSource extends Crumb.DalSource { override def name = "DALDDC" }
private[optimus] object DalDdcDiagCrumbSource extends Crumb.DalSource { override def name = "DALDDCDIAG" }
private[optimus] object DalDiagnosticCrumbSource extends Crumb.DalSource { override def name = "DALDIAG" }
private[optimus] object DalMgoIdxCrumbSource extends Crumb.DalSource { override def name = "DALMGOIDX" }
private[optimus] object DalMetaCrumbSource extends Crumb.DalSource { override def name = "DALMETA" }
private[optimus] object DalMetaConnCrumbSource extends Crumb.DalSource { override def name = "DALMETACON" }
private[optimus] object DalPubSubCrumbSource extends Crumb.DalSource { override def name = "DALPS" }
private[optimus] object DalPubSubClientCrumbSource extends Crumb.DalSource { override def name = "DALPSCLT" }
private[optimus] object DalMessagesServerCrumbSource extends Crumb.DalSource { override def name = "DALMSGS" }
private[optimus] object DalMessagesClientCrumbSource extends Crumb.DalSource { override def name = "DALMSGSCLT" }
private[optimus] object DalReconCrumbSource extends Crumb.DalSource { override def name = "DALRCN" }
private[optimus] object DalRecoveryCrumbSource extends Crumb.DalSource { override def name = "DALREC" }
private[optimus] object DalReplicationCrumbSource extends Crumb.DalSource { override def name = "DALREP" }
private[optimus] object DalScriptCrumbSource extends Crumb.DalSource { override def name = "DALSCRPT" }
private[optimus] object DalThrottlingCrumbSource extends Crumb.DalSource { override def name = "DALTHR" }
private[optimus] object DalThrottlingStatsCrumbSource extends Crumb.DalSource { override def name = "DALTHRSTATS" }
private[optimus] object DalAlertCrumbSource extends Crumb.DalSource { override def name = "DALERT" }
private[optimus] object DalDeriv1CrumbSource extends Crumb.DalSource { override def name = "DALDERIV1" }
private[optimus] object DdcCrumbSource extends Crumb.Source { override val name = "DDC" }
private[optimus] object DdcMetaCrumbSource extends Crumb.Source { override def name = "DDCMETA" }
private[optimus] object DpcCrumbSource extends Crumb.Source { override val name = "DPC" }
private[optimus] object PayloadCacheDiagnosticCrumbSource extends Crumb.Source { override val name = "PYLDDIAG" }
private[optimus] object PrcCrumbSource extends Crumb.Source { override val name = "PRC" }
private[optimus] object PrcDiagnosticCrumbSource extends Crumb.Source { override val name = "PRCDIAG" }
private[optimus] object PrcMetaCrumbSource extends Crumb.Source { override def name = "PRCMETA" }
private[optimus] object SkCrumbSource extends Crumb.Source { override val name = "SK" }
private[optimus] object SkDiagnosticCrumbSource extends Crumb.Source { override val name = "SKDIAG" }
private[optimus] object SatelliteCrumbSource extends Crumb.Source { override val name = "SSV" }
private[optimus] object CrumbsBackfillSource extends Crumb.Source { override val name = "CCB" }
private[optimus] object DalPerformanceTestBreadcrumbsSource extends Crumb.Source { override val name = "DALPT" }
private[optimus] object DalUowKafkaCrumbSource extends Crumb.Source { override val name = "UOWKFKA" }
private[optimus] object DalKafkaMonitorCrumbSource extends Crumb.Source { override val name = "DALKAFKAMON" }

private[optimus] object DalEvents {
  object Client {
    val RequestSubmitted = Events.DalClientReqSent
    val ResponseReceived = Events.DalClientRespRcvd
    val RequestTimedOut = Events.DalClientReqTimedOut
  }

  object Satellite {
    val RequestReceived = Events.DalSatReqRecvd
    val RequestHandled = Events.DalSatReqHndld
  }

  object Server {
    val RequestReceived = Events.DalServerReqRcvd
    val RequestHandled = Events.DalServerReqHndld
    val LongRunningRequest = Events.DalServerLRR
    val GarbageCollection = Events.DalServerGC
    val PartialDalBrokerCacheHit = Events.DalServerParBrkrCacheHit
    val FullDalBrokerCacheHit = Events.DalServerFullBrkrCacheHit
    val ExecInDDC = Events.DalServerExecInDDC
    val ExecLocally = Events.DalServerExecLocally
    val DdcRequest = Events.DalServerDdcReq
    val DdcResponse = Events.DalServerDdcRes
    val DalPrcRequest = Events.DalServerPrcReq
    val ThrottlingZoneConfiguration = Events.DalServerZnConfig
    val ThrottlingZoneLimitCrossing = Events.DalServerZnLmtBrch
    val WriteVisited = Events.DalServerWriteVisited
    val TxRangeThresholdViolation = Events.DalServerTxRangeViolation
  }

  object WorkManager {
    val RecoveredEntry = Events.DalWorkMgrRecoveredEntry
  }
}
