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
package optimus.platform.dal.monitoring

object BreadcrumbsSchema {
  object Global {
    object Properties {
      final val env = "env"
      final val host = "host"
      final val port = "port"
      final val typeProperty = "type"
      final val plant = "plant"
      final val cluster = "cluster"
      final val event = "event"
      final val namespaceHash = "ns_hash"
      final val clientHost = "client_host"
      final val serverhost = "server_host"
      final val logdir = "hc_log_dir"
      final val partition = "ptn"
      final val events = "evts"
      final val appId = "aid"
      final val zoneId = "zid"
      final val proid = "proid"
    }
    object Values {
      final val silverking = "silverking"
      final val prc = "prc"
      final val server = "server"
      final val kafka = "kafka"
      final val ui = "ui"
      final val entityBrowser = "entityviewer"
      final val dht = "DhtHealthCheck" // from keySpace in DhtHealthCheckMain
      final val ddc = "DdcHealthCheck"
      final val inequality = "inequality"
      final val broker = "broker"
    }
    object TimeSummary {
      final val startTime = "startTime"
      final val endTime = "endTime"
      final val time = "time"
      final val timeTaken = "timeTaken"
      final val eta = "estimatedTime"
    }
  }
  object Exceptions {
    object Properties {
      final val cause = "cause"
      final val stacktrace = "st"
      final val reason = "reason"
    }
    object Values {
      final val error = "error"
      final val inequality = "inequality"
    }
  }
  object Metrics {
    object Properties {
      final val typeProperty = "metrics_type"
      final val subtypeProperty = "metrics_subtype"
      final val rejectedProperty = "TaskRejected"
      final val checkedNodes = "nodes_chk_tot"
      final val nodesFailed = "nodes_fail_tot"
      final val failedHosts = "failed_hosts"
    }
    object Values {
      final val status = "status"
    }
  }
  object HealthCheck {
    object Properties {
      final val typeProperty = "hc_type"
      final val result = "hc_result"
      final val readTime = "hc_read_t"
      final val writeTime = "hc_write_t"
      final val totalTime = "hc_total_t"
      final val treadmillApp = "tm_app"
      final val partition = "ptn"
    }
    object Values {
      final val success = "success"
      final val failure = "fail"
      final val acc = "acc"
      final val pubsub = "pubsub"
      final val deriv1 = "deriv1"
      final val default = "default"
      final val fts = "fts"
    }
  }
  object SilverKing {
    object Properties {
      final val reapTime = "reapTime"
      final val evtType = "kind"
      final val queueLength = "qlen"
      final val segmentNumber = "segnum"
      final val creationTime = "ctime"
      final val keys = "keys"
      final val channel = "chl"
      final val sendUuid = "sndId"
      final val traceId = "traceId"

      // For msg timeout
      final val lastPollTime = "lastPoll"
      final val currPollTime = "currPoll"
      final val currQueueSize = "currQSize"
      final val msgCreationTime = "msgCTime"
      final val deadline = "fakeDdl"
      final val timeoutAt = "timeoutAt"
    }
  }
  object MapDb {
    final val key = "mapdb"
    object Properties {
      final val store = "store"
      final val keys = "keys"
      final val writeMillis = "writeMs"
    }
  }
  object Upgrade {
    final val key = "aut"
    final val version = "version"
    final val revert = "revertedTo"
    object Properties {
      final val autAlert = "alert"
      final val autAlertType = "alertType"
      final val failureException = "failureException"
      final val manualInterventionMsg = "manualInterventionMsg"
      final val stage = "stage"
      final val codeVersion = "code"
      final val configVersion = "cfg"
      final val user = "usr"
      final val tcm = "tcm"
      final val timeTaken = "ms"
      final val zappPlant = "zPlant"
      final val zappEnv = "zEnv"
      final val bounceMethod = "bm"
      final val numThreads = "nt"
      final val autosysEnv = "autosys"
      final val interactive = "int"
      final val parallelBouncePercentage = "pbp"
      final val zappPolicy = "zPolicy"
      final val configOnly = "cfgOnly"
      final val laasEnv = "laas"
      final val logCopyStatus = "logCopyStatus"
    }
  }

  object ZoneRequest {
    final val key = "tzr"
    object Properties {
      final val stage = "stage"
      final val zoneRequestId = "tzrId"
      final val zoneId = "zoneId"
      final val regionId = "reqId"
      final val appId = "appId"
      final val partitionId = "partId"
      final val status = "status"
      final val requester = "req"
      final val assignee = "assignee"
      final val dalEnv = "env"
      final val tcmId = "tcm"
      final val owner = "owner"
      final val user = "usr"
    }
  }

  object Deriv1 {
    object Properties {
      final val entityType = "entityType"
      final val asset = "asset"
      final val partition = "partition"
      final val dbType = "dbType"
      final val eref = "eref"
      final val entityClass = "entityClass"
      final val fieldName = "fld"
      final val tf = "tf"
      final val vf = "vf"
      final val transformationStage = "transfStage"
      final val txnUuid = "txnUuid"
      final val txnTime = "txnTime"
      final val txnNumberOfEntities = "txnNumberOfEntities"
      final val transformationStageStartTime = "transfStageStT"
      final val transformationStageDurationMs = "transfStageDurMs"
      final val targetDb = "targetDb"
      final val deriv1Process = "deriv1Process"
      final val processingStats = "processingStats"
    }
    object Values {
      def broker = "broker"
      def transformer = "transformer"
    }
  }

  object Tr {
    object Properties {
      final val entityType = "entityType"
      final val entityClass = "entityClass"
      final val eref = "eref"
      final val fieldName = "fld"
      final val tf = "tf"
      final val vf = "vf"
    }
  }
  object Deriv1ProcessingStage {
    object Properties {
      final val brokerReceive = "brokerReceive"
      final val brokerSerialize = "brokerSerialize"
      final val brokerMapTimeslice = "brokerMapTimeslice"
      final val brokerGenerateMessage = "brokerGenerateMessage"
      final val brokerSendMessage = "brokerSendMessage"
      final val transformerReceive = "transformerReceive"
      final val transformerDeserialize = "transformerDeserialize"
      final val transformerToDeriv1Entity = "transformerToDeriv1Entity"
      final val transformerPersist = "transformerPersist"
      final val transformerPersistAsync = "transformerPersistAsync"
      final val transformerGenerateResponse = "transformerGenerateResponse"
      final val transformerSendResponse = "transformerSendResponse"
      final val brokerReceiveLsqtResponse = "brokerReceiveLsqtResponse"
      final val brokerPersistLsqt = "brokerPersistLsqt"
      /* Entity processing stages for disabled transformer mode */
      final val brokerDeserialize = "brokerDeserialize"
      final val brokerTransform = "brokerTransform"
      final val brokerPersist = "brokerPersist"
    }
  }

  object Throttling {
    object Properties {
      final val zoneId = "zid"
      final val limit = "limit"
      final val read = "read"
      final val write = "write"
      final val debt = "debt"
      final val globalLimit = "gLimit"
      final val globalDebt = "gDebt"
      final val throttleMultiplier = "mult"
      final val dynamicThrottleMultiplier = "dMult"
      final val movAvgEpochWinLen = "winLen"
    }
    object Values {
      def invalidZone = "invalidZone"
    }
  }
  object PostCommit {
    object Properties {
      final val actions = "acts"
      final val heartbeats = "hb"
      final val flushLatencyMean = "meanFlushMs"
      final val flushLatencyMax = "maxFlushMs"
      final val lsqt = "lsqt"
    }
    object Values {
      final val key = "pstcmt"
    }
  }

  object Database {
    object Properties {
      val dataSource = "ds"
      val hostnameSample = "hs"
      val replicaSet = "rs"
      val database = "db"
      val collection = "coll"
      val index = "idx"
    }
    object Values {
      val missing = "missing"
      val extra = "extra"
    }
  }

  object BrokerMetadata {
    object Properties {
      val size = "size"
      val prefix = "prefix"
      val pools = "pools"
      val metadata = "metadata"
      val minTxn = "minTxn"
      val maxTxn = "maxTxn"
      val delayTT = "delay"
      val zone = "zone"
      val host = "host"
      val port = "port"
      val env = "env"
      val partitions = "partitions"
      val dalRelease = "dal_release"
      val typ = "type"
      val subtyp = "subtype"
      val broker = "broker"
      val peer = "peer"
      val repub = "repub"
      val read = "read"
      val write = "write"
      val acc = "acc"
      val capability = "capability"
      val postgres = "postgres"
      val li = "li"
      val name = "name"
      val ip = "ip"
    }
  }

  object Recorder {
    object Properties {
      final val id = "id"
      final val key = "key"
      final val commitTime = "cmt_t"
      final val stallTime = "stall_t"
      final val fetchTime = "fetch_t"
      final val processingTime = "prc_t"
      final val publishTime = "pub_t"
      final val latency = "lat"
      final val queueTime = "q_t"
      final val topic = "topic"
      final val backfillerEnv = "backfiller_env"
      final val zookeeper = "zookeeper"
      final val zkEnv = "zk_env"
      final val zkPath = "zk_path"
      final val pollerPerThreadFlag = "poller_per_thread_flag"
      final val batchCommitSize = "batch_commit_size"
      final val propagateCpsFlag = "propagate_cps_flag"
      final val waitInMillisForConnectionIssues = "wait_in_millis_for_connection_issues"
      final val enableLeaderWatcherOnSelf = "enable_leader_watcher"
      final val enableRecorderFlowWithoutZkArgs = "enable_recorder_flow_without_zk_args"
      final val bridgeMode = "bridge_mode"
      final val mqManager = "mq_manager"
      final val queueName = "queue_name"
      final val errorMqManager = "error_mq_manager"
      final val errorQueueName = "error_queue_name"
      final val messageKafkaKeyRegexes = "message_kafka_key_regexes"
      final val concurrencyLevel = "concurrency_level"
    }
  }

  object Backfiller {
    object Properties {
      final val ids = "ids"
      final val keys = "keys"
      final val source = "Source"
      final val enIn = "en_in"
      final val enOut = "en_out"
      final val grpIn = "grp_in"
      final val grpOut = "grp_out"
      final val failed = "failed"
      final val time = "timer"
      final val save = "Saved"
      final val backfillStats = "backfillStats"
      final val kafkaTopic = "kafka_topic"
      final val kafkaLag = "kafka_lag"
      final val overallLatency = "msg_ete_latency"
      final val recordsSize = "record_s"
      final val bridgeConsumerGroup = "bcg"
      final val sourceRoutees = "source_routees"
      final val sinkRoutees = "sink_routees"
      final val convertRoutees = "convert_routees"
      final val reconcilerRoutees = "reconciler_routees"
      final val slotterRoutees = "slotter_routees"
      final val objectCompressRoutees = "o_compress_routees"
      final val entityCompressRoutees = "e_compress_routees"
      final val sinkRetries = "sink_retries"
      final val convertRetries = "convert_retries"
      final val sourceRetries = "source_retries"
      final val ensureOrdering = "ensure_ordering"
      final val batchSize = "batch_size"
      final val sliceCount = "slice_count"
      final val expandAfterCompression = "expand_after_compression"
      final val batchMaxTime = "batch_max_time"
      final val compressionReadFromDal = "compression_read_from_dal"
      final val enableSliceBreaker = "enable_slice_breaker"
    }
  }

  object Cloud {
    object Properties {
      final val clusterName = "clstr_n"
      final val releaseVersion = "release_v"
      final val startTime = "start_t"
      final val endTime = "end_t"
      final val stage = "stage"
      final val component = "component"
      final val deploymentType = "type"
      final val result = "result"
      final val status = "status"
      final val podCount = "p_count"
      final val expectedPodCount = "ep_count"
    }
  }

  object PipelineMonitor {
    object Properties {
      final val processor = "processor"
      final val success = "successCount"
      final val completed = "completedCount"
      final val failure = "failureCount"
      final val total = "totalCount"
      final val appName = "appName"
      final val requestId = "requestID"
    }
    object Status {
      final val stage = "stage"
      final val status = "status"
    }
  }
  object IndexMonitor {
    object ModifyIndex {
      final val className = "className"
      final val index = "index"
    }
    object BackfillPayload {
      final val backfillState = "backfillState"
      final val appName = "appName"
      final val status = "status"
    }
    object Values {
      final val pendingCount = "pendingCount"
      final val ongoingCount = "ongoingCount"
      final val failedCount = "failedCount"
      final val completedCount = "completedCount"
    }
    object DHT {
      final val DHTRegion = "dhtRegion"
    }
  }

}
