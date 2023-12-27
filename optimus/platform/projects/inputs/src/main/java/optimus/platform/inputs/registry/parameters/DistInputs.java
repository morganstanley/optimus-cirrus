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
package optimus.platform.inputs.registry.parameters;

import java.util.List;

import optimus.platform.inputs.EngineForwarding;
import optimus.platform.inputs.NodeInputs.ScopedSINodeInput;
import optimus.platform.inputs.NodeInputs;
import optimus.platform.inputs.dist.DistNodeInputs;
import optimus.platform.inputs.registry.CombinationStrategies;
import optimus.platform.inputs.registry.Source;

public class DistInputs {

  // used for back compatibility until we can move completely to NodeInput<T>
  public static class SourceNames {
    public static final String OPTIMUS_DIST_GSF_PREFIX = "optimus.dist.gsf.";
    public static final String CancelTasksOnExit = "client.cancelTasksOnExit";
    public static final String EngineCmdLine = "client.engine.cmdLine";
    public static final String ProvideEngineClasspath = "client.engine.provideClasspath";
    public static final String ProvideEngineEnv = "client.engine.provideEngineEnv";
    public static final String EngineEnv = "client.engine.env";
    public static final String EngineExecutablePath = "client.engine.execCmd.path";
    public static final String EngineReleaseLink = "client.engine.execCmd.releaseLink";
    public static final String EngineAppDir = "client.engine.appDir";
    public static final String CompressData = "client.compressData";
    public static final String EngineConfig = "client.engine.config";
    public static final String EngineLogConfig = "client.engine.logConfig";
    public static final String EngineExecutable = "client.engine.execCmd";
    public static final String ClientLoggingInterval = "client.logging.interval";
    public static final String ClientThrottlingPeriod = "client.throttling.period";
    public static final String BrokerAvailabilityTimeout = "broker.availability.timeout";
    public static final String BrokerZookeeperPath = "broker.zookeeper.path";
    public static final String BrokerZookeeperMode = "broker.zookeeper.mode";
    public static final String BrokerRetryTimeout = "broker.connection.max.retry.time";
    public static final String BrokerHostportList = "broker.hostport.list";
    public static final String BrokerHeartbeatInterval = "broker.heartbeat.interval";
    public static final String BrokerConnectionSslEnabled = "broker.connection.sslEnabled";
    public static final String BrokerConnectionSocksProxy = "broker.connection.socks.proxy";
    public static final String BrokerConnectionMaxRetries = "broker.connection.max.retries";
    public static final String BrokerConnectionKerberised = "broker.connection.kerberised";
    public static final String AllowUnmappablePathForDev = "optimus.dist.allowUnmappablePathForDev";
    private static final String StratosphereGridClasspath =
        "stratosphere.grid.launcher.app.classpath";
    private static final String VersionToken = "optimus.dist.versionToken";
    private static final String ClasspathExcludeRegexes = "client.engine.classPathExcludeRegexes";
    private static final String ImmutableLocations = "client.immutableLocations";
    private static final String LocalExecution = "client.localExecution";
    public static final String LegacyLogFileDirectory = "localdist.engine.logFileDirectory";
    public static final String DefaultTaskTimeout = "client.defaultTaskTimeout";
    public static final String DepthLimit = "client.depthLimit";
    public static final String EngineSpecEntryPoint = "client.engineSpec.entryPoint";
    public static final String MonitorReleaseLink = "client.monitorRellink";
    public static final String ReplayPersistDir = "client.replay.persistDir";
    public static final String BrokerZookeeperWaitTimeout = "broker.zookeeper.waitTimeout";
    public static final String AppInfoFile = "interop.appInfoFile";
  }

  // Environment Variables

  public static final ScopedSINodeInput<String> AppDir =
      NodeInputs.newTransient(
          "AppDir",
          "The directory from where the application launcher is invoked",
          Source.fromEnvironmentVariable("APP_DIR"),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Boolean> OptimusDistPropagateEffectiveId =
      DistNodeInputs.newAutoForwardingOptimusDistEnvironmentVariableWithDefault(
          "OptimusDistPropagateEffectiveId",
          "I am not sure!!!",
          false,
          Source.fromBoolEnvironmentVariable("OPTIMUS_DIST_PROPAGATE_EFFECTIVE_ID"),
          Object::toString,
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> BrokerHostportOverride =
      NodeInputs.newTransient(
          "BrokerHostportOverride",
          "An environment variable detailing which explicit broker to connect to",
          Source.fromEnvironmentVariable("GSFBROKER_HOSTPORT"),
          CombinationStrategies.distCombinator());

  // Grid Classpath

  public static final ScopedSINodeInput<Boolean> AllowUnmappablePathForDev =
      NodeInputs.newTransientJavaOptWithDefault(
          "AllowUnmappablePathForDev",
          "Required on Windows to support distributing from an NFS-mounted location set as a local drive",
          false,
          SourceNames.AllowUnmappablePathForDev,
          Object::toString,
          Source.fromBoolJavaProperty(SourceNames.AllowUnmappablePathForDev),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> StratosphereGridClasspath =
      NodeInputs.newTransientJavaOpt(
          "StratosphereGridClasspath",
          "The classpath used, if defined by Stratosphere",
          SourceNames.StratosphereGridClasspath,
          String::toString,
          Source.fromJavaProperty(SourceNames.StratosphereGridClasspath),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> VersionToken =
      NodeInputs.newTransientJavaOpt(
          "VersionToken",
          "The override for the code version used by distribution",
          SourceNames.VersionToken,
          String::toString,
          Source.fromJavaProperty(SourceNames.VersionToken),
          CombinationStrategies.distCombinator());

  // Grid Classpath Config

  public static final ScopedSINodeInput<String> ClasspathExcludeRegexes =
      NodeInputs.newSerializable(
          "ClasspathExcludeRegexes",
          "Regexes to exclude classpaths from engines",
          Source.fromJavaProperties(prefixed(SourceNames.ClasspathExcludeRegexes)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> ImmutableLocations =
      NodeInputs.newSerializable(
          "ImmutableLocations",
          "Locations which are defined as containing immutable code",
          Source.fromJavaProperties(prefixed(SourceNames.ImmutableLocations)),
          CombinationStrategies.distCombinator());

  // Engine Configuration

  public static final ScopedSINodeInput<Boolean> CancelTasksOnExit =
      NodeInputs.newSerializableWithDefault(
          "CancelTasksOnExit",
          "True if tasks are cancelled by GSF broker if the client process exits",
          true,
          Source.fromBoolJavaProperties(prefixed(SourceNames.CancelTasksOnExit)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Boolean> CompressData =
      NodeInputs.newSerializableWithDefault(
          "CompressData",
          "True if the distribution payload should be compressed",
          false,
          Source.fromBoolJavaProperties(
              prefixed(SourceNames.CompressData, "client.dataCompression")),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Integer> DefaultTaskTimeout =
      NodeInputs.newSerializableWithDefault(
          "DefaultTaskTimeout",
          "Timeout before the task is automatically marked as failed by GSF broker",
          -1,
          Source.fromIntJavaProperties(prefixed(SourceNames.DefaultTaskTimeout)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Integer> DepthLimit =
      NodeInputs.newSerializableWithDefault(
          "DepthLimit",
          "Maximum number of allowed recursive distribution levels",
          3,
          Source.fromIntJavaProperties(prefixed(SourceNames.DepthLimit)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> EngineAppDir =
      NodeInputs.newSerializable(
          "EngineAppDir",
          "I am not sure!!!",
          Source.fromJavaProperties(prefixed(SourceNames.EngineAppDir)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> EngineCmdLine =
      NodeInputs.newSerializableWithDefault(
          "EngineCmdLine",
          "The launcher used to start the engine process",
          "",
          Source.fromJavaProperties(prefixed(SourceNames.EngineCmdLine)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> EngineConfig =
      NodeInputs.newSerializableWithDefault(
          "EngineConfig",
          "The executable used to start the engine",
          "",
          Source.fromJavaProperties(prefixed(SourceNames.EngineConfig)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> EngineExecutable =
      NodeInputs.newSerializableWithDefault(
          "EngineExecutable",
          "The executable used to start the engine",
          "",
          Source.fromJavaProperties(prefixed(SourceNames.EngineExecutable)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> EngineExecutablePath =
      NodeInputs.newSerializable(
          "EngineExecutablePath",
          "The directory in which the engine executable resides",
          Source.fromJavaProperties(prefixed(SourceNames.EngineExecutablePath)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> EngineLogConfig =
      NodeInputs.newSerializableWithDefault(
          "EngineLogConfig",
          "The configuration file for logging to be passed to the remote engine",
          "",
          Source.fromJavaProperties(prefixed(SourceNames.EngineLogConfig)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> EngineSpecEntryPoint =
      NodeInputs.newSerializableWithDefault(
          "EngineSpecEntryPoint",
          "I am not sure!!!",
          "",
          Source.fromJavaProperties(prefixed(SourceNames.EngineSpecEntryPoint)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> EngineExplicitCodeVersion =
      NodeInputs.newTransient(
          "EngineExplicitCodeVersion",
          "Overrides the code version used when generating the engine spec",
          Source.fromJavaProperties(prefixed("client.engine.explicitCodeVersion")),
          EngineForwarding.Behavior.NEVER,
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Boolean> MonitorReleaseLink =
      NodeInputs.newSerializableWithDefault(
          "MonitorReleaseLink",
          "Restart engine if it's running from a release link, and the release link is flipped",
          false,
          Source.fromBoolJavaProperties(prefixed(SourceNames.MonitorReleaseLink)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Boolean> ProvideEngineClasspath =
      NodeInputs.newSerializableWithDefault(
          "ProvideEngineClasspath",
          "I am not sure!!!",
          true,
          Source.fromBoolJavaProperties(prefixed(SourceNames.ProvideEngineClasspath)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Boolean> ProvideEngineEnv =
      NodeInputs.newSerializableWithDefault(
          "ProvideEngineEnv",
          "I am not sure!!!",
          true,
          Source.fromBoolJavaProperties(prefixed(SourceNames.ProvideEngineEnv)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> ReplayPersistDir;

  static {
    List<Source<?, ?, String>> replayPersistDirSources =
        Source.fromJavaProperties(prefixed(SourceNames.ReplayPersistDir));
    replayPersistDirSources.add(Source.fromEnvironmentVariable("REPLAY_PERSIST_DIR"));
    ReplayPersistDir =
        NodeInputs.newSerializableWithDefault(
            "ReplayPersistDir",
            "I am not sure!!!",
            "",
            replayPersistDirSources,
            CombinationStrategies.distCombinator());
  }

  // Client configuration

  public static final ScopedSINodeInput<Integer> BrokerAvailabilityTimeout =
      NodeInputs.newSerializableWithDefault(
          "BrokerAvailabilityTimeout",
          "Timeout before broker connection is failed",
          90,
          Source.fromIntJavaProperties(prefixed(SourceNames.BrokerAvailabilityTimeout)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Boolean> BrokerConnectionKerberised =
      NodeInputs.newSerializableWithDefault(
          "BrokerConnectionKerberised",
          "Enable Kerberos authentication on GSF broker connection",
          false,
          Source.fromBoolJavaProperties(
              prefixed(SourceNames.BrokerConnectionKerberised, "client.kerberos")),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Integer> BrokerConnectionMaxRetries =
      NodeInputs.newSerializable(
          "BrokerConnectionMaxRetries",
          "Maximum number of retries when connection to broker",
          Source.fromIntJavaProperties(prefixed(SourceNames.BrokerConnectionMaxRetries)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> BrokerConnectionSocksProxy =
      NodeInputs.newSerializable(
          "BrokerConnectionSocksProxy",
          "Proxy settings used when connecting to GSF broker",
          Source.fromJavaProperties(
              prefixed(
                  SourceNames.BrokerConnectionSocksProxy,
                  "optimus.dist.broker.connection.socks.proxy")),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Boolean> BrokerConnectionSslEnabled =
      NodeInputs.newSerializableWithDefault(
          "BrokerConnectionSslEnabled",
          "Enable SSL authentication on GSF broker connection",
          false,
          Source.fromBoolJavaProperties(
              prefixed(
                  SourceNames.BrokerConnectionSslEnabled,
                  "optimus.dist.broker.connection.sslEnabled")),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Integer> BrokerHeartbeatInterval =
      NodeInputs.newSerializableWithDefault(
          "BrokerHeartbeatInterval",
          "Heartbeat interval (in seconds)",
          30,
          Source.fromIntJavaProperties(
              prefixed(SourceNames.BrokerHeartbeatInterval, "heartbeat.interval.secs")),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<List<String>> BrokerHostportList =
      NodeInputs.newSerializable(
          "BrokerHostportList",
          "List of GSF broker hostports to connect to",
          Source.fromListJavaProperties(
              prefixed(SourceNames.BrokerHostportList, "client.hostport")),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Long> BrokerRetryTimeout =
      NodeInputs.newSerializable(
          "BrokerRetryTimeout",
          "Timeout before retrying connection to broker (in milliseconds)",
          Source.fromLongJavaProperties(
              prefixed(SourceNames.BrokerRetryTimeout, "client.brokerRetryTimeout")),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> BrokerZookeeperMode =
      NodeInputs.newSerializable(
          "BrokerZookeeperMode",
          "Zookeeper mode when connecting to GSF broker",
          Source.fromJavaProperties(
              prefixed(SourceNames.BrokerZookeeperMode, "client.brokerZkMode")),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> BrokerZookeeperPath =
      NodeInputs.newSerializable(
          "BrokerZookeeperPath",
          "Zookeeper path used when connecting to GSF broker",
          Source.fromJavaProperties(
              prefixed(SourceNames.BrokerZookeeperPath, "client.brokerZkPath")),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Integer> BrokerZookeeperWaitTimeout =
      NodeInputs.newSerializableWithDefault(
          "BrokerZookeeperWaitTimeout",
          "Timeout used for Zookeeper connection (in seconds)",
          120,
          Source.fromIntJavaProperties(prefixed(SourceNames.BrokerZookeeperWaitTimeout)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Integer> ClientLoggingInterval =
      NodeInputs.newSerializableWithDefault(
          "ClientLoggingInterval",
          "How often the client should log its state (in seconds)",
          30,
          Source.fromIntJavaProperties(prefixed(SourceNames.ClientLoggingInterval)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Integer> ClientThrottlingPeriod =
      NodeInputs.newSerializableWithDefault(
          "ClientThrottlingPeriod",
          "How often the client wait while throttling",
          10,
          Source.fromIntJavaProperties(prefixed(SourceNames.ClientThrottlingPeriod)),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Boolean> LocalExecution =
      NodeInputs.newTransientJavaOptWithDefault(
          "LocalExecution",
          "Marks the execution as local",
          false,
          SourceNames.LocalExecution,
          Object::toString,
          Source.fromBoolJavaProperties(prefixed(SourceNames.LocalExecution)),
          CombinationStrategies.distCombinator());

  // Legacy

  public static final ScopedSINodeInput<String> LegacyLogFileDirectory =
      NodeInputs.newTransientJavaOpt(
          "LegacyLogFileDirectory",
          "Directory for log files for Legacy Engine",
          SourceNames.LegacyLogFileDirectory,
          String::toString,
          Source.fromJavaProperties(prefixed(SourceNames.LegacyLogFileDirectory)),
          CombinationStrategies.distCombinator());

  // Interop

  public static final ScopedSINodeInput<String> AppInfoFile =
      NodeInputs.newTransient(
          "AppInfoFile",
          "App info file for Optimus Interop",
          Source.fromJavaProperty(SourceNames.AppInfoFile),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> InteropSerializationClassName =
      NodeInputs.newTransient(
          "InteropSerializationClassName",
          "Class to use for Optimus Interop serialization",
          Source.fromJavaProperty("interop.serialisation.className"),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> InteropDeserializationClassName =
      NodeInputs.newTransient(
          "InteropDeserializationClassName",
          "Class to use for Optimus Interop deserialization",
          Source.fromJavaProperty("interop.deserialisation.className"),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Boolean> InteropDisableVtTt =
      NodeInputs.newTransientWithDefault(
          "InteropDisableVtTt",
          "Disables VT/TT in Optimus Interop",
          false,
          Source.fromBoolJavaProperty("optimus.interop.disableVtTt"),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<Boolean> InteropDisableDalSession =
      NodeInputs.newTransientWithDefault(
          "InteropDisableDalSession",
          "Disables DAL in Optimus Interop",
          false,
          Source.fromBoolJavaProperty("optimus.interop.disableDalSession"),
          CombinationStrategies.distCombinator());

  public static final ScopedSINodeInput<String> ReleaseLink =
      NodeInputs.newTransientJavaOpt(
          "ReleaseLink",
          "Release link for Optimus Interop",
          SourceNames.EngineReleaseLink,
          String::toString,
          Source.fromJavaProperties(prefixed(SourceNames.EngineReleaseLink)),
          CombinationStrategies.distCombinator());

  private static String[] prefixed(String... properties) {
    // creates a new alias for each property, by prefixing the property with `optimus.dist.gsf.`
    // needed because GSF / Dist provides aliasing
    String[] prefixed = new String[properties.length * 2];
    for (int i = 0; i < properties.length; i++) {
      prefixed[i] = properties[i];
      prefixed[i + properties.length] = SourceNames.OPTIMUS_DIST_GSF_PREFIX + properties[i];
    }
    return prefixed;
  }
}
