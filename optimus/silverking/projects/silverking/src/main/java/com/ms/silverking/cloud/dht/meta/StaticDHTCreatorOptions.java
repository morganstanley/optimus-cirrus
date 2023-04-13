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
package com.ms.silverking.cloud.dht.meta;

import org.kohsuke.args4j.Option;

class StaticDHTCreatorOptions {
  StaticDHTCreatorOptions() {
  }

  static final int defaultPort = 7575;

  @Option(name = "-z", usage = "zkEnsemble", required = true)
  String zkEnsemble;

  @Option(name = "-f", usage = "serverFile", required = false)
  String serverFile;

  @Option(name = "-a", usage = "aliasMapNameAndFile", required = false)
  String aliasMapNameAndFile;

  @Option(name = "-s", usage = "servers", required = false)
  String servers;

  @Option(name = "-g", usage = "gridConfig", required = false)
  String gridConfig;

  @Option(name = "-G", usage = "gridConfigDir", required = true)
  String gridConfigDir;

  @Option(name = "-L", usage = "skInstanceLogBaseVar", required = false)
  String skInstanceLogBaseVar;

  @Option(name = "-D", usage = "dataBaseVar", required = false)
  String dataBaseVar;

  @Option(name = "-d", usage = "dhtName", required = false)
  String dhtName;

  @Option(name = "-r", usage = "replication", required = false)
  int replication = 1;

  @Option(name = "-p", usage = "port", required = false)
  int port = defaultPort;

  @Option(name = "-i", usage = "initialHeapSize", required = false)
  int initialHeapSize = 1024;

  @Option(name = "-x", usage = "maxHeapSize", required = false)
  int maxHeapSize = 4096;

  @Option(name = "-n", usage = "nsCreationOptions", required = false)
  String nsCreationOptions;

  @Option(name = "-v", usage = "verbose")
  boolean verbose;

  @Option(name = "-k", usage = "skfsConfigurationFile", required = false)
  String skfsConfigurationFile;

  @Option(name = "-c", usage = "classVarsFile", required = false)
  String classVarsFile;

  @Option(name = "-m", usage = "nsOptionsMode", required = false)
  String nsOptionsMode;

  @Option(name = "-t", usage = "enableMsgGroupTrace", required = false)
  boolean enableMsgGroupTrace;

  @Option(name = "-rec", usage = "enableMsgGroupRecorder", required = false)
  boolean enableMsgGroupRecorder;

  @Option(name = "-throt", usage = "enablemsgThrottling", required = false)
  boolean enableMsgThrottling;
}