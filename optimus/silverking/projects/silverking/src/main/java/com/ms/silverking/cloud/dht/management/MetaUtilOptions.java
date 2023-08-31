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
package com.ms.silverking.cloud.dht.management;

import org.kohsuke.args4j.Option;

public class MetaUtilOptions {
  MetaUtilOptions() {}

  public static final int dhtVersionUnspecified = -1;

  @Option(name = "-d", usage = "dhtName", required = true)
  String dhtName;

  @Option(name = "-v", usage = "")
  int dhtVersion = dhtVersionUnspecified;

  @Option(name = "-c", usage = "command", required = true)
  Command command;

  @Option(name = "-f", usage = "filterOption", required = false)
  FilterOption filterOption;

  @Option(name = "-t", usage = "filterOption", required = false)
  String targetFile;

  @Option(name = "-h", usage = "hostGroups")
  String hostGroups;

  @Option(name = "-z", usage = "zkEnsemble", required = true)
  String zkEnsemble;

  @Option(name = "-e", usage = "exclusionsFile", required = false)
  String exclusionsFile;

  @Option(name = "-w", usage = "workersFile", required = false)
  String workersFile;
}
