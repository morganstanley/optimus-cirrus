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
package com.ms.silverking.cloud.skfs.management;

import org.kohsuke.args4j.Option;
// import com.ms.silverking.cloud.dht.management.Command;

enum Command {
  GetFromZK,
  LoadFromFile
}

public class MetaUtilOptions {
  MetaUtilOptions() {}

  static final int dhtVersionUnspecified = -1;

  @Option(name = "-g", usage = "gridConfigName", required = false)
  String gridConfigName;

  @Option(name = "-d", usage = "skfsConfigName", required = false)
  String skfsConfigName;

  @Option(name = "-v", usage = "version", required = false)
  int skfsVersion = dhtVersionUnspecified;

  @Option(name = "-c", usage = "command", required = true)
  Command command;

  @Option(name = "-t", usage = "targetFile", required = false)
  String targetFile;

  @Option(name = "-z", usage = "zkEnsemble", required = true)
  String zkEnsemble;
}
