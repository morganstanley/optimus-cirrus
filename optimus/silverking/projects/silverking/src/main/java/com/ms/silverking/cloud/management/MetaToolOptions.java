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
package com.ms.silverking.cloud.management;

import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

public class MetaToolOptions {
  @Option(name = "-t", usage = "tool", required = true)
  public String tool;

  @Option(name = "-s", usage = "source", required = true)
  public String source;

  @Option(name = "-d", usage = "dest", required = true)
  public String dest;

  @Option(name = "-n", usage = "name", required = true)
  public String name;

  @Option(name = "-z", usage = "zkConfig", required = true)
  public String zkConfig;

  @Option(name = "-f", usage = "file", required = false)
  public String file;

  @Option(name = "-v", usage = "version", required = true)
  public String version;

  @Argument public List<String> arguments = new ArrayList<String>();
}
