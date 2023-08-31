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
package com.ms.silverking.pssh;

import org.kohsuke.args4j.Option;

public class TwoLevelParallelSSHOptions {
  TwoLevelParallelSSHOptions() {}

  private static final int defaultWorkerThreads = 20;
  private static final int defaultTimeoutSeconds = 60;
  private static final int defaultMaxAttempts = 1;

  @Option(name = "-c", usage = "command", required = true)
  String command;

  @Option(name = "-h", usage = "hostsFile[:optionalGroup]", required = true)
  String hostsFile_optionalGroup;

  @Option(name = "-e", usage = "exclusionsFile", required = false)
  String exclusionsFile;

  @Option(name = "-w", usage = "workerCandidatesFile", required = false)
  String workerCandidatesFile;

  @Option(name = "-n", usage = "numWorkerThreads", required = false)
  int numWorkerThreads = defaultWorkerThreads;

  @Option(name = "-t", usage = "timeoutSeconds", required = false)
  int timeoutSeconds = defaultTimeoutSeconds;

  @Option(name = "-m", usage = "maxAttempts", required = false)
  int maxAttempts = defaultMaxAttempts;
}
