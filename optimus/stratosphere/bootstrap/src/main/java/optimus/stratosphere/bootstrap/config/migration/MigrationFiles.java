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
package optimus.stratosphere.bootstrap.config.migration;

import java.nio.file.Path;

import optimus.stratosphere.bootstrap.OsSpecific;

public class MigrationFiles {
  public final Path stratosphereWsUserHome;
  public final Path customConfFile;
  public final Path scalaVersionFile;
  public final Path workspaceScriptFile;
  public final Path useBigHeapFile;
  public final Path gitConfig;
  public final Path gitDir;
  public final Path srcDir;

  public MigrationFiles(Path workspaceRoot) {
    stratosphereWsUserHome =
        OsSpecific.userHome().resolve(".stratosphere").resolve(workspaceRoot.getFileName());
    scalaVersionFile = stratosphereWsUserHome.resolve(".scalaVersion");
    workspaceScriptFile = stratosphereWsUserHome.resolve("workspace." + OsSpecific.scriptExt);
    useBigHeapFile = stratosphereWsUserHome.resolve(".useBigHeap");
    customConfFile = workspaceRoot.resolve("config/custom.conf");
    srcDir = workspaceRoot.resolve("src");
    gitDir = srcDir.resolve(".git");
    gitConfig = gitDir.resolve("config");
  }
}
