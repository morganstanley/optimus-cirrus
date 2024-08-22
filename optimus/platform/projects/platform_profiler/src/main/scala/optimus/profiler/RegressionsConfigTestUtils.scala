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
package optimus.profiler

import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths

import optimus.graph.diagnostics.gridprofiler.GridProfiler.ProfilerOutputStrings
import org.junit.rules.TemporaryFolder

// useful methods for recreating directory structure expected in regressions (in MergeTracesTests and ValidateGraphConfigTests)
private[optimus] object RegressionsConfigTestUtils {
  val profilerDirectoryName = RegressionsConfigApps.profilerDirName
  val metaDirectoryName = RegressionsConfigApps.metaDirName

  def names(s: String, n: Int = 10): Seq[String] = (1 to n) map { i => s"$s-$i" }

  def putFilesInDirectories(regProfilerDirs: Seq[String], filename: String, extension: String): Seq[String] =
    regProfilerDirs.zipWithIndex map { case (path, i) =>
      Files.createFile(Paths.get(path, s"$i-$filename.$extension")).toAbsolutePath.toString
    }

  def writeMetaFilesInDirectories(
      dirs: Seq[String],
      moduleGroups: Seq[String] = Nil,
      modules: Seq[String] = Nil): Seq[String] = {
    val moduleGroupNames = if (moduleGroups.isEmpty) dirs.zipWithIndex.map { case (_, i) => s"module$i" }
    else moduleGroups
    val moduleNames = if (modules.isEmpty) dirs.zipWithIndex.map { case (_, i) => s"module$i" }
    else modules
    val dirsAndGroupsAndModules = dirs.zip(moduleGroupNames).zip(moduleNames)
    dirsAndGroupsAndModules map { case ((path, moduleGroup), module) =>
      val file = Files.createFile(Paths.get(path, RegressionsConfigApps.testplanDetailsFileName))
      val writer = new FileWriter(file.toFile)
      writer.write(s"${RegressionsConfigApps.moduleGroupName},$moduleGroup\n")
      writer.write(s"${RegressionsConfigApps.moduleName},$module")
      writer.close()
      file.toAbsolutePath.toString
    }
  }

  def createDirUnderRegressionsWithTempFolder(
      tempFolder: TemporaryFolder,
      regressions: Seq[String],
      profilerDirName: String,
      dirName: String = "regressions"): Seq[String] =
    regressions map { test =>
      tempFolder.newFolder(dirName, test, profilerDirName).getAbsolutePath
    }

  def setupExpectedStructure(tempFolder: TemporaryFolder, dirName: String = "regressions", n: Int = 10): Seq[String] = {
    val regressions = names("test", n)
    // create regressions/test-i/profiler directories for each test
    createDirUnderRegressionsWithTempFolder(tempFolder, regressions, "profiler", dirName)
  }

  def putTraceFilesInDirectories(
      regProfilerDirs: Seq[String],
      extension: String = ProfilerOutputStrings.ogtraceExtension): Seq[String] =
    putFilesInDirectories(regProfilerDirs, "trace", extension)

  def traceFilesFor(tempFolder: TemporaryFolder, regressionsToModules: Seq[(String, String)]): Seq[String] =
    writeFilesWithMeta(tempFolder, regressionsToModules, "trace", ProfilerOutputStrings.ogtraceExtension)

  def writeFilesWithMeta(
      tempFolder: TemporaryFolder,
      regressionsToModules: Seq[(String, String)],
      filename: String,
      extension: String) =
    // create regressions/test-i/profiler directories for each test and regressions/test-i/META to contain module details
    regressionsToModules.flatMap { case (regression, module) =>
      // put testplan details in corresponding META directories
      val regMetaDir = createDirUnderRegressionsWithTempFolder(tempFolder, regression :: Nil, metaDirectoryName)
      writeMetaFilesInDirectories(regMetaDir, modules = module :: Nil)

      // put trace files in test-i/profiler directories - these are the ones we expect to find in the file walk
      val regProfilerDirs =
        createDirUnderRegressionsWithTempFolder(tempFolder, regression :: Nil, profilerDirectoryName)
      putFilesInDirectories(regProfilerDirs, filename, extension)
    }
}
