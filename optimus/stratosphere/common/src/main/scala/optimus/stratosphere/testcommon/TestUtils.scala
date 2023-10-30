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
package optimus.stratosphere.testcommon

import optimus.stratosphere.bootstrap.OsSpecific
import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.utils.EnvironmentUtils
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.not
import org.junit.Assert._

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.immutable.Seq
import scala.reflect.ClassTag
import scala.util.Random

trait TestUtils {

  private val random = new Random()

  protected def assertExists(path: Path*): Unit = path.foreach(doAssertExists)

  protected def assertOnlyGivenPathsExists(root: Path)(paths: Seq[Path]): Unit = {
    paths.foreach(doAssertExists)
    val filesInRootDir = root.dir.list()
    val remainingFiles = filesInRootDir.toSet -- paths.toSet
    assert(
      remainingFiles.isEmpty,
      s"Unexpected files found in the $root: ${remainingFiles.map(_.toString).mkString("[", ", ", "]")}")
  }

  protected def assertNotExists(path: Path*): Unit = path.foreach(doAssertNotExists)

  protected def assertOutputContains(str: Seq[String])(msgs: String*): Unit = {
    assertOutputContains(str.mkString("\n"))(msgs: _*)
  }

  protected def assertOutputNotContains(str: Seq[String])(msgs: String*): Unit = {
    assertOutputNotContains(str.mkString("\n"))(msgs: _*)
  }

  protected def assertOutputContains(str: String)(msgs: String*): Unit =
    msgs.foreach(msg => assertThat(str, containsString(msg)))

  protected def assertOutputNotContains(str: String)(msgs: String*): Unit =
    msgs.foreach(msg => assertThat(str, not(containsString(msg))))

  protected def expect[A <: Throwable: ClassTag](body: => Unit): Unit = {
    val expectedExcName = implicitly[ClassTag[A]].runtimeClass.getCanonicalName
    try body
    catch {
      case e if e.getClass.getCanonicalName == expectedExcName => return
    }
    fail(s"Exception of type $expectedExcName should be thrown, but wasn't.")
  }

  def withTempDir(testCode: Path => Unit): Unit = {
    val dir = if (OsSpecific.isCi && OsSpecific.isWindows) {
      // on Windows CI we need shorter paths
      Paths
        .get(s"C:/MSDE/${EnvironmentUtils.userName}/tmp")
        .resolve(random.alphanumeric.take(10).mkString)
        .dir
        .create()
    } else {
      Files.createTempDirectory(null)
    }
    println(s"temp dir created at $dir")
    try testCode(dir)
    finally dir.delete()
  }

  private def doAssertExists(path: Path): Unit = {
    def siblings =
      Option(path.getParent).map(_.toFile).flatMap(file => Option(file.listFiles())).getOrElse(Array.empty)
    assertTrue(s"File $path should exist! Siblings: ${siblings.mkString("[", ", ", "]")}", Files.exists(path))
  }

  private def doAssertNotExists(path: Path): Unit =
    assertTrue(s"File $path should not exist!", !Files.exists(path))

}
