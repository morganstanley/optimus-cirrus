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
package optimus.buildtool.compilers.zinc.setup

import java.io.File

import optimus.buildtool.compilers.zinc.ClassInJarVirtualFile
import sbt.internal.inc.Analysis
import xsbti.VirtualFile
import xsbti.VirtualFileRef
import xsbti.compile.AnalysisContents
import xsbti.compile.ClassFileManager

import scala.collection.immutable.Seq
import scala.collection.mutable

class TrackingClassFileManager(previousAnalysis: Option[AnalysisContents]) extends ClassFileManager {

  private val _staleClasses = mutable.Buffer[Array[ClassInJarVirtualFile]]()
  override def delete(classes: Array[VirtualFile]): Unit =
    _staleClasses += classes.map {
      case cvf: ClassInJarVirtualFile => cvf
      case x                          => throw new IllegalArgumentException(s"Unexpected file: $x (${x.getClass})")
    }

  // not Array[ClassInJarVirtualFile] since WriteReportingJavaFileObject always creates PlainVirtualFile objects
  private val _generatedClasses = mutable.Buffer[Array[VirtualFile]]()
  override def generated(classes: Array[VirtualFile]): Unit = _generatedClasses += classes

  override def complete(success: Boolean): Unit = ()

  @Deprecated override def delete(classes: Array[File]): Unit = ()
  @Deprecated override def generated(classes: Array[File]): Unit = ()

  def staleSources: Seq[VirtualFileRef] = {
    val binaryToSource: Map[VirtualFileRef, Set[VirtualFileRef]] =
      previousAnalysis.map(_.getAnalysis.asInstanceOf[Analysis].relations.srcProd.reverseMap).getOrElse(Map.empty)
    def asSources(classfiles: Array[ClassInJarVirtualFile]): Set[VirtualFileRef] =
      classfiles.iterator.flatMap(classfile => binaryToSource.getOrElse(classfile, Set.empty)).toSet
    _staleClasses.iterator.flatMap(asSources).toIndexedSeq
  }

  def staleClasses: Seq[ClassInJarVirtualFile] = _staleClasses.flatten.toIndexedSeq

  def generatedClasses: Seq[VirtualFile] = _generatedClasses.flatten.toIndexedSeq

  def deletedClasses: Seq[ClassInJarVirtualFile] = staleClasses.filterNot(generatedClasses.contains)
}
