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
package optimus.platform.util

import scala.jdk.CollectionConverters._
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.RuntimeSource
import optimus.breadcrumbs.crumbs.Crumb.SamplingProfilerSource
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.crumbs.PropertiesCrumb

import java.lang.management.ManagementFactory

object ArgumentPublisher {

  private def javaOpts: Iterable[String] =
    ManagementFactory.getRuntimeMXBean.getInputArguments.asScala

  private def optimusEnv: Iterable[String] = {
    val interesting = Seq("OPTIMUS", "MALLOC", "LD_PRELOAD")
    System.getenv().asScala.collect {
      case (k, v) if interesting.exists(k.contains(_)) => s"$k=$v"
    }
  }

  private def extracted: Iterable[String] = Version.properties.toMap.map { case (k, v) =>
    s"$k=$v"
  }

  // Divides the list of tokens into batches such that the sum of the lengths in each
  // batch doesn't exceed maxLength
  private def batchTokens(args: Iterable[String], maxLength: Int): Seq[Seq[String]] = {
    args
      .foldLeft[(List[List[String]], Int)]((List.empty[String] :: Nil, 0)) {
        case ((Nil, _), _) =>
          throw new RuntimeException("This can't happen")
        case ((currBatch :: earlierBatches, batchLen), arg) =>
          val newLen = batchLen + arg.length
          if (newLen < maxLength) // add arg to current batch
            ((arg :: currBatch) :: earlierBatches, newLen)
          else // start a new batch
            ((arg :: Nil) :: currBatch :: earlierBatches, arg.length)
      }
      ._1
      .reverse
      .map(_.reverse)
  }

  val crumbSource = RuntimeSource + SamplingProfilerSource

  private val maxCharCount = 5000

  private def publish(argType: String, args: Iterable[String]): Unit = {
    val batches = batchTokens(args, maxCharCount)
    batches.zipWithIndex.foreach { case (batch, i: Int) =>
      Breadcrumbs.info(
        ChainedID.root,
        PropertiesCrumb(
          _,
          crumbSource,
          Properties.crumbType -> "CommandLine" ::
            Properties.argsType -> argType ::
            Properties.args -> batch ::
            Properties.batchId -> (i + 1) :: Elems.Nil
        )
      )
    }
  }

  def publishArgs(programArgs: Iterable[String]): Unit = {
    publish("java", javaOpts)
    publish("optimus", programArgs)
    publish("optimusenv", optimusEnv)
    publish("extracted", extracted)
  }
}
