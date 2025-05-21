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
package optimus.platform.dsi.bitemporal

import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.EvaluationContext

import scala.util.control.NonFatal

object MessageContainsDALEnv {
  private val log = getLogger[MessageContainsDALEnv]

  private var detectedEnv: Option[String] = None
  private[bitemporal] def tryDetectEnv: Option[String] =
    try {
      detectedEnv.orElse {
        val env = Option(EvaluationContext.scenarioStackOrNull)
          .flatMap(s => Option(s.env))
          .flatMap(e => Option(e.config))
          .flatMap(c => Option(c.runtimeConfig))
          .flatMap(rc => Option(rc.env))
        if (env.isDefined && env.get.nonEmpty) detectedEnv = env
        env
      }
    } catch {
      case NonFatal(ex) =>
        log.warn(s"Exception occurred while extracting env from the runtime. Will set empty env", ex)
        None
    }
}

trait MessageContainsDALEnv extends Exception {
  import MessageContainsDALEnv._

  private val prefix = {
    val detectedEnv = tryDetectEnv

    val toPrefix = (txt: String) => s"[${txt}] "

    detectedEnv.map(toPrefix).getOrElse {
      log.debug(
        s"exception ${this.getClass.getName} is initialized outside of an evaluation context, will not be able to display DAL env")
      ""
    }
  }

  override def getMessage = prefix + super.getMessage

}
