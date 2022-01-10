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
package optimus.utils

import sun.management.ManagementFactoryHelper

import scala.concurrent.duration._

/**
 * @param findClassTime   Total time to load classes
 * @param initClassTime   Total time to run <clinit> methods
 * @param verifyClassTime Total time spent in HotSpot verifier
 *
 * All times are in nanoseconds.
 */
final case class ClassLoaderStats(
    findClassTime: Long,
    initClassTime: Long,
    verifyClassTime: Long,
)
object ClassLoaderStats {
  // previous versions of this utility used sun.misc.PerfCounter, which reports in nanos
  // the below methods report in millis; to maintain compatibility with call sites this is preserved
  def snap() = ClassLoaderStats(
    findClassTime = mxBean.getClassLoadingTime.millis.toNanos,
    initClassTime = mxBean.getClassInitializationTime.millis.toNanos,
    verifyClassTime = mxBean.getClassVerificationTime.millis.toNanos,
  )

  private val mxBean = ManagementFactoryHelper.getHotspotClassLoadingMBean
}
