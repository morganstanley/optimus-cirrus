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
package optimus.graph.tracking

import optimus.graph.TweakableKey
import optimus.platform.{Scenario, Tweak}

import scala.collection.mutable.LinkedHashMap

/**
 * Oversimplified 'batcher'
 */
final class TweakBatch {
  val _instanceTweaks = new LinkedHashMap[TweakableKey, Tweak]
  val _propertyTweaks = new LinkedHashMap[AnyRef, Tweak]

  private def +=(tweaks: Seq[Tweak]): Unit = {
    val it = tweaks.iterator
    while (it.hasNext) {
      val tweak = it.next()
      val target = tweak.target
      val hashKey = target.hashKey
      if (hashKey ne null) _instanceTweaks.put(hashKey, tweak)
      else _propertyTweaks.put(target.propertyInfo, tweak)
    }
  }

  def tweaks: Seq[Tweak] = {
    (_instanceTweaks.values ++ _propertyTweaks.values).toSeq
  }
}

object TweakBatch {
  def fromTweaks(tweaks: Seq[Tweak]*) = {
    val tb = new TweakBatch()
    val it = tweaks.iterator
    while (it.hasNext) {
      tb += it.next()
    }
    tb
  }

  def fromScenarios(scenarios: Seq[Scenario]) = {
    val tb = new TweakBatch()
    val it = scenarios.iterator
    while (it.hasNext) {
      tb += it.next().topLevelTweaks.toSeq
    }
    tb
  }
}
