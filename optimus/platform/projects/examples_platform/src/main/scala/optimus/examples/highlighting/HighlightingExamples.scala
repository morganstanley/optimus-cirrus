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
package optimus.examples.highlighting

import optimus.platform._
import optimus.platform.dal.config.DalEnv
import optimus.platform.dal.config.DalLocation
import optimus.platform.util.Log

import scala.annotation.nowarn

@event class MyEvent(i: Int = 1)

object HighlightingExamples {
  object EventUniqueInstanceHighlighting {
    @entity object Foo {
      @async def foo(): Unit = {
        MyEvent.uniqueInstance(1) // works fine

        MyEvent.uniqueInstance(validTime = At.now) // works fine

        MyEvent.uniqueInstance(1, At.now) // works fine
      }
    }
  }

  // docs-snippet:optimus-warnings
  @entity object SyncStackHighlighting {
    @async private def asyncDef: Int = 0

    // regex-ignore-line (17001) intentional new warning added for demo/testing
    def syncStackDef: Int = asyncDef + 1 // Expect a gutter icon warning AFTER building this examples_platform scope
  }
  // docs-snippet:optimus-warnings
}

object HighlightingExamplesApp extends OptimusApp with Log {
  override def dalLocation: DalLocation = DalEnv.none
  @entersGraph override def run(): Unit = {
    log.info(s"syncStackDef: ${HighlightingExamples.SyncStackHighlighting.syncStackDef}")
  }
}
