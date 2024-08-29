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
package optimus.platform.dal

import java.time.Instant

import optimus.platform._

/**
 * Methods here may violate referential transparency for one-off data migrations or brain surgery style administration.
 * As such these APIs will be subject to draconian entitlements and change management.
 *
 * If you have not been explicitly directed to this packages by optimus management then you should not even be looking
 * at the code in this package. You have been warned.
 */
trait DALDangerous {
  object dangerous {

    def transaction[T](txTime: Instant)(f: => T): PersistResult = resolver.inNewAppEvent(f, Some(txTime))._2

    def heartbeat(txTime: Instant) = transaction(txTime) {}

    def delayedTransaction(txTime: Instant)(f: => Unit): Transaction =
      resolver.inDelayedAppEvent(asAsync(() => f), Some(txTime))

    private[this] def resolver: ResolverImpl = EvaluationContext.env.entityResolver.asInstanceOf[ResolverImpl]
  }
}
