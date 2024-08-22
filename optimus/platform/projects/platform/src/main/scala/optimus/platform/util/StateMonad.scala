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

import optimus.platform.NodeFunction1
import optimus.platform.asNode
import optimus.platform.node

final case class StateMonad[S, +A](runWithState: NodeFunction1[S, (A, S)]) {

  // actually runs the function and returns the updated (value, state)
  @node def runState(s: S): (A, S) = runWithState(s)

  // runs the function and get the state value
  @node def valueForState(s: S): A = {
    val (value, _) = runWithState(s)
    value
  }

  // [.map] - simple function
  def map[B](f: Function1[A, B]): StateMonad[S, B] = {
    val newRun = asNode { s: S =>
      val (a, s2) = runState(s)
      (f(a), s2)
    }
    StateMonad(newRun)
  }

  // [.map] - auto asynced
  def map$NF[B](nf: NodeFunction1[A, B]): StateMonad[S, B] = {
    val newRun = asNode { s: S =>
      val (a, s2) = runState(s)
      (nf(a), s2)
    }
    StateMonad(newRun)
  }

  // [.flatMap] - plain function
  def flatMap[B](f: Function1[A, StateMonad[S, B]]): StateMonad[S, B] = {
    val newRun = asNode { s: S =>
      val (a, s2) = runState(s)
      f(a) runState s2
    }
    StateMonad(newRun)
  }

  // [.flatMap] - auto asynced
  def flatMap$NF[B](nf: NodeFunction1[A, StateMonad[S, B]]): StateMonad[S, B] = {
    val newRun = asNode { s: S =>
      val (a, s2) = runState(s)
      nf(a) runState s2
    }
    StateMonad(newRun)
  }
}
