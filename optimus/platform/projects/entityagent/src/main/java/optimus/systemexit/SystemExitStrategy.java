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
package optimus.systemexit;

/**
 * Represents System.exit instrumentation handling.
 */
public enum SystemExitStrategy {
  /**
   * Standard exit handling - expect Runtime.exit to be called
   */
  EXIT,

  /**
   * Standard exit handling with logging - expect both Runtime.exit to be called
   * and environment info (stack trace, CPU load, env variables) to be logged
   */
  LOG_AND_EXIT,

  /**
   * Expect environment info (stack trace, CPU load, env variables) to be logged
   * and SystemExitInterceptedException to be thrown (it suppresses Runtime.exit
   * to be called as well)
   */
  LOG_AND_THROW,

  /**
   * Expect neither Runtime.exit to be called nor any runtime exception to be
   * thrown during System.exit call (treat System.exit as a no-op)
   */
  SUPPRESS_EXIT
}
