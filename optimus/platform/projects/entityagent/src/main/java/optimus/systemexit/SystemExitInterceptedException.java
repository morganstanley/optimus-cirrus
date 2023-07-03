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
 * Runtime exception to be thrown to suppress Runtime.exit to be called for instrumented System.exit
 * invocations (e.g. for application unit/functional tests). Utilized when exit.intercept system
 * property is set to 'intercept-all'
 */
public class SystemExitInterceptedException extends RuntimeException {
  private final int exitStatus;

  public SystemExitInterceptedException(int exitStatus) {
    super("[EXIT-INTERCEPT] instrumented System.exit(" + exitStatus + ") exception");
    this.exitStatus = exitStatus;
  }

  public int exitStatus() {
    return exitStatus;
  }
}
