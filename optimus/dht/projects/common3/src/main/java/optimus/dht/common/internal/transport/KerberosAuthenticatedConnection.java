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
/*
package optimus.dht.common.internal.transport;

import optimus.dht.common.util.StringTool;
import msjava.msnet.auth.MSNetAuthContext;

public class KerberosAuthenticatedConnection implements AuthenticatedConnection {

  private final MSNetAuthContext authContext;
  private final KerberosHandshakeDetails kerberosHandshakeDetails;

  public KerberosAuthenticatedConnection(
      MSNetAuthContext authContext, KerberosHandshakeDetails kerberosHandshakeDetails) {
    this.authContext = authContext;
    this.kerberosHandshakeDetails = kerberosHandshakeDetails;
  }

  @Override
  public String authenticatedUsername() {
    return authContext.getAuthID();
  }

  @Override
  public String toString() {
    return "KerberosAuthenticatedConnection[authContext="
        + authContext
        + ", krbTotalTime="
        + StringTool.formatNanosAsMillisFraction(kerberosHandshakeDetails.totalTime())
        + ", krbBlockingTime="
        + StringTool.formatNanosAsMillisFraction(kerberosHandshakeDetails.blockingTime())
        + ", krbQueuingTime="
        + StringTool.formatNanosAsMillisFraction(kerberosHandshakeDetails.queuingTime())
        + "]";
  }
}
*/
