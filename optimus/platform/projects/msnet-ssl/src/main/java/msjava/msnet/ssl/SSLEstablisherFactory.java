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

package msjava.msnet.ssl;

import javax.annotation.Nullable;

import msjava.msnet.MSNetEstablisher;
import msjava.msnet.MSNetEstablisherFactory;

/**
 * For general library overview and code examples refer to the {@link SSLEstablisher} documentation.
 */
public class SSLEstablisherFactory implements MSNetEstablisherFactory {

  private final boolean encryptionOnly; // no authentication
  @Nullable private final String serviceHostname;

  public SSLEstablisherFactory() {
    this(false, null);
  }

  public SSLEstablisherFactory(boolean encryptionOnly, @Nullable String serviceHostname) {
    this.encryptionOnly = encryptionOnly;
    this.serviceHostname = serviceHostname;
  }

  @Override
  public MSNetEstablisher createEstablisher() {
    return new SSLEstablisher(
        SSLEstablisher.DEFAULT_ESTABLISHER_PRIORITY, encryptionOnly, serviceHostname);
  }
}
