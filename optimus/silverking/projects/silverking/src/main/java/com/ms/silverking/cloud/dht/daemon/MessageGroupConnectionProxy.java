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
package com.ms.silverking.cloud.dht.daemon;

import java.io.IOException;
import java.util.Optional;

import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.async.AsyncSendListener;

/**
 * Provides the communication functionality required for operation processing while hiding whether
 * or not the actual communication is via an actual MessageGroupConnection or via an LWT worker.
 * <p>
 * This allows local communication to use the same methods as remote communication without incurring
 * the overhead of (local) network communication.
 */
interface MessageGroupConnectionProxy {
  void sendAsynchronous(Object data, long deadline) throws IOException;

  void sendAsynchronousWithTrace(Object data, long deadline, UUIDBase sendID, AsyncSendListener asyncSendListener) throws IOException;

  String getConnectionID();

  MessageGroupConnection getConnection();

  Optional<String> getAuthenticatedUser();
}
