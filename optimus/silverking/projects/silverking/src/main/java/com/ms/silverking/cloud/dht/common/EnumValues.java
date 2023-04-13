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
package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.ForwardingMode;

public class EnumValues {
  public static final VersionConstraint.Mode[] versionConstraint_Mode = VersionConstraint.Mode.values();
  public static final ChecksumType[] checksumType = ChecksumType.values();
  public static final Compression[] compression = Compression.values();
  public static final MessageType[] messageType = MessageType.values();
  public static final OpResult[] opResult = OpResult.values();
  public static final ForwardingMode[] forwardingMode = ForwardingMode.values();
  public static final ConsistencyProtocol[] consistencyProtocol = ConsistencyProtocol.values();
  public static final RetrievalType[] retrievalType = RetrievalType.values();
  public static final WaitMode[] waitMode = WaitMode.values();
  public static final OwnerQueryMode[] ownerQueryMode = OwnerQueryMode.values();
}
