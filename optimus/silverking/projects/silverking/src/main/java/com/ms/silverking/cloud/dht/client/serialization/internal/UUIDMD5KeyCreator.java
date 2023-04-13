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
package com.ms.silverking.cloud.dht.client.serialization.internal;

import java.util.UUID;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.id.UUIDUtil;

public class UUIDMD5KeyCreator extends BaseKeyCreator<UUID> {
  public UUIDMD5KeyCreator() {
    super();
  }

  @Override
  public DHTKey createKey(UUID key) {
    return md5KeyDigest.computeKey(UUIDUtil.uuidToBytes(key));
  }
}
