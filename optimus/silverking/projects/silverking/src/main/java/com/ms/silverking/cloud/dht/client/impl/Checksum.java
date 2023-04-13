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
package com.ms.silverking.cloud.dht.client.impl;

import java.nio.ByteBuffer;

public interface Checksum {
  public void checksum(byte[] source, int position, int length, ByteBuffer dest);

  public byte[] checksum(byte[] source, int position, int length);

  public void checksum(ByteBuffer source, ByteBuffer dest);

  public byte[] checksum(ByteBuffer source);

  public void emptyChecksum(ByteBuffer dest);

  public boolean isEmpty(byte[] actualChecksum);

  public boolean uniquelyIdentifiesValues();
}
