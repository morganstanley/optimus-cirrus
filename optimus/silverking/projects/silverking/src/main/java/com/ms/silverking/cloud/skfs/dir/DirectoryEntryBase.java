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
package com.ms.silverking.cloud.skfs.dir;

import com.ms.silverking.numeric.NumConversion;

public abstract class DirectoryEntryBase implements DirectoryEntry {
  protected static final int DE_MAGIC = 0xadab;

  protected static final int magicLength = NumConversion.BYTES_PER_SHORT;
  protected static final int sizeLength = NumConversion.BYTES_PER_SHORT;
  protected static final int statusLength = NumConversion.BYTES_PER_SHORT + 2; /* 2 is padding to match C alignment */
  protected static final int versionLength = NumConversion.BYTES_PER_LONG;

  protected static final int magicOffset = 0;
  protected static final int sizeOffset = magicOffset + magicLength; // size is name length + terminator + alignment
  // padding if necessary
  protected static final int statusOffset = sizeOffset + sizeLength;
  protected static final int versionOffset = statusOffset + statusLength;
  protected static final int dataOffset = versionOffset + versionLength;

  protected static final int headerSize = dataOffset;

}
