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

public class FileStatus {
  public static final int SIZE_BYTES = NumConversion.BYTES_PER_SHORT;

  public static final int F_DELETED = 1;
  public static final String S_DELETED = "deleted";
  public static final String S_NOT_DELETED = "";

  public static int getDeleted(short fs) {
    return fs & F_DELETED;
  }

  public static short setDeleted(short fs, int deleted) {
    if (deleted != 0) {
      return (short) (fs | F_DELETED);
    } else {
      return (short) (fs & (~F_DELETED));
    }
  }

  public static String toString(short fs) {
    if (getDeleted(fs) != 0) {
      return S_DELETED;
    } else {
      return S_NOT_DELETED;
    }
  }
}
