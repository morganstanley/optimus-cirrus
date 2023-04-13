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
package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.io.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSegmentStorageFormat {
  private final int storageFormat;

  private static final String fileName = "storageFormat";
  private static final int defaultStorageFormat = 0;

  private static Logger log = LoggerFactory.getLogger(FileSegmentStorageFormat.class);

  /*
   * Storage formats:
   * 0 - Base format. Data + offset lists
   * 1 - Adds invalidation index
   * 2 - Puts data segment in ltv format?
   * 3 - Adds length index
   */

  public FileSegmentStorageFormat(int storageFormat) {
    this.storageFormat = Math.max(storageFormat, 0);
  }

  public boolean dataSegmentIsLTV() {
    return storageFormat >= 2;
  }

  public boolean metaDataIsLTV() {
    return storageFormat >= 1;
  }

  public boolean containsFSMHeader() {
    return storageFormat >= 1;
  }

  public boolean invalidationsAreIndexed() {
    return storageFormat >= 1;
  }

  public boolean lengthsAreIndexed() {
    return storageFormat >= 3;
  }

  ///////////////////

  private static File getFile(File dir) {
    return new File(dir, fileName);
  }

  public static FileSegmentStorageFormat read(File dir) throws IOException {
    File f;

    f = getFile(dir);
    return new FileSegmentStorageFormat(f.exists() ? FileUtil.readFileAsInt(f) : 0);
  }

  public static void write(File dir, int storageFormat) throws IOException {
    FileUtil.writeToFile(getFile(dir), Integer.toString(storageFormat));
  }

  public static FileSegmentStorageFormat parse(String storageFormat) {
    if (storageFormat == null) {
      return new FileSegmentStorageFormat(defaultStorageFormat);
    } else {
      try {
        return new FileSegmentStorageFormat(Integer.parseInt(storageFormat));
      } catch (Exception e) {
        log.warn("Using defaultStorageFormat: {}", defaultStorageFormat, e);
        return new FileSegmentStorageFormat(defaultStorageFormat);
      }
    }
  }

  @Override
  public String toString() {
    return Integer.toString(storageFormat);
  }
}
