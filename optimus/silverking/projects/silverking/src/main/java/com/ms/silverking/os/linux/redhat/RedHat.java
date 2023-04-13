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
package com.ms.silverking.os.linux.redhat;

import java.io.File;

import com.ms.silverking.io.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedHat {
  private static final File versionFile = new File("/etc/redhat-release");
  private static final String releaseString = "release ";

  private static Logger log = LoggerFactory.getLogger(RedHat.class);

  public static final double notRedHatLinux = 0.0;

  public static double getRedHatVersion() {
    try {
      String version;
      int i0;
      int i1;

      version = FileUtil.readFileAsString(versionFile);
      i0 = version.indexOf(releaseString);
      if (i0 < 0) {
        return notRedHatLinux;
      }
      i1 = version.indexOf(' ', i0 + releaseString.length());
      if (i1 < 0) {
        return notRedHatLinux;
      }
      return Double.parseDouble(version.substring(i0 + releaseString.length(), i1));
    } catch (Exception e) {
      return notRedHatLinux;
    }
  }

  public static final void main(String[] args) {
    log.info("RedHat Version {}", getRedHatVersion());
  }
}
