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
package optimus.graph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import optimus.config.InstallPathLocator;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

// Interface to libfileinterceptor.so
public class FileInterceptor {

  // returns the list of all files that were opened by this JVM so far
  public static native String[] getFiles();

  // returns the list of IP:Port connections made by this JVM so far
  public static native String[] getConnects();

  // clears the list of files that were opened by this JVM so far
  public static native void eraseFiles();

  // clears the list of connect strings that were recorded in this JVM so far
  private static native void eraseConnects();

  // preloads a file of interest.
  // expects short form file name: holidays.dat, holidaysNonBusDays.dat, weekendSchedule.dat,
  // weekendtypes.dat
  // but will accepts longer names (with full path): it only looks from the last '/', if any
  public static native void preloadFile(String name, byte[] data);

  // Called by fileinterceptor.so if the file of interest was not preloaded, but is now being loaded
  // (this is on whatever thread called fopen/fstream)
  public static byte[] preloadCallback(String name) throws IOException {
    System.out.println("FileInterceptor: preloadCallback for file " + name);
    return Files.readAllBytes(Paths.get(name));
  }

  private static native int initialize();

  public static native void erasePreloadedFiles();

  private static Logger log = LoggerFactory.getLogger(PTools.class);

  private static volatile boolean loadFailed = false;
  private static volatile boolean enabled = false;

  public static boolean isEnabled() {
    return enabled;
  }

  public static boolean enableIfRequired() {
    return Settings.interceptFiles && enable();
  }

  public static synchronized boolean enable() {
    if (enabled) return true;
    if (loadFailed) return false;
    InstallPathLocator.Resolver resolver = InstallPathLocator.system();
    try {
      resolver.loadInterceptor();
      int i = initialize();
      if (i != 0) {
        log.warn("file interceptor initialization failed with " + i);
        enabled = false;
        loadFailed = true;
      } else enabled = true;
      return enabled;
    } catch (UnsatisfiedLinkError ex) {
      log.warn("file interceptor loading failed with exception {}", ex.toString());
      enabled = false;
      loadFailed = true;
      return false;
    }
  }

  public static ArrayList<String> openedFiles() {
    ArrayList<String> ret = new ArrayList<>();
    if (!enabled) return ret;
    // this filters out the most low-level files
    for (String s : openedFilesRaw()) {
      if (!s.startsWith("/proc")
          && !s.startsWith("/etc")
          && !s.startsWith("/sys")
          && !s.startsWith("/dev")) ret.add(s);
    }
    return ret;
  }

  public static ArrayList<String> openedFilesRaw() {
    ArrayList<String> ret = new ArrayList<>();
    if (enabled) {
      String[] files = getFiles();
      ret.addAll(Arrays.asList(files));
    }
    return ret;
  }

  public static ArrayList<String> connectionsRaw() {
    ArrayList<String> ret = new ArrayList<>();
    if (enabled) {
      String[] conns = getConnects();
      ret.addAll(Arrays.asList(conns));
    }
    return ret;
  }
}
