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
package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.serialization.internal.StringMD5KeyCreator;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.io.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirFilterCreator {
  private final Set<DHTKey> paths;
  private final Set<Pair<String, String>> datelessPaths;
  private final LocalDate currentDate;
  private int pathsProcessed;
  private final StringMD5KeyCreator keyCreator;

  private static Logger log = LoggerFactory.getLogger(DirFilterCreator.class);

  private static final String yearPrefix = "201";
  private static final int dateLength = 4 + 2 + 2;
  private static final int dateSuffixLength = dateLength - yearPrefix.length();
  private static final int retentionDays = 7;
  private static final int retentionMillis = retentionDays * 86400 * 1000;

  public DirFilterCreator() {
    paths = new HashSet();
    datelessPaths = new HashSet<>();
    currentDate = LocalDate.now();
    keyCreator = new StringMD5KeyCreator();
  }

  private void filterPath(String path) {
    //System.err.printf("Filtering %s\n", path);
    paths.add(keyCreator.createKey(path));
  }

  public void readPaths(String file) throws IOException {
    readPaths(new File(file));
  }

  public void readPaths(File file) throws IOException {
    readPaths(sanitizePaths(FileUtil.readFileAsLineList(file)));
  }

  private List<String> sanitizePaths(List<String> paths) {
    List<String> sanitizedPaths;

    sanitizedPaths = new ArrayList<>(paths.size());
    for (String path : paths) {
      if (path.charAt(0) != '/') {
        sanitizedPaths.add("/" + path);
      } else {
        sanitizedPaths.add(path);
      }
    }
    return sanitizedPaths;
  }

  public void readPaths(List<String> paths) {
    for (String path : paths) {
      Triple<String, String, LocalDate> datelessPathAndDate;

      datelessPathAndDate = pathToDatelessPathAndDate(path);
      //System.out.printf("%s\t%s\n", path, datelessPathAndDate);
      if (datelessPathAndDate != null) {
        Pair<String, String> datelessPath;
        LocalDate d;

        datelessPath = datelessPathAndDate.getPairAt1();
        d = datelessPathAndDate.getV3();
        //System.out.printf("%s %s\n", currentDate, d);
        if (shouldBeRetained(d)) {
          //System.out.printf("Keeping (recent) %s\n", path);
        } else {
          //System.out.printf("Filtering %s\n", path);
          filterPath(path);
        }
        datelessPaths.add(datelessPath);
      } else {
        //System.out.printf("Keeping (no date) %s\n", path);
      }
    }

    log.info("datelessPaths {}", datelessPaths.size());
    log.info("namespaces    {}", paths.size());
  }

  private int getInt(String s, int i0, int i1) {
    //System.out.printf("%s %d\n", s, Integer.parseInt(s.substring(i0, i1)));
    return Integer.parseInt(s.substring(i0, i1));
  }

  private LocalDate getLocalDate(String ds) {
    return LocalDate.of(getInt(ds, 0, 4), getInt(ds, 4, 6), getInt(ds, 6, 8));
  }

  private boolean shouldBeRetained(LocalDate d) {
    return currentDate.toEpochDay() - d.toEpochDay() <= retentionDays;
  }

  private boolean shouldBeRetained(String ds) {
    return shouldBeRetained(getLocalDate(ds));
  }

  private Triple<String, String, LocalDate> pathToDatelessPathAndDate(String path) {
    int i0;

    i0 = path.indexOf(yearPrefix);
    while (i0 >= 0) {
      boolean found;
      int i1;

      found = true;
      i1 = i0 + yearPrefix.length();
      for (int i = 0; i < dateSuffixLength; i++) {
        if (!Character.isDigit(path.charAt(i1))) {
          found = false;
          break;
        }
        i1++;
      }
      if (found) {
        String s0;
        String s1;
        String ds;
        LocalDate d;

        s0 = path.substring(0, i0);
        s1 = path.substring(i0 + dateLength);
        ds = path.substring(i0, i0 + dateLength);
        d = getLocalDate(ds);
        //System.out.printf("%d %d %d %s\n", d.getYear(), d.getMonth().getValue(), d.getDayOfMonth(), d);
        return new Triple<>(s0, s1, d);
      }
      i0 = path.indexOf(yearPrefix, i1 + 1);
    }
    return null;
  }

  public void processYear(String year) {
    int _p;
    int _n;

    _p = pathsProcessed;
    _n = paths.size();
    log.info("processYear {}", year);
    log.info("namespaces    {}", paths.size());
    for (int month = 1; month <= 12; month++) {
      processMonth(year, month);
    }
    log.info("namespaces    {}  {}  {} ", paths.size(), pathsProcessed - _p, paths.size() - _n);
    log.info("");
  }

  public void processMonth(String year, int month) {
    //System.out.printf("%s %d\n", year, month);
    // generating invalid dates is fine here
    for (int date = 1; date <= 31; date++) {
      processDate(String.format("%s%02d%02d", year, month, date));
    }
  }

  public void processDate(String date) {
    try {
      if (!shouldBeRetained(date)) {
        log.info("Filtering all of {}", date);
                /*
                System.out.printf("\nprocessYear %s\n", date);
                System.out.printf("datelessPaths %d\n", datelessPaths.size());
                System.out.printf("namespaces    %d\n", namespaces.size());
                */
        for (Pair<String, String> datelessPath : datelessPaths) {
          String path;

          ++pathsProcessed;
          path = datelessPath.getV1() + date + datelessPath.getV2();
          filterPath(path);
          //System.out.println(path);
        }
                /*
                System.out.printf("datelessPaths %d\n", datelessPaths.size());
                System.out.printf("namespaces    %d\n", namespaces.size());
                System.out.println();
                */
      } else {
        log.info("Keeping all of {}", date);
      }
    } catch (DateTimeException dte) {
      // skip invalid dates
    }
  }

  public Set<DHTKey> getNamespaces() {
    return ImmutableSet.copyOf(paths);
  }

  public static Set<DHTKey> getFilteredDirectories(String dirFile) {
    try {
      DirFilterCreator dirHashCreator;

      dirHashCreator = new DirFilterCreator();
      dirHashCreator.readPaths(dirFile);
      //dirHashCreator.processYear("2017");
      dirHashCreator.processYear("2018");
      return dirHashCreator.getNamespaces();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public static void main(String[] args) {
    try {
      getFilteredDirectories(args[0]);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
