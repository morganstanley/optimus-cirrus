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
package optimus.stratosphere.bootstrap.config;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import optimus.stratosphere.bootstrap.StratosphereException;

/**
 * Represents the Scala version and the repository where it's located (if it's not the standard
 * ossjava metaproject layout in AFS).
 */
public class ScalaDependency {

  public static class IvyRepository {
    public final String ivyPattern;
    public final String artifactPattern;

    public IvyRepository(String ivyPattern, String artifactPattern) {
      this.ivyPattern = ivyPattern;
      this.artifactPattern = artifactPattern;
    }

    @Override
    public String toString() {
      return String.format(
          "IvyRepository(ivyPattern: %s, artifactPattern: %s)", ivyPattern, artifactPattern);
    }

    // start of generated code
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      IvyRepository that = (IvyRepository) o;

      if (!ivyPattern.equals(that.ivyPattern)) {
        return false;
      }
      return artifactPattern.equals(that.artifactPattern);
    }

    @Override
    public int hashCode() {
      int result = ivyPattern.hashCode();
      result = 31 * result + artifactPattern.hashCode();
      return result;
    }
    // end of generated code
  }

  public final String version;
  public final Optional<IvyRepository> customRepo;

  // someone can request latest-nightly-2.11.9 but it can be also latest-nightly-2.11
  // (working e.g. in case we'd have 2.11.10)
  private static final Pattern latestNightlyVersionPattern =
      Pattern.compile("latest-nightly-(?<scalaVersion>\\d\\.\\d\\d(\\.\\d(\\d)?)?)");
  private static final Pattern nightlyPattern =
      Pattern.compile("\\d\\.\\d\\d\\.\\d(\\d)?-msde-NIGHTLY-(?<trainReleaseVersion>[\\w.-]+)");
  private static final Pattern releasePattern =
      Pattern.compile("\\d\\.\\d\\d\\.\\d(\\d)?-msde-(?<trainReleaseVersion>[\\w.-]+)");
  private static final Pattern pathPattern =
      Pattern.compile("(\\\\|([a-zA-Z]:)?)([\\\\/][\\w.-]+)+");

  ScalaDependency(String version, Optional<IvyRepository> customRepo) {
    this.version = version;
    this.customRepo = customRepo;
  }

  String getMajorVersion() {
    String[] segments = version.split("\\.");
    return segments[0] + "." + segments[1];
  }

  @Override
  public String toString() {
    return String.format("ScalaDependency(version: %s, customRepo: %s)", version, customRepo);
  }

  // start of generated code
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ScalaDependency that = (ScalaDependency) o;

    if (!version.equals(that.version)) {
      return false;
    }
    return customRepo.equals(that.customRepo);
  }

  @Override
  public int hashCode() {
    int result = version.hashCode();
    result = 31 * result + customRepo.hashCode();
    return result;
  }
  // end of generated code

  /**
   * Current Scala version may be just the information which version should be finally set and where
   * to find it (so except a real version there can be set: snapshot, nightly build etc.). We can
   * check this and find out the proper name of a version. In such case it's also needed to
   * configure additional repository where such a version can be found.
   */
  public static ScalaDependency get(
      String origVersionEntry, String nightliesPath, String msdeReleasesPath) {
    Matcher latestNightlyMatcher = latestNightlyVersionPattern.matcher(origVersionEntry);
    Matcher nightlyMatcher = nightlyPattern.matcher(origVersionEntry);
    Matcher releaseMatcher = releasePattern.matcher(origVersionEntry);
    Matcher pathMatcher = pathPattern.matcher(origVersionEntry);

    if (latestNightlyMatcher.matches()) {
      String baseScalaVersion = latestNightlyMatcher.group("scalaVersion");
      return getLatestNightly(baseScalaVersion, nightliesPath);
    } else if (nightlyMatcher.matches()) {
      return getExactNightly(origVersionEntry, nightliesPath);
    } else if (releaseMatcher.matches()) {
      String trainReleaseVersion = releaseMatcher.group("trainReleaseVersion");
      return getRelease(origVersionEntry, trainReleaseVersion, msdeReleasesPath);
    } else if (pathMatcher.matches()) {
      return getForPath(origVersionEntry);
    } else {
      return new ScalaDependency(origVersionEntry, Optional.empty());
    }
  }

  private static ScalaDependency getLatestNightly(String baseScalaVersion, String nightliesPath) {
    String group = baseScalaVersion.startsWith("2.11") ? "ossjava" : "ossscala";
    String ossScalaPath = (nightliesPath + "/" + group + "/scala").replace("/", File.separator);
    File nightliesLocation = new File(ossScalaPath);
    if (!nightliesLocation.exists()) {
      throw new StratosphereException(
          "Couldn't find the expected location of Scala: " + ossScalaPath);
    }
    String scalaVersionToFind =
        baseScalaVersion.length() > 4 ? baseScalaVersion : baseScalaVersion + ".";
    File[] dirs =
        nightliesLocation.listFiles(
            f -> f.isDirectory() && f.getName().startsWith(scalaVersionToFind));
    if (dirs.length == 0) {
      throw new StratosphereException("Couldn't find Scala installations in: " + ossScalaPath);
    }
    // okay as long as we don't have Scala 2.11.10
    Arrays.sort(dirs, (f1, f2) -> f2.getName().compareTo(f1.getName()));
    String nightlyScalaVersion = dirs[0].getName();
    return getExactNightly(nightlyScalaVersion, nightliesPath);
  }

  private static ScalaDependency getExactNightly(String scalaVersion, String nightliesPath) {
    IvyRepository ivyRepo = getCustomRepo(nightliesPath);
    return new ScalaDependency(scalaVersion, Optional.of(ivyRepo));
  }

  private static ScalaDependency getRelease(
      String scalaVersion, String trainReleaseVersion, String msdeReleasesPath) {
    String releasePath =
        msdeReleasesPath + File.separator + trainReleaseVersion + File.separator + "install";
    IvyRepository ivyRepo = getCustomRepo(releasePath);
    return new ScalaDependency(scalaVersion, Optional.of(ivyRepo));
  }

  /**
   * Used in situations where we have a repository with only one disted installation inside.
   * Basically local snapshots and artifacts generated by CI - which we'd like to use in downstream
   * jobs (to compile a given project using our custom version built by Jenkins).
   */
  private static ScalaDependency getForPath(String pathToScala) {
    String properPathToScala = pathToScala.replace("\\", File.separator);
    Path scalaPath = Paths.get(properPathToScala);

    if (scalaPath == null
        || scalaPath.getParent() == null
        || scalaPath.getParent().getParent() == null) {
      throw new StratosphereException(
          "Scala is not in a correct [organization]/[module] repository." + properPathToScala);
    }

    String basePath = scalaPath.getParent().getParent().toString();
    File dirWithScala = scalaPath.toFile();

    if (!dirWithScala.exists()) {
      throw new StratosphereException(
          "[ERROR] Couldn't find the expected location of Scala: " + properPathToScala);
    }

    File[] dirs = dirWithScala.listFiles(File::isDirectory);

    if (dirs == null || dirs.length == 0) {
      throw new StratosphereException(
          "[ERROR] Couldn't find Scala installation in: " + properPathToScala);
    }

    File scalaLocation = dirs[0];
    IvyRepository ivyRepo = getCustomRepo(basePath);
    String scalaVersion = scalaLocation.getName();
    return new ScalaDependency(scalaVersion, Optional.of(ivyRepo));
  }

  private static IvyRepository getCustomRepo(String basePath) {
    String ivyPattern =
        basePath + "/[organization]/[module]/[revision]/ivy.xml".replace("/", File.separator);
    String artifactPattern =
        basePath
            + "/[organization]/[module]/[revision]/lib/[artifact].[ext]"
                .replace("/", File.separator);
    return new IvyRepository(ivyPattern, artifactPattern);
  }
}
