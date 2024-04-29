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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import optimus.stratosphere.bootstrap.FsCampus;
import optimus.stratosphere.bootstrap.OsSpecific;
import optimus.stratosphere.bootstrap.StratosphereException;
import optimus.stratosphere.bootstrap.WorkspaceRoot;
import optimus.stratosphere.bootstrap.config.migration.truncation.HistoryTruncationConfigKeys;

public class StratosphereConfig {

  private Config config;

  public static String CONFIG_USERNAME = "userName";
  public static String userConfigName = "strato-user.conf";

  public static String stratosphereVersionProperty = "stratosphereVersion";

  public static String obtVersionProperty = "obt-version";

  private final String stratosphereInfraOverride = System.getenv("STRATOSPHERE_INFRA_OVERRIDE");
  private final String buildtoolOverride = System.getenv("BUILDTOOL_OVERRIDE");

  public static String stratosphereMigrationLocationOverride =
      System.getenv("STRATOSPHERE_MIGRATION_LOCATION_OVERRIDE");

  private static final ConfigResolveOptions allowUnresolved =
      ConfigResolveOptions.defaults().setAllowUnresolved(true);

  private static String prepareConfigExceptionMessage(ConfigException exception) {
    String msg = exception.getMessage();

    if (msg.contains(CONFIG_USERNAME)) {
      msg +=
          "\n[ERROR] Missing '"
              + CONFIG_USERNAME
              + "' variable is typically caused by incomplete OS configuration"
              + " (Windows 10 migration, Office upgrade, etc.). Typically the variable will set up correctly after "
              + "rebooting the machine";
    }
    return msg;
  }

  /**
   * Loads the workspace config from current dir. This operation is relatively expensive and should
   * be cached.
   *
   * @return config for the given workspace
   */
  public static Config loadFromCurrentDir() {
    return loadFromLocation(WorkspaceRoot.find());
  }

  /**
   * Loads the workspace config from current dir. This operation is relatively expensive and should
   * be cached.
   *
   * @param workspaceRoot location to load the workspace config from.
   * @return config for the given workspace
   */
  public static Config loadFromLocation(Path workspaceRoot) {
    return new StratosphereConfig(
            workspaceRoot, workspaceRoot == null ? "no-workspace.conf" : "workspace.conf")
        .config;
  }

  public static String getRepositoryConfigName(Path workspaceRoot) {
    if (workspaceRoot == null) {
      return null;
    }

    try {
      Path gitConfigPath = workspaceRoot.resolve("src/.git/config");
      String contents = Files.readString(gitConfigPath);
      String repoRegex = "\\[remote \"origin\"\\][\n\r\\s]*url = .*[/\\\\](.+)";
      Matcher matcher = Pattern.compile(repoRegex).matcher(contents);
      if (matcher.find()) {
        String repoName = matcher.group(1);
        String name = Paths.get(new URI(repoName).getPath()).getFileName().toString();
        return name.replace(".git", "") + ".conf";
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  private StratosphereConfig(Path workspaceRoot, String configName) {
    try {
      String repositoryConfigName = getRepositoryConfigName(workspaceRoot);

      // Configs must be sorted in order from most generics to most specifics
      addConfig("/etc/" + configName);
      addConfig("/etc/" + OsSpecific.platformString + ".conf");
      addConfig("/etc/application.conf");

      if (stratosphereInfraOverride != null) {
        updateProperty(
            "intellij.jetfire.path",
            replaceLast(stratosphereInfraOverride, "stratosphere", "optimusIDE"));
      }

      if (workspaceRoot != null) {
        addConfig(workspaceRoot, "src/" + WorkspaceRoot.STRATOSPHERE_CONFIG_FILE);
        addConfig(workspaceRoot, "src/profiles/" + StratosphereChannelsConfig.configFile);
        if (repositoryConfigName != null) {
          addConfig(workspaceRoot, "src/profiles/" + repositoryConfigName);
        }
      }

      addConfig(OsSpecific.stratoUserHome(), userConfigName);
      if (repositoryConfigName != null) {
        addConfig(OsSpecific.stratoUserHome(), repositoryConfigName);
      }

      if (workspaceRoot != null) {
        addConfig(workspaceRoot, "config/custom.conf");
        addConfig(workspaceRoot, "config/intellij.conf");
      }

      updateProperty("userHome", OsSpecific.userHome().toString());

      // resolving to have version-mapping property
      Config tmpResolvedConfig = config.resolve(allowUnresolved);

      if (buildtoolOverride != null && !buildtoolOverride.isEmpty()) {
        String location = buildtoolOverride + "/optimus/buildtool/local/install/common";
        updateProperty("internal.obt.install", location);
      } else {
        mapProperty(obtVersionProperty, tmpResolvedConfig);
      }

      if (stratosphereInfraOverride != null) {
        updateProperty(
            stratosphereVersionProperty,
            Paths.get(stratosphereInfraOverride)
                .normalize()
                .getParent()
                .getParent()
                .getFileName()
                .toString());
      } else {
        mapProperty(stratosphereVersionProperty, tmpResolvedConfig);
      }

      if (workspaceRoot != null) {
        /*
         * If sources with config are in other location than 'workspace' (build directories, ../.stratosphere/, etc.)
         * during the setup we add `stratosphereHome` to custom.conf. We do that because all commands are run inside
         * src directory, and they need to be able to locate rest of the 'workspace'.
         */
        updateProperty(
            "stratosphereHome",
            trimTrailingSlashes(workspaceRoot.getParent().toAbsolutePath().toString()));
        updateProperty("stratosphereWorkspace", workspaceRoot.getFileName().toString());
        String srcDir =
            trimTrailingSlashes(workspaceRoot.resolve("src").toAbsolutePath().toString());
        updateProperty("stratosphereSrcDir", srcDir);
      }

      // resolving to have stratosphereSrcDir, we need it to add sysloc property
      tmpResolvedConfig = config.resolve(allowUnresolved);

      FsCampus fsCampus =
          new FsCampus(
              Paths.get(tmpResolvedConfig.getString("stratosphereSrcDir")).resolve("config"));
      updateProperty("region.default", fsCampus.getValue());
      updateProperty("region.reports", fsCampus.getAlternativeMapping("report-upload"));
      updateProperty("region.obt-dht", fsCampus.getAlternativeMapping("obt-dht"));

      if (StratosphereChannelsConfig.shouldUseStratosphereChannels(
          tmpResolvedConfig, stratosphereInfraOverride)) {
        Channel selectedChannel = StratosphereChannelsConfig.selectChannel(tmpResolvedConfig);
        if (selectedChannel != null) {
          System.out.printf("Used Stratosphere Channel: %s\n", selectedChannel.name);
          config = selectedChannel.config.withFallback(config);
          tmpResolvedConfig = config.resolve(allowUnresolved);
        }
      }

      if (stratosphereInfraOverride == null) {
        String stratosphereVersion = config.getString(stratosphereVersionProperty);
        String resolvedSymlinkVersion = resolveSymlinkVersion(tmpResolvedConfig);
        if (!stratosphereVersion.equals(resolvedSymlinkVersion)) {
          updateProperty(stratosphereVersionProperty, resolvedSymlinkVersion);
          updateProperty("stratosphereVersionSymlink", stratosphereVersion);
        }
      }

      if (workspaceRoot != null) {
        if (config.hasPath("profile")) {
          String profileName = config.getString("profile");
          addConfig(workspaceRoot, "src/profiles/" + profileName + ".conf");
        }
      }

      // We need to add this on the bootstrap level,
      // because potential migration affects also workspaces with older versions of stratosphere
      addConfig("/etc/migration-messages.conf");
      Path migrationConfigLocation =
          getMigrationConfigLocation(
              tmpResolvedConfig.getString("internal.paths.migration-config"));
      addConfig(migrationConfigLocation, "repoMigration.conf");
      updateProperty(
          HistoryTruncationConfigKeys.selectedMigrationConfigLocation,
          migrationConfigLocation.toAbsolutePath().toString());

      // resolving to use proper Scala version in further resolves
      tmpResolvedConfig = config.resolve(allowUnresolved);
      ScalaDependency scalaDependency =
          ScalaDependency.get(
              tmpResolvedConfig.getString("scalaVersion"),
              tmpResolvedConfig.getString("internal.scala.nightly-location"),
              tmpResolvedConfig.getString("internal.scala.release-location"));
      updateProperty("scalaVersion", scalaDependency.version);
      updateProperty("scalaMajorVersion", scalaDependency.getMajorVersion());
      scalaDependency.customRepo.ifPresent(this::addCustomScalaRepository);

      // resolving to have version visible
      config = config.resolve(allowUnresolved);
      String infraPath = getStratoInfra();
      updateProperty("stratosphereInfra", infraPath);
      updateProperty("internal.stratosphere.scripts-dir", infraPath);
      if (stratosphereInfraOverride != null) {
        String version = config.getString(stratosphereVersionProperty);
        String stratosphereInfraOverrideIvy =
            infraPath
                .replace(version, "[revision]")
                .replaceFirst("[\\\\/]optimus[\\\\/]", "/[meta]/")
                .replaceFirst("[\\\\/]stratosphere[\\\\/]", "/[project]/");
        updateProperty("stratosphereInfraOverrideIvy", stratosphereInfraOverrideIvy);
      }

      // on Linux hosts we can use Java from AFS without performance problems
      if (OsSpecific.isLinux) {
        updateProperty("internal.java.home", config.getString("internal.java.install"));
      }

      String javaShortVersion = getJavaShortVersion(config.getString("javaVersion"));
      updateProperty("javaShortVersion", javaShortVersion);
      if (!config.hasPath("javaTargetCompatibility")) {
        updateProperty("javaTargetCompatibility", javaShortVersion);
      }
      if (!config.hasPath("javaSourceCompatibility")) {
        updateProperty("javaSourceCompatibility", config.getString("javaTargetCompatibility"));
      }

      // set home if outside workspace AND no override is set by user
      if (workspaceRoot == null && !config.hasPath("stratosphereHome")) {
        updateProperty("stratosphereHome", stratosphereHomeOutsideOfWorkspace());
      }

      String stratosphereHome = config.getString("stratosphereHome");
      String stratosphereWorkspace = config.getString("stratosphereWorkspace");
      Path stratosphereWsDir = Paths.get(stratosphereHome).resolve(stratosphereWorkspace);

      if (!config.hasPath("stratosphereWsDir")) {
        updateProperty("stratosphereWsDir", stratosphereWsDir.toString());
      }

      updateProperty("java.io.tmpdir", System.getProperty("java.io.tmpdir"));

      if (workspaceRoot != null) {
        if (config.hasPath("intellijProfile")) {
          String intellijProfile = config.getString("intellijProfile");
          addConfig(workspaceRoot, "src/profiles/" + intellijProfile + ".conf");
        }
      }

      // final resolve - we don't allow unresolved params anymore
      config = config.resolve();

      String overridesPrefix = "strato";
      Config defaultOverrides = ConfigFactory.defaultOverrides();
      if (defaultOverrides.hasPath(overridesPrefix)) {
        defaultOverrides
            .getConfig(overridesPrefix)
            .entrySet()
            .forEach(override -> updateProperty(override.getKey(), override.getValue()));
      }
    } catch (ConfigException.UnresolvedSubstitution unresolvedSubstitution) {
      String msg = prepareConfigExceptionMessage(unresolvedSubstitution);
      throw new StratosphereException(msg);
    } catch (ConfigException configException) {
      String msg =
          "[ERROR] Error in the config:\n"
              + "[ERROR] "
              + configException.toString().replace("com.typesafe.config.ConfigException$", "");

      if (configException.origin() != null && configException.origin().filename() != null) {
        String filename = configException.origin().filename();
        msg +=
            "\n[ERROR] If you don't know how to fix that problem, please remove "
                + filename
                + " and try again.";
        throw new StratosphereException(msg);
      } else {
        throw new StratosphereException(msg, configException);
      }
    }
  }

  private String stratosphereHomeOutsideOfWorkspace() {
    String userTestDir = System.getProperty("stratosphere.homeDir");
    if (userTestDir != null) {
      return userTestDir;
    }

    try {
      String userName = config.getString(CONFIG_USERNAME);
      if (OsSpecific.isWindows) {
        return System.getenv("SystemDrive") + "\\MSDE\\" + userName;
      } else {
        return "/var/tmp/" + userName;
      }
    } catch (ConfigException configException) {
      String msg = prepareConfigExceptionMessage(configException);
      throw new StratosphereException(msg);
    }
  }

  private String getStratoInfra() {
    if (stratosphereInfraOverride != null) {
      return stratosphereInfraOverride;
    } else {
      final String version = config.getString(stratosphereVersionProperty);
      return config.getString("internal.stratosphere.infra-path").replace("{version}", version);
    }
  }

  private Path getMigrationConfigLocation(String defaultPath) {
    String path = Objects.requireNonNullElse(stratosphereMigrationLocationOverride, defaultPath);
    return Paths.get(path);
  }

  private String trimTrailingSlashes(String basePath) {
    if (basePath.endsWith("/") || basePath.endsWith("\\")) {
      return basePath.substring(0, basePath.length() - 1);
    } else {
      return basePath;
    }
  }

  private void addConfig(String resource) {
    Config newConfig = ConfigFactory.parseResources(StratosphereConfig.class, resource);
    config = (config != null) ? newConfig.withFallback(config) : newConfig;
  }

  private void addConfig(Path basePath, String path) {
    config = withConfig(basePath, path, config);
  }

  private Config withConfig(Path basePath, String path, Config config) {
    Path confFile = basePath.resolve(path);
    if (Files.exists(confFile)) {
      Config newConfig = ConfigFactory.parseFile(confFile.toFile());
      return (config != null) ? newConfig.withFallback(config) : newConfig;
    }
    return config;
  }

  private void mapProperty(final String propertyName, final Config tmpConfig) {
    if (tmpConfig.hasPath("internal.version-mapping")) {
      final String currentVersion = tmpConfig.getString(propertyName);
      final Path configFile = Paths.get(tmpConfig.getString("internal.version-mapping"));
      final String newVersion = getMappedProperty(currentVersion, configFile);
      if (!newVersion.equals(currentVersion)) {
        System.out.printf(
            "Detected version change for '%s' from '%s' to '%s'%n",
            propertyName, currentVersion, newVersion);
        updateProperty(propertyName, newVersion);
      }
    }
  }

  protected static String getMappedProperty(final String currentVersion, final Path configFile) {
    try {
      if (Files.exists(configFile)) {
        final Config mapping = ConfigFactory.parseFile(configFile.toFile());
        return mapping.getString("version-mapping." + "\"" + currentVersion + "\"");
      } else {
        return currentVersion;
      }
    } catch (ConfigException.Missing e) {
      return currentVersion;
    }
  }

  private void updateProperty(String propertyName, Object newValue) {
    config = config.withValue(propertyName, ConfigValueFactory.fromAnyRef(newValue));
  }

  private void updateProperty(String propertyName, ConfigValue newValue) {
    config = config.withValue(propertyName, newValue);
  }

  private void addCustomScalaRepository(ScalaDependency.IvyRepository repo) {
    updateProperty("scala.customRepo.ivyPattern", repo.ivyPattern);
    updateProperty("scala.customRepo.artifactPattern", repo.artifactPattern);
  }

  private String replaceLast(String string, String substring, String replacement) {
    int index = string.lastIndexOf(substring);
    if (index == -1) {
      return string;
    } else {
      return string.substring(0, index)
          + replacement
          + string.substring(index + substring.length());
    }
  }

  private String getJavaShortVersion(String javaVersion) {
    try {
      String[] javaParts = javaVersion.split("\\.");
      return javaParts[0] + "." + javaParts[1];
    } catch (Exception e) {
      throw new StratosphereException(
          "It's expected that JAVA_VERSION has MAJOR.MINOR[.SECURITY[.PATH]] format but found '"
              + javaVersion
              + "'");
    }
  }

  private static String resolveSymlinkVersion(Config config) {
    String location = config.getString("internal.stratosphere.infra-path");
    String stratoVersion = config.getString(stratosphereVersionProperty);
    String infraPath = location.replace("{version}", stratoVersion);
    try {
      Path resolvedPath = Paths.get(infraPath);
      if (Files.exists(resolvedPath))
        return resolvedPath.toRealPath().getParent().getFileName().toString();
      else return stratoVersion;
    } catch (Exception e) {
      throw new StratosphereException("Problem with resolving stratosphere symlink", e);
    }
  }
}
