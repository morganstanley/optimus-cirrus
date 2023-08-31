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
package com.ms.silverking.cloud.dht.management.aws;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DeleteKeyPairRequest;
import com.amazonaws.services.ec2.model.DeleteKeyPairResult;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.InstanceStatusDetails;
import com.amazonaws.services.ec2.model.InstanceStatusSummary;
import com.amazonaws.services.ec2.model.Reservation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

  private static Logger log = LoggerFactory.getLogger(Util.class);

  public enum InstanceState {
    STOPPED,
    RUNNING
  }

  public static final String USER_HOME = System.getProperty("user.home");
  static final String NEW_LINE = System.getProperty("line.separator");

  static boolean debugPrint = false;

  private static final String KEY_PAIR_PREFIX = "sk_key";

  static void printInstance(Instance instance) {
    if (debugPrint)
      log.info(
          "Found instance with id {}, "
              + "AMI {}, "
              + "type {}, "
              + "state {} "
              + "and monitoring state {}",
          instance.getInstanceId(),
          instance.getImageId(),
          instance.getInstanceType(),
          instance.getState().getName(),
          instance.getMonitoring().getState());
  }

  static void printNoDot(String text) {
    printHelper(text, "");
    log.info("");
  }

  public static void print(String text) {
    printHelper(text, "......");
  }

  static void printHelper(String text, String spacer) {
    System.out.printf("%-65s %s ", text, spacer);
  }

  public static void printDone(List<String> values) {
    printDone(String.join(", ", values));
  }

  public static void printDone() {
    printDone("");
  }

  public static void printDone(String value) {
    System.out.println("done (" + value + ")");
  }

  static void debugPrint(String text) {
    if (debugPrint) log.debug("{}", text);
  }

  static List<String> getInstanceIds(List<Instance> instances) {
    List<String> ids = new ArrayList<>();

    for (Instance instance : instances) ids.add(instance.getInstanceId());

    return ids;
  }

  static List<String> getIps(List<Instance> instances) {
    List<String> ips = new ArrayList<>();

    for (Instance instance : instances) ips.add(instance.getPrivateIpAddress());

    return ips;
  }

  static void writeToFile(String filename, String content) {
    File file = new File(filename);

    try {
      // mkdirs and createNewFile only create iff those paths/file don't already exist. so no need
      // to check for
      // existence beforehand.
      file.getParentFile().mkdirs();
      file.createNewFile();
      FileWriter writer = new FileWriter(file);
      writer.write(content);
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    // needs to be done in this order, b/c "everyone else" wipes out all permissions (including
    // "owner")...
    // "everyone else"
    file.setExecutable(false, false);
    file.setReadable(false, false);
    file.setWritable(false, false);

    // "owner"
    file.setExecutable(false);
    file.setReadable(true);
    file.setWritable(true);

    //        System.out.println("Is Execute allow : " + file.canExecute());
    //        System.out.println("Is Write allow : " +   file.canWrite());
    //        System.out.println("Is Read allow : " +    file.canRead());
  }

  public static void writeToFile(String filename, List<String> content) {
    writeToFile(filename, String.join(NEW_LINE, content) + NEW_LINE);
  }

  public static List<String> readFile(String filename) {
    try {
      return Files.readAllLines(Paths.get(filename));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  static void waitForInstancesToBeRunning(AmazonEC2 ec2, List<Instance> instances) {
    print("  Waiting for Instances to be running");

    DescribeInstancesRequest diRequest = new DescribeInstancesRequest();
    diRequest.withInstanceIds(getInstanceIds(instances));

    long lastMinutePrinted = 0;
    long sleepSeconds = 5;
    long totalRunTimeSeconds = 2 * 60;
    int retriesCount = 0;
    List<String> ips = getIps(instances);
    while (!ips.isEmpty()) {
      DescribeInstancesResult response = ec2.describeInstances(diRequest);
      for (Reservation reservation : response.getReservations()) {
        for (Instance instance : reservation.getInstances()) {
          //                    System.out.println(instance.getState().getName());
          if (isRunning(instance)) {
            //                        System.out.println(instance.getPrivateIpAddress());
            ips.remove(instance.getPrivateIpAddress());
          }
        }
      }

      if (ips.isEmpty()) break;

      checkForTimeoutException(sleepSeconds, totalRunTimeSeconds, retriesCount, "running");

      sleep(sleepSeconds);
      retriesCount++;
      lastMinutePrinted = printMinutesElapsed(sleepSeconds, retriesCount, lastMinutePrinted);
    }

    printDone(getIps(instances));
  }

  private static void checkForTimeoutException(
      long sleepSeconds, long totalRunTimeSeconds, int retriesCount, String status) {
    if (retriesCount * sleepSeconds > totalRunTimeSeconds) throwTimeoutException(status);
  }

  private static void throwTimeoutException(String status) {
    throwRuntimeException("instances should have been " + status + " by now...");
  }

  static void waitForInstancesToBeReachable(AmazonEC2 ec2, List<Instance> instances) {
    printNoDot("  Waiting for Instances to be reachable");
    printNoDot("    reachable = BOTH (System and Instance) status checks PASS");
    printNoDot("    be patient, it usually takes ~5mins");

    DescribeInstanceStatusRequest disRequest = new DescribeInstanceStatusRequest();
    disRequest.withInstanceIds(getInstanceIds(instances));

    long lastMinutePrinted = 0;
    long sleepSeconds = 15;
    long totalRunTimeSeconds = 10 * 60;
    int retriesCount = 0;
    List<String> ips = getIps(instances);
    while (!ips.isEmpty()) {
      DescribeInstanceStatusResult response = ec2.describeInstanceStatus(disRequest);
      for (InstanceStatus status : response.getInstanceStatuses()) {
        if (passedSystemStatusCheck(status.getSystemStatus())
            && passedInstanceStatusCheck(status.getInstanceStatus())) {
          Instance instance = getInstance(status.getInstanceId(), instances);
          String ip = instance.getPrivateIpAddress();
          if (ips.contains(
              ip)) { // avoids multiple printing of those that are already 'good', while waiting for
            // the rest to
            // reach 'good' status
            log.info("  {} is good", ip);
            ips.remove(ip);
          }
        }
      }

      if (ips.isEmpty()) break;

      checkForTimeoutException(sleepSeconds, totalRunTimeSeconds, retriesCount, "reachable");

      sleep(sleepSeconds);
      retriesCount++;
      lastMinutePrinted = printMinutesElapsed(sleepSeconds, retriesCount, lastMinutePrinted);
    }

    print("");
    printDone(getIps(instances));
  }

  static boolean passedSystemStatusCheck(InstanceStatusSummary statusSummary) {
    return isReachableAndStatusIsOk(statusSummary);
  }

  static boolean passedInstanceStatusCheck(InstanceStatusSummary statusSummary) {
    return isReachableAndStatusIsOk(statusSummary);
  }

  private static boolean isReachableAndStatusIsOk(InstanceStatusSummary statusSummary) {
    return reachabilityPassed(statusSummary.getDetails()) && statusIsOk(statusSummary.getStatus());
  }

  private static boolean reachabilityPassed(List<InstanceStatusDetails> details) {
    for (InstanceStatusDetails detail : details)
      if (detail.getName().equals("reachability") && detail.getStatus().equals("passed"))
        return true;

    return false;
  }

  private static boolean statusIsOk(String status) {
    return status.equals("ok");
  }

  private static Instance getInstance(String id, List<Instance> instances) {
    for (Instance instance : instances) if (instance.getInstanceId().equals(id)) return instance;

    throw new RuntimeException("instance '" + id + "' not found");
  }

  private static void sleep(long seconds) {
    try {
      Thread.sleep(seconds * 1_000);
    } catch (InterruptedException ex) {
      throwRuntimeException(ex);
    }
  }

  private static long printMinutesElapsed(long sleepSeconds, int retries, long lastMinutePrinted) {
    long minute = sleepSeconds * retries / 60;
    if (minute != 0 && minute != lastMinutePrinted) {
      log.info("      *{} mins elapsed*", minute);
      lastMinutePrinted = minute;
    }

    return lastMinutePrinted;
  }

  static List<Instance> findRunningInstancesWithKeyPair(AmazonEC2 ec2, String keyPair) {
    return findInstancesStateWithKeyPair(ec2, keyPair, InstanceState.RUNNING);
  }

  static List<Instance> findStoppedInstancesWithKeyPair(AmazonEC2 ec2, String keyPair) {
    return findInstancesStateWithKeyPair(ec2, keyPair, InstanceState.STOPPED);
  }

  static List<Instance> findInstancesStateWithKeyPair(
      AmazonEC2 ec2, String keyPair, InstanceState state) {
    print("Finding " + state + " Instances");

    List<Instance> instances = new ArrayList<>();

    DescribeInstancesRequest request = new DescribeInstancesRequest();
    while (true) {
      DescribeInstancesResult response = ec2.describeInstances(request);

      for (Reservation reservation : response.getReservations())
        for (Instance instance : reservation.getInstances()) {
          printInstance(instance);
          switch (state) {
            case STOPPED:
              if (isStopped(instance) && isKeyPair(instance, keyPair)) instances.add(instance);
              break;
            case RUNNING:
              if (isRunning(instance) && isKeyPair(instance, keyPair)) instances.add(instance);
              break;
            default:
              throwRuntimeException("Unknown Instance state: " + state);
          }
        }

      if (response.getNextToken() == null) break;

      debugPrint("token: " + response.getNextToken());
      request.setNextToken(response.getNextToken());
    }

    printDone("found " + instances.size());

    return instances;
  }

  static List<String> getIds(List<InstanceStateChange> instanceStateChanges) {
    List<String> ids = new ArrayList<>();

    for (InstanceStateChange instanceStateChange : instanceStateChanges)
      ids.add(instanceStateChange.getInstanceId());

    return ids;
  }

  static boolean isStopped(Instance instance) {
    return isState(instance, InstanceState.STOPPED);
  }

  static boolean isRunning(Instance instance) {
    return isState(instance, InstanceState.RUNNING);
  }

  private static boolean isState(Instance instance, InstanceState state) {
    return instance.getState().getName().equals(state.toString().toLowerCase());
  }

  static boolean isKeyPair(Instance instance, String keyPair) {
    return instance.getKeyName().equals(keyPair);
  }

  static void deleteKeyPair(AmazonEC2 ec2, String keyPairName) {
    print("Deleting Old Key Pair");

    DeleteKeyPairRequest deleteKeyPairRequest = new DeleteKeyPairRequest();
    deleteKeyPairRequest.withKeyName(keyPairName);

    DeleteKeyPairResult deleteKeyPairResult = ec2.deleteKeyPair(deleteKeyPairRequest);

    printDone(keyPairName);
  }

  public static void checkNumInstances(int numInstances) {
    if (numInstances <= 0)
      throwIllegalArgumentException(
          "numInstances",
          numInstances,
          "must be > 0. You need to set \"-n\", if you haven't already.");
  }

  public static void throwIllegalArgumentException(
      String variableName, Object variableValue, String msg) {
    throw new IllegalArgumentException(
        "Invalid " + variableName + ": \"" + variableValue + "\" .... " + msg);
  }

  public static void throwRuntimeException(String msg) {
    throw new RuntimeException(msg);
  }

  public static void throwRuntimeException(Exception ex) {
    throw new RuntimeException(ex);
  }

  public static String getMyIp() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static String getUniqueKeyPairName(String uniquifier) {
    return KEY_PAIR_PREFIX + "_" + uniquifier;
  }
}
