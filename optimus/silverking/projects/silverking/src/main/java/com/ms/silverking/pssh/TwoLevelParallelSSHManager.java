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
package com.ms.silverking.pssh;

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.SKConstants;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.LightLinkedBlockingQueue;
import com.ms.silverking.io.StreamParser;
import com.ms.silverking.util.PropertiesHelper;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwoLevelParallelSSHManager extends UnicastRemoteObject implements SSHManager {
  private static final long serialVersionUID = 2826568720882391661L;

  private static Logger log = LoggerFactory.getLogger(TwoLevelParallelSSHWorker.class);

  private Set<String> hosts;

  private LightLinkedBlockingQueue<HostAndCommand> pendingHostCommands;
  private Set<HostAndCommand> activeHostCommands;
  private Set<HostAndCommand> incompleteHostCommands;
  private Set<HostAndCommand> completeHostCommands;
  private int maxAttempts;
  private int attempts;
  private Lock retryLock;

  private String[] workerCommand;
  private int timeoutSeconds;
  private String url;
  private String classpath;

  private Set<String> workers;
  private ParallelSSH workerSSH;
  private Semaphore completedWorkers;
  private int numWorkers;
  private int workerTimeoutSeconds;
  private boolean terminateUponCompletion;

  private static final HostAndCommand DONE_MARKER = new HostAndCommand("DONE_MARKER", new String[0]);

  private static final String registryName = "SSHManager";
  private static final int registryStartPort = 1097;
  private static final int registryEndPort = 1297;

  private static final int terminationSeconds = 5;
  private static final int maxWorkers = 20;
  private static final double workerFraction = 0.05;
  private static final int maxWorkerThreads = 20;
  private static final double workerSecondsPerHost = 15;
  private static final int workerTimeoutMinSeconds = 5 * 60;

  private static final String javaCmd;

  private static AtomicBoolean terminated;

  static {
    terminated = new AtomicBoolean(false);
    javaCmd = PropertiesHelper.envHelper.getString(SKConstants.javaHomeEnv,
        System.getProperty(DHTConstants.javaHomeProperty)) + "/bin/java";
  }

  public TwoLevelParallelSSHManager() throws RemoteException {
  }

  public TwoLevelParallelSSHManager(List<HostAndCommand> hostCommands, List<String> workerCandidateHosts,
                                    int numWorkerThreads, int timeoutSeconds, int maxAttempts, boolean terminateUponCompletion) throws IOException {
    Registry registry;
    int registryPort;
    String envCmd;

    numWorkerThreads = Math.min(numWorkerThreads, maxWorkerThreads);

    log.info("numWorkerThreads: {}", numWorkerThreads);
    log.info("timeoutSeconds: {}", timeoutSeconds);

    this.hosts = HostAndCommand.getHosts(hostCommands);
    this.maxAttempts = maxAttempts;
    this.terminateUponCompletion = terminateUponCompletion;

    pendingHostCommands = new LightLinkedBlockingQueue<HostAndCommand>(hostCommands);
    activeHostCommands = new ConcurrentSkipListSet<>(hostCommands);
    incompleteHostCommands = new ConcurrentSkipListSet<>(hostCommands);
    completeHostCommands = new ConcurrentSkipListSet<>();
    attempts = 1;
    retryLock = new ReentrantLock();

    this.timeoutSeconds = timeoutSeconds;

    workerTimeoutSeconds = (int) Math.max(workerTimeoutMinSeconds, (double) hosts.size() * workerSecondsPerHost);
    log.info("workerTimeoutSeconds: {}", workerTimeoutSeconds);
    log.info("hosts.size(): {}", hosts.size());
    if (hosts.size() == 0) {
      terminate();
      throw new RuntimeException("Empty hosts");
    }

    workers = createWorkers(workerCandidateHosts, hosts);
    numWorkers = workers.size();
    log.info("numWorkers: {}", numWorkers);
    completedWorkers = new Semaphore(-(numWorkers - 1));

    classpath = System.getProperty("java.class.path");
    log.info("classpath: {}", classpath);

    registry = null;
    for (registryPort = registryStartPort; registryPort <= registryEndPort; registryPort++) {
      try {
        log.info("Attempted to create registry on: {}", registryPort);
        registry = LocateRegistry.createRegistry(registryPort);
        log.info("Registry bound to port: {}", registryPort);
        break;
      } catch (RemoteException re) {
        re.printStackTrace();
      }
    }
    if (registryPort > registryEndPort) {
      throw new RuntimeException("Unable to find a port for registry");
    }
    registry.rebind(registryName, this);
    url = "rmi://" + InetAddress.getLocalHost().getCanonicalHostName() + ":" + registryPort + "/" + registryName;

    workerCommand = new String[1];
    workerCommand[0] =
        javaCmd + " -cp " + classpath + " com.ms.silverking.pssh.TwoLevelParallelSSHWorker rmi://" + InetAddress.getLocalHost().getCanonicalHostName() + ":" + registryPort + "/SSHManager " + numWorkerThreads + " " + timeoutSeconds;
    log.info(workerCommand[0]);

    log.info("Manager URL: {}" , url );
  }

  public TwoLevelParallelSSHManager(Map<String, String[]> hostCommandsMap, List<String> workerCandidateHosts,
                                    int numWorkerThreads, int timeoutSeconds, int maxAttempts, boolean terminateUponCompletion) throws IOException {
    this(createHostCommands(hostCommandsMap), workerCandidateHosts, numWorkerThreads, timeoutSeconds, maxAttempts,
        terminateUponCompletion);
  }

  private static List<HostAndCommand> createHostCommands(Map<String, String[]> hostCommandsMap) {
    List<HostAndCommand> hostCommands;

    hostCommands = new ArrayList<>();
    for (Map.Entry<String, String[]> entry : hostCommandsMap.entrySet()) {
      hostCommands.add(new HostAndCommand(entry.getKey(), entry.getValue()));
    }
    return hostCommands;
  }

  private static Set<String> createWorkers(List<String> workerCandidateHosts, Set<String> hosts) {
    Random random;
    int randomIndex;
    Set<String> workers;
    List<String> hostList;
    int numWorkers;

    hostList = ImmutableList.copyOf(workerCandidateHosts);
    random = new Random();
    numWorkers = (int) Math.min((double) workerCandidateHosts.size() * workerFraction, maxWorkers);
    ;
    numWorkers = Math.max(numWorkers, 1);
    randomIndex = random.nextInt(workerCandidateHosts.size());
    workers = new HashSet<>();
    for (int i = 0; i < Math.min(numWorkers, workerCandidateHosts.size()); i++) {
      workers.add(hostList.get((i + randomIndex) % workerCandidateHosts.size()));
    }
    return workers;
  }

  public void startWorkers(HostGroupTable hostGroups) {
    startWorkers(workers, numWorkers, hostGroups);
  }

  private void startWorkers(Set<String> workers, int numWorkers, HostGroupTable hostGroups) {
    workerSSH = new ParallelSSH(workers, workerCommand, numWorkers, workerTimeoutSeconds, hostGroups);
  }

  public boolean waitForWorkerCompletion() {
    while (true) {
      log.info("workerSSH.numCompleted(): {}    numWorkers: {}" , workerSSH.numCompleted() , numWorkers);
      if (workerSSH.numCompleted() == numWorkers) {
        return workerSSH.getFailed().size() == 0;
      }
      try {
        boolean complete;

        complete = completedWorkers.tryAcquire(1, TimeUnit.SECONDS);
        if (complete) {
          return workerSSH.getFailed().size() == 0;
        }
      } catch (InterruptedException ie) {
      }
    }
  }

  //////////////////////////////////////////////////////////////////////

  @Override
  public String getSSHCmd() throws RemoteException {
    if (workerSSH.sshCmdIsDefault()) {
      return null;
    } else {
      return workerSSH.getSSHCmd();
    }
  }

  @Override
  public Map<String, String> getSSHCmdMap() throws RemoteException {
    return workerSSH.getSSHCmdMap();
  }

  @Override
  public HostGroupTable getHostGroups() throws RemoteException {
    return workerSSH.getHostGroups();
  }

  @Override
  public HostAndCommand getHostAndCommand() {
    return getHostAndCommand(true, timeoutSeconds);
  }

  private HostAndCommand getHostAndCommand(boolean retryIfNeeded, int timeout) {
    HostAndCommand hostAndCommand;

    try {
      hostAndCommand = pendingHostCommands.poll(timeout, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      throw new RuntimeException();
    }
    if (hostAndCommand != null) {
      if (hostAndCommand != DONE_MARKER) {
        activeHostCommands.add(hostAndCommand);
        logState();
      } else {
        try {
          pendingHostCommands.put(DONE_MARKER);
        } catch (InterruptedException ie) {
        }
        hostAndCommand = null;
      }
    } else {
      if (retryIfNeeded) {
        retryLock.lock();
        try {
          // check with lock to ensure that it's really empty
          // (we can remove unlocked, but can't add)
          hostAndCommand = getHostAndCommand(false, 0);
          if (attempts < maxAttempts) {
            // now while holding the lock, we can add for retry
            attempts++;
            log.info("Commencing attempt: {}", attempts);
            try {
              pendingHostCommands.putAll(incompleteHostCommands);
            } catch (InterruptedException ie) {
            }
            hostAndCommand = getHostAndCommand(false, 0);
          }
        } finally {
          retryLock.unlock();
        }
      }
    }
    try {
      log.info("Sending {}   to worker: {}" , hostAndCommand , getClientHost());
    } catch (ServerNotActiveException snae) {
      snae.printStackTrace();
    }

    return hostAndCommand;
  }

  private void logState() {
    log.info(
        "Complete: {} Incomplete: {}  Active: {}  Pending: {}" ,completeHostCommands.size(), incompleteHostCommands.size(), activeHostCommands.size(),  pendingHostCommands.size());
  }

  @Override
  public void setHostResult(HostAndCommand hostAndCommand, HostResult result) {
    try {
      log.info("set host result {} from {}", hostAndCommand ,getClientHost());
    } catch (ServerNotActiveException snae) {
      snae.printStackTrace();
    }
    activeHostCommands.remove(hostAndCommand);
    incompleteHostCommands.remove(hostAndCommand);
    completeHostCommands.add(hostAndCommand);
    if (incompleteHostCommands.size() == 0) {
      try {
        pendingHostCommands.put(DONE_MARKER);
      } catch (InterruptedException ie) {
      }
      if (terminateUponCompletion) {
        if (beginTermination()) {
          displayIncomplete();
        }
      } else {
        displayIncomplete();
      }
    }
    logState();
  }

  @Override
  public void workerComplete() throws RemoteException {
    try {
      log.info("worker complete  from " , getClientHost());
    } catch (ServerNotActiveException snae) {
      snae.printStackTrace();
    }
    completedWorkers.release();
    log.info("Worker complete: {}", completedWorkers.availablePermits());
  }

  //////////////////////////////////////////////////////////////////////

  public void displayIncomplete() {
    for (HostAndCommand hostAndCommand : incompleteHostCommands) {
      log.info("Incomplete: {}" , hostAndCommand);
    }
  }

  public static boolean beginTermination() {
    if (!terminated.getAndSet(true)) {
      new com.ms.silverking.process.Terminator(terminationSeconds, 10);
      return true;
    }
    return false;
  }

  public static List<String> readHostsFile(String hostsFile, String description, List<String> exclusionHosts)
      throws IOException {
    List<String> hostList;

    if (hostsFile != null) {
      hostList = StreamParser.parseFileLines(hostsFile);
    } else {
      log.info("No {} file specified",description );
      hostList = ImmutableList.of();
    }
    if (exclusionHosts != null) {
      hostList.removeAll(exclusionHosts);
      log.info( "{}  [after exclusions]: {}",description , hostList.size());
    } else {
      log.info("{}: {}",description , hostList.size());
    }
    log.info(CollectionUtil.toString(hostList));
    return hostList;
  }

  public static List<String> readHostsFileSelectHostgroup(String vaDhtFile, String hostGroup) throws IOException {
    List<String> hostList;
    List<String> fileLines; //store lines extracted from files

    hostList = new ArrayList<>();
    fileLines = StreamParser.parseFileLines(vaDhtFile);
    for (String aLine : fileLines) {
      String[] lineFields = aLine.split("\t");  //store split elements of a line
      for (int i = 1; i < lineFields.length; i++) {
        if (hostGroup.equals(lineFields[i])) {
          hostList.add(lineFields[0]);
          break;

        }
      }
    }
    log.info("Hosts: {}", hostList.size());
    return hostList;
  }

  public void doSSH() {
    boolean workersComplete;

    workersComplete = false;
    while (!workersComplete) {
      startWorkers(null);
      workersComplete = waitForWorkerCompletion();
    }
    if (terminateUponCompletion) {
      if (beginTermination()) {
        displayIncomplete();
      }
      terminate();
    } else {
      displayIncomplete();
    }
  }

  public void terminate() {
    try {
      boolean result;

      result = UnicastRemoteObject.unexportObject(this, true);
      log.info("TwoLevelParallelSSHManager terminated:{}", result);
    } catch (NoSuchObjectException e) {
      log.error("",e);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      List<String> hosts;
      List<HostAndCommand> hostCommands;
      List<String> workerCandidateHosts;
      List<String> excludedHosts;
      String[] cmd;
      TwoLevelParallelSSHManager parallelSSH;
      TwoLevelParallelSSHOptions options;

      CmdLineParser parser;

      options = new TwoLevelParallelSSHOptions();
      parser = new CmdLineParser(options);
      try {
        parser.parseArgument(args);
      } catch (CmdLineException cle) {
        System.err.println(cle.getMessage());
        parser.printUsage(System.err);
        return;
      }

      // read exclusions
      excludedHosts = readHostsFile(options.exclusionsFile, "Exclusions", null);

      // read hosts
      String[] fields = options.hostsFile_optionalGroup.split(":");
      if (fields.length > 1) {
        hosts = readHostsFileSelectHostgroup(fields[0], fields[1]);
        //hosts.removeAll(excludedHosts);
      } else {
        //hosts = readHostsFile(options.hostsFile_optionalGroup, "Hosts", excludedHosts);
        hosts = readHostsFile(options.hostsFile_optionalGroup, "Hosts", null);
      }

      // Note that code has been changed to only exclude for the purposes of workers.
      // Containing script filters hosts externally.
      // FUTURE - Could add an option to push that into here.

      workerCandidateHosts = readHostsFile(options.workerCandidatesFile, "Worker Candidates", excludedHosts);

      cmd = options.command.split("\\s+");

      hostCommands = new ArrayList<>();
      for (String host : hosts) {
        hostCommands.add(new HostAndCommand(host, cmd));
      }

      parallelSSH = new TwoLevelParallelSSHManager(hostCommands, workerCandidateHosts, options.numWorkerThreads,
                                                   options.timeoutSeconds, options.maxAttempts, true);
      parallelSSH.doSSH();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
