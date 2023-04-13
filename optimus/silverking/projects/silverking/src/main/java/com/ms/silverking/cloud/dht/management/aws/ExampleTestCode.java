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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleTestCode {

  private AmazonEC2 ec2;
  private static final String amiId = "ami-b77b06cf";
  private static final String keyPairName = "bph";
  private static final String securityGroupName = "bph";

  private static Logger log = LoggerFactory.getLogger(ExampleTestCode.class);

  public ExampleTestCode() {
    /*
     * The ProfileCredentialsProvider will return your [default]
     * credential profile by reading from the credentials file located at
     * (C:\\Users\\ben-pc\\.aws\\credentials).
     */
    ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
    try {
      credentialsProvider.getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException(
          "Cannot load the credentials from the credential profiles file. " + "Please make sure that your " +
              "credentials" + " file is at the correct " + "location (C:\\Users\\ben-pc\\.aws\\credentials), and is " +
              "in valid format.",
          e);
    }
    ec2 = AmazonEC2ClientBuilder.standard().withRegion("us-west-2").build();
  }

  public void run() {
    try {
      DescribeAvailabilityZonesResult availabilityZonesResult = ec2.describeAvailabilityZones();
     log.info(
          "You have access to  {}  Availability Zones.", availabilityZonesResult.getAvailabilityZones().size() );

      DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
      List<Reservation> reservations = describeInstancesRequest.getReservations();
      Set<Instance> instances = new HashSet<Instance>();

      log.info("Reserves size: {}" , reservations.size());
      for (Reservation reservation : reservations) {
        instances.addAll(reservation.getInstances());
      }

      log.info("You have  {}  Amazon EC2 instance(s) running.", instances.size() );
    } catch (AmazonServiceException ase) {
      log.info("Caught Exception: {}" , ase.getMessage());
      log.info("Reponse Status Code: {}" , ase.getStatusCode());
      log.info("Error Code: {}" , ase.getErrorCode());
      log.info("Request ID: {}" , ase.getRequestId());
    }
  }

  public void runInstances() {
    RunInstancesRequest runInstancesRequest = new RunInstancesRequest();
    runInstancesRequest.withImageId(amiId).withInstanceType("t2.micro").withMinCount(1).withMaxCount(1).withKeyName(
        keyPairName).withSecurityGroups(securityGroupName);

    RunInstancesResult result = ec2.runInstances(runInstancesRequest);
  }

  public void test() {
    final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
    boolean done = false;

    DescribeInstancesRequest request = new DescribeInstancesRequest();
    while (!done) {
      DescribeInstancesResult response = ec2.describeInstances(request);

      for (Reservation reservation : response.getReservations()) {
        for (Instance instance : reservation.getInstances()) {
          log.info(
                "Found instance with id {},   AMI {},  type {},  state {}  and monitoring state {}",
              instance.getInstanceId(), instance.getImageId(), instance.getInstanceType(),
              instance.getState().getName(), instance.getMonitoring().getState());
        }
      }

      request.setNextToken(response.getNextToken());

      if (response.getNextToken() == null) {
        done = true;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    log.info("===========================================");
    log.info("Welcome to the AWS Java SDK!");
    log.info("===========================================");

    ExampleTestCode tester = new ExampleTestCode();
    tester.run();
    tester.test();
    //        tester.runInstances();
  }

}
