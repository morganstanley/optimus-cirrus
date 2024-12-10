package com.ms.zookeeper.clientutils;

public class ZkEnv {
  public String name;
  public static ZkEnv valueOf(String arg) { return new ZkEnv(); }
  public static ZkEnv qa = new ZkEnv();
  public static ZkEnv dev = new ZkEnv();
  public static ZkEnv prod = new ZkEnv();
}
