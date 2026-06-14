# Distrifly: Enterprise Distributed System Replication Grid

This project is a Java-based simulation of a distributed system replication grid named **Distrifly**. It demonstrates the differences in performance, latency, and reliability between different data replication engines (Sequential vs. Eventual Consistency) and naming topology resolutions (Flat vs. Structured Naming).

## Features
* **Replication Engines:** Eventual Consistency (Optimistic) and Sequential Consistency (Strong).
* **Naming Service:** Flat Naming and Structured Naming (DHT/Tree-based).
* **Network Simulation:** Simulated packet drops, network latency, and node crashes.
* **Concurrency Testing:** Simulates multiple clients concurrently modifying distributed memory.

## Test Scenarios
The simulation is built to test **3 different deployment scenarios**. Every time you run a scenario, the engine will automatically execute **4 performance test configurations**.

**The 3 Scenarios:**
1. `NORMAL`: Standard network traffic (10 users, 100 operations).
2. `HIGH_CONCURRENCY`: Heavy simulated traffic storm (100 users, 1000 operations).
3. `NODE_FAILURE`: Simulates hardware crashes and recovery (a node drops offline, reboots, and synchronizes).

**The 4 Configurations (Tests) run in each scenario:**
1. Eventual Consistency + Structured Naming
2. Eventual Consistency + Flat Naming
3. Sequential Consistency + Structured Naming
4. Sequential Consistency + Flat Naming

## How to Run in NetBeans IDE
1. Open NetBeans IDE.
2. Go to **File > Open Project...**
3. Navigate to your downloaded folder and select the `DistributedSystem` project (it will have a coffee cup icon).
4. Click **Open Project**.
5. In the Projects pane on the left, expand **DistributedSystem > Source Packages > distributedsystem**.
6. Right-click on `DistributedSystem.java` and select **Run File** (or press `Shift + F6`).
7. The simulation will run, and the performance metrics will be printed in the NetBeans Output console at the bottom of the screen.

## How to Change the Scenario
Currently, the `main` method triggers the tests using the `NORMAL` scenario by default. To test the high concurrency or node failure scenarios, you need to change **one line** inside `DistributedSystem.java`.

1. Open `src/distributedsystem/DistributedSystem.java`.
2. Scroll down to the `runPerformanceTest` method (around **Line 52**).
3. Look for this exact block of code:

   ```java
   public static void runPerformanceTest(boolean useSequentialEngine, NameServer.NamingMode activeNaming) throws InterruptedException {
       // CONTROL PANEL FOR SYSTEM MODE TESTING
       Scenario activeScenario = Scenario.NORMAL;
   For High Concurrency: change it to Scenario activeScenario = Scenario.HIGH_CONCURRENCY;
   For Node Failure: change it to Scenario activeScenario = Scenario.NODE_FAILURE;
