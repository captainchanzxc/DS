# Distributed System
This repository is MIT6.824 2018 Spring Distributed System Test. I have passed most part of the tests, which list below:  
* Lab 1: MapReduce
   * Part I (pass all tests)
   * Part II (pass all tests)
   * Part II (pass all tests)
* Lab 2: Raft 
   * Part 2A (pass all tests)
   * Part 2B (pass all tests)
   * Part 2C (not pass all tests, details described below)
* Lab 3: Fault-tolerant Key/Value Service
   * Part A (pass all tests)
   * Part B (pass all tests)
* Lab 4: Sharded Key/Value Service
   * Part A (pass all tests)
   * Part B (not pass all tests, details described below)
   
   Because I don't implement optimization of log replication which is described in the extended Raft paper (starting at the bottom of page 7 and top of page 8, marked by a gray line), I didn't Lab2 Part 2C last 3 unreliable tests. In fact, as described in Raft paper, this optimization is not necessary, because the situation in Lab2 Part 2C tests is not unlikely.
   
   Besides, I only passed 2/13 Lab4 Part B tests. I find the bugs but they are kind of complex to solve. When I have spare time, I will fix it.
   
   Finally, hope I will have spare time.
