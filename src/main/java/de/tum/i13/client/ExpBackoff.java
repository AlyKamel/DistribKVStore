package de.tum.i13.client;

import java.util.concurrent.ThreadLocalRandom;

public class ExpBackoff {
  private int attempts = 0;
  private String waitID;
  private final int BASE = 100;
  private final int CAP = 6000;
  private final int MAX_ATTEMPTS = 10;
  
  public void waitExp(String server) throws InterruptedException {
    if (waitID != server) { // waiting for a different server
      waitID = server;
      attempts = 0;
    }
    
    if (attempts > MAX_ATTEMPTS) {
      throw new InterruptedException("max attempt number exceeded");
    }
    
    int expattempts = BASE * (int) Math.pow(2, attempts++);
    int maxvalue = Integer.min(CAP + 1, expattempts);
    int sleeptime = ThreadLocalRandom.current().nextInt(0, maxvalue);
    Thread.sleep(sleeptime);
  }
}
