package de.tum.i13.server.ecs;

public class CloseLock {
  private static boolean locked = false;
  
  public synchronized void lock() {
    while (locked) {
      try {
        wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    locked = true;
  }
  
  public synchronized void unlock() {
    locked = false;
    notify();
  }
}
