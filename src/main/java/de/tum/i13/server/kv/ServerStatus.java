package de.tum.i13.server.kv;

public enum ServerStatus {
  SUCCESS, UPDATE, ERROR;

  @Override
  public String toString() {
    return super.toString().toLowerCase();
  }

  /**
   * Used to make sure that both status are successful
   * 
   * @param s1
   * @param s2
   * @return ServerStatus.ERROR if one of them is a failure or ServerStatus.SUCCESS if both are
   *         successful (other results are not important for us)
   */
  public static ServerStatus maxValue(ServerStatus s1, ServerStatus s2) {
    if (s1 == null || s2 == null) {
      return null;
    }
    return s1.ordinal() > s2.ordinal() ? s1 : s2;
  }
}
