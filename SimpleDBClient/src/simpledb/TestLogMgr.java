package simpledb;

import simpledb.server.SimpleDB;

/* This is a version of the StudentMajor program that
 * accesses the SimpleDB classes directly (instead of
 * connecting to it as a JDBC client).  You can run it
 * without having the server also run.
 *
 * These kind of programs are useful for debugging
 * your changes to the SimpleDB source code.
 */

public class TestLogMgr {
   public static void main(String[] args) {
      try {
         // analogous to the driver
         SimpleDB.init("studentdb");

         SimpleDB.logMgr().printLogPageBuffer();

      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
