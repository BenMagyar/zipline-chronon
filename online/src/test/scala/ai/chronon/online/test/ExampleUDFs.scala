package ai.chronon.online.test

// A couple of toy UDFs to help test Hive UDF registration in CatalystUtil
class Minus_Two extends org.apache.hadoop.hive.ql.exec.UDF {
  def evaluate(x: Integer): Integer = {
    x - 2
  }
}

class Cat_Str extends org.apache.hadoop.hive.ql.exec.UDF {
  def evaluate(x: String): String = {
    x + "123"
  }
}

// Mirrors the runtime class lookup performed by externally loaded Hive UDFs. Keeping the
// lookup inside evaluate makes a reused worker's context-classloader lifecycle deterministic.
class Require_Context_Class_Loader extends org.apache.hadoop.hive.ql.exec.UDF {
  def evaluate(x: String): String = {
    Class.forName(getClass.getName, false, Thread.currentThread().getContextClassLoader)
    x
  }
}
