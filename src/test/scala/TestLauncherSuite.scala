import com.holdenkarau.spark.testing.{DataFrameSuiteBase}
import org.scalatest.FunSuite

class TestLauncherSuite extends FunSuite with DataFrameSuiteBase {
  test("TestLauncher") {

    val res = TestLauncher.launchHALSTest(spark, "~/TDT2.txt", 10, 4, 10, 10, 1)
    res.foreach(q => System.out.println(q));

    val res1 = TestLauncher.launchALSTest(spark, "~/TDT2.txt", 10, 4, 10, 1, true)
    res1.foreach(q => System.out.println(q));
  }
}
