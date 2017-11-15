import com.holdenkarau.spark.testing.{DataFrameSuiteBase}
import org.scalatest.FunSuite

class TestLauncherSuite extends FunSuite with DataFrameSuiteBase {
  test("TestLauncher") {

    //TestLauncher.launchALSTest(spark, "/Users/krzysztoffonal/ratings.csv", 10, 2, 50, 2)
    TestLauncher.launchHALSTest(spark, "/Users/krzysztoffonal/ratings.csv", 10, 2, 50, 10, 2)
  }
}
