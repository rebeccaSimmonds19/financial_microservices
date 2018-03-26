package radanalytics.eligibleclientfilter;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.websocket.common.frames.DataFrame;

/**
 * Filter clients which have not defaulted on payments
 * so the left over clients can be sent to the rules engine
 * the rules engine can then determine if they are eligible for a credit card
 *
 */
public class Filter {

    //constructor
    public Filter()
    {

    }

    public void filterData(String data)
    {
        //create a spark session

// Inside class
        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Name")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset df =spark.read().csv("UCI_Credit_Card.csv");
        //coloumn constraint
        Dataset result = df.where("PAY_0> 0 && PAY_2 > 0 && PAY_3 > 0 && PAY_4 > 0 && PAY_5 > 0 && PAY_6 >0");
        System.out.println("this is the result "+result);
    }
}
