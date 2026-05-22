package com.backpackr.de;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.backpackr.de.util.SqlLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SessionizationTest {

    private static SparkSession spark;

    @BeforeAll
    static void setup() {
        spark = SparkSession.builder()
                .appName("sessionization-test")
                .master("local[2]")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.ansi.enabled", "true")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
    }

    @AfterAll
    static void teardown() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static Row ev(String time, String type, String session) {
        return RowFactory.create(time, type, "1", "1", null, null, "1.0", "564068124", session);
    }

    private Dataset<Row> sessionize(List<Row> rows) {
        StructType schema = new StructType()
                .add("event_time", DataTypes.StringType)
                .add("event_type", DataTypes.StringType)
                .add("product_id", DataTypes.StringType)
                .add("category_id", DataTypes.StringType)
                .add("category_code", DataTypes.StringType)
                .add("brand", DataTypes.StringType)
                .add("price", DataTypes.StringType)
                .add("user_id", DataTypes.StringType)
                .add("user_session", DataTypes.StringType);
        spark.createDataFrame(rows, schema).createOrReplaceTempView("raw_events");
        String sql = SqlLoader.loadAndBind("02_sessionize_events.sql", Map.of("gap_seconds", "300"));
        return spark.sql(sql);
    }

    @Test
    void splitsIntoThreeSessionsByGap() {
        Dataset<Row> result = sessionize(Arrays.asList(
                ev("2019-10-25 14:17:08 UTC", "view", "e7c7-24bc"),
                ev("2019-10-25 15:16:02 UTC", "view", "e7c7-24bc"),
                ev("2019-10-25 15:17:14 UTC", "purchase", "e7c7-24bc"),
                ev("2019-10-25 15:18:08 UTC", "view", "e7c7-24bc"),
                ev("2019-10-25 17:40:13 UTC", "view", "11a8-5b96")));

        assertEquals(3, result.select("session_id").distinct().count());
    }

    @Test
    void sessionIdUsesUtcStartEpoch() {
        Dataset<Row> result = sessionize(Arrays.asList(
                ev("2019-10-25 14:17:08 UTC", "view", "e7c7-24bc")));

        assertEquals("564068124_1572013028", result.select("session_id").first().getString(0));
    }

    @Test
    void sameOriginalSessionSplitsWhenGapExceeded() {
        Dataset<Row> result = sessionize(Arrays.asList(
                ev("2019-10-25 14:17:08 UTC", "view", "e7c7-24bc"),
                ev("2019-10-25 15:16:02 UTC", "view", "e7c7-24bc")));

        assertEquals(2, result.select("session_id").distinct().count());
    }

    @Test
    void kstPartitionCrossesDayBoundary() {
        Dataset<Row> result = sessionize(Arrays.asList(
                ev("2019-10-25 14:17:08 UTC", "view", "e7c7-24bc"),
                ev("2019-10-25 15:16:02 UTC", "view", "e7c7-24bc")));

        List<Row> dates = result.selectExpr("cast(event_date_kst as string) as d")
                .distinct().orderBy("d").collectAsList();
        assertEquals(2, dates.size());
        assertEquals("2019-10-25", dates.get(0).getString(0));
        assertEquals("2019-10-26", dates.get(1).getString(0));
    }
}
