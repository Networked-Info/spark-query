import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class SparkMain {
	public static final String FILENAME = "index";

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().
				setAppName("Example Spark App").
				setMaster("local[*]"); // Delete this line when submitting to cluster
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> stringJavaRDD = sparkContext.textFile(FILENAME);
		stringJavaRDD.map(new Function<String, JsonObject>() {
			private static final long serialVersionUID = 1L;
			public JsonObject call(String line) throws Exception {
				Gson gson = new Gson();
				JsonObject json = gson.fromJson(line, JsonObject.class);
				return json;
			}
		}).foreach(new VoidFunction<JsonObject>() {
			private static final long serialVersionUID = 1L;
			public void call(JsonObject json) {
				System.out.println(json.isJsonObject());
			}
		});
		sparkContext.close();
	}
}
