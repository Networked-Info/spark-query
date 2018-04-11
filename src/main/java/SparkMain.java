import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SparkMain {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().
				setAppName("Example Spark App").
				setMaster("local[*]"); // Delete this line when submitting to cluster
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> stringJavaRDD = sparkContext.textFile("index");
		JavaRDD<JsonObject> jsonRDD = stringJavaRDD.map(new Function<String, JsonObject>() {
			private static final long serialVersionUID = 1L;
			public JsonObject call(String line) throws Exception {
				Gson gson = new Gson();
				JsonObject json = gson.fromJson(line, JsonObject.class);
				return json;
			}
		});
		JavaRDD<String> rdd1 = sparkContext.parallelize(parse("abdel",jsonRDD.filter(e -> e.get("abdel")!=null).collect().get(0)));
		JavaRDD<String> rdd2 = sparkContext.parallelize(parse("abdallah",jsonRDD.filter(e -> e.get("abdallah")!=null).collect().get(0)));
		rdd1.union(rdd2).saveAsTextFile("output");
		sparkContext.close();
	}

	private static List<String> parse(String target, JsonObject json) {
		List<String> files = new ArrayList<String>();
		JsonArray arr = json.get(target).getAsJsonArray();
		Iterator<JsonElement> i = arr.iterator();
		while (i.hasNext()) {
			Iterator<Entry<String, JsonElement>> inner = i.next().getAsJsonObject().entrySet().iterator();
			while (inner.hasNext()) {
				files.add(inner.next().getKey());
			}
		}
		return files;
	}
}