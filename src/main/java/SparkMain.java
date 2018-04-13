import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SparkMain {

	public static void main(String[] args) throws JSONException {
		String word1 = "aap";
		String word2 = "aard";

		SparkConf sparkConf = new SparkConf().
				setAppName("Example Spark App").
				setMaster("local[*]"); // Delete this line when submitting to cluster
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> stringJavaRDD = sparkContext.textFile("index");
		JavaRDD<JSONObject> jsonRDD = stringJavaRDD.map(new Function<String, JSONObject>() {
			private static final long serialVersionUID = 1L;
			public JSONObject call(String line) throws Exception {
				JSONObject json = new SerializableJson(line);
				return json;
			}
		});
		
		List<Integer> fileList1 = parse(word1,jsonRDD.filter(e -> e.has(word1)).first());
		List<Integer> fileList2 = parse(word2,jsonRDD.filter(e -> e.has(word2)).first());

		JavaRDD<Integer> rdd1 = sparkContext.parallelize(fileList1);
		JavaRDD<Integer> rdd2 = sparkContext.parallelize(fileList2);	
		rdd1.union(rdd2).distinct().sortBy(f -> f, true, 1).saveAsTextFile("unionoutput");
		rdd1.intersection(rdd2).sortBy(f -> f, true, 1).saveAsTextFile("interoutput");
		rdd1.subtract(rdd2).sortBy(f -> f, true, 1).saveAsTextFile("suboutput");
		sparkContext.close();
	}

	// parses out the file id list
	private static List<Integer> parse(String target, JSONObject json) throws JSONException {
		List<Integer> files = new ArrayList<Integer>();
		JSONObject temp = new JSONObject(json.toString());
		JSONArray arr = temp.getJSONArray(target);
		
		for (int i = 0; i < arr.length(); i++) {
			@SuppressWarnings("unchecked")
			Iterator<String> iter = arr.getJSONObject(i).keys();
			files.add(Integer.parseInt(iter.next()));
		}
		return files;
	}
}