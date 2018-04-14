import java.util.Map;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

	
public class SparkMain {
	
	static SparkConf sparkConf = new SparkConf().
			setAppName("Example Spark App").
			setMaster("local[*]"); // Delete this line when submitting to cluster
	
	public static void main(String[] args) throws IOException {
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> stringJavaRDD = sparkContext.textFile("index");
		stringJavaRDD.map(new Function<String, JsonObject>() {
			private static final long serialVersionUID = 1L;
			public JsonObject call(String line) throws Exception {
				Gson gson = new Gson();
				JsonObject json = gson.fromJson(line, JsonObject.class);
				return json;
			}
		}).filter(e -> e.get("abdeen")!=null).
		
		foreach(new VoidFunction<JsonObject>() {
			private static final long serialVersionUID = 1L;
			public void call(JsonObject json) {
				System.out.println(json.get("abdeen"));
			}
		});
		
		String word1 = "aap";
		String word2 = "aard";
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
		rdd1.union(rdd2).distinct().sortBy(f -> f, true, 1).saveAsTextFile("output/unionoutput");
		rdd1.intersection(rdd2).sortBy(f -> f, true, 1).saveAsTextFile("output/interoutput");
		rdd1.subtract(rdd2).sortBy(f -> f, true, 1).saveAsTextFile("output/suboutput");
		
		
		sparkContext.close();
	}
	
	private static JavaRDD<Integer> operateAND(JavaRDD<Integer> set1, JavaRDD<Integer> set2) {
		return set1.intersection(set2).distinct().sortBy(f -> f, true, 1);
	}
	
	private static JavaRDD<Integer> operateOR(JavaRDD<Integer> set1, JavaRDD<Integer> set2) {
		return set1.union(set2).sortBy(f -> f, true, 1);
	}
	
	private static JavaRDD<Integer> operateSUB(JavaRDD<Integer> set1, JavaRDD<Integer> set2) {
		return set1.subtract(set2).sortBy(f -> f, true, 1);
	}
	
	private static String getSnippest(JavaRDD<Integer> result) throws IOException {
		Map<Integer, Integer> map = new HashMap<>();
		BufferedReader br = new BufferedReader(new FileReader("docMap.csv"));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] info = line.split(",");
			int csvId = Integer.valueOf(info[0]);
			int docId = Integer.valueOf(info[1]);
			
			map.put(docId, csvId);
			System.out.println(docId);
		}
		
		JavaSparkContext docContext = new JavaSparkContext(sparkConf);
		for (Integer docId : result.collect()) {
			
		}
		return "";
		
	}
}