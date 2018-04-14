import java.util.NavigableMap;
import java.util.TreeMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

	
public class SparkMain {
	
	static SparkConf sparkConf = new SparkConf().
			setAppName("Example Spark App").
			setMaster("local[*]"); // Delete this line when submitting to cluster
	
	static JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	
	public static void main(String[] args) throws IOException, JSONException {
		
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
		
		// test three methods
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
		
		JavaRDD<Integer> unionResult = rdd1.union(rdd2).distinct().sortBy(f -> f, true, 1);
		getSnippest(unionResult);
		
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
	
	private static List<String> getSnippest(JavaRDD<Integer> result) throws IOException {
		List<String> snippest = new ArrayList<>();
		
		// generate the document and csv map
		NavigableMap<Integer, Integer> map = new TreeMap<Integer,Integer>();
		BufferedReader br = new BufferedReader(new FileReader("docMap.csv"));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] info = line.split(",");
			int csvId = Integer.valueOf(info[0]);
			int startDocId = Integer.valueOf(info[1]);
			map.put(startDocId, csvId);
		}
		
		// for each document find corresponding csv file and output the document
		for (Integer docId : result.collect()) {
			int csvId = map.floorEntry(docId).getValue();
			String filename = "data/wiki_0" + csvId + ".csv"; // need modify when run on cluster
			
			sparkContext
				.textFile(filename)
				.filter(e -> e.split(",")[0].equals(Integer.toString(docId)))
				.foreach(new VoidFunction<String>(){
					private static final long serialVersionUID = 1L;
					public void call(String content) {
						snippest.add(content);
						System.out.println(content);
					}
				});
		}
		
		return snippest;
	}
}