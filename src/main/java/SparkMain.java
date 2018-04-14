
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SparkMain {

	


//	private static final String CSV_PATH = "/resources/docMap.csv";
	private static final String CSV_PATH = "/docMap.csv";
	private static StringBuilder sb;
	private static HashMap<String, JavaRDD<Integer>> rddMap;
	private static JavaRDD<Integer> last;
	private static SparkConf sparkConf = new SparkConf().
				setAppName("Example Spark App").
				setMaster("local[*]"); // Delete this line when submitting to cluster
	private static JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	private static JavaRDD<JSONObject> jsonRDD;

	public static void main(String[] args) throws JSONException, IOException {
		String idx_dir = args[0];
		String docMap_dir = args[1];
		String wiki_dir = args[2];
		String out_dir = args[3];
		String query = args[4];
		query = query.replaceAll("^\"|\"$", ""); 
		final String queryRef = args[4];
		
		sb = new StringBuilder(query);
		rddMap = new HashMap<String, JavaRDD<Integer>>();	


		JavaRDD<String> stringJavaRDD = sparkContext.textFile(idx_dir + "/part*");
		jsonRDD = stringJavaRDD.map(new Function<String, JSONObject>() {

			private static final long serialVersionUID = 1L;
			public JSONObject call(String line) throws Exception {
				JSONObject json = new SerializableJson(line);
				return json;
			}
		});
		
		
		recurseQuery();
		List<String> articles = getArticles(last, docMap_dir, wiki_dir);
		
		List<String> snippets = articles.stream().map(a -> {
			String firstTerm = queryRef.split(" ")[0].replaceAll("[^A-Za-z]", "");
			int idx = a.indexOf(" " + firstTerm + " ");
			if (idx < 0) {
				return "";
			}

			String docId = a.split(",")[0];
			int startIdx = idx - 30, endIdx = idx + 30;
			if (startIdx <= 0) {
				startIdx = idx;
			}
			if (endIdx >= a.length()) {
				endIdx = idx;
			}
			String snippet = "..." + a.substring(startIdx, endIdx) + "...";
			return docId + " -> " + snippet;
		}).collect(Collectors.toList());
		
		List<String> toRemove = new ArrayList<>();

		for (String s : snippets) {
			if (s.equals("")) {
				toRemove.add(s);
			}
		}
		for (String s : toRemove) {
			snippets.remove(s);
		}
		sparkContext.parallelize(snippets).saveAsTextFile(out_dir);

//		last.saveAsTextFile(out_dir);

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
	

	private static List<String> getArticles(JavaRDD<Integer> result, String docMap_dir, String wiki_dir) throws IOException {
		List<String> articles = new ArrayList<>();
		
		// generate the document and csv map
		NavigableMap<Integer, Integer> map = new TreeMap<Integer,Integer>();
		InputStreamReader isr = new InputStreamReader(SparkMain.class.getResourceAsStream(CSV_PATH));
		BufferedReader br = new BufferedReader(isr);
//		BufferedReader br = new BufferedReader(new FileReader(docMap_dir));

		String line = "";
		while ((line = br.readLine()) != null) {
			String[] info = line.split(",");
			int csvId = Integer.valueOf(info[0]);
			int startDocId = Integer.valueOf(info[1]);
			map.put(startDocId, csvId);
		}
		br.close();
		
		
		List<List<String>> snips = result.map(new Function<Integer, List<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public List<String> call(Integer docId) throws Exception {
			int csvId = map.floorEntry(docId).getValue();
			String filename = wiki_dir + "/" + "wiki" + csvId + ".csv"; // need modify when run on cluster
			return	sparkContext
				.textFile(filename)
				.filter(e -> e.split(",")[0].equals(Integer.toString(docId))).collect();
			}
		}).collect();
		
		for (List<String> l : snips) {
			for (String s : l) {
				articles.add(s);
			}
		}
		
		return articles;
	}
	
	public static void recurseQuery() throws JSONException {
		if (sb.length() == 0) return;
		if (sb.length() == 1) {
			String term = sb.toString();
			if (rddMap.get(term) != null) {
				last = rddMap.get(sb.toString());
				return;
			}
			List<Integer> fileList = parse(term,jsonRDD.filter(e -> e.has(term)).first());
			last = sparkContext.parallelize(fileList);
			return;
		}
		if (StringUtils.countMatches(sb.toString(), "(") == 0) {
			String[] subTerms = sb.toString().split("[ ]");
			if (subTerms.length == 1) {
				JavaRDD<Integer> rdd;
				String term = subTerms[0];
				if (rddMap.get(term) != null) {
					rdd = rddMap.get(term);
				} else {
					List<Integer> fileList = parse(term, jsonRDD.filter(e -> e.has(term)).first());
					rdd = sparkContext.parallelize(fileList);
				}
				last = rdd;
				return;
			}
			String term1 = subTerms[0], op = subTerms[1], term2 = subTerms[2];
			JavaRDD<Integer> rdd1, rdd2;

			if (rddMap.get(term1) != null) {
				rdd1 = rddMap.get(term1);
			} else {
				List<Integer> fileList = parse(term1, jsonRDD.filter(e -> e.has(term1)).first());
				rdd1 = sparkContext.parallelize(fileList);
			}
			if (rddMap.get(term2) != null) {
				rdd2 = rddMap.get(term2);
			} else {
				List<Integer> fileList = parse(term2, jsonRDD.filter(e -> e.has(term2)).first());
				rdd2 = sparkContext.parallelize(fileList);
			}
			
			switch (op) {
				case "and": last = operateAND(rdd1, rdd2); break;
				case "or": last = operateOR(rdd1, rdd2); break;
			}
			return;
		}

		int closeIdx = 0;
		while (sb.charAt(closeIdx) != ')') {
			closeIdx++;
		}
		int openIdx = closeIdx;
		while (sb.charAt(openIdx) != '(') {
			openIdx--;
		}
		String clause = sb.substring(openIdx + 1, closeIdx);
		String[] subTerms = clause.split("[ ]");
		if (subTerms.length == 1) {
			JavaRDD<Integer> rdd;
			String term = subTerms[0];
			if (rddMap.get(term) != null) {
				rdd = rddMap.get(term);
			} else {
				List<Integer> fileList = parse(term, jsonRDD.filter(e -> e.has(term)).first());
				rdd = sparkContext.parallelize(fileList);
			}
			rddMap.put(term, rdd);
			sb.delete(openIdx, closeIdx + 1);
			sb.insert(openIdx, term);
		} else {
			JavaRDD<Integer> rdd1, rdd2, mergedRDD;
			String term1 = subTerms[0], op = subTerms[1], term2 = subTerms[2];
			if (rddMap.get(term1) != null) {
				rdd1 = rddMap.get(term1);
			} else {
				List<Integer> fileList = parse(term1, jsonRDD.filter(e -> e.has(term1)).first());
				rdd1 = sparkContext.parallelize(fileList);
			}
			if (rddMap.get(term2) != null) {
				rdd2 = rddMap.get(term2);
			} else {
				List<Integer> fileList = parse(term2, jsonRDD.filter(e -> e.has(term2)).first());
				rdd2 = sparkContext.parallelize(fileList);
			}
			
			switch (op) {
				case "and": mergedRDD = operateAND(rdd1, rdd2); break;
				case "or": mergedRDD = operateOR(rdd1, rdd2); break;
				default: mergedRDD = null;
			}
			String newTerm = term1 + op + term2;
			rddMap.remove(term1); rddMap.remove(term2);
			rddMap.put(newTerm, mergedRDD);
			sb.delete(openIdx, closeIdx + 1);
			sb.insert(openIdx, newTerm);
		}
		recurseQuery();
	}	
}