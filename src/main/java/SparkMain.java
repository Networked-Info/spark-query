
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.commons.lang.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import scala.Tuple2;

public class SparkMain {

	


//	private static final String CSV_PATH = "/resources/docMap.csv";
	private static final String CSV_PATH = "/docMap.csv";
	private static StringBuilder sb;
	private static HashMap<String, JavaPairRDD<String,Double[]>> rddMap;
	private static JavaPairRDD<String,Double[]> last;
	private static SparkConf sparkConf = new SparkConf().
				setAppName("Example Spark App").
				setMaster("local[*]"); // Delete this line when submitting to cluster
	private static JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	private static String idx_dir;

	public static void main(String[] args) throws JSONException, IOException {
		idx_dir = args[0];
	//	String docMap_dir = args[1];
		String wiki_dir = args[1];
	//	String out_dir = args[3];
		String query = args[2];
		query = query.replaceAll("^\"|\"$", ""); 
	//	final String queryRef = args[4];
		
		sb = new StringBuilder(query);
		rddMap = new HashMap<String, JavaPairRDD<String,Double[]>>();	
		
		
		recurseQuery();
	//	List<String> articles = getArticles(last, docMap_dir, wiki_dir);
		
		List<String[]> content = getSnippets(last, wiki_dir);
		
		for (String[] res: content) {
			System.out.println(Arrays.toString(res));
		}
		
//		List<String> snippets = articles.stream().map(a -> {
//			String firstTerm = queryRef.split(" ")[0].replaceAll("[^A-Za-z]", "");
//			int idx = a.indexOf(" " + firstTerm + " ");
//			if (idx < 0) {
//				return "";
//			}
//
//			String docId = a.split(",")[0];
//			int startIdx = idx - 30, endIdx = idx + 30;
//			if (startIdx <= 0) {
//				startIdx = idx;
//			}
//			if (endIdx >= a.length()) {
//				endIdx = idx;
//			}
//			String snippet = "..." + a.substring(startIdx, endIdx) + "...";
//			return docId + " -> " + snippet;
//		}).collect(Collectors.toList());
		
//		List<String> toRemove = new ArrayList<>();
//
//		for (String s : snippets) {
//			if (s.equals("")) {
//				toRemove.add(s);
//			}
//		}
//		for (String s : toRemove) {
//			snippets.remove(s);
//		}
//		sparkContext.parallelize(snippets).saveAsTextFile(out_dir);

//		last.saveAsTextFile(out_dir);

		sparkContext.close();
	}
	
	private static JavaPairRDD<String,Double[]> operateAND(JavaPairRDD<String,Double[]> set1, JavaPairRDD<String,Double[]> set2) {
        return set1.intersection(set2).reduceByKey((Function2<Double[], Double[], Double[]>) (a, b) -> new Double[]{a[0]+b[0], Math.min(a[1], b[1])});
    }
    
	private static JavaPairRDD<String,Double[]> operateOR(JavaPairRDD<String,Double[]> set1, JavaPairRDD<String,Double[]> set2) {
    	return set1.union(set2).reduceByKey((Function2<Double[], Double[], Double[]>) (a, b) -> new Double[]{a[0]+b[0], Math.min(a[1], b[1])});
    }
    
    private static JavaPairRDD<String,Double[]> operateSUB(JavaPairRDD<String,Double[]> set1, JavaPairRDD<String,Double[]> set2) {
        return set1.subtractByKey(set2);
    }	
    
    @SuppressWarnings({ "unchecked", "serial", "rawtypes" })
	private static JavaRDD<Integer[]> rank(JavaPairRDD<String,Double[]> set) {
    	return set.mapToPair(new PairFunction() {
			public Tuple2<Double,Integer[]> call(Object o) {
		    	Tuple2<String,Double[]> t = (Tuple2<String,Double[]>)o;
		        return new Tuple2(t._2[0], new Integer[]{Integer.parseInt(t._1), t._2[1].intValue()});
		      }
		  }).sortByKey(false).values();
    }


	// parses out the file id list
	private static List<Tuple2<String,Double[]>> parse(String target, JSONObject json) throws JSONException {
		List<Tuple2<String,Double[]>> files = new ArrayList<Tuple2<String,Double[]>>();
		JSONObject temp = new JSONObject(json.toString());
		JSONArray arr = temp.getJSONArray(target);
		int n = arr.length();
		double ratio = 100.0/n;
		
		for (int i = 0; i < n; i++) {
			JSONObject singleDoc = arr.getJSONObject(i);
			@SuppressWarnings("unchecked")
			Iterator<String> iter = singleDoc.keys();
			String docID = iter.next();
			JSONArray pos = (JSONArray) singleDoc.get(docID);
			double firstPos = Double.parseDouble(pos.getString(0));
			double score = (n-i)*ratio;
			files.add(new Tuple2<String,Double[]>(docID, new Double[]{score, firstPos}));
		}
		return files;
	}
	

	private static List<String[]> getSnippets(JavaPairRDD<String,Double[]> rawResult, String wiki_dir) throws IOException {
		JavaRDD<Integer[]> result = rank(rawResult);
		List<String[]> articles = new ArrayList<>();
		
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
		
		
		@SuppressWarnings("serial")
		List<String[]> snips = result.map(new Function<Integer[], String[]>() {
			public String[] call(Integer[] s) throws Exception {
			int docId = s[0];
			int idx = s[1];
			int csvId = map.floorEntry(docId).getValue();
			String filename = wiki_dir + "/" + "wiki" + csvId + ".csv"; // need modify when run on cluster
			return	sparkContext.textFile(filename)
				.filter(e -> e.split(",")[0].equals(Integer.toString(docId)))
				.map(e -> {
					String[] row = e.split(",");
					int startIdx = Math.max(idx - 30, 0);
					int endIdx = Math.min(idx + 30, row[3].length());
					return new String[]{row[1],row[2],row[3].substring(startIdx, endIdx)};
				}).first();
			}
		}).collect();
			
		for (String[] l : snips) 
			articles.add(l);
		
		return articles;
	}
	
	public static List<Tuple2<String,Double[]>> getList(String term) throws JSONException {
		String start = term.substring(0, 2).toLowerCase();
		JavaRDD<String> stringJavaRDD = sparkContext.textFile(idx_dir + "/" + start);
		JSONObject js = stringJavaRDD.map(new Function<String, JSONObject>() {

			private static final long serialVersionUID = 1L;
			public JSONObject call(String line) throws Exception {
				JSONObject json = new SerializableJson(line);
				return json;
			}
		}).filter(e -> e.has(term)).first();
		return parse(term, js);
		
	}
	
	
	public static void recurseQuery() throws JSONException {
		if (sb.length() == 0) return;
		if (sb.length() == 1) {
			String term = sb.toString();
			if (rddMap.get(term) != null) {
				last = rddMap.get(sb.toString());
				return;
			}
			List<Tuple2<String,Double[]>> fileList = getList(term);
			last = sparkContext.parallelizePairs(fileList);
			return;
		}
		if (StringUtils.countMatches(sb.toString(), "(") == 0) {
			String[] subTerms = sb.toString().split("[ ]");
			if (subTerms.length == 1) {
				JavaPairRDD<String,Double[]> rdd;
				String term = subTerms[0];
				if (rddMap.get(term) != null) {
					rdd = rddMap.get(term);
				} else {
					List<Tuple2<String,Double[]>> fileList = getList(term);
					rdd = sparkContext.parallelizePairs(fileList);
				}
				last = rdd;
				return;
			}
			String term1 = subTerms[0], op = subTerms[1], term2 = subTerms[2];
			JavaPairRDD<String,Double[]> rdd1, rdd2;

			if (rddMap.get(term1) != null) {
				rdd1 = rddMap.get(term1);
			} else {
				List<Tuple2<String,Double[]>> fileList = getList(term1);
				rdd1 = sparkContext.parallelizePairs(fileList);
			}
			if (rddMap.get(term2) != null) {
				rdd2 = rddMap.get(term2);
			} else {
				List<Tuple2<String,Double[]>> fileList = getList(term2);
				rdd2 = sparkContext.parallelizePairs(fileList);
			}
			
			switch (op) {
				case "and": last = operateAND(rdd1, rdd2); break;
				case "or": last = operateOR(rdd1, rdd2); break;
				case "not": last = operateSUB(rdd1, rdd2); break;
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
			JavaPairRDD<String,Double[]> rdd;
			String term = subTerms[0];
			if (rddMap.get(term) != null) {
				rdd = rddMap.get(term);
			} else {
				List<Tuple2<String,Double[]>> fileList = getList(term);
				rdd = sparkContext.parallelizePairs(fileList);
			}
			rddMap.put(term, rdd);
			sb.delete(openIdx, closeIdx + 1);
			sb.insert(openIdx, term);
		} else {
			JavaPairRDD<String,Double[]> rdd1, rdd2, mergedRDD;
			String term1 = subTerms[0], op = subTerms[1], term2 = subTerms[2];
			if (rddMap.get(term1) != null) {
				rdd1 = rddMap.get(term1);
			} else {
				List<Tuple2<String,Double[]>> fileList = getList(term1);
				rdd1 = sparkContext.parallelizePairs(fileList);
			}
			if (rddMap.get(term2) != null) {
				rdd2 = rddMap.get(term2);
			} else {
				List<Tuple2<String,Double[]>> fileList = getList(term2);
				rdd2 = sparkContext.parallelizePairs(fileList);
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