import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import javax.script.ScriptException;

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

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.*;

public class SparkStack {

	private static JavaPairRDD<String,Double[]> last;
	private static SparkConf sparkConf = new SparkConf().
			setAppName("Spark Query").
			setMaster("local[*]").set("spark.ui.showConsoleProgress", "false"); // Delete this line when submitting to cluster
	private static JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

	private static Deque<JavaPairRDD<String,Double[]>> rddStack = new ArrayDeque<JavaPairRDD<String,Double[]>>();
	private static Deque<String> opStack = new ArrayDeque<String>();

	private static String idx_dir;
	private static String wiki_dir;
	private static String query;


	public static void main(String[] args) throws JSONException, IOException, InterruptedException, ScriptException {

		idx_dir = args[0];
		wiki_dir = args[1];
		query = args[2];
		String[] queryTerms = query.replaceAll("^\"|\"$", "").split("[ ]"); 

		rddStack = new ArrayDeque<JavaPairRDD<String,Double[]>>();
		opStack = new ArrayDeque<String>();

		Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.OFF);

		iterateQuery(queryTerms);
		System.out.println("finished processing query");

		List<String[]> snippets = getSnippets(last, wiki_dir); 
		int len = 0;
		for (String s[] : snippets) {
			len++;
			System.out.println(Arrays.toString(s));
		}
		System.out.println(len);
		sparkContext.close();
	}

	private static void iterateQuery(String[] queryTerms) throws JSONException {
		System.out.println("Iterating queries...");

		for (String term : queryTerms) {
			switch (term) {
			case "(": break;
			case "and": opStack.push(term); break;
			case "or": opStack.push(term); break;
			case "!and": opStack.push(term); break;
			case ")": mergeRDDs(); break;
			default: createRDD(term); break;
			}
		}
		System.out.println("Finished iterating queries...");
		last = rddStack.pop();
	}

	private static void createRDD(String term) throws JSONException {
		System.out.println("Creating RDD: " + term);

		String path = idx_dir + "/" + term.substring(0, 2);
		System.out.println("PATH: " + path);
		JavaRDD<String> stringJavaRDD = sparkContext.textFile(path);
		JSONObject json = new SerializableJson(stringJavaRDD.filter(s -> 
		s.substring(StringUtils.ordinalIndexOf(s, "\"", 1) + 1, StringUtils.ordinalIndexOf(s, "\"", 2))
		.equals(term))
				.first());
		System.out.println(json.toString());
		List<Tuple2<String,Double[]>> fileList = parse(term, json);
		//		for (Integer i : fileList) {
		//			System.out.print(i + " ");
		//		}
		System.out.println();
		rddStack.push(sparkContext.parallelizePairs(fileList));
		System.out.println("New rddStack Size: " + rddStack.size());
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
			double score = (n-i)*ratio;
			String pos = (String)singleDoc.get(docID);
			double firstPos = Double.parseDouble(pos);
			files.add(new Tuple2<String,Double[]>(docID, new Double[]{score, firstPos}));
		}
		return files;
	}

	private static void mergeRDDs() {
		System.out.println("Merging RDDs...");
		JavaPairRDD<String,Double[]> rdd1;
		JavaPairRDD<String,Double[]> rdd2;
		JavaPairRDD<String,Double[]> mergedRDD;

		if (!opStack.isEmpty()) {
			rdd1 = rddStack.pop();
			rdd2 = rddStack.pop();

			switch (opStack.pop()) {
			case "and": mergedRDD = operateAND(rdd1, rdd2); break;
			case "or": mergedRDD = operateOR(rdd1, rdd2); break;
			case "!and": mergedRDD = operateSUB(rdd1, rdd2); break;
			default: mergedRDD = null; break;
			}
			rddStack.push(mergedRDD);
		}
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


	public static String getWikiFile(int wikiID) throws NumberFormatException, IOException {
		BufferedReader bf = new BufferedReader(new FileReader("/class/cs132/wiki_ranges.csv"));
		String line;
		while ((line = bf.readLine()) != null) {
			String[] entry = line.split(",");
			String wikiFile = entry[0];
			int start = Integer.parseInt(entry[1]);
			int stop = Integer.parseInt(entry[2]);
			if (wikiID >= start && wikiID <= stop) {
				bf.close();
				return wikiFile;
			}
		}
		bf.close();
		return "";
	}

	public static String getWikiArticle(int wikiID, String wikiFile) throws IOException {
		BufferedReader bf = new BufferedReader(new FileReader("/class/cs132/wiki_csv/" + wikiFile));
		String article;
		while ((article = bf.readLine()) != null) {
			try {
				int currID = Integer.parseInt(article.substring(0, article.indexOf(",")));
				if (currID == wikiID) {
					bf.close();
					return article;
				}

			} catch (NumberFormatException e) {
				System.out.println(e);
			}
		}
		bf.close();
		return "";
	}
	
	
	private static List<String[]> getSnippets(JavaPairRDD<String,Double[]> rawResult, String wiki_dir) {
		JavaRDD<Integer[]> result = rank(rawResult);

		List<String[]> snips = result.map(new Function<Integer[], String[]>() {
			private static final long serialVersionUID = 1L;

			public String[] call(Integer[] s) throws Exception {
				int docId = s[0];
				int idx = s[1];
				String wikiFile = getWikiFile(docId);
			//	System.out.println("WOWOWOWOWOW: " + wikiFile);
				String article = getWikiArticle(docId, wikiFile);
				String[] row = article.split(",");
				int startIdx = Math.max(idx - 30, 0);
				int endIdx = Math.min(idx + 30, row[3].length());
				return new String[]{row[1],row[2],row[3].substring(startIdx, endIdx)};
			}
		}).take(25);

		return snips;
	}
}