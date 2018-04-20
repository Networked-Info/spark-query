import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.*;

public class SparkStack {

	private static JavaRDD<Integer> last;
	private static SparkConf sparkConf = new SparkConf().
				setAppName("Spark Query").
				setMaster("local[*]").set("spark.ui.showConsoleProgress", "false"); // Delete this line when submitting to cluster
	private static JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

	private static Deque<JavaRDD<Integer>> rddStack = new ArrayDeque<JavaRDD<Integer>>();
	private static Deque<String> opStack = new ArrayDeque<String>();
	
	private static String idx_dir;
	private static String wiki_dir;
	private static String query;
	
	
	public static void main(String[] args) throws JSONException, IOException, InterruptedException, ScriptException {
		
		idx_dir = args[0];
		wiki_dir = args[1];
		query = args[2];
		String[] queryTerms = query.replaceAll("^\"|\"$", "").split("[ ]"); 
		
		rddStack = new ArrayDeque<JavaRDD<Integer>>();
		opStack = new ArrayDeque<String>();
		
		Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.OFF);
		
		iterateQuery(queryTerms);
		System.out.println("finished processing query");

		List<String> articles = getArticles(last, wiki_dir);
		
		List<String> snippets = getSnippets(articles, queryTerms); 
		int len = 0;
		for (String s : snippets) {
			len++;
			System.out.println(s);
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
		List<Integer> fileList = parse(term, json);
		for (Integer i : fileList) {
			System.out.print(i + " ");
		}
		System.out.println();
		rddStack.push(sparkContext.parallelize(fileList));
		System.out.println("New rddStack Size: " + rddStack.size());
	}
	
	private static List<Integer> parse(String term, JSONObject json) throws JSONException {
		List<Integer> files = new ArrayList<Integer>();
		JSONObject temp = new JSONObject(json.toString());
		JSONArray arr = temp.getJSONArray(term);
		
		for (int i = 0; i < arr.length(); i++) {
			@SuppressWarnings("unchecked")
			Iterator<String> iter = arr.getJSONObject(i).keys();
			files.add(Integer.parseInt(iter.next()));
		}
		return files;
	}
	private static void mergeRDDs() {
		System.out.println("Merging RDDs...");
		JavaRDD<Integer> rdd1;
		JavaRDD<Integer> rdd2;
		JavaRDD<Integer> mergedRDD;

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
	
	private static List<String> getArticles(JavaRDD<Integer> result, String wiki_dir) throws IOException {
		System.out.println("Getting articles...");
		List<String> articles = new ArrayList<>();
		
		System.out.println("Count: " + result.count());
		result = operateOR(result, sparkContext.emptyRDD());
		List<Integer> docIDs = result.collect();
		articles = docIDs.stream().map(id -> {
			System.out.println("\nDOC ID: " + id);
			try {
				String wikiFile = getWikiFile(id);
				System.out.println("WOWOWOWOWOW: " + wikiFile);
				String article = getWikiArticle(id, wikiFile);
				System.out.println("COOOOOL: " + article.substring(0, 20));
				return article;
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return "";
//		}).collect(Collectors.toList());
		}).limit(25).collect(Collectors.toList());
		
		return articles;
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
	private static List<String> getSnippets(List<String> articles, String[] queryTerms) {
		
		List<String> snippets = articles.stream().map(a -> {

			String firstTerm = "";
			int i = 0;
			while (queryTerms[i].equals("(")) {
				i++;
				firstTerm = queryTerms[i];
			}
			int idx = a.indexOf(firstTerm);
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
		return snippets;
	}
}