import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class DocMap {
	
	public HashMap<Integer, Integer> map;
	
	public DocMap() throws IOException {
		System.out.println("here");
		BufferedReader br = new BufferedReader(new FileReader("docMap.csv"));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] info = line.split(",");
			int csvId = Integer.valueOf(info[0]);
			int docId = Integer.valueOf(info[1]);
			
			map.put(docId, csvId);
			System.out.println(docId);
		}
		br.close();
	}
}
