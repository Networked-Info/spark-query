import java.io.Serializable;

import org.json.JSONException;
import org.json.JSONObject;


@SuppressWarnings("serial")
public class SerializableJson extends JSONObject implements Serializable {
	String s;

	public SerializableJson(String s) throws JSONException {
		super(s);		
		this.s = s;
	}
	
	public boolean has(String key) {
		return super.has(key);
	}
	
	public String toString() {
		return s;
	}
}
