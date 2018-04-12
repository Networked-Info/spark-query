import java.io.Serializable;

import org.json.JSONException;
import org.json.JSONObject;


public class SerializableJson extends JSONObject implements Serializable {

	public SerializableJson(String s) throws JSONException {
		super(s);		
	}
	
	public boolean has(String key) {
		return super.has(key);
	}
	
}
