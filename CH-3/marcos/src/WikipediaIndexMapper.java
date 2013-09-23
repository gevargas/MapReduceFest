import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WikipediaIndexMapper extends Mapper<Object, Text, Text, Text> {

	public static String getWikipediaURL(String text) {

		int idx = text.indexOf("\"http://en.wikipedia.org");
		if (idx == -1) {
			return null;
		}
		int idx_end = text.indexOf('"', idx + 1);

		if (idx_end == -1) {
			return null;
		}

		int idx_hash = text.indexOf('#', idx + 1);

		if (idx_hash != -1 && idx_hash < idx_end) {
			return text.substring(idx + 1, idx_hash);
		} else {
			return text.substring(idx + 1, idx_end);
		}
	}
	
	

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// Parse the input string into a nice map
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
				.toString());

		// Grab the necessary XML attributes
		String txt = parsed.get("Body");
		String posttype = parsed.get("PostTypeId");
		String row_id = parsed.get("Id");

		// if the body is null, or the post is a question (1), skip
		if (txt == null || (posttype != null && posttype.equals("1"))) {
			return;
		}
		txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
		String wikiURL = getWikipediaURL(txt);
		if (wikiURL !=null && row_id!=null)
			context.write(new Text(getWikipediaURL(txt)), new Text(row_id));
	}
}
