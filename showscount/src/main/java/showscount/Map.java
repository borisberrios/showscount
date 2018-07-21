package showscount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object, Text, Text, LongWritable> {

	private final static int TWEET_FIELD = 17;

	private final HashMap<String, Long> showsSet;
	private List<String> bands;
	private Pattern filter;

	public Map() throws IOException {

		showsSet = new HashMap<String, Long>();

		if (bands == null) {
			bands = getBandList();
		}

	}

	private List<String> getBandList() throws IOException {

		InputStream is = Map.class.getClassLoader().getResourceAsStream("shows.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

		String line = null;
		List<String> shows = new ArrayList<String>();
		while ((line = br.readLine()) != null) {
			shows.add(line.trim().toLowerCase());
		}

		br.close();

		return shows;
	}


	@Override
	public void setup(Context context) throws IOException {
		Configuration configuration = context.getConfiguration();

		String strFileter = configuration.get("filter");
		if (strFileter != null) {
			filter = Pattern.compile(strFileter);
		}

	}

	private void search(String tweet) {

		for (String band : bands) {

			if (tweet.contains(band)) {

				// se van acumulando los matchs
				if (showsSet.containsKey(band)) {
					showsSet.put(band, showsSet.get(band) + 1L);
				} else {
					showsSet.put(band, 1L);
				}

			}
		}

	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		if (value == null || key == null) {
			return;
		}

		String line = value.toString();
		String[] fields = line.split("\t");

		if (fields.length == TWEET_FIELD) {
			String tweet = fields[TWEET_FIELD - 1].toLowerCase();

			if (filter != null) {
				Matcher matcher = filter.matcher(tweet);
				if (matcher.find()) {
					search(tweet);
				}
			} else {
				search(tweet);
			}

		}

	}

	/**
	 * finalmente todo las coincidencias de bandas en tweets que fueron acumuladas
	 * se comunican para que puedan ir al reduce
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Text key = new Text();
		LongWritable value = new LongWritable();
		for (Entry<String, Long> entry : showsSet.entrySet()) {
			key.set(entry.getKey());
			value.set(entry.getValue());
			context.write(key, value);
		}
	}

}