package light;

/**
 * 从文件中抽取文本内容
 * @author r2space@gmail.com
 */

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;

public class Extractor {

	public static final String CONTENTS = "Contents";

	public static void main(String[] args) throws Exception {
		parse("/Users/lilin/Desktop/ETL説明.xlsx");
		parse("/Users/lilin/Desktop/Documents/CMA/2013年第五期CMA招生简章.pdf");
	}

	/**
	 * 提取指定文件的纯文本内容，返回结果里包含文件的Metadata信息。文本内容用Key:Contents来保存。
	 *
	 * @param file
	 * @return
	 * @throws Exception
	 */
	public static Map<String, String> parse(String file) throws Exception {

		Map<String, String> result = new HashMap<String, String>();
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		InputStream is = null;

		try {
			Metadata metadata = new Metadata();

			// read file
			is = TikaInputStream.get(getURL(file), metadata);

			Detector detector = new DefaultDetector();
			Parser parser = new AutoDetectParser(detector);
			parser.parse(is, new BodyContentHandler(out), metadata, new ParseContext());

			// add metadata
			for (String name : metadata.names()) {
				String value = metadata.get(name);
				if (value != null) {
					result.put(name, value);
				}
			}

			result.put(CONTENTS, new String(out.toByteArray(), "UTF-8"));
		} finally {
			if (is != null) {
				is.close();
			}
		}

		return result;
	}

	protected static URL getURL(String urlstring) throws MalformedURLException {
		URL url;

		File file = new File(urlstring);
		if (file.isFile()) {
			url = file.toURI().toURL();
		} else {
			url = new URL(urlstring);
		}

		return url;
	}
}
