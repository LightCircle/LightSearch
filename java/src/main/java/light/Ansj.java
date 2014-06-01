package light;

import java.util.ArrayList;
import java.util.List;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

public class Ansj {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		parse("此次更新，对tree-split中潜伏了n久的偏移量错误进行了修正。");
	}
	
	public static List<String> parse(String source) {
		System.out.println(source);
		List<String> result = new ArrayList<String>();
		
		for (Term term : ToAnalysis.parse(source)) {
			result.add(term.getName());
		}
		
		return result;
	}
	
}
