package hongs.study.cascading;

import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.tap.hadoop.Lfs;
import cascading.tap.local.FileTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * Cascading 基础学习笔记
 * 执行举例:
 * java -cp target/hongs-study-cascading-0.0.1-jar-with-dependencies.jar hongs.study.cascading.Basic words.txt words_ext.txt words_out.txt
 * 要在 Hadoop 上运行请在参数后面加 hadoop
 * @author Hong
 */
public class Basic {

	/**
	 * Function 举例
	 * 将文本中的单词拆出来, 如果每行一个单词可用:
	 * new RegexParser(new Fields("word"), "^(.*)$", new int[] {1});
	 * @author Hong
	 */
	private static class WordsParser extends BaseOperation implements Function {

		// 拆出里面的单词
		Pattern parser = Pattern.compile("\\w+");
		
		public WordsParser() {
			super(1, new Fields("word"));
		}
		
		public WordsParser(Fields fieldDeclaration) {
			super(1, fieldDeclaration);
		}
		
		public void operate(FlowProcess proc, FunctionCall call) {
			TupleEntryCollector coll = call.getOutputCollector();
			TupleEntry args = call.getArguments();
			
			String line = args.getString("line");
			Matcher mat = parser.matcher(line);
			while (mat.find()) {
				String word = mat.group();
				coll.add(new Tuple(word));
			}
		}

	}
	
	/**
	 * Buffer 举例
	 * 注意! 此类不能用来统计全量或计算唯一, Count 类似的操作请参考:
	 * https://github.com/Cascading/cascading/blob/2.5/cascading-core/src/main/java/cascading/operation/aggregator/Count.java
	 * @author Hong
	 */
	private static class WordsBuffer extends BaseOperation implements Buffer {

		public WordsBuffer() {
			super(1, new Fields("line"));
		}
		
		public WordsBuffer(Fields fieldDeclaration) {
			super(1, fieldDeclaration);
		}
		
		public void operate(FlowProcess proc, BufferCall call) {
			TupleEntryCollector coll = call.getOutputCollector();
			Iterator iter = call.getArgumentsIterator();
			
			// 啥事也不干，就遍历一遍，输出来看看
			// 实际开发时可以做过滤及行转列等操作
			StringBuilder sb = new StringBuilder();
			while (iter.hasNext()) {
				TupleEntry args = (TupleEntry)iter.next();
				String word = args.getString("word");
				coll.add(new Tuple(word));
				sb.append(word).append("\t");
			}
			System.out.println(sb.toString().trim());
		}
		
	}
	
	/**
	 * SubAssermbly 举例
	 * 可将多个管道操作封装在一个 SubAssermbly 里
	 * @author Hong
	 */
	private static class WordsPipes extends SubAssembly {
		
		public WordsPipes(Pipe p1, Pipe p2) {
			Function parser;

			// 拆分单词
			parser = new WordsParser();
			// 一号管道, 读取单词
			p1 = new Each(p1, new Fields("line"), parser);
			// 测试一下自定义缓冲
			p1 = new GroupBy(p1, new Fields("word"));
			p1 = new Every(p1, new Fields("word"), new WordsBuffer());
			// 计算重复的单词数量
			p1 = new GroupBy(p1, new Fields("word"));
			p1 = new Every(p1, new Fields("word"), new Count(), new Fields("word", "count"));
			
			// 一行一条, 格式: 单词 扩展
			parser = new RegexParser(new Fields("word", "ext_words"), "^(.*?)\\s+(.*)$", new int[] {1, 2});
			// 二号管道, 读取扩展
			p2 = new Each(p2, new Fields("line"), parser);
			
			// 关联两个管道, 相同的字段为 word
			/**
			 * 关联分组输出格式:
			 * InnerJoin	[管道1字段],[管道2字段]
			 * LeftJoin		[管道1字段],[管道2字段(可能为null)]
			 * RightJoin	[管道1字段(可能为null)],[管道2字段]
			 * OuterJoin    [管道1字段(可能为null)],[管道2字段(可能为null)](不同为null)
			 */
			Pipe po = new CoGroup(
					  new Pipe[] {p1, p2},
					  new Fields[] {new Fields("word"), new Fields("word")},
					  new Fields("word1", "count", "word2", "ext_words"),
					  new InnerJoin()
					  );
			// 去除重复的 word 字段
			po = new Each(po, new Fields("count", "word1", "ext_words"), new Identity());
			
			this.setTails(po);
		}
		
	}
	
	/**
	 * 主函数
	 * 参数说明:
	 * 参数1 文本文件, 任意单词, 空格或行分割
	 * 参数2 扩展文件, 一行一条, 空格分割单词和扩展
	 * 参数3 输出文件或目录, 当第4个参数为'hadoop'时是输出目录, 否则输出到文件
	 * 参数4 可选, 为'hadoop'时将使用 hadoop 处理数据
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 3 && args.length != 4) {
			System.out.println("Params: fn extFn outFnOrDn ['hadoop']");
			return;
		}
		
		/** 构造管道 **/

		Pipe p1 = new Pipe("read1");
		Pipe p2 = new Pipe("read2");
		Pipe po = new WordsPipes(p1, p2);

		/** 接入数据 **/

		Properties pps = new Properties();
		AppProps.setApplicationJarClass(pps, Basic.class);
		
		Flow flow;
		
		if (args.length > 3 && "hadoop".equals(args[3])) {
			Tap in1 = new Lfs(new cascading.scheme.hadoop.TextLine(), args[0]);
			Tap in2 = new Lfs(new cascading.scheme.hadoop.TextLine(), args[1]);
			Tap out = new Lfs(new cascading.scheme.hadoop.TextLine(), args[2]);

			FlowDef def = new FlowDef()
				.setName("flow")
				.addSource(p1, in1)
				.addSource(p2, in2)
				.addTailSink(po, out);
			flow = new HadoopFlowConnector(pps).connect(def);
		}
		else {
			Tap in1 = new FileTap(new cascading.scheme.local.TextLine(), args[0]);
			Tap in2 = new FileTap(new cascading.scheme.local.TextLine(), args[1]);
			Tap out = new FileTap(new cascading.scheme.local.TextLine(), args[2]);

			FlowDef def = new FlowDef()
				.setName("flow")
				.addSource(p1, in1)
				.addSource(p2, in2)
				.addTailSink(po, out);
			flow = new LocalFlowConnector(pps).connect(def);
		}
		
		/** 开始执行 **/
		
		Cascade casc = new CascadeConnector().connect(flow);
		casc.complete();
	}

}
