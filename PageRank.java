

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.CharacterCodingException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class PageRank {
	enum NCounterEnum {
		NVALUE;
	}

	/**
	 * This class will parse the wiki dump file
	 * 
	 */
	public static class LinkerMap extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		Set<String> eliminateCopies;


		@Override
		public void map(LongWritable arg0, Text value,
				OutputCollector<Text, Text> output, Reporter arg3) throws IOException {
			String line = value.toString();
			eliminateCopies = new HashSet<String>();
			InputSource file = new InputSource(new StringReader(line));

			DocumentBuilderFactory docFact = DocumentBuilderFactory.newInstance();
			DocumentBuilder db;
			try {
				db = docFact.newDocumentBuilder();
				Document docFile = db.parse(file);
				XPathFactory xPFact = XPathFactory.newInstance();
				XPath xpath = xPFact.newXPath();

				String pageTitle = xpath.evaluate("/page/title", docFile);
				pageTitle = pageTitle.replaceAll(" ", "_");
				String hRef = xpath.evaluate("/page/revision/text", docFile);
				Pattern expression = Pattern.compile("\\[\\[(.+?)\\]\\]");
				Matcher matchVar = expression.matcher(hRef);


				Text tVal = new Text(pageTitle);

				output.collect(tVal, new Text("!"));
				while (matchVar.find()) {

					String quote = matchVar.group();
					quote = quote.replace("[[", "");
					quote = quote.replace("]]", "");
					try {
						quote = quote.split("\\|")[0].trim();

						word.set(quote);
						if(checkIfLinkIsValid(quote)) {
							if(!eliminateCopies.contains(quote)) {
								quote = quote.replaceAll(" ", "_");
								eliminateCopies.add(quote);
								output.collect(new Text(quote), tVal);    
							}
						}
					}catch(ArrayIndexOutOfBoundsException e) {
						e.printStackTrace();
					}

				}    
				eliminateCopies.clear();

			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (XPathExpressionException e) {
				e.printStackTrace();
			}
		}
	}    


	private static boolean checkIfLinkIsValid(String quote) {
		if ( quote.contains("{") || quote.contains("}") || 
				quote.contains("<") || quote.contains(">") || quote.contains("#")){
			return false;
		} else if ( quote.toLowerCase().contains("image:")){
			return false;
		} else if ( quote.toLowerCase().contains("file:")){
			return false;
		}else {
			return true;
		}
	}

	public static class TickLinkReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter arg3)
						throws IOException {
			Set<String> hrefSet = new HashSet<String>();
			boolean tick =  false;
			while ( value.hasNext()){
				String val = value.next().toString();
				if ( val.equals("!") ){
					tick  = true;        
				}else {
					hrefSet.add(val);
				}
			}
			if ( tick ){
				output.collect(key, new Text("!"));
				for ( String index :  hrefSet){
					output.collect(new Text(index), key);
				}

			}else {
				hrefSet.clear();
			}
		}

	}
	
	public static class NullPageParser extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			int num = value.find("\t");
			String page = Text.decode(value.getBytes(), 0, num);
			//System.out.println(value.toString());
			String val = Text.decode(value.getBytes(), num+1,value.getLength()-(num+1));
			output.collect(new Text(page), new Text(val));
		}
	}


	public static class EmptyLinksReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter arg3)
						throws IOException {
			String hrefLinks = "";
			int count  = 0 ;
			while ( value.hasNext()){
				String val = value.next().toString();
				if(!val.equals("!")) {
					if ( count == 0 ) {
						hrefLinks = val;
					}
					else{
						hrefLinks = hrefLinks + "\t" + val;
					}
					count++;
				}
			}
			//System.out.println(key.toString()+"\t"+"1.0\t"+hrefLinks);
			output.collect(key, new Text(hrefLinks));
		}

	}


	public static class HrefOutParser extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output,
				Reporter reporter) throws IOException {
			output.collect(new Text("N"),new LongWritable(1));
			//System.out.println("N "+1);
			reporter.incrCounter(NCounterEnum.NVALUE, 1);		
		}
	}

	public static class NodeReducer extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text, LongWritable>{
		long nodeVal =0;
		@Override
		public void reduce(Text key, Iterator<LongWritable> value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
						throws IOException {
			while(value.hasNext()){
				nodeVal = nodeVal + value.next().get() ;
				//	reporter.getCounter(NCounterEnum.NVALUE).increment(1);
			}
			output.collect(new Text("N ="), new LongWritable(nodeVal));
			//reporter.getCounter(NCounterEnum.NVALUE).increment(nValue);
		}
	}

	public static class IncomingLinks extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text>{
		long nodeVal = 0;
		int index= 0;
		public void configure(JobConf task) {
			// TODO Auto-generated method stub
			super.configure(task);
			nodeVal = task.getLong("NVALUE", 0);
			index = task.getInt("COUNT", 0);
			//System.out.println("index = "+index);
		}

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			String[] strArr = value.toString().split("\t");
			String header = strArr[0];
			Double pageRank = 1.0 / nodeVal;
			int i = 1 ;
			if(index > 0) {
				pageRank = Double.parseDouble(strArr[1]);
				i++;
			}
			int linkCounter = strArr.length-i;
			int initialize = i;
			String res = "";
			while ( i < strArr.length) {
				//System.out.println(value_string[i]+"\t "+pageRank+"\t"+title+"\t"+num_outlinks);
				output.collect(new Text(strArr[i]), 
						new Text(pageRank+"\t"+header+"\t"+linkCounter));
				if(i == initialize) {
					String val = strArr[i].replace("$%$", "");
					res = res+val;
				}else
					res = res+"\t"+strArr[i];

				i++;
			}   	
			output.collect(new Text(header), new Text("$%$"+res));		
		}
	}

	// Heres where we calculate the page rank.
	public static class RankReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text>{
		long nodeVal = 0;
		int counter= 0;
		@Override
		public void configure(JobConf task) {
			// TODO Auto-generated method stub
			super.configure(task);
			nodeVal = task.getLong("NVALUE", 0);
			counter = task.getInt("COUNT", 0);
			//System.out.println("count = "+count);
		}


		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			//nValue = reporter.getCounter(NCounterEnum.NVALUE).getValue();
			double updateRank =  0.15/nodeVal;
			String hrefStr = "";
			while ( value.hasNext()){
				Text txt = value.next();
				if ( !txt.toString().startsWith("$%$")){
					String[] rank_info = txt.toString().split("\t");
					double pageRank = Double.parseDouble(rank_info[0]);
					//if(count == 0) {
					//	pageRank = pageRank/nValue;
					//}
					double num_links = Double.parseDouble(rank_info[2]);
					updateRank = updateRank + (pageRank/num_links)*0.85;
				} else {
					hrefStr = txt.toString().replace("$%$", "");			
				}
			}

			output.collect(key, new Text(Double .toString(updateRank) +"\t"+hrefStr));
		}
	}

	// This mapper and its corresponding reducer is used to put the pageranks in 
	// descending order.
	public static class RankMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text> {

		double nodeVal = 0; 
		@Override
		public void configure(JobConf task) {

			super.configure(task);
			nodeVal = 5.0/(task.getLong("NVALUE", 0));
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, 
				Reporter arg3) throws IOException {

			String[] pageRankStrArr = fetchPageRankComb(value);
			Double parsePageRank = Double.parseDouble(pageRankStrArr[1]);
			Text page = new Text(pageRankStrArr[0]);
			Text rank = new Text(""+parsePageRank);
			if(parsePageRank > nodeVal) {
				output.collect(rank, page);
			}

		}

		private String[] fetchPageRankComb( Text txt) throws CharacterCodingException {

			String[] pageRankStrArr = new String[2];

			int recordPageVal = txt.find("\t");
			int recordRankVal = txt.find("\t", recordPageVal + 1);

			// no tab after rank (when there are no links)
			int terminate;
			if (recordRankVal == -1) {
				terminate = txt.getLength() - (recordPageVal + 1);
			} else {
				terminate = recordRankVal - (recordPageVal + 1);
			}
			pageRankStrArr[0] = Text.decode(txt.getBytes(), 0, recordPageVal);
			pageRankStrArr[1] = Text.decode(txt.getBytes(), recordPageVal + 1, terminate);

			return pageRankStrArr;
		}

	}

	public static class ClassifyByOrderSpec extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter arg3)
						throws IOException {

			while(value.hasNext()){

				output.collect(value.next(), key);
			}
		}

	}

	public static class DoubleComparator extends DoubleWritable.Comparator {

		@Override
		public int compare(byte[] byte_a, int s_a, int line_a, byte[] b2, int s_b, int line_b) {
			// TODO Auto-generated method stub
			return -super.compare(byte_a, s_a, line_a, b2, s_b, line_b);
		}
	}

	public static class SwitchComp extends WritableComparator {

		protected SwitchComp() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable w_a, WritableComparable w_b) {

			//consider only zone and day part of the key
			Text text_a = (Text) w_a;
			Text text_b = (Text) w_b;
			Double t1Item = Double.parseDouble(w_a.toString());
			Double t2Item = Double.parseDouble(w_b.toString());

			int cmpVal = t1Item.compareTo(t2Item);
			return -cmpVal;

		}
	}


	public static void main(String[] args) throws Exception {
		String ars1 = args[0];
		String ars2= args[1];
		
		//Start of Job1
		JobConf conf = new JobConf(PageRank.class);
		Path input = conf.getLocalPath("input");
		conf.setJobName("parse raw data");
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		conf.set("io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(LinkerMap.class);
		conf.setReducerClass(TickLinkReducer.class);
		conf.setNumReduceTasks(1);
		
		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);


		FileInputFormat.setInputPaths(conf, new Path(ars1));
		FileOutputFormat.setOutputPath(conf, new Path(ars2+"/result/intermediateoutput"));

		Job job1 = new Job(conf); 
		job1.waitForCompletion(true);
		
		
		JobConf conf2 = removeRedLinks(ars2+"/result/intermediateoutput", ars2+"/result/PageRank.outlink.out");
		Job job2 = new Job(conf2); 
		job2.waitForCompletion(true);
	 
		JobConf conf3 = addCountNJob(ars2+"/result/PageRank.outlink.out", ars2+"/result/PageRank.n.out");
		Job job3 = new Job(conf3); 
		job3.waitForCompletion(true);
		
		long counter = job3.getCounters().findCounter(NCounterEnum.NVALUE)
				.getValue();
		String intermediateOutput = "";
		for(int i = 0; i < 8 ; i++) {
			
			JobConf confIter = calculateRank(ars2+"/result/PageRank.outlink.out"+(i== 0?"":i), ars2+"/result/PageRank.outlink.out"+(i+1), counter, i);
			Job jobIter = new Job(confIter);
			intermediateOutput = ars2+"/result/PageRank.outlink.out"+(i+1);
			jobIter.waitForCompletion(true);
			
			if(i == 0) {
				JobConf orderConf = orderRank(intermediateOutput, ars2+"/result/PageRank.iter1.out", counter);
				Job orderJob = new Job(orderConf);
				orderJob.waitForCompletion(true);
			}
		}


		
		JobConf conf5 = orderRank(intermediateOutput, ars2+"/result/PageRank.iter8.out", counter);	
		Job job5 = new Job(conf5);
		job5.waitForCompletion(true);
	}

	private static JobConf removeRedLinks(String inputFile, String outputFile) throws IOException {   
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("count n");


		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(NullPageParser.class);
		conf.setReducerClass(EmptyLinksReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputFile));
		FileOutputFormat.setOutputPath(conf, new Path(outputFile));
		
		return conf;
	}

	private static JobConf addCountNJob(String inputFile, String outputFile) throws IOException {   
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("count n");


		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(HrefOutParser.class);
		conf.setReducerClass(NodeReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputFile));
		FileOutputFormat.setOutputPath(conf, new Path(outputFile));

		return conf;
	}

	private static JobConf calculateRank(String inputFile, String outputFile, long counter, int iterationCount) 
			throws IOException {   
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("calculate pagrank");
		conf.setLong("NVALUE", counter);
		conf.setInt("COUNT", iterationCount);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(IncomingLinks.class);
		conf.setReducerClass(RankReducer.class);
		//conf.setNumReduceTasks(1);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputFile));
		FileOutputFormat.setOutputPath(conf, new Path(outputFile));

		return conf;
	}

	private static JobConf orderRank(String inputFile, String outFile, long counter) 
			throws IOException {   
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("order values");
		conf.setLong("NVALUE", counter);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		

		conf.setMapperClass(RankMapper.class);
		conf.setReducerClass(ClassifyByOrderSpec.class);
		conf.setOutputKeyComparatorClass(SwitchComp.class);
		//conf.setNumReduceTasks(1);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inputFile));
		FileOutputFormat.setOutputPath(conf, new Path(outFile));

		return conf;
	}

}
