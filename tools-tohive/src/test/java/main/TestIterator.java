package main;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TestIterator {
	private String currentValue; // 当前值
	private long length; // 当前数据位置
	private long max = 26;
	private Iterator<String> iterator;
	public Iterator<String> initIterator() {
		List<String> list = new ArrayList<String>();
		list.add("1");
		list.add("2");
		list.add("3");
		list.add("4");
		list.add("5");
		return list.iterator();
	}
	public void init() {
		this.iterator = initIterator();
	}
	public boolean nextValue() {
		if(this.iterator == null) {
			this.initIterator();
		}
		boolean hasNext = this.iterator.hasNext();
		if(!hasNext && (length < max)) {
			this.iterator = initIterator();
			hasNext = this.iterator.hasNext();
		}
		if(hasNext) {
			this.currentValue = this.iterator.next();
			this.length++;
		}
		return hasNext;
	}
	public Text getValue() {
		return new Text(this.currentValue);
	}
	public LongWritable getKey() {
		return new LongWritable(this.length);
	}
	
	public static void main(String[] args) {
		TestIterator it = new TestIterator();
		it.init();
		
		int i = 0;
		while(it.nextValue()) {
			System.err.println(i + " key : " + it.getKey() + ", value : " + it.getValue());
			i++;
		}
	}
	
}
