package com.yhaitao.common;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.analyzer.lucene.IKAnalyzer;

import com.yhaitao.common.utils.StringUtils;

/**
 * IK分词统计工具。
 * @author yhaitao
 *
 */
public class IKAnalyzerCollect {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(IKAnalyzerCollect.class);
	
	/**
	 * 默认权重
	 */
	public final static double DEFAULT_WEIGHT = 1.0;
	
	/**
	 * 分词器。
	 */
	private Analyzer analyzer;
	
	/**
	 * 初始化IK分词器。
	 */
	public IKAnalyzerCollect() {
		this.analyzer = new IKAnalyzer(true);
	}
	
	/**
	 * 对输入文本使用IK分词器分词， 统计词频。
	 * @param context 需要分词的文本
	 * @return 输入文本的词频统计
	 */
	public Map<String, Double> analyzer(String context, double weight) {
		// weight默认值， 最小为1.0
		weight = weight < DEFAULT_WEIGHT ? DEFAULT_WEIGHT : weight;
		
		// 构造文本输入流
		Map<String, Double> analyzerMap = new HashMap<String, Double>();
		StringReader reader = new StringReader(context);
		try {
			//根据文本输入流， 构造分词器。
			TokenStream tokenStream = analyzer.tokenStream("", reader);
			// 遍历分词数据
			CharTermAttribute term = tokenStream.getAttribute(CharTermAttribute.class);
			while (tokenStream.incrementToken()) {
				String key = term.toString();
				/** 去掉包含英文或者標點的字符串 **/
				if(StringUtils.hasEn(key) || key.length() <= 1 || key.length() >= 10) {
					// LOGGER.error("analyzer throw away key : {}." , key);
					continue;
				}
				/** 权重累加 **/
				if(analyzerMap.containsKey(key)) {
					analyzerMap.put(key, analyzerMap.get(key) + weight);
				} else {
					analyzerMap.put(key, weight);
				}
			}
		} catch (IOException e) {
			LOGGER.error("analyzer IO Exception : {}. " , e.getMessage());
		} finally {
			reader.close();
		} 
		return analyzerMap;
	}
	
	/**
	 * 关闭IK分词器。
	 */
	public void close() {
		this.analyzer.close();
	}
}
