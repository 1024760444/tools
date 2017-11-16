package com.yhaitao.common.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * 提供二进制海明码编码解码，以及纠错功能。
 * @author yhaitao
 *
 */
public class HmCodeUtils {
	/**
	 * 默认校验码。
	 */
	private final static Integer DEFAULT_CODE = -1;
	
	/**
	 * 输入字符串为空，返回的编码。
	 */
	private final static Integer CHECK_TEXT_NULL = -1;
	
	/**
	 * 输入字符串，海明码正确，返回的编码。
	 */
	private final static Integer CHECK_TEXT_GOOD = 0;
	
	/**
	 * 海明码编码方法。 
	 * 算法：输入数据text
	 * 1、维护一个编码结果列表codeList，按海明码编码顺序包含输入数据和校验码，校验码初始值为-1。
	 * 2、维护一个codeList中校验码下标的列表codeIndexList。
	 * 3、遍历输入数据text的每一个字符，做如下操作
	 * （1）如果编码结果列表的下一个序号是2的某次方(codeList.size() + 1)，则为校验码使用-1填充并codeIndexList记录下标。
	 * （2）持续执行（1）步骤，直到编码结果列表codeList的下一个序号不是2的某次方。
	 * （3）text当前字符（1或者0），以数字形式插入到编码结果列表codeList中。
	 * （4）将text当前字符在codeList中的下标，根据校验码下标的列表codeIndexList进行最少完整拆分，得到拆分下标（codeList的下班）集合splitIndexList。
	 * 例如：当前字符下标为11，codeIndexList内容依次为：1, 2, 4, 8；那么，将11拆分为： 8, 2, 1。
	 * （5）遍历拆分下标集合splitIndexList（准确的说这是下标+1的集合）
	 * （5.1）根据splitIndexList的内容codeIndex - 1，获取codeList的值（某一个校验码）。
	 * （5.2）设置codeList下标codeIndex - 1的值。如果原始值为-1，那么设置为text当前字符的数字；否则设置为原始值与text当前字符的数字的亦或的值。
	 * 4、将codeList根据下标顺序，组织成字符串，得到海明码。
	 * @param text 需要编码的二进制字符串
	 * @return 编码结果字符串
	 */
	public static String code(String text) {
		int text_size = text.length();
		List<Integer> codeList = new ArrayList<Integer>();       // 编码结果顺序列表
		List<Integer> codeIndexList = new ArrayList<Integer>();  // 校验码在codeList中的位置下标列表
		for(int index = 0; index < text_size; index++) {
			// 插入校验码
			while (isIntegerForDouble(Math.log((double) (codeList.size() + 1)) / Math.log((double) 2))) {
				codeList.add(DEFAULT_CODE);
				codeIndexList.add(codeList.size());
			}
			// 插入数据
			Integer valueOf = Integer.valueOf(String.valueOf(text.charAt(index)));
			codeList.add(valueOf);
			
			// 划分当前数据下标，使用校验码下标，拆分得到的codeList下标列表
			List<Integer> splitIndexList = split(codeIndexList, codeList.size());
			
			// 修正校验码，亦或运算
			for(Integer codeIndex : splitIndexList) {
				Integer integer = codeList.get(codeIndex - 1);
				codeList.set(codeIndex - 1, integer == -1 ? valueOf : integer^valueOf);
			}
		}
		
		// list组装成字符串
		StringBuffer sb = new StringBuffer();
		for(Integer i : codeList) {
			sb.append(i);
		}
		return sb.toString();
	}
	
	/**
	 * 校验编码是否传错。 
	 * 算法：输入海明码text
	 * 1、text非空判断，为空返回-1。
	 * 2、维护一个校验码列表checkCodeList，依次保存遍历到的校验码。
	 * 3、维护一个校验码下标列表codeIndexList，保存当前校验码字符在海明码字符串的位置。
	 * 4、遍历海明码text每一个字符
	 * （1）获取当前字符的数字curNum = Integer.valueOf(String.valueOf(text.charAt(index)));
	 * （2）如果当前下标+1是2的某次方。将当前数字curNum，放到checkCodeList中；当前序号index+1，放到codeIndexList中。
	 * （3）否则，将当前下标根据codeIndexList进行最少完整拆分，得到下标拆分集合splitIndexList。
	 * （4）遍历拆分下标集合splitIndexList（准确的说这是下标+1的集合）：splitIndex
	 * （4.1）获取校验码列表checkCodeList的下标checkCodeListIndex=（以2为底splitIndex的对数）。
	 * （4.2）设置校验码列表checkCodeList，下标为checkCodeListIndex的值为：当前值与curNum的亦或的值。
	 * 5、校验码列表checkCodeList看成是二进制数，下标是二进制为的阶数；将该二进制数转换为十进制数返回。
	 * @param text 海明码
	 * @return 错误位置。text为空或者null，返回-1；未错，返回0；错误，返回错误位置。
	 */
	public static int check(String text) {
		// 空判定
		if(StringUtils.isNull(text)) {
			return CHECK_TEXT_NULL;
		}
		// 遍历字符串，校验海明码
		List<Integer> checkCodeList = new ArrayList<Integer>(); // 校验码列表
		List<Integer> codeIndexList = new ArrayList<Integer>();  // 校验码在codeList中的位置下标列表
		int text_size = text.length();
		for(int index = 0; index < text_size; index++) {
			int curNum = Integer.valueOf(String.valueOf(text.charAt(index)));
			if(isIntegerForDouble(Math.log((double) (index + 1)) / Math.log((double) 2))) {
				// 对于2的指数次方上的数据节点，放到checkCodeList中；起在海明码中的下标放到codeIndexList中
				checkCodeList.add(curNum);
				codeIndexList.add(index + 1);
			} else {
				// 划分当前数据下标，使用校验码下标，拆分得到的codeList下标列表
				List<Integer> splitIndexList = split(codeIndexList, index + 1);
				for(Integer splitIndex : splitIndexList) {
					// 对于当前数据下标所有的拆分，起2的对数作为下标的checkCodeList 与当前数据亦或运算
					int checkCodeListIndex = (int) (Math.log((double) (splitIndex)) / Math.log((double) 2));
					checkCodeList.set(checkCodeListIndex, checkCodeList.get(checkCodeListIndex)^curNum);
				}
			}
		}
		// 将checkCodeList看成二进制数据，下标是起位数；转换为十进制数字
		int size = checkCodeList.size();
		int tenNumber = CHECK_TEXT_GOOD;
		for(int index = 0; index < size; index++) {
			tenNumber = tenNumber + (int) (checkCodeList.get(index) * Math.pow(2, index));
		}
		return tenNumber;
	}

	/**
	 * 不纠错解码方法。
	 * @param text 二进制字符串
	 * @return 解码后的字符串
	 */
	public static String decode(String text) {
		int text_size = text.length();
		StringBuffer sb = new StringBuffer();
		for(int index = 0; index < text_size; index++) {
			if(!isIntegerForDouble(Math.log((double) (index + 1)) / Math.log((double) 2))) {
				sb.append(String.valueOf(text.charAt(index)));
			}
		}
		return sb.toString();
	}
	
	/**
	 * 判断输入double数字是否是整数。
	 * @param obj double数字
	 * @return 是整数返回true，否则返回false。
	 */
	private static boolean isIntegerForDouble(double obj) {
		double eps = 1e-10; // 精度范围
		return obj - Math.floor(obj) < eps;
	}
	
	/**
	 * 数字划分。 例如在有序数组：1,2,4,8中，将数组11划分为：8,2,1。
	 * @param codeIndexList 有序数组
	 * @param index 需要划分的数字
	 * @return 划分序列
	 */
	private static List<Integer> split(List<Integer> codeIndexList, int codeIndex) {
		int indexSize = codeIndexList.size();
		List<Integer> resultIndexList = new ArrayList<Integer>();
		for(int index = indexSize -1; index >= 0; index--) {
			int temporary = codeIndex - codeIndexList.get(index);
			if(temporary >= 0) {
				codeIndex = temporary;
				resultIndexList.add(codeIndexList.get(index));
			}
		}
		return resultIndexList;
	}
}
