/**  
* @Project: Algorithm
* @Title: BloomFilter.java
* @Package big.data
* @Description: 布隆过滤算法
* @author paul.wei2011@gmail.com
* @date Jul 9, 2013 10:26:27 AM
* @version V1.0  
*/

package com.gewara.util;

import java.io.Serializable;
import java.util.BitSet;

public class BloomFilter implements Serializable {
	private static final long serialVersionUID = 5829598197124113258L;
	private static final  int  DEFAULT_SIZE  =2 << 24 ; //2的24次方
    private static final  int [] seeds =new  int []{5, 7, 11 , 13 , 31 , 37 , 61};//构造不同因子，用于不同散列函数
    private BitSet bits= new  BitSet(DEFAULT_SIZE);
    private SimpleHash[]  func=new  SimpleHash[seeds.length];//对不同的数字构造不同Hash函数避免冲突
    private int size = 0;
    public BloomFilter() {
    	for (int i = 0; i < seeds.length; i++) {//初始化构造函数
			func[i] = new SimpleHash(DEFAULT_SIZE, seeds[i]);
		}
	}
    public void add(String value) {
    	if(!contains(value)){
    		size++;
    	}
		for (SimpleHash f : func) { //添加hash值映射到相应位
			bits.set(f.hash(value), true);
		}
	}
    public boolean contains(String value) {
		if (value == null) {
			return false;
		}
		boolean ret = true;
		for (SimpleHash f : func) {
			ret = ret && bits.get(f.hash(value));//按照添加时候hash函数计算是否true
		}
		return ret;
	}
    
    public int getSize(){
    	return size;
    }
    
	public static class SimpleHash  implements Serializable {
		private static final long serialVersionUID = 1286028965900771610L;
		private int cap;
    	private int seed;
    	public SimpleHash(int cap, int seed) {
    		this.cap = cap;
    		this.seed = seed;
    	}
    	public int hash(String value) {//hash值通过数字个数和数字散列后得到值
    		int result = 0;
    		int len = value.length();
    		for (int i = 0; i < len; i++) {
    			result = seed * result + value.charAt(i);
    		}
    		return (cap - 1) & result;
    	}
    }
	
    public static void main(String[] args) {
		String value = "honglin.wei@gewara.com";
		String value2 = "honglin.wei@gewara.com";
		BloomFilter filter = new BloomFilter();
		System.out.println(filter.contains(value));
		filter.add(value);
		filter.add(value2);
		System.out.println(filter.contains(value));
		System.out.println(filter.getSize());
	}
}

