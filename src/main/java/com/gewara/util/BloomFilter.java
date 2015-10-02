/**  
* @Project: Algorithm
* @Title: BloomFilter.java
* @Package big.data
* @Description: ��¡�����㷨
* @author paul.wei2011@gmail.com
* @date Jul 9, 2013 10:26:27 AM
* @version V1.0  
*/

package com.gewara.util;

import java.io.Serializable;
import java.util.BitSet;

public class BloomFilter implements Serializable {
	private static final long serialVersionUID = 5829598197124113258L;
	private static final  int  DEFAULT_SIZE  =2 << 24 ; //2��24�η�
    private static final  int [] seeds =new  int []{5, 7, 11 , 13 , 31 , 37 , 61};//���첻ͬ���ӣ����ڲ�ͬɢ�к���
    private BitSet bits= new  BitSet(DEFAULT_SIZE);
    private SimpleHash[]  func=new  SimpleHash[seeds.length];//�Բ�ͬ�����ֹ��첻ͬHash���������ͻ
    private int size = 0;
    public BloomFilter() {
    	for (int i = 0; i < seeds.length; i++) {//��ʼ�����캯��
			func[i] = new SimpleHash(DEFAULT_SIZE, seeds[i]);
		}
	}
    public void add(String value) {
    	if(!contains(value)){
    		size++;
    	}
		for (SimpleHash f : func) { //���hashֵӳ�䵽��Ӧλ
			bits.set(f.hash(value), true);
		}
	}
    public boolean contains(String value) {
		if (value == null) {
			return false;
		}
		boolean ret = true;
		for (SimpleHash f : func) {
			ret = ret && bits.get(f.hash(value));//�������ʱ��hash���������Ƿ�true
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
    	public int hash(String value) {//hashֵͨ�����ָ���������ɢ�к�õ�ֵ
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

