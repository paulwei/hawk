/**  
* @Project: hawk
* @Title: TestStack.java
* @Package com.gewara.test
* @Description: TODO
* @author honglin.wei@gewara.com
* @date Apr 2, 2014 4:20:02 PM
* @version V1.0  
*/

package com.gewara.test;

import java.util.Stack;

/**
 * @ClassName TestStack
 * @Description TODO
 * @author weihonglin pau.wei2011@gmail.com
 * @date Apr 2, 2014
 */

public class TestStack {
	public static void main(String[] args){
	    Stack<Integer> s = new Stack<Integer>(); 
	    for (int i = 0; i < 10; i++) { 
	            s.push(i); 
	    } 
	    //���ϱ�����ʽ 
	    for (Integer x : s) { 
	            System.out.println(x); 
	    } 
	    System.out.println("------1-----"); 
	    //ջ����������ʽ 
//	    while (s.peek()!=null) {     //����׳���жϷ�ʽ���������쳣����ȷд��������� 
	    while (!s.empty()) { 
	            System.out.println(s.pop()); 
	    } 
	    System.out.println("------2-----"); 
	    //����ı�����ʽ 
//	    for (Integer x : s) { 
//	            System.out.println(s.pop()); 
//	    } 
	} 
	}
