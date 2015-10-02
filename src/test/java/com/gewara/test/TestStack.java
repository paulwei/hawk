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
	    //集合遍历方式 
	    for (Integer x : s) { 
	            System.out.println(x); 
	    } 
	    System.out.println("------1-----"); 
	    //栈弹出遍历方式 
//	    while (s.peek()!=null) {     //不健壮的判断方式，容易抛异常，正确写法是下面的 
	    while (!s.empty()) { 
	            System.out.println(s.pop()); 
	    } 
	    System.out.println("------2-----"); 
	    //错误的遍历方式 
//	    for (Integer x : s) { 
//	            System.out.println(s.pop()); 
//	    } 
	} 
	}
