package com.gewara.util;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

public class DBManager implements Serializable{
    private static final Logger logger = LoggerFactory.getLogger(DBManager.class); 

	private  static final long serialVersionUID = 1L;
	private  static String driver="org.postgresql.Driver";
	 private static String url="jdbc:postgresql://192.168.2.224/monitor";
	 private static String uName="monitor";
	 private static String uPwd="monitor22";
	 private static Connection conn;
	 
	 //获得连接
	 public static Connection getConn(){
	  try {
	   Class.forName(driver);
	   conn=DriverManager.getConnection(url, uName,uPwd);
	  } catch (ClassNotFoundException e) {
	   logger.error("",e);
	  } catch (SQLException e) {
	   logger.error("",e);
	  }
	  return conn;
	 }
	 
	 public String getSQL(Tuple input){
		 String pid = input.getString(0);
		 Integer count = input.getInteger(1);
		 Timestamp addtime = new Timestamp(System.currentTimeMillis());
		 String sql=" insert into monitor.uv(pid,addtime,uvcount) values("+pid+","+addtime+","+count+")";
		 logger.debug(sql);
		 return sql;
	 }
}
