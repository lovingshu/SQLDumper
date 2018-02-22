package com.ex.sample;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能描述:MYSQL数据dump工具,需要dump的表必须要满足一个条件就是用于order的列每次查询出来的数量不能超过batch指定的数量
 * @createTime: 2017年9月11日 上午10:41:05
 * @author: <a href="mailto:liushulin@fenbei">liushulin</a>
 * @version: 0.1
 * @lastVersion: 0.1
 * @updateTime: 2017年9月11日 上午10:41:05
 * @updateAuthor: <a href="mailto:liushulin@fenbei.com">liushulin</a>
 * @changesSum:
 */
public class SQLDumper {
	static{
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static Logger log=LoggerFactory.getLogger(SQLDumper.class);
	//private static final String regEx="[~·`@#￥$%^……&*（()）\\-——\\-_=+【\\\\】｛{}｝\\|\\\\‘'“”\"《<》>/]";
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		Properties pro=new Properties();
		//pro.load(new FileReader("./data/pro"));
		pro.load(new FileReader("./data/pro"));
		String connection=pro.getProperty("connection");
		String user=pro.getProperty("user");
		String pwd=pro.getProperty("pwd");
		String table=pro.getProperty("table");
		String columns=pro.getProperty("columns");
		int batchSize=Integer.parseInt(pro.getProperty("batch","1000"));
		String where=pro.getProperty("where");
		String orderColumn=pro.getProperty("orderColumn");
		String orderType=pro.getProperty("orderType","asc");
		boolean stillNull=Integer.parseInt(pro.getProperty("stillNull","1"))==1;
		boolean hasWhere=where!=null;
		boolean hasOrder=orderColumn!=null;
		boolean isAsc=orderType.equals("asc");
		boolean isReplace=Integer.parseInt(pro.getProperty("isReplace","0"))==1;
		String storeFile="./data/dumped_"+table+".sql";
		log.info("check info blow type y to continue");
		log.info("connection:{}",connection);
		log.info("table:{}",table);
		log.info("columns:{}",columns);
		log.info("batch:{}",batchSize);
		log.info("where:{}",where);
		log.info("orderColumn:{}",orderColumn);
		log.info("orderType:{}",orderType);
		log.info("stillNull:{}",stillNull);
		log.info("storePath:{}",storeFile);
		log.info("isReplaceInto:{}",isReplace);
		log.info("isInsertInto:{}",!isReplace);
		Scanner scanner=new Scanner(System.in);
		FileWriter writer=new FileWriter(storeFile);
		log.info("your choice is [y/n]:");
		if(!scanner.nextLine().equals("y")){
			log.info("bye~~");
			scanner.close();
			System.exit(0);
		}
		scanner.close();
		String [] cols=columns.split(",");
		int resSize;
		Object lastOrderColumnValue=null;
		do{
			resSize=0;
			Connection con=null;
			try{
				long begin=System.currentTimeMillis();
				String sql=String.format("select %s from %s", columns,table);
				if(hasWhere){
					sql+=" where "+where;
				}
				if(lastOrderColumnValue!=null){
					sql+=(hasWhere?" and ":" where ")+orderColumn+(isAsc?" > ":" < ")+lastOrderColumnValue;
				}
				if(hasOrder){
					sql+=" order by "+orderColumn+" "+orderType;
				}
				sql+=" limit "+batchSize;
				log.info("executing:{}",sql);
				con=DriverManager.getConnection(connection,user,pwd);
				Statement stat=con.createStatement();
				ResultSet rs=stat.executeQuery(sql);
				StringBuilder builder=new StringBuilder((isReplace?"replace":"insert")+" into "+table+"("+columns+") values ");
				while(rs.next()){
					builder.append("(");
					String vals="";
					for(String col : cols){
						Object v=rs.getObject(col);
						if(v==null&&stillNull){
							vals+="null,";
						}else{
							if(v instanceof Number){
								vals+=v+",";
							}else{
								String vString=String.valueOf(v);
								vString=vString.replaceAll("'","\\'");
								vString=vString.replaceAll("\\\\","\\");
								vals+="'"+vString+"',";
								vals=vals.replaceAll("\r","");
								vals=vals.replaceAll("\n","");
							}
						}
					}
					if(vals.length()>0){
						builder.append(vals.substring(0, vals.length()-1));
					}
					builder.append("),");
					resSize++;
					lastOrderColumnValue=rs.getObject(orderColumn);
				}
				writer.write(builder.deleteCharAt(builder.length()-1).toString()+";\n");
				writer.flush();
				log.info("cost:{}",System.currentTimeMillis()-begin);
			}catch(Throwable e){
				log.error("",e);
				log.info("sorry,I can't run with errors!!");
				break;
			}finally{
				try {
					con.close();
				} catch (SQLException e) {
					log.error("",e);
				}
			}
		}while(resSize==batchSize);
		writer.close();
		log.info("done~");
	}
}