package com.gewara.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
/**
 * @author <a href="mailto:acerge@163.com">gebiao(acerge)</a>
 * @since 2007-9-28 02:05:17
 */
public class DateUtil {
	public static final long m_second=1000;
	public static final long m_minute=m_second*60;
	public static final long m_hour=m_minute*60;
	public static final long m_day=m_hour*24;
	/**
	* <p>DateUtil instances should NOT be constructed in standard programming.</p>
	* <p>This constructor is public to permit tools that require a JavaBean instance
	* to operate.</p>
	 */
	public DateUtil(){}
	
	/**
	 * ��ȡϵͳʱ��������뼶
	 * @return
	 */
	public static final long timeMillis(){
		return System.currentTimeMillis();
	}
	
	/**
	 * ��ǰ�����ַ�����yyyy-MM-dd
	 * @return
	 */
	public static final String currentTimeStr(){
		return formatDate(currentTime());
	}
	
	/**
	 * ��ȡ��ǰ����
	 * <br>�μ�{@link #timeMillis()}
	 * @return
	 */
	public static final Date currentTime(){
		return new Date();
	}
	
	/**
	 * ��ǰtimestamp�ַ�����yyyy-MM-dd HH:mm:ss
	 * <br>�μ�{@link #format(Date, String)}
	 * @return
	 */
	public static final String getCurFullTimestampStr(){
		return formatTimestamp(getCurFullTimestamp());
	}
	
	/**
	 * ��ǰtimestamp
	 * <br>�ַ������ͷ��أ��μ�{@link  #currentTimestampStr()}
	 * @return
	 */
	public static final Timestamp getCurFullTimestamp(){
		return new Timestamp(System.currentTimeMillis());
	}
	
	/**
	 * ��ǰ�·ݵ���һ����
	 * <br>1�·ݵ���һ����Ϊ 2��12�·ݵ���һ����Ϊ1
	 * @return
	 */
	public static final int nextMonth(){
		String next = format(new Date(), "M");
		int nextMonth = Integer.parseInt(next) + 1;
		if(nextMonth==13) return 1;
		return nextMonth;
	}
	/**
	 * parse date using default pattern yyyy-MM-dd
	 * @param strDate
	 * @return ʧ�ܷ���null
	 */
	public static final Date parseDate(String strDate){
		Date date = null;
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			date = dateFormat.parse(strDate);
			return date;
		} catch (Exception pe) {
			return null;
		}
	}
	public static int getWeekOfYear(Timestamp time){
		Calendar cal = Calendar.getInstance();
		cal.setTime(DateUtil.getDateFromTimestamp(time));
		int week = cal.get(Calendar.WEEK_OF_YEAR);
		return week;
	}

	/**
	 * ����date�ַ�������ȡtimestamp
	 * @param strDate ����Ϊ yyyy-MM-dd hh:mm:ss[.fffffffff]��ʽ
	 * @return ʧ�ܷ���null
	 */
	public static final Timestamp parseTimestamp(String strDate){
		try {
			// Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff]
			Timestamp result = Timestamp.valueOf(strDate);
			return result;
		} catch (Exception pe) {
			return null;
		}
	}
	/**
	 * @param strDate
	 * @param format
	 * @return
	 */
	public static final Timestamp parseTimestamp(String strDate, String pattern){
		Date date = null;
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
			date = dateFormat.parse(strDate);
			return new Timestamp(date.getTime());
		} catch (Exception pe) {
			return null;
		}
	}


	/**
	 * @param strDate
	 * @param pattern
	 * @return
	 */
	public static final Date parseDate(String strDate, String pattern){
		SimpleDateFormat df = null;
		Date date = null;
		df = new SimpleDateFormat(pattern);
		try {
			date = df.parse(strDate);
			return date;
		} catch (Exception pe) {
			return null;
		}
	}

	/**
	 * @param date
	 * @return formated date by yyyy-MM-dd
	 */
	public static final <T extends Date> String formatDate(T date) {
		if(date==null) return null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return dateFormat.format(date);
	}

	/**
	 * @param aDate
	 * @return formated time by HH:mm:ss
	 */
	public static final <T extends Date> String formatTime(T date) {
		if(date==null) return null;
		SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
		return timeFormat.format(date);
	}
	/**
	 * @param aDate
	 * @return formated time by yyyy-MM-dd HH:mm:ss
	 */
	public static final <T extends Date> String formatTimestamp(T date) {
		if(date==null) return null;
		SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return timestampFormat.format(date);
	}
	public static final String formatTimestamp(Long mills){
		return formatTimestamp(new Date(mills));
	}

	/**
	 * @param date
	 * @param pattern: Date format pattern
	 * @return
	 */
	public static final <T extends Date> String format(T date, String pattern) {
		if(date==null) return null;
		try{
			SimpleDateFormat df = new SimpleDateFormat(pattern);
			String result = df.format(date);
			return result;
		}catch(Exception e){
			return null;
		}
	}

	/**
	 * @param original
	 * @param days
	 * @param hours
	 * @param minutes
	 * @param seconds
	 * @param mill
	 * @return original+day+hour+minutes+seconds+millseconds
	 */
	public static final <T extends Date> T addTime(T original,int days, int hours, int minutes, int seconds){
		if(original==null) return null;
		long newTime=original.getTime()+m_day*days+m_hour*hours+m_minute*minutes+m_second*seconds;
		T another = (T) original.clone();
		another.setTime(newTime);
		return another;
	}
	public static final <T extends Date> T addDay(T original,int days){
		if(original==null) return null;
		long newTime=original.getTime() + m_day * days;
		T another = (T) original.clone();
		another.setTime(newTime);
		return another;
	}
	public static final <T extends Date> T addHour(T original, int hours){
		if(original==null) return null;
		long newTime=original.getTime()+m_hour*hours;
		T another = (T) original.clone();
		another.setTime(newTime);
		return another;
	}
	public static final <T extends Date> T addMinute(T original, int minutes){
		if(original==null) return null;
		long newTime=original.getTime() + m_minute*minutes;
		T another = (T) original.clone();
		another.setTime(newTime);
		return another;
	}
	public static final <T extends Date> T addSecond(T original, int second){
		if(original==null) return null;
		long newTime=original.getTime() + m_second*second;
		T another = (T) original.clone();
		another.setTime(newTime);
		return another;
	}
	
	/**
	 * @param day
	 * @return for example ,1997/01/02 22:03:00,return 1997/01/02 00:00:00.0
	 */
	public static final <T extends Date> T getBeginningTimeOfDay(T day){
		if(day==null) return null;
		//new Date(0)=Thu Jan 01 08:00:00 CST 1970
		String strDate = formatDate(day);
		Long mill = parseDate(strDate).getTime();
		T another = (T) day.clone();
		another.setTime(mill);
		return another;
	}
	/**
	 * @param day
	 * @return for example ,1997/01/02 22:03:00,return 1997/01/02 23:59:59.999
	 */
	public static final <T extends Date> T getLastTimeOfDay(T day){
		if(day==null) return null;
		Long mill = getBeginningTimeOfDay(day).getTime() + m_day - 1;
		T another = (T) day.clone();
		another.setTime(mill);
		return another;
	}
	/**
	 * 09:00:00,09:07:00 ---> 9:00,9:7:00
	 * @param time
	 * @return
	 */
	public static final String formatTime(String time){
		if(time==null) return null;
		time = StringUtils.trim(time);
		if(StringUtils.isBlank(time)) throw new IllegalArgumentException("ʱ���ʽ�д���");
		time = time.replace("��",":");
		String[] times = time.split(":");
		String result="";
		if(times[0].length()<2) result += "0" + times[0]+":";
		else result += times[0]+":";
		if(times.length > 1){
			if(times[1].length()<2) result += "0" + times[1];
			else result += times[1];
		}else{
			result += "00";
		}
		java.sql.Timestamp.valueOf("2001-01-01 " + result + ":00");
		return result;
	}
	public static boolean isTomorrow(Date date) {
		if(date==null) return false;
		if(formatDate(addTime(new Date(), 1, 0, 0, 0)).equals(formatDate(date))) return true;
		return false;
	}
	/***
	 * @param date
	 * @return 1,2,3,4,5,6,7
	 */
	private static int[] chweek = new int[]{0,7,1,2,3,4,5,6};
	/**
	 * @param date
	 * @return 1,2,3,4,5,6,7
	 */
	public static Integer getWeek(Date date) {
		if(date==null) return null;
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		return chweek[c.get(Calendar.DAY_OF_WEEK)];
	}
	
	public static Date getCurDateByWeek(Integer week){
		if(week == null || week<0 || week >7) return DateUtil.currentTime();
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DAY_OF_WEEK, week);
		return calendar.getTime();
	}
	
	private static String[] cnweek = new String[]{"", "����", "��һ", "�ܶ�", "����", "����", "����", "����"};
	private static String[] cnSimpleweek = new String[]{"", "��", "һ", "��", "��", "��", "��", "��"};
	/**
	 * @param date
	 * @return "����", "��һ", "�ܶ�", "����", "����", "����", "����"
	 */
	public static String getCnWeek(Date date){
		if(date==null) return null;
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		return cnweek[c.get(Calendar.DAY_OF_WEEK)];
	}
	/**
	 * @param date
	 * @return "��", "һ", "��", "��", "��", "��", "��"
	 */
	public static String getCnSimpleWeek(Date date){
		if(date==null) return null;
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		return cnSimpleweek[c.get(Calendar.DAY_OF_WEEK)];
	}
	public static Integer getCurrentDay(){
		return getDay(new Date());
	}
	public static Integer getCurrentMonth(){
		return getMonth(new Date());
	}
	public static Integer getCurrentYear() {
		return getYear(new Date());
	}
	public static Integer getYear(Date date) {
		if(date==null) return null;
		String year = DateUtil.format(date, "yyyy");
		return Integer.parseInt(year);
	}
	public static Integer getDay(Date date) {
		if(date==null) return null;
		String year = DateUtil.format(date, "d");
		return Integer.parseInt(year);
	}
	/**
	 * @param date
	 * @return ���������·�
	 */
	public static Integer getMonth(Date date) {
		if(date==null) return null;
		String month = format(date, "M");
		return Integer.parseInt(month);
	}

	public static Integer getCurrentHour(Date date) {
		if(date==null) return null;
		String hour = DateUtil.format(date, "H");
		return Integer.parseInt(hour);
	}
	public static Integer getCurrentMin(Date date) {
		if(date==null) return null;
		String hour = DateUtil.format(date, "m");
		return Integer.parseInt(hour);
	}
	public static String getCurDateStr(){
		return DateUtil.formatDate(new Date());
	}
	public static String getCurTimeStr(){
		return DateUtil.formatTimestamp(new Date());
	}
	public static boolean isAfter(Date date){
		if(date == null) return false;
		if(date.after(new Date())){
			return true;
		}
		return false;
	}
	/**
	 * ��ȡdate�����·ݵ�����Ϊweektype��������date֮�󣨻���ڣ�����������
	 * @param weektype
	 * @return
	 */
	public static List<Date> getWeekDateList(Date date, String weektype) {
		int curMonth = getMonth(date);
		int week = Integer.parseInt(weektype);
		int curWeek = getWeek(date);
		int sub = (7 + week - curWeek) % 7;
		Date next = addDay(date, sub);
		List<Date> result = new ArrayList<Date>();
		while(getMonth(next) == curMonth){
			result.add(next);
			next = addDay(next, 7);
		}
		return result;
	}
	/**
	 * ��ȡdate֮��(����date)��num������Ϊweektype���ڣ��������·ݣ�
	 * @param weektype
	 * @return
	 */
	public static List<Date> getWeekDateList(Date date, String weektype, int num) {
		int week = Integer.parseInt(weektype);
		int curWeek = getWeek(date);
		List<Date> result = new ArrayList<Date>();
		int sub = (7 + week - curWeek) % 7;
		Date next = addDay(date, sub);
		for(int i=0; i<num; i++){
			result.add(next);
			next = addDay(next, 7);
		}
		return result;
	}
	/**
	 * ��ȡdate�������ڵ���һ�����յ�����
	 * @param date
	 * @return
	 */
	public static List<Date> getCurWeekDateList(Date date){
		int curWeek = getWeek(date);
		List<Date> dateList = new ArrayList<Date>();
		for(int i=1;i<=7;i++) dateList.add(DateUtil.addDay(date, -curWeek + i ));
		return dateList;
	}
	public static Date getWeekLastDay(Date date){
		int curWeek = getWeek(date);
		return DateUtil.addDay(date, 7 - curWeek);
	}
	public static Date getCurDate(){
		return getBeginningTimeOfDay(new Date());
	}
	/**
	 * ��ȡ���������·ݵĵ�һ��
	 * @param date
	 * @return
	 */
	public static <T extends Date> T getMonthFirstDay(T date) {
		if(date == null) return null;
		String dateStr = format(date, "yyyy-MM") + "-01";
		Long mill = parseDate(dateStr).getTime();
		T another = (T) date.clone();
		another.setTime(mill);
		return another;
	}
	
	public static <T extends Date> T getNextMonthFirstDay(T day) {
		if(day==null) return null;
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(day);
		int month = calendar.get(Calendar.MONTH);
		calendar.set(Calendar.MONTH, month +1);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		String datefor = format(calendar.getTime(), "yyyy-MM-dd");
		Long mill = parseDate(datefor).getTime();
		T another = (T) day.clone();
		another.setTime(mill);
		return  another;
	}
	/**
	 * ��ȡ���������·ݵ����һ��
	 * @param days
	 * @return
	 */
	public static <T extends Date> T  getMonthLastDay(T date){
		if(date == null) return null;
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		String dateStr = format(date, "yyyy-MM") + "-" + c.getActualMaximum(Calendar.DAY_OF_MONTH);
		Long mill = parseDate(dateStr).getTime();
		T another = (T) date.clone();
		another.setTime(mill);
		return another;
	}
	
	public static String formatDate(int days){
		return formatDate(addDay(new Date(), days));
	}
	/**
	 * ��ȡʱ������ʱ��
	 * @return
	 */
	public static Timestamp getCurTruncTimestamp() {
		return getBeginningTimeOfDay(new Timestamp(System.currentTimeMillis()));
	}
	public static Integer getHour(Date date) {
		if(date==null) return null;
		String hour = format(date, "H");
		return Integer.parseInt(hour);
	}
	public static String getTimeDesc(Timestamp time) {
		if(time==null) return "";
		String timeContent;
		Long ss = System.currentTimeMillis()-time.getTime();
		Long minute = ss/60000;
		if (minute<1) {
			Long second = ss/1000;
			timeContent =  second+"��ǰ";
		}else if(minute>=60){
			Long hour = minute/60;
			if(hour>=24){
				if(hour>720)timeContent= "1��ǰ";
				else if(hour>168 && hour<=720) timeContent= (hour/168)+"��ǰ";
				else timeContent = (hour/24)+"��ǰ";
			}else{
				timeContent =  hour+"Сʱǰ";
			}
		}else{
			timeContent = minute+"����ǰ";
		}
		return timeContent;
	}
	
	public static String getDateDesc(Date time) {
		if(time==null) return "";
		String timeContent;
		Long ss = System.currentTimeMillis()-time.getTime();
		Long minute = ss/60000;
		if (minute<1) {
			Long second = ss/1000;
			timeContent = second+"��ǰ";
		}else if(minute>=60){
			Long hour = minute/60;
			if(hour>=24){
				if(hour>720)timeContent= "1��ǰ";
				else if(hour>168 && hour<=720) timeContent= (hour/168)+"��ǰ";
				else timeContent = (hour/24)+"��ǰ";
			}else{
				timeContent =  hour+"Сʱǰ";
			}
		}else{
			timeContent = minute+"����ǰ";
		}
		return timeContent;
	}
	
	/**
	 *  author: bob
	 *  date:	20100729
	 *  ��ȡ����, ȥ�����
	 *  param: 	date1
	 *  eg. ����"1986-07-28", ���� 07-28 
	 */
	public static String getMonthAndDay(Date date){
		return formatDate(date).substring(5);
	}
	public static Date getMillDate(){
		return new Date();
	}
	public static Timestamp getMillTimestamp(){
		return new Timestamp(System.currentTimeMillis());
	}
	/**
	 * ʱ��day1-day2
	 * @param day1
	 * @param day2
	 * @return
	 */
	public static final <T extends Date> String getDiffDayStr(T day1, T day2){
		if(day1==null || day2==null) return "---";
		long diff = day1.getTime() - day2.getTime();
		long sign = diff/Math.abs(diff);
		if(sign < 0) return "�Ѿ�����";
		diff = Math.abs(diff)/1000;
		long day = diff/3600/24;
		long hour = (diff-(day*3600*24))/3600;
		long minu = diff%3600/60;
		return (day==0?"":day+"��") + (hour==0?"":hour+"Сʱ") + (minu==0?"":minu+"��");
	}
	/**
	 * ʱ��day1-day2
	 * @param day1
	 * @param day2
	 * @return
	 */
	public static final <T extends Date> String getDiffStr(T day1, T day2){
		if(day1==null || day2==null) return "---";
		long diff = day1.getTime() - day2.getTime();
		long sign = diff/Math.abs(diff);
		diff = Math.abs(diff)/1000;
		long hour = diff/3600;
		long minu = diff%3600/60;
		long second = diff%60;
		return (sign<0?"-":"+") + (hour==0?"":hour+"Сʱ") + (minu==0?"":minu+"��") + (second==0?"":second+"��");
	}
	/**
	 * ʱ���룩��day1-day2
	 * @param day1
	 * @param day2
	 * @return
	 */
	public static final <T extends Date> long getDiffSecond(T day1, T day2){
		if(day1==null || day2==null) return 0;
		long diff = day1.getTime() - day2.getTime();
		if(diff == 0) return 0;
		long sign = diff/Math.abs(diff);
		diff = Math.abs(diff)/1000;
		return sign * diff;
	}
	/**
	 * ʱ�����ӣ���day1-day2
	 * @param day1
	 * @param day2
	 * @return
	 */
	public static final <T extends Date> double getDiffMinu(T day1, T day2){
		if(day1==null || day2==null) return 0;
		long diff = day1.getTime() - day2.getTime();
		if(diff == 0) return 0;
		long sign = diff/Math.abs(diff);
		diff = Math.abs(diff)/1000;
		return Math.round(diff * 1.0d * 10/6.0)/100.0 * sign;//��λС��
	}
	/**
	 * ʱ���֣���time1 - time2
	 * @param time1
	 * @param time2
	 * @return
	 */
	public static final double getMillDiffMinu(long time1, long time2){
		long diff = time1 - time2;
		if(diff == 0) return 0;
		long sign = diff/Math.abs(diff);
		diff = Math.abs(diff)/1000;
		return Math.round(diff * 1.0d * 10/6.0)/100.0 * sign;//��λС��
	}
	/**
	 * ʱ��Сʱ����day1 - day2
	 * @param day1
	 * @param day2
	 * @return
	 */
	public static final <T extends Date> double getDiffHour(T day1, T day2){
		if(day1==null || day2==null) return 0;
		long diff = day1.getTime() - day2.getTime();
		long sign = diff/Math.abs(diff);
		diff = Math.abs(diff)/1000;
		return Math.round(diff * 1.0d /3.6)/1000.0 * sign;//��λС��
	}
	/**
	 * @param day1
	 * @param day2
	 * @return �����������round(abs��day1-day2))
	 */
	public static final <T extends Date> int getDiffDay(T day1, T day2){
		if(day1==null || day2==null) return 0;
		long diff = day1.getTime() - day2.getTime();
		diff = Math.abs(diff)/1000;
		return Math.round(diff/(3600*24));
	}
	public static boolean isAfterOneHour(Date date,String time){
		String datetime = formatDate(date)+" "+time+":00";
		if(addHour(parseTimestamp(datetime),-1).after(getMillTimestamp())){
			return true;
		}
		return false;
	}
	public static boolean isValidDate(String fyrq) {
		return DateUtil.parseDate(fyrq)!=null;
	}
	
	public static <T extends Date> long getCurDateMills(T date){
		if(date == null) return 0;
		return date.getTime();
	}
	
	/**
	 *  eg.  1997/01/02 22:03:00,return 1997/01/02 00:00:00.0
	 **/
	public static Timestamp getBeginTimestamp(Date date){
		return new Timestamp(getBeginningTimeOfDay(date).getTime());
	}
	public static Timestamp getEndTimestamp(Date date){
		return new Timestamp(getLastTimeOfDay(date).getTime());
	}
	/**
	 * @param timestamp
	 * @return
	 */
	public static Date getDateFromTimestamp(Timestamp timestamp){
		if(timestamp==null) return null;
		return new Date(timestamp.getTime());
	}
	
	public static int after(Date date1, Date date2){
		date1 = getBeginningTimeOfDay(date1);
		date2 = getBeginningTimeOfDay(date2);
		return date1.compareTo(date2);
	}
	public static Timestamp mill2Timestamp(Long mill){
		if(mill==null) return null;
		return new Timestamp(mill);
	}
	
	public static int subCurTimeSend(){
		Timestamp curtime = DateUtil.getCurFullTimestamp();
		Timestamp endtime = DateUtil.getLastTimeOfDay(curtime);
		Long scopeSecond = DateUtil.getDiffSecond(endtime, curtime);
		return scopeSecond.intValue();
	}
}

