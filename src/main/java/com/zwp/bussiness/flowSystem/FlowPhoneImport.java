package com.zwp.bussiness.flowSystem;

import com.mysql.jdbc.StringUtils;
import com.zwp.core.MyBatisUtil;
import com.zwp.mapper.flowSystem.SysAreaMapper;
import com.zwp.mapper.flowSystem.SysPhoneAreaMapper;
import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.apache.ibatis.session.SqlSession;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

/**
 * 流量系统号段更新导入
 *
 * @author: zhouwp
 * @date:Create in 16:59 2021/10/19
 */
public class FlowPhoneImport {

    final static Map<String, Integer> cmap = new HashMap<String, Integer>() {{
        put("中国电信", 0);
        put("中国移动", 1);
        put("中国联通", 2);
    }};

    static ThreadPoolExecutor executor = null;

    static {
        PooledDataSource dataSource = (PooledDataSource) MyBatisUtil.getSqlSessionFactory()
                .getConfiguration().getEnvironment().getDataSource();
        //获取数据库链接最大数作为线程池的最大线程数，避免连接池耗尽引发异常
        int maxPoolSize = dataSource.getPoolMaximumActiveConnections();
        new ThreadPoolExecutor(maxPoolSize, maxPoolSize, 1000,
                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(5050),(runnable, poolExecutor) -> {

            while (poolExecutor.getQueue().remainingCapacity() < 50) {
                try {
                    //添加主线程休眠半秒
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //有空位了 再次添加
            poolExecutor.execute(runnable);
        });
    }

    public static void main(String[] args) throws Exception {
//        phoneFilter();
//        phoneImport();
//        phoneImport2();
        System.out.println("11");

    }

    /**
     *
     * 过滤空行数据
     *
     * @throws IOException
     */
    private static void phoneFilter() throws IOException {
//        SqlSession sqlSession = null;
        BufferedWriter writer = null;
        try {
            long oldTIme = System.currentTimeMillis();
//            CSV文件内容格式：
//            Prefix,Mobile,Province,City,ISP,AreaCode,PostCode,ProvinceCode,CityCode,LNG,LAT
//            133,1330001,安徽,安庆,中国电信,556,246000,340000,340800,117.063754,30.543494
            String filePath = "D:\\work\\虚拟\\流量系统\\2021号段\\Mobile.csv";
            File file = new File(filePath);
            Charset gbk = Charset.forName("GBK");

            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\work\\虚拟\\流量系统\\2021号段\\filter.csv")));


            int rowCount = 0;
            try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(file), gbk))) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    int lineNumber = reader.getLineNumber();
                    String[] paramsArr = line.split(",");
                    if (!(paramsArr.length > 4)) {
                        System.out.println("-->" + lineNumber + "->读取到空数据" + line);
                        continue;
                    }
                    String phone = paramsArr[1];
                    if (StringUtils.isNullOrEmpty(phone)) {
                        System.out.println("-->" + lineNumber + "->读取到空数据" + line);
                        continue;
                    }

                    writer.write(line);
                    writer.newLine();
                    rowCount++;
                }
            }
            long currTime = System.currentTimeMillis();
            long time = (currTime - oldTIme) / 1000;
            System.out.println(time + "-->全部完成<-- 新行数数：" + rowCount);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    /**
     * 多线程导入
     * @throws IOException
     */
    private static void phoneImport2() throws IOException {
        BufferedWriter writer = null;
        try {
            long oldTIme = System.currentTimeMillis();
//            CSV文件内容格式：
//            Prefix,Mobile,Province,City,ISP,AreaCode,PostCode,ProvinceCode,CityCode,LNG,LAT
//            133,1330001,安徽,安庆,中国电信,556,246000,340000,340800,117.063754,30.543494
            String filePath = "D:\\work\\虚拟\\流量系统\\2021号段\\filter.csv";
            File file = new File(filePath);
            Charset gbk = Charset.forName("UTF-8");

            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\work\\虚拟\\流量系统\\2021号段\\error.csv")));

            int maxLinNum = (int) Files.lines(Paths.get(file.getPath()), gbk).count();
            int pageSize = 5000;
            int countRunner = maxLinNum % pageSize == 0 ? maxLinNum / pageSize : maxLinNum / pageSize + 1;
            CountDownLatch downLatch = new CountDownLatch(countRunner);

            List<Future<Map<String, Integer>>> futureList = new ArrayList<>(countRunner);

            try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(file), gbk))) {
                String line = null;
                List<Line> list = new ArrayList<>(pageSize);
                while ((line = reader.readLine()) != null) {
                    int lineNumber = reader.getLineNumber();
                    list.add(new Line(lineNumber, line));
                    if (list.size() == pageSize) {
                        Future<Map<String, Integer>> mapFuture = executor.submit(new ImportRunner(list, writer, downLatch));
                        futureList.add(mapFuture);
                        list = new ArrayList<>(pageSize);
                    }
                }
                Future<Map<String, Integer>> mapFuture = executor.submit(new ImportRunner(list, writer, downLatch));
                futureList.add(mapFuture);
            }

            downLatch.await();

            int updateCount = 0;
            int insertCount = 0;
            int failCount = 0;
            for (Future<Map<String, Integer>> mapFuture : futureList) {
                updateCount += mapFuture.get().get("updateCount");
                insertCount += mapFuture.get().get("insertCount");
                failCount += mapFuture.get().get("failCount");
            }
            System.out.println("总行数：" + maxLinNum);
            long currTime = System.currentTimeMillis();
            long time = (currTime - oldTIme) / 1000;
            System.out.println(time + "-->全部完成<-- 新增数：" + insertCount + ",更新数：" + updateCount + ",失败数：" + failCount);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    /**
     * 单线程导入
     * @throws IOException
     */
    private static void phoneImport() throws IOException {
        SqlSession sqlSession = null;
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
//            CSV文件内容格式：
//            Prefix,Mobile,Province,City,ISP,AreaCode,PostCode,ProvinceCode,CityCode,LNG,LAT
//            133,1330001,安徽,安庆,中国电信,556,246000,340000,340800,117.063754,30.543494
            InputStream fileStream = new FileInputStream("D:\\work\\虚拟\\流量系统\\2021号段\\Mobile.csv");
            reader = new BufferedReader(new InputStreamReader(fileStream, "GBK"));
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\work\\虚拟\\流量系统\\2021号段\\error.csv")));

            String line = null;
            sqlSession = MyBatisUtil.getSqlSession(false);
            SysAreaMapper prvinceMapper = sqlSession.getMapper(SysAreaMapper.class);
            SysPhoneAreaMapper cityMapper = sqlSession.getMapper(SysPhoneAreaMapper.class);

            int i = 0;
            int updateCount = 0;
            int insertCount = 0;
            int failCount = 0;
            while ((line = reader.readLine()) != null) {
                System.out.println(i + "->读取文件内容：" + line);
                if (line.contains("Mobile")) {
                    writer.write(line);
                    writer.newLine();
                    continue;
                }
                String[] paramsArr = line.split(",");
                if (!(paramsArr.length > 4)) {
                    continue;
                }
                String phone = paramsArr[1];
                if (StringUtils.isNullOrEmpty(phone)) {
                    System.out.println(i + "->读取到空数据" + line);
                    continue;
                }
                String province = paramsArr[2];
                String city = paramsArr[3];
                Integer carrier = cmap.get(paramsArr[4]);

                Map<String, Object> provinceMap = prvinceMapper.selectByName(province);
                if (provinceMap == null) {
                    System.out.println(i + "->未找到省份：" + province);
                    writer.write(line);
                    writer.newLine();
                    failCount++;
                    continue;
                }
                int provinceCode = (int) provinceMap.get("sa_id");

                Map<String, Object> cityMap = cityMapper.selectByPhone(phone);
                //找到更新，未找到新增
                if (cityMap == null) {
                    //phone_num, province_code, cart_type, phone_type, row_update_time, row_create_time
                    Date date = new Date();
                    Map<String, Object> newCity = new HashMap<>(6);
                    newCity.put("phone_num", phone);
                    newCity.put("province_code", provinceCode);
                    newCity.put("cart_type", city);
                    newCity.put("phone_type", carrier);
                    newCity.put("row_update_time", date);
                    newCity.put("row_create_time", date);
                    cityMapper.insert(newCity);
                    insertCount++;
                } else {
                    Map<String, Object> updateCity = new HashMap<>(4);
                    updateCity.put("phone_num", phone);
                    updateCity.put("province_code", provinceCode);
                    updateCity.put("cart_type", city);
                    updateCity.put("phone_type", carrier);
                    cityMapper.updatePhoneNum(updateCity);
                    updateCount++;
                }
                if (i % 5000 == 0) {
                    sqlSession.commit();
                }
                i++;
            }
            sqlSession.commit();
            System.out.println("-->完成<-- 新增数：" + insertCount + ",更新数：" + updateCount + ",失败数：" + failCount);
        } catch (Exception e) {
            e.printStackTrace();
            if (sqlSession != null) {
                sqlSession.rollback();
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
            MyBatisUtil.closeSqlSession(sqlSession);
        }
    }


    static class ImportRunner implements Callable<Map<String, Integer>> {

        //        private int startNum;
//        private int endNum;
//        private LineNumberReader reader;
//        public static SqlSession sqlSession = MyBatisUtil.getSqlSession(true);
        private List<Line> lineList;
        private BufferedWriter writer;
        private CountDownLatch downLatch;


        public ImportRunner(List<Line> lineList, BufferedWriter writer, CountDownLatch downLatch) {
            this.lineList = lineList;
            this.writer = writer;
            this.downLatch = downLatch;
        }

        @Override
        public Map<String, Integer> call() throws Exception {
            SqlSession sqlSession = null;
            try {
                sqlSession = MyBatisUtil.getSqlSession(false);
                SysAreaMapper prvinceMapper = sqlSession.getMapper(SysAreaMapper.class);
                SysPhoneAreaMapper cityMapper = sqlSession.getMapper(SysPhoneAreaMapper.class);
                int updateCount = 0;
                int insertCount = 0;
                int failCount = 0;

                for (int i = 0; i < lineList.size(); i++) {
                    Line lineObj = lineList.get(i);
                    int lineNumber = lineObj.getLineNumber();
                    String line = lineObj.getLine();
//                    System.out.println(lineNumber + "-->" + Thread.currentThread().getName() + "-->" + i + "->文件内容：" + line);
                    if (line.contains("Mobile")) {
                        writer.write(line);
                        writer.newLine();
                        continue;
                    }
                    String[] paramsArr = line.split(",");
                    if (!(paramsArr.length > 4)) {
                        continue;
                    }
                    String phone = paramsArr[1];
                    if (StringUtils.isNullOrEmpty(phone)) {
                        System.out.println(Thread.currentThread().getName() + "-->" + i + "->读取到空数据" + line);
                        continue;
                    }
                    String province = paramsArr[2];
                    String city = paramsArr[3];
                    Integer carrier = cmap.get(paramsArr[4]);

                    Map<String, Object> provinceMap = prvinceMapper.selectByName(province);
                    if (provinceMap == null) {
                        System.out.println(Thread.currentThread().getName() + "-->" + i + "->未找到省份：" + province);
                        writer.write(line);
                        writer.newLine();
                        failCount++;
                        continue;
                    }
                    int provinceCode = (int) provinceMap.get("sa_id");

                    Map<String, Object> cityMap = cityMapper.selectByPhone(phone);
                    //找到更新，未找到新增
                    if (cityMap == null) {
                        //phone_num, province_code, cart_type, phone_type, row_update_time, row_create_time
                        Date date = new Date();
                        Map<String, Object> newCity = new HashMap<>(6);
                        newCity.put("phone_num", phone);
                        newCity.put("province_code", provinceCode);
                        newCity.put("cart_type", city);
                        newCity.put("phone_type", carrier);
                        newCity.put("row_update_time", date);
                        newCity.put("row_create_time", date);
                        cityMapper.insert(newCity);
                        insertCount++;
                    } else {
                        Map<String, Object> updateCity = new HashMap<>(4);
                        updateCity.put("phone_num", phone);
                        updateCity.put("province_code", provinceCode);
                        updateCity.put("cart_type", city);
                        updateCity.put("phone_type", carrier);
                        cityMapper.updatePhoneNum(updateCity);
                        updateCount++;
                    }
                }

                System.out.println(Thread.currentThread().getName() + "-->完成<-- 新增数：" + insertCount + ",更新数：" + updateCount + ",失败数：" + failCount);
                HashMap<String, Integer> resultMap = new HashMap<>(3);
                resultMap.put("insertCount", insertCount);
                resultMap.put("updateCount", updateCount);
                resultMap.put("failCount", failCount);
                sqlSession.commit();
                return resultMap;
            } catch (Exception e) {
                e.printStackTrace();
                if (sqlSession != null) {
                    sqlSession.rollback();
                }
                System.exit(-3);
                return null;
            } finally {
                MyBatisUtil.closeSqlSession(sqlSession);
                downLatch.countDown();
            }

        }


    }

}
