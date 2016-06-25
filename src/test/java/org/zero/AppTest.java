package org.zero;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.TableCallback;

/**
 * Hello world!
 *
 */
public class AppTest 
{
    /*public static void main( String[] args ) throws Exception {
        createTable("test2", new String[]{"fff"});
    }
    
    static Configuration conf = null;
    
    static {
        conf = HBaseConfiguration.create();
    }
    
    public static void createTable(String tableName, String[] family) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (int i = 0; i < family.length; i ++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        
        if (admin.tableExists(tableName)) {
            System.out.println("table exists!");
            System.exit(0);
        } else {
            admin.createTable(desc);
            System.out.println("create table Success!");
        }
    }
    
    public static void addData(
        String rowKey, 
        String tableName, 
        String[] column1, 
        String[] value1, 
        String[] column2, 
        String[] value2) throws Exception {

        Put put = new Put(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i ++) {
            String familyName = columnFamilies[i].getNameAsString();

            if (familyName.equals("article")) {

                for (int j = 0 ; j < column1.length; j ++) {
                    put.add(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }

            if (familyName.equals("author")) {

                for (int j = 0; j < column2.length; j ++) {
                    put.add(Bytes.toBytes(familyName), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }
    }

    public static Result getResult(String tableName, String rowKey) throws Exception {
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Result result = table.get(get);

        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value :" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp : " + kv.getTimestamp());
        }
        return result;
    }

    public static void getResult(String tableName) throws Exception {
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));

        try {
            rs = table.getScanner(scan);

            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:" + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
                    System.out.println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out.println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }


    public static void getResult(String tableName, String start_rowkey, String stop_rowKey) throws Exception {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowKey));
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));

        rs = table.getScanner(scan);

        for (Result r : rs) {
            for (KeyValue kv : r.list()) {
                System.out.println("row:" + Bytes.toString(kv.getRow()));
                System.out.println("family:" + Bytes.toString(kv.getFamily()));
                System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
                System.out.println("value:" + Bytes.toString(kv.getValue()));
                System.out.println("timestamp:" + kv.getTimestamp());
                System.out.println("-------------------------------------------");
            }
        }
    }

    public static void getResultByColumn(String tableName, String rowKey, String familyName, String columnName) throws Exception {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        Result result = table.get(get);

        for (KeyValue kv : result.list()) {
            System.out.println("row:" + Bytes.toString(kv.getRow()));
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }

    public static void updateTable(String tableName, String rowKey, String familyName, String columnName, String value) throws Exception {

        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        table.put(put);
        System.out.println("update table Success!");
    }

    public static void getResultByVersion(String tableName, String rowKey, String familyName, String columnName) throws Exception {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);

        for (KeyValue kv : result.list()) {
            System.out.println("row:" + Bytes.toString(kv.getRow()));
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }

    public static void deleteColumn(String tableName, String rowKey, String familyName, String columnName) throws Exception {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.deleteColumns(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println(familyName + ":" + columnName + "is deleted");
    }

    public static void deleteAllColumn(String tableName, String rowKey) throws Exception {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);
        System.out.println("all column are deleted");
    }

    public static void deleteTable(String tableName) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName + " is deleted");
    }*/
    
    
    public static void main(String[] args) {
        AppTest app = new AppTest();
        app.execute("table");
    }
    
    public Boolean execute(String tableName) {
        ApplicationContext context = new ClassPathXmlApplicationContext("spring_hbase.xml");
        HbaseTemplate template = (HbaseTemplate) context.getBean("htemplate");
        
        return template.execute(tableName, new TableCallback<Boolean>() {
            public Boolean doInTable(HTableInterface table) throws Throwable {
                
                byte[] rowkey = "test".getBytes();
                Put put = new Put(rowkey);
                put.add(Bytes.toBytes("familyColumn"), Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
                table.put(put);
                return true;
            }
        });
    }
    
    
    
    
    
}
