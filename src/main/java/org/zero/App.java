package org.zero;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.TableCallback;

import com.yahoo.omid.transaction.HBaseTransactionManager;
import com.yahoo.omid.transaction.RollbackException;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionException;
import com.yahoo.omid.transaction.TransactionManager;

/**
 * Hello world!
 *
 */
public class App 
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
    
    
    public static void main(String[] args) throws IOException, TransactionException, RollbackException {
        /*App app = new App();
        app.execute("test2");*/
        
        //Yahoo omid, the tso server cannot start, the version is 0.8.1.24
        TransactionManager tm = HBaseTransactionManager.newInstance();
        Transaction tx = tm.begin();
        try (TTable txTable = new TTable("table1");){
            
            byte[] family = Bytes.toBytes("col1");
            byte[] qualifier = Bytes.toBytes("say");
            byte[] invalidFamily = Bytes.toBytes("invalidData");
            
            Put row1 = new Put(Bytes.toBytes("row15"));
            row1.addColumn(family, qualifier, Bytes.toBytes("fff"));
            txTable.put(tx, row1);
            
            Put row2 = new Put(Bytes.toBytes("row14"));
            row2.addColumn(invalidFamily, qualifier, Bytes.toBytes("Hello Worlds"));
            txTable.put(tx, row2);
            
            tm.commit(tx);
        } catch(Exception e) {
            tm.rollback(tx);
        }
        
        /*TransactionManager tm = HBaseTransactionManager.newInstance();
                TTable txTable = new TTable("table1");
            
            byte[] family = Bytes.toBytes("col1");
            byte[] qualifier = Bytes.toBytes("say");
            byte[] invalidFamily = Bytes.toBytes("invalidData");
            
            Transaction tx = tm.begin();
            Put row1 = new Put(Bytes.toBytes("row9"));
            row1.addColumn(family, qualifier, Bytes.toBytes("fff"));
            txTable.put(tx, row1);
            
            Put row2 = new Put(Bytes.toBytes("row10"));
            row2.addColumn(invalidFamily, qualifier, Bytes.toBytes("Hello Worlds"));
            txTable.put(tx, row2);
            
            tm.commit(tx);*/
        
        /*HaeinsaTablePool pool = new HaeinsaTablePool();
        HaeinsaTransactionManager tm = new HaeinsaTransactionManager(pool);
        HaeinsaTableIface table = pool.getTable("haeinsatable");
        
        byte[] family = Bytes.toBytes("data");
        byte[] qualifier = Bytes.toBytes("say");
        byte[] invalidFamily = Bytes.toBytes("invalidData");
        
        HaeinsaTransaction tx = tm.begin();
        
        HaeinsaPut put1 = new HaeinsaPut(Bytes.toBytes("row3"));
        put1.add(family, qualifier, Bytes.toBytes("Hello World"));
        table.put(tx, put1);
        
        HaeinsaPut put2 = new HaeinsaPut(Bytes.toBytes("row2"));
        put2.add(family, qualifier, Bytes.toBytes("Hello Worlds"));
        table.put(tx, put2);
        
        HaeinsaGet get = new HaeinsaGet(Bytes.toBytes("row3"));
        get.addColumn(family, qualifier);
        HaeinsaResult result = table.get(tx, get);
        byte[] value = result.getValue(family, qualifier);
        System.out.println(Bytes.toString(value));
        
        tx.commit();*/
    }
    
    public Boolean execute(String tableName) {
        ApplicationContext context = new ClassPathXmlApplicationContext("spring_hbase.xml");
        HbaseTemplate template = (HbaseTemplate) context.getBean("htemplate");
        
        return template.execute(tableName, new TableCallback<Boolean>() {
            public Boolean doInTable(HTableInterface table) throws Throwable {
                
                byte[] rowkey = "test2".getBytes();
                Put put = new Put(rowkey);
                put.addColumn(Bytes.toBytes("cfg2"), Bytes.toBytes("lablab"), Bytes.toBytes("hehe"));
                byte[] rowkey2 = "test3".getBytes();
                Put put2 = new Put(rowkey2);
                put2.addColumn(Bytes.toBytes("cfg2"), Bytes.toBytes("balabala"), Bytes.toBytes("heihei"));
                List<Put> list = Arrays.asList(put, put2);
                table.put(list);
                return true;
            }
        });
    }
    
}
