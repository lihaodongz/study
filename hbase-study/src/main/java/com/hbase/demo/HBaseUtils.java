package com.hbase.demo;


import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
public class HBaseUtils {

    /*创建表
            admin.createTable()
    * */
    public static boolean createTable(String tableName,String[] cfs) {
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin();) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;

    }


    /**
     * 构建put对象。table.put(new put)
     * @param tableName
     * @param rowKey
     * @param cfName
     * @param qualifier
     * @param data
     * @return
     */
    public static boolean purRow(String tableName,
                                 String rowKey,
                                 String cfName,
                                 String qualifier,
                                 String data){
        try ( Table table = HBaseConn.getTable(tableName);) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualifier),Bytes.toBytes(data));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;

    }

    public static boolean putRows(String tableName, List<Put> puts){
        try ( Table  table = HBaseConn.getTable(tableName)){
            table.put(puts);
        }catch ( Exception e){
            e.printStackTrace();
        }
        return true;
    }

    public static Result getRow(String tableName,String rowKey){
        try ( Table  table = HBaseConn.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            return result;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static Result getRow(String tableName, String rowKey, FilterList filterList){
        try ( Table  table = HBaseConn.getTable(tableName)){
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setFilter(filterList);
            Result result = table.get(get);
            return result;
        }catch ( Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /*删除表
    * 1.admin.disable()
    * 2. amdin.deletetable()
    * */
    public static boolean deleteTable(String tableName){
        try   ( HBaseAdmin admin =(HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }
    /*全表扫描*/
    public static ResultScanner getScanner(String tableName){
        try (Table table = HBaseConn.getTable(tableName)){
            Scan scan = new Scan();
            scan.setCaching(1000);
            return table.getScanner(scan);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static ResultScanner getScanner(String tableName, String startRowkay,
                                           String endRowkey,
                                           FilterList filterList){
        try (Table table= HBaseConn.getTable(tableName)){

            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowkay));
            scan.setStopRow(Bytes.toBytes(endRowkey));
            scan.setFilter(filterList);
            scan.setCaching(1000);
            return table.getScanner(scan);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public static boolean deleteRow(String tableName,String rowKey){
        try (Table table = HBaseConn.getTable(tableName)){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }
    public static boolean deleteColumnFamily(String tableName,
                                             String cfName){
        try (HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin()){
            admin.deleteColumn(TableName.valueOf(tableName),Bytes.toBytes(cfName));
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    public static boolean deleteQualifier(String tableName, String rowKey, String cfName,
                                          String qualifier) {
        try (Table table = HBaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier));
            table.delete(delete);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return true;
    }
}
