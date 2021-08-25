package myHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.*;

public class myHbaseJob {

//    protected Connection conn = null;
//    protected  TableName myhbasetablename = TableName.valueOf("liuyang:student");
//    protected  String nameSpace ="liuyang";
//    protected  void init() throws IOException {
//        Configuration hBaseConfiguration = HBaseConfiguration.create();
//        hBaseConfiguration.set("hbase.zookeeper.quorum", "10.0.24.116:2181");
//        hBaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
//        if (conn==null || conn.isClosed()){
//            try{
//                conn = ConnectionFactory.createConnection(hBaseConfiguration);
//            }catch (Exception e){
//                e.printStackTrace();
//            }
//        }
//    }
//
//
//    protected void create() throws IOException{
//        Admin admin = conn.getAdmin();
//        if(admin.getNamespaceDescriptor(nameSpace)==null)   {
//            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
//            admin.createNamespace(namespaceDescriptor);
//        }
//        if(admin.tableExists(myhbasetablename)) {
//            admin.disableTable(myhbasetablename);
//            admin.deleteTable(myhbasetablename);
//        }
//        HTableDescriptor tableDescriptor = new HTableDescriptor(myhbasetablename);//添加表名称
//        HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");//定义列簇
//        HColumnDescriptor columnDescriptor2 = new HColumnDescriptor("score");
//        tableDescriptor.addFamily(columnDescriptor);//添加列簇
//        tableDescriptor.addFamily(columnDescriptor2);
//        admin.createTable(tableDescriptor);
//    }
//
//    protected boolean putData(){
//        try(Table table = conn.getTable(myhbasetablename)){
//            Put put = new Put(Bytes.toBytes(getRowKey()));
//            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("student_id"),Bytes.toBytes("G20210735010031"));
//            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("class"),Bytes.toBytes("1"));
//            put.addColumn(Bytes.toBytes("score"),Bytes.toBytes("understanding"),Bytes.toBytes("80"));
//            put.addColumn(Bytes.toBytes("score"),Bytes.toBytes("programming"),Bytes.toBytes("90"));
//            table.put(put);
//        }catch (Exception e){
//            e.printStackTrace();
//            return false;
//        }
//        return  true;
//    }
//
//    public boolean deleteRow(String rowkey){
//        try( Table table = conn.getTable(myhbasetablename)){
//            Delete delete = new Delete(Bytes.toBytes(rowkey));
//            table.delete(delete);
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        return true;
//    }
//
//
//    protected String getRowKey()
//    {
//        //Date date = new Date();
//        return "liuyang";
//    }
//
//    public void getRow(String rowkey){
//        try( Table table = conn.getTable(myhbasetablename)){
//            Get get = new Get(Bytes.toBytes(rowkey));
//            Result result = table.get(get);
//            System.out.println("Get: " + resultToMap(result).toString());
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//    }
//
//    public static Map<String, Object> resultToMap(Result result) {
//        Map<String, Object> resMap = new HashMap<String, Object>();
//        List<Cell> listCell = result.listCells();
//        Map<String, Object> tempMap = new HashMap<String, Object>();
//        String rowname = "";
//        List<String> familynamelist = new ArrayList<String>();
//        for (Cell cell : listCell) {
//            byte[] rowArray = cell.getRowArray();
//            byte[] familyArray = cell.getFamilyArray();
//            byte[] qualifierArray = cell.getQualifierArray();
//            byte[] valueArray = cell.getValueArray();
//            int rowoffset = cell.getRowOffset();
//            int familyoffset = cell.getFamilyOffset();
//            int qualifieroffset = cell.getQualifierOffset();
//            int valueoffset = cell.getValueOffset();
//            int rowlength = cell.getRowLength();
//            int familylength = cell.getFamilyLength();
//            int qualifierlength = cell.getQualifierLength();
//            int valuelength = cell.getValueLength();
//
//            byte[] temprowarray = new byte[rowlength];
//            System.arraycopy(rowArray, rowoffset, temprowarray, 0, rowlength);
//            String temprow = Bytes.toString(temprowarray);
////            System.out.println(Bytes.toString(temprowarray));
//
//            byte[] tempqulifierarray = new byte[qualifierlength];
//            System.arraycopy(qualifierArray, qualifieroffset, tempqulifierarray, 0, qualifierlength);
//            String tempqulifier = Bytes.toString(tempqulifierarray);
////            System.out.println(Bytes.toString(tempqulifierarray));
//
//            byte[] tempfamilyarray = new byte[familylength];
//            System.arraycopy(familyArray, familyoffset, tempfamilyarray, 0, familylength);
//            String tempfamily = Bytes.toString(tempfamilyarray);
////            System.out.println(Bytes.toString(tempfamilyarray));
//
//            byte[] tempvaluearray = new byte[valuelength];
//            System.arraycopy(valueArray, valueoffset, tempvaluearray, 0, valuelength);
//            String tempvalue = Bytes.toString(tempvaluearray);
////            System.out.println(Bytes.toString(tempvaluearray));
//
//
//            tempMap.put(tempfamily + ":" + tempqulifier, tempvalue);
////            long t= cell.getTimestamp();
////            tempMap.put("timestamp",t);
//            rowname = temprow;
//            String familyname = tempfamily;
//            if (familynamelist.indexOf(familyname) < 0) {
//                familynamelist.add(familyname);
//            }
//        }
//        resMap.put("rowname", rowname);
//        for (String familyname : familynamelist) {
//            HashMap<String, Object> tempFilterMap = new HashMap<String, Object>();
//            for (String key : tempMap.keySet()) {
//                String[] keyArray = key.split(":");
//                if (keyArray[0].equals(familyname)) {
//                    tempFilterMap.put(keyArray[1], tempMap.get(key));
//                }
//            }
//            resMap.put(familyname, tempFilterMap);
//        }
//
//        return resMap;
//    }
//
//    public static void main(String[] args) throws IOException{
//        myHbaseJob myHbaseJob = new myHbaseJob();
//        myHbaseJob.init();
////        myHbaseJob.create();
////        myHbaseJob.deleteRow(myHbaseJob.getRowKey());
////        myHbaseJob.putData();
//        myHbaseJob.getRow(myHbaseJob.getRowKey());
//    }
}
