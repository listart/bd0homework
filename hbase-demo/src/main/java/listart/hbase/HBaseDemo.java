package listart.hbase;

import listart.hbase.entity.Student;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class HBaseDemo {
    public static final String STUDENT_NAME = "Listart";
    public static final String STUDENT_ID = "20200579010082";

    public static final String NAMESPACE = STUDENT_NAME.toLowerCase();
    public static final String TABLE_NAME = NAMESPACE + ":student";

    public static final List<Student> STUDENTS = Arrays.asList(
        new Student("20210000000001", "Tom", 1, 75, 82),
        new Student("20210000000002", "Jerry", 1, 85, 67),
        new Student("20210000000003", "Jack", 2, 80, 80),
        new Student("20210000000004", "Rose", 2, 60, 61),
        new Student(STUDENT_ID, STUDENT_NAME, 1, null, null)
    );

    private static void createOrOverwrite(Admin admin, TableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            try {
                admin.disableTable(table.getTableName());
            } catch (TableNotEnabledException e) {
                System.out.println(e.getClass().getCanonicalName());
            }

            admin.deleteTable(table.getTableName());
        }

        // create namespace
        try {
            admin.deleteNamespace(NAMESPACE);
        } catch (NamespaceNotFoundException e) {
            System.out.println(e.getClass().getCanonicalName());
        }
        admin.createNamespace(NamespaceDescriptor.create(NAMESPACE).build());

        // create table
        admin.createTable(table);
    }

    private static void createSchemaTable(Admin admin) throws IOException {
        /*HTableDescriptor, HColumnDescriptor Deprecated
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Compression.Algorithm.NONE));*/
        TableDescriptor table = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(TABLE_NAME))
                .setColumnFamilies(
                        Arrays.asList(
                                ColumnFamilyDescriptorBuilder.of(Student.CF_NAME),
                                ColumnFamilyDescriptorBuilder.of(Student.CF_INFO),
                                ColumnFamilyDescriptorBuilder.of(Student.CF_SCORE)
                        )
                )
                .build();

        System.out.println("Creating table. ");
        createOrOverwrite(admin, table);
        System.out.println("Done. ");
    }

    private static void dropSchema(Admin admin) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        if (!admin.tableExists(tableName)) {
            System.out.println("Table does not exists.");
            System.exit(-1);
        }

        // Disable an existing table
        admin.disableTable(tableName);

        // Delete an existing column family
        /* deleteColumn Deprecated
        admin.deleteColumn(tableName, CF_DEFAULT.getBytes(StandardCharsets.UTF_8));*/
        admin.deleteColumnFamily(tableName, Student.CF_NAME.getBytes(StandardCharsets.UTF_8));

        // Delete a table (Need to be disable first)
        admin.deleteTable(tableName);

        // Delete namespace
        admin.deleteNamespace(NAMESPACE);
    }

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "jikehadoop01,jikehadoop02,jikehadoop03");

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            // test namespace & table admin apis
            createSchemaTable(admin);

            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            // test put api
            puts(table);

            // test get api
            Student tom = STUDENTS.get(0);
            get(table, tom.getRowKey());
            Student me = STUDENTS.get(STUDENTS.size() - 1);
            get(table, me.getRowKey());

            // test scan api
            scan(table);

            // test delete api
            delete(table, tom);
            scan(table);

            // test namespace & table admin apis
            dropSchema(admin);
        }
    }

    private static void delete(Table table, Student tom) throws IOException {
        System.out.println("Deleting tom = " + tom);
        tom.delete(table);
        System.out.println("Done.");
    }

    private static void scan(Table table) throws IOException {
        List<Student> students = Student.scan(table);

        for (Student student : students) {
            System.out.println("Scan " + student);
        }
    }

    private static void get(Table table, String rowKey) throws IOException {
        Student got = Student.get(table, rowKey);
        System.out.println("Got " + got);
    }

    private static void puts(Table table) throws IOException {
        for (Student student : STUDENTS) {
            student.save(table);
        }
    }
}
