package listart.hbase.entity;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Student {
    public static final String CF_NAME = "name";
    public static final String CF_INFO = "info";
    public static final String CF_SCORE = "score";
    public static final String C_INFO_STUDENT_ID = "student_id";
    public static final String C_INFO_CLASS = "class";
    public static final String C_SCORE_UNDERSTANDING = "understanding";
    public static final String C_SCORE_PROGRAMMING = "programming";

    private final String rowKey;
    private final String id;
    private final String name;
    private final Integer classNum;
    private final Integer understandingScore;
    private final Integer programmingScore;

    public Student(String rowKey, String id, String name, Integer classNum, Integer understandingScore, Integer programmingScore) {
        this.rowKey = rowKey;
        this.id = id;
        this.name = name;
        this.classNum = classNum;
        this.understandingScore = understandingScore;
        this.programmingScore = programmingScore;
    }

    public Student(String id, String name, Integer classNum, Integer understandingScore, Integer programmingScore) {
        // row key = reversed(student id)
        this.rowKey = new StringBuffer(id).reverse().toString();

        this.id = id;
        this.name = name;
        this.classNum = classNum;
        this.understandingScore = understandingScore;
        this.programmingScore = programmingScore;
    }

    public String getRowKey() {
        return rowKey;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Integer getClassNum() {
        return classNum;
    }

    public Integer getUnderstandingScore() {
        return understandingScore;
    }

    public Integer getProgrammingScore() {
        return programmingScore;
    }

    @Override
    public String toString() {
        return "Student{" +
                "rowKey='" + rowKey + '\'' +
                ", name='" + name + '\'' +
                ", id='" + id + '\'' +
                ", classNum=" + classNum +
                ", understandingScore=" + understandingScore +
                ", programmingScore=" + programmingScore +
                '}';
    }

    private static byte [] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public void save(Table table) throws IOException {
        Put put = new Put(utf8(rowKey));

        // name:
        put.addColumn(utf8(CF_NAME), null, utf8(name));
        // info:student_id
        put.addColumn(utf8(CF_INFO), utf8(C_INFO_STUDENT_ID), utf8(id));
        // info:student_class
        if (classNum != null)
            put.addColumn(utf8(CF_INFO), utf8(C_INFO_CLASS), utf8(classNum.toString()));
        // score:understanding
        if (understandingScore != null)
            put.addColumn(utf8(CF_SCORE), utf8(C_SCORE_UNDERSTANDING), utf8(understandingScore.toString()));
        // score:programming
        if (programmingScore != null)
            put.addColumn(utf8(CF_SCORE), utf8(C_SCORE_PROGRAMMING), utf8(programmingScore.toString()));

        // save to table
        table.put(put);

        System.out.println("Saved " + this);
    }

    private static Student parseResult(Result result) {
        String rowKey = Bytes.toString(result.getRow());
        String id = null;
        String name = null;
        Integer classNum = null;
        Integer understandingScore = null;
        Integer programmingScore = null;

        for (Cell cell : result.rawCells()) {
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));

            if (CF_NAME.equals(family)) {
                name = value;
            } else if (C_INFO_STUDENT_ID.equals(qualifier)) {
                id = value;
            } else if (C_INFO_CLASS.equals(qualifier) ) {
                classNum = Integer.parseInt(value);
            } else if (C_SCORE_UNDERSTANDING.equals(qualifier)) {
                understandingScore = Integer.parseInt(value);
            } else if (C_SCORE_PROGRAMMING.equals(qualifier)) {
                programmingScore = Integer.parseInt(value);
            } else {
                System.out.println("Unknown cell: " + cell);
            }
        }

        return new Student(rowKey, id, name, classNum, understandingScore, programmingScore);
    }

    public static Student get(Table table, String rowKey) throws IOException {
        Result result = table.get(new Get(utf8(rowKey)));

        if (result.isEmpty()) {
            System.out.println("Load " + rowKey + " Failed.");
            return null;
        }

        return parseResult(result);
    }

    public static List<Student> scan(Table table) throws IOException {
        ResultScanner scanner = table.getScanner(new Scan());

        return StreamSupport.stream(scanner.spliterator(), false)
                .map(Student::parseResult)
                .collect(Collectors.toList());
    }

    public void delete(Table table) throws IOException {
        table.delete(new Delete(utf8(rowKey)));
    }
}
