package main.java;

public class TestClass {
    public static void main(String[] args) {
        String line1 = "ALEXEY 1\n";
        String line2 = "ALEXANDROVICH 1\n";
        String line3 = "TITORENKO 1\n";
        String lines = line1.concat(line2).concat(line3);
        int begin = 0;
        int end = 0;
        while (end != lines.length() - 1) {
            end = lines.indexOf('\n', begin);
            System.out.printf(lines.substring(begin, end - 1));
            begin = end+1;
        }
    }
}
