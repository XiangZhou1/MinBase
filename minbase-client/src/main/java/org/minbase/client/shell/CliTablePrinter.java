package org.minbase.client.shell;

import java.util.ArrayList;
import java.util.List;

/**
 * 一个用于在命令行打印文本表格的工具类.
 */
public class CliTablePrinter {

    /**
     * 打印一个文本表格.
     *
     * @param headers 表头列表.
     * @param rows    数据行列表, 每个内部列表代表一行.
     */
    public static void printTable(List<String> headers, List<List<String>> rows) {
        if (headers == null || headers.isEmpty()) {
            System.err.println("Table headers cannot be null or empty.");
            return;
        }

        // 1. 计算每一列的最大宽度
        int[] columnWidths = new int[headers.size()];
        for (int i = 0; i < headers.size(); i++) {
            columnWidths[i] = headers.get(i).length();
        }

        for (List<String> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                if (row.get(i).length() > columnWidths[i]) {
                    columnWidths[i] = row.get(i).length();
                }
            }
        }

        // 2. 打印上边框
        printHorizontalLine(columnWidths);

        // 3. 打印表头
        System.out.print("|");
        for (int i = 0; i < headers.size(); i++) {
            String format = " %-" + columnWidths[i] + "s |";
            System.out.printf(format, headers.get(i));
        }
        System.out.println();

        // 4. 打印表头和数据之间的分隔线
        printHorizontalLine(columnWidths);

        // 5. 打印数据行
        if (rows.isEmpty()) {
            System.out.print("|");
            // 创建一个跨所有列的提示信息
            int totalWidth = -1; // -1 for the initial '|'
            for (int width : columnWidths) {
                totalWidth += width + 3; // width + 2 spaces + 1 '|'
            }
            String emptyMessage = "(0 rows)";
            String format = " %-" + totalWidth + "s|";
            System.out.printf(format, emptyMessage);
            System.out.println();
        } else {
            for (List<String> row : rows) {
                System.out.print("|");
                for (int i = 0; i < row.size(); i++) {
                    String format = " %-" + columnWidths[i] + "s |";
                    System.out.printf(format, row.get(i));
                }
                System.out.println();
            }
        }


        // 6. 打印下边框
        printHorizontalLine(columnWidths);
    }

    private static void printHorizontalLine(int[] columnWidths) {
        System.out.print("+");
        for (int width : columnWidths) {
            for (int i = 0; i < width + 2; i++) { // +2 for padding
                System.out.print("-");
            }
            System.out.print("+");
        }
        System.out.println();
    }
}