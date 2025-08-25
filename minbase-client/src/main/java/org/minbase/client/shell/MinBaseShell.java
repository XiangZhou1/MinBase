package org.minbase.client.shell;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.minbase.client.MinClient;
import org.minbase.common.conf.Configuration;
import org.minbase.common.exception.ServerException;
import org.minbase.common.exception.TableNotExistException;
import org.minbase.common.exception.TransactionNotExistException;
import org.minbase.common.table.Row;
import org.minbase.common.table.TableInfo;
import org.minbase.common.table.TxTable;
import org.minbase.common.table.op.ColumnValues;
import org.minbase.common.table.op.Delete;
import org.minbase.common.table.op.Get;
import org.minbase.common.table.transaction.Transaction;
import org.minbase.common.utils.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class MinBaseShell {
    private static final Logger LOG = LoggerFactory.getLogger(MinBaseShell.class);
    private static MinClient client;

    private static Transaction transaction;

    public static void main(String[] args) throws IOException {
        Configuration configuration = null;
        if (args == null || args.length == 0) {
            configuration = new Configuration();
        } else {
            String configFile = args[0];
            LOG.info("Load config file:{}", args[0]);
            configuration = new Configuration(new File(configFile));
        }
        String ip = configuration.get("server.ip", "127.0.0.1");
        int port = configuration.getInt("server.port", 4444);
        client = new MinClient(ip, port);

        System.out.println("Welcome to MinBase Command Line Interface.");
        System.out.println("Type 'help' for a list of commands, 'exit' to quit.");

        try (Terminal terminal = TerminalBuilder.builder().system(true).build()) {
            LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .build();

            while (true) {
                String prompt = "minbase> ";
                if (transaction != null) {
                    prompt = "minbase(tx:" + transaction.txId() + ")> ";
                }

                String line = null;
                try {
                    // reader.readLine 会处理所有按键, 包括方向键和历史
                    line = reader.readLine(prompt);
                } catch (UserInterruptException e) {
                    // 用户按下了 Ctrl+C
                    exit();
                    continue;
                } catch (EndOfFileException e) {
                    // 用户按下了 Ctrl+D
                    exit();
                    continue;
                }

                if (line == null || line.trim().isEmpty()) {
                    continue;
                }

                // 使用正则表达式按一个或多个空白符分割，更健壮
                String[] parts = line.trim().split("\\s+");
                String command = parts[0].toLowerCase(); // 命令不区分大小写

                try {
                    handleCommand(command, parts);
                } catch (TableNotExistException e) {
                    System.err.println("Table not exist");
                }  catch (TransactionNotExistException e) {
                    System.err.println("Transaction not exist");
                } catch (Exception e) {
                    System.err.println("An error occurred: " + e.getMessage());
                    // e.printStackTrace(); // 在调试时可以打开
                }
            }
        }

    }

    private static void handleCommand(String command, String[] parts) throws IOException {
        switch (command) {
            case "createtable":
                creatTable(parts);
                break;

            case "droptable":
                dropTable(parts);
                break;

            case "put":
                put(parts);
                break;

            case "get":
                get(parts);
                break;

            case "delete":
                delete(parts);
                break;
            case "scan":
                scan(parts);
                break;

            case "checkandput":
                chechAndPut(parts);
                break;

            case "listtable":
                listTable(parts);
                break;

            case "begintransaction":
                beginTransaction(parts);
                break;

            case "commit":
                commit(parts);
                break;

            case "rollback":
                rollBack(parts);
                break;

            case "help":
                printHelp();
                break;

            case "exit":
            case "quit":
                exit();
                break;

            default:
                System.err.println("Unknown command: '" + command + "'. Type 'help' for assistance.");
                break;
        }
    }

    private static void scan(String[] parts) throws ServerException, TableNotExistException {
        if (transaction != null) {
            System.err.println("Can not scan in transaction");
            return;
        }

        if (parts.length < 2) {
            System.err.println("Usage: scan <tableName> [<startKey>] [<endKey>] [<rowCountLimit>]");
            return;
        }

        String tableName = parts[1];
        String startKey = null;
        String endKey = null;
        int rowCountLitmi = 10000;
        if (parts.length >= 3) {
            startKey = parts[2];
        }
        if (parts.length >= 4) {
            endKey = parts[3];
        }
        if (parts.length >= 5) {
            rowCountLitmi = Integer.parseInt(parts[4]);
        }
        List<Row> scanResult = client.getTable(tableName).scan(startKey, endKey, rowCountLitmi);


        // 定义表头
        List<String> headers = Arrays.asList("KEY", "COLUMN:VALUE");

        // 将 Map 转换为 List<List<String>> 以便打印
        List<List<String>> printRows = new ArrayList<>();
        for (Row row : scanResult) {
            Set<Map.Entry<byte[], byte[]>> entries = row.getColumnValues().getColumnValues().entrySet();
            StringBuilder columnValues = null;
            for (Map.Entry<byte[], byte[]> entry : entries) {
                String columnValue = new String(entry.getKey()) + ":" + new String(entry.getValue());
                if (columnValues == null) {
                    columnValues = new StringBuilder(columnValue);
                } else {
                    columnValues.append(",").append(columnValue);
                }
            }
            printRows.add(Arrays.asList(row.getRowKey(), columnValues == null ? "null" : columnValues.toString()));
        }

        CliTablePrinter.printTable(headers, printRows);
    }

    private static void exit() {
        if (transaction != null) {
            System.out.println("Can not exit MinBase CLI, transaction exist");
        } else {
            client.close();
            System.out.println("Exiting MinBase CLI...");
            System.exit(0);
        }
    }

    private static void rollBack(String[] parts) throws IOException {
        if (parts.length != 1) {
            System.err.println("Usage: rollback");
            return;
        }
        if (transaction == null) {
            System.out.println("No transaction to rollBack");
            return;
        }
        try {
            transaction.rollback();
            System.out.println("Transaction " + transaction.txId() + " rollback success");
        } finally {
            transaction = null;
        }
    }

    private static void commit(String[] parts) throws IOException {
        if (parts.length != 1) {
            System.err.println("Usage: commit");
            return;
        }
        if (transaction == null) {
            System.out.println("No transaction to commit");
            return;
        }
        try {
            transaction.commit();
            System.out.println("Transaction " + transaction.txId() + " commit success");
        } catch (Exception e) {
            transaction.rollback();
            System.out.println("Transaction " + transaction.txId() + " commit failed, rollback success");
        } finally {
            transaction = null;
        }
    }

    private static void beginTransaction(String[] parts) throws TransactionNotExistException {
        if (parts.length != 1) {
            System.err.println("Usage: beginTransaction");
            return;
        }
        if (transaction != null) {
            System.out.println("Can not beginTransaction, already in transaction " + transaction.txId());
            return;
        }
        Transaction transaction1 = client.beginTransaction();
        if (transaction1 == null) {
            System.out.println("BeginTransaction failed");
        } else {
            transaction = transaction1;
            System.out.println("Transaction started with ID: " + transaction.txId());
        }
    }

    private static void listTable(String[] parts) {
        if (parts.length != 1) {
            System.err.println("Usage: listTable");
            return;
        }

        List<TableInfo> tableInfos = client.listTables();
        if (tableInfos == null || tableInfos.isEmpty()) {
            System.out.println("Empty table");
        } else {
            System.out.println("Tables:");
            // 定义表头
            List<String> headers = Arrays.asList("TABLE", "COLUMNS");

            // 将 Map 转换为 List<List<String>> 以便打印
            List<List<String>> rows = new ArrayList<>();
            for (TableInfo tableInfo : tableInfos) {
                rows.add(Arrays.asList(tableInfo.getName(), String.join(",", tableInfo.getColumns())));
            }
            // 调用我们的新工具来打印表格
            CliTablePrinter.printTable(headers, rows);
        }
    }

    private static void chechAndPut(String[] parts) throws ServerException, TableNotExistException, TransactionNotExistException {
        if (parts.length != 8) {
            System.err.println("Usage: checkAndPut <tableName> <checkKey> <checkColumn> <checkValue> <putKey> <putColumn> <putValue>");
            return;
        }
        boolean success = false;
        if (transaction == null) {
            success = client.getTable(parts[1]).checkAndPut(parts[2], parts[3], parts[4], parts[5], parts[6], parts[7]);
        } else {
            TxTable table = transaction.getTable(parts[1]);
            success = table.checkAndPut(parts[2], parts[3], parts[4], parts[5], parts[6], parts[7]);
        }
        System.out.println("CheckAndPut " + (success ? "success" : "failed"));
    }

    private static void delete(String[] parts) throws ServerException, TableNotExistException, TransactionNotExistException {
        if (parts.length < 3) {
            System.err.println("Usage: delete <tableName> <key> [<column>...]");
            return;
        }

        String tableName = parts[1];
        String key = parts[2];
        Delete delete = new Delete(ByteUtil.toBytes(key));

        for (int i = 3; i < parts.length; i++) {
            delete.addColumn(ByteUtil.toBytes(parts[i]));
        }

        if (transaction == null) {
            client.getTable(tableName).delete(delete);
        } else {
            TxTable table = transaction.getTable(tableName);
            table.delete(delete);
        }

        System.out.println("Delete success");
    }

    private static void get(String[] parts) throws TableNotExistException, ServerException, TransactionNotExistException {
        if (parts.length < 3) {
            System.err.println("Usage: get <tableName> <key> [<column>...]");
            return;
        }

        ColumnValues columnValues = null;
        String tableName = parts[1];
        String key = parts[2];
        Get get = new Get(ByteUtil.toBytes(key));

        if (parts.length >= 4) {
            for (int i = 3; i < parts.length; i++) {
                get.addColumn(ByteUtil.toBytes(parts[i]));
            }
        }

        if (transaction == null) {
            columnValues = client.getTable(tableName).get(get);
        } else {
            TxTable table = transaction.getTable(tableName);
            columnValues = table.get(get);
        }

        // 定义表头
        List<String> headers = Arrays.asList("COLUMN", "VALUE");

        // 将 Map 转换为 List<List<String>> 以便打印
        TreeMap<byte[], byte[]> columnValuesMap = columnValues.getColumnValues();
        List<List<String>> rows = new ArrayList<>();
        if (columnValuesMap != null && !columnValuesMap.isEmpty()) {
            for (Map.Entry<byte[], byte[]> entry : columnValuesMap.entrySet()) {
                rows.add(Arrays.asList(new String(entry.getKey()), new String(entry.getValue())));
            }
        }

        // 调用我们的新工具来打印表格
        CliTablePrinter.printTable(headers, rows);
    }

    private static void put(String[] parts) throws TableNotExistException, ServerException, TransactionNotExistException {
        if (parts.length != 5) {
            System.err.println("Usage: put <tableName> <key> <column> <value>");
            return;
        }
        String tableName3 = parts[1];
        String key1 = parts[2];
        String column1 = parts[3];
        String value1 = parts[4];
        if (transaction == null) {
            client.getTable(tableName3).put(key1, column1, value1);
        } else {
            TxTable table = transaction.getTable(tableName3);
            table.put(key1, column1, value1);
        }
        System.out.println("Put success");
    }

    private static void creatTable(String[] parts) {
        if (parts.length != 2) {
            System.err.println("Usage: createTable <tableName>");
            return;
        }
        String tableName1 = parts[1];
        boolean created = client.createTable(tableName1);
        System.out.println("CreateTable " + (created ? "Success" : "Failed"));
    }

    private static void dropTable(String[] parts) {
        if (parts.length != 2) {
            System.err.println("Usage: dropTable <tableName>");
            return;
        }
        String tableName2 = parts[1];
        boolean dropped = client.dropTable(tableName2);
        System.out.println("DropTable: " + (dropped ? "Success" : "Failed"));
    }

    private static void printHelp() {
        System.out.println("\nMinBase CLI Commands:");
        System.out.println("---------------------");
        System.out.println("  Table Operations:");
        System.out.println("    createTable <tableName>");
        System.out.println("    dropTable <tableName>");
        System.out.println("    listTable");
        System.out.println("\n  Data Operations:");
        System.out.println("    put <tableName> <key> <column> <value>");
        System.out.println("    get <tableName> <key>");
        System.out.println("    get <tableName> <key> <column>...");
        System.out.println("    delete <tableName> <key>");
        System.out.println("    delete <tableName> <key> <column>...");
        System.out.println("    scan <tableName> [<startKey>] [<endKey>] [<rowCountLimit>]");
        System.out.println("    checkAndPut <tableName> <checkKey> <checkCol> <checkVal> <putKey> <putCol> <putVal>");
        System.out.println("\n  Transaction Operations:");
        System.out.println("    beginTransaction");
        System.out.println("    commit");
        System.out.println("    rollback");
        System.out.println("\n  Other Commands:");
        System.out.println("    help         - Shows this help message.");
        System.out.println("    exit | quit  - Exits the CLI.");
        System.out.println("---------------------\n");
    }
}
