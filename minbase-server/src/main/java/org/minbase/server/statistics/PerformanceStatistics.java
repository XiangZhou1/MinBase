package org.minbase.server.statistics;

import org.minbase.server.MinBaseServer;
import org.minbase.server.kv.Key;
import org.minbase.server.kv.storage.StoreFile;
import org.minbase.server.table.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class PerformanceStatistics {
    private static final Logger LOG = LoggerFactory.getLogger(MinBaseServer.class);
    public static ExecutorService STATICS_PRINT_SERVICE = Executors.newSingleThreadExecutor();
    public static AtomicLong TASK_ID = new AtomicLong(0);
    public static boolean ENABLE_RECODE_TIME = true;

    public static  ConcurrentHashMap<Long, DoingTask> DOING_TASKS = new ConcurrentHashMap<>();

    public static final int TASK_TIME_OUT = 3 * 1000;

    static {
        STATICS_PRINT_SERVICE.submit(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    for (Map.Entry<Long, DoingTask> entry : DOING_TASKS.entrySet()) {
                        DoingTask doingTask = entry.getValue();
                        long spendTime = System.currentTimeMillis() - doingTask.startTime;
                        if (spendTime > TASK_TIME_OUT) {
                            LOG.warn("Timeut taskId:{}, op:{}, spendTime:{}, taskInfo:{}", doingTask.taskId, doingTask.op, spendTime, doingTask.infoGenerator.info());
                        }
                    }
                    try {
                        Thread.sleep(TASK_TIME_OUT);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    public static StatInfoGenerator getUpdateStoreFileIteratorByCompactGenerator(List<StoreFile> toDelete) {
        return new StatInfoGenerator() {
            @Override
            public String info() {
                StringBuilder sb = new StringBuilder();
                sb.append("Update storfile iterstors, deleteFiles:");
                for (StoreFile storeFile : toDelete) {
                    sb.append(storeFile).append(", ");
                }
                return sb.toString();
            }
        };
    }

    public static StatInfoGenerator getCommitGenerator(Transaction transaction) {
        return new StatInfoGenerator() {
            @Override
            public String info() {
                return transaction.toString();
            }
        };
    }

    public enum Op {
        UPDATE_STOR_FILE_ITERSTOR_BY_COMPACT,
        CREATE_STOR_FILE_ITERATOR,
        COMMIT
    }

    public static StatInfoGenerator getCreateStoreFileIteratorGenerator(Key startKey, Key endKey) {
        return new StatInfoGenerator() {
            @Override
            public String info() {
                return "Create storfile iterstor, startKey:" + startKey + ", endKey:" + endKey;
            }
        };
    }

    public static StatInfoGenerator getUpdateStoreFileIteratorByCompactGenerator(Key startKey, Key endKey) {
        return new StatInfoGenerator() {
            @Override
            public String info() {
                return "Create storfile iterstor, startKey:" + startKey + ", endKey:" + endKey;
            }
        };
    }




    public static class DoingTask {
        long startTime = 0;
        long taskId = 0;
        StatInfoGenerator infoGenerator = null;
        Op op;
        public DoingTask(long taskId, Op op, StatInfoGenerator infoGenerator) {
            this.taskId = taskId;
            this.infoGenerator = infoGenerator;
            this.op = op;
            this.startTime = System.currentTimeMillis();
        }
    }

    public static long recordDoingTask(Op op, StatInfoGenerator infoGenerator) {
        if (ENABLE_RECODE_TIME) {
            long taskId = TASK_ID.incrementAndGet();
            DoingTask doingTask = new DoingTask(taskId, op, infoGenerator);
            DOING_TASKS.put(taskId, doingTask);
            return taskId;
        } else {
            return -1;
        }
    }

    public static void removeDoingTask(long taskId) {
        if (taskId != -1) {
            DOING_TASKS.remove(taskId);
        }
    }


    public static class Stat {
        LongAdder time = new LongAdder();
        LongAdder count = new LongAdder();
    }
}
