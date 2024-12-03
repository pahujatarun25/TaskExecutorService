import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tasks can be submitted concurrently. Task submission should not block the submitter.
 * 2. Tasks are executed asynchronously and concurrently. Maximum allowed concurrency
 * may be restricted.
 * 3. Once task is finished, its results can be retrieved from the Future received during task
 * submission.
 * 4. The order of tasks must be preserved.
 * o The first task submitted must be the first task started.
 * o The task result should be available as soon as possible after the task completes.
 * 5. Tasks sharing the same TaskGroup must not run concurrently.
 */
public class TaskExecutorService implements TaskExecutor{

    /**
     * In general we can use following matrix to find optimal number of threads
     * Ncpu = Number of CPU
     * Ucpu = Target CPU utilization
     * W/C = Ration of waiting time to computing time
     *
     * Nthreads = Ncpu * Ucpu * (1 + W/C)
     * For this application, Assuming all tasks are CPU bound ( W/C = 0)
     * Hence Nthreads = Ncpu * Ucpu
     */
    private final int DEFAULT_CORE_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private final long DEFAULT_KEEP_ALIVE_TIME = 10l;
    private final int QUEUE_CAPACITY = 5000;
    // Maintains task' future in the order task were submitted
    LinkedBlockingDeque<FutureTask> queue;
    ExecutorService executor;
    // Poll tasks from the queue and dispatch it to the executor.
    private Thread taskDispatcher;
    private volatile boolean isRunning;
    private Lock startLock = new ReentrantLock(true);
    private Condition startCondition = startLock.newCondition();

    private volatile boolean canStart = true;
    private ConcurrentHashMap<UUID, Lock> groupLocks = new ConcurrentHashMap<>();


    // Tracks Groups of the tasks that are currently being executed
    ConcurrentHashMap<TaskGroup, UUID> activeTaskGroups = new ConcurrentHashMap<>();
    public TaskExecutorService(int maxConcurrentExecution, long keepAliveTime) {
        queue = new LinkedBlockingDeque<>();
        if(maxConcurrentExecution <= 0 || DEFAULT_CORE_POOL_SIZE > maxConcurrentExecution) {
            maxConcurrentExecution = DEFAULT_CORE_POOL_SIZE;
        }
        if(keepAliveTime < 0) {
            keepAliveTime = DEFAULT_KEEP_ALIVE_TIME;
        }
        executor = new ThreadPoolExecutor(
            DEFAULT_CORE_POOL_SIZE,
            maxConcurrentExecution,
            keepAliveTime,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(QUEUE_CAPACITY)
        );
        if(!isRunning) {
            isRunning = true;
            taskDispatcher = new Thread( () -> {start();});
            taskDispatcher.start();
        }
    }
    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        Objects.requireNonNull(task);
        FutureTask<T> future = getFuture(task);
        queue.add(future);
        return future;
    }

    private <T> FutureTask<T> getFuture(Task<T> task) {
        groupLocks.putIfAbsent(task.taskGroup().groupUUID(), new ReentrantLock());
        return new FutureTask<T>(() -> {
            System.out.println("Starting Task: "+ task.taskUUID());
            signalToStartTask(task);
            try {
                groupLocks.get(task.taskGroup().groupUUID()).lock();
                return task.taskAction().call();
            } finally {
                System.out.println("Group Lock is Free: "+task.taskGroup().groupUUID());
                groupLocks.get(task.taskGroup().groupUUID()).unlock();
            }
        });
    }

    private <T> void signalToStartTask(Task<T> task) throws InterruptedException {
        try{
            startLock.lock();
            canStart = true;
            startCondition.signal();
        }finally{
            startLock.unlock();
        }
    }

    private <T> void execute(FutureTask<T> futureTask) throws InterruptedException {
        waitForTaskToStart(futureTask);
        try {
            executor.submit(futureTask);
        }catch (RejectedExecutionException ex) {
            if(!executor.isShutdown()){
                System.out.println("Queue Space Full");
            }else {
                System.out.println("Executor Shutdown");
            }
        }
    }

    private void waitForTaskToStart(FutureTask futureTask) throws InterruptedException {
        startLock.lock();
        try{
            while(!canStart) {
                startCondition.await();
            }
            canStart = false;
        }finally{
            startLock.unlock();
        }
    }

    private void start() {
        while(isRunning) {
            try {
                FutureTask futureTask = queue.take();
                this.execute(futureTask);
            } catch (InterruptedException e) {
                System.out.println("Interrupted: Shutdown event occurred.");
            }
        }
    }

    public void stop() {
        isRunning = false;
        boolean interrupted = false;
        taskDispatcher.interrupt();
        executor.shutdown();
        try {
            if(executor.awaitTermination(60, TimeUnit.MILLISECONDS)){
                System.out.print("Terminated Gracefully with execution of all pending tasks.");
            }else {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            interrupted = true;
        }finally {
            //set the flag so that code higher up in the hierarchy could take a decision.
            if (interrupted) {
               Thread.currentThread().interrupt();
            }
        }
    }
}
