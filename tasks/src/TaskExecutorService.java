import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    // If the tasks are I/O Bound, this number could be increased
    private final int DEFAULT_CORE_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private final long DEFAULT_KEEP_ALIVE_TIME = 10l;
    private final int QUEUE_CAPACITY = 5000;
    // Maintains task' future in the order task were submitted
    LinkedBlockingDeque<Future> queue;
    ExecutorService executor;
    // Poll tasks from the queue and dispatch it to the executor.
    private Thread taskDispatcher;
    private volatile boolean isRunning;

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
        Future<T> future = new TaskGroupFuture<T>(task.taskUUID(), task.taskAction(), task.taskGroup());
        queue.add(future);
        return future;
    }

    private <T> void execute(FutureTask<T> futureTask) {
        try {
            executor.submit(futureTask);
        }catch (RejectedExecutionException ex) {
            if(!executor.isShutdown()){
                System.out.println("Queue Space Full: Failed to execute following task: "+((TaskGroupFuture)futureTask).taskId);
            }else {
                System.out.println("Executor Shutdown: Failed to execute following task: "+((TaskGroupFuture)futureTask).taskId);
            }
        }
    }

    private void start() {
        while(isRunning) {
            TaskGroupFuture future = null;
            try {
                future = (TaskGroupFuture) queue.take();
            } catch (InterruptedException e) {
                System.out.println("Interrupted: Shutdown event occurred.");
                continue;
            }
            if(activeTaskGroups.putIfAbsent(future.group, future.taskId) == null){
                System.out.println("Starting following TaskID: {"+future.taskId+"}, GroupId: {"+future.group.groupUUID()+"}");
                this.execute(future);
            }else {
                /**
                 * If a task with same group id is already being executed, put this task again at front of the queue
                 * Yes it will lead to starvation for other tasks that are in queue but the requirement says
                 * we have to process task in the order they are submitted.
                 */
                System.out.println("A task with same group already being executed");
                queue.addFirst(future);
            }
        }
    }

    public void stop() {
        isRunning = false;
        taskDispatcher.interrupt();
        executor.shutdown();
        try {
            if(executor.awaitTermination(60, TimeUnit.MILLISECONDS)){
                System.out.print("Terminated Gracefully with execution of all pending tasks.");
            }else {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private class TaskGroupFuture<V> extends FutureTask<V> {
        private final TaskGroup group;
        private final UUID taskId;
        TaskGroupFuture(
            UUID taskId,
            Callable task,
            TaskGroup group) {
            super(task);
            this.group = group;
            this.taskId = taskId;
        }
        @Override
        protected void done() {
            activeTaskGroups.remove(group);
        }
    }
}
