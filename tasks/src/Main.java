import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Main {
    static List<Task> tasks = new ArrayList<>();
    public static void init() {
        UUID groupUUID1 = UUID.randomUUID();
        UUID groupUUID2 = UUID.randomUUID();
        UUID groupUUID3 = UUID.randomUUID();

        UUID taskUUID1 = UUID.randomUUID();
        UUID taskUUID2 = UUID.randomUUID();
        UUID taskUUID3 = UUID.randomUUID();
        UUID taskUUID4 = UUID.randomUUID();
        UUID taskUUID5 = UUID.randomUUID();
        UUID taskUUID6 = UUID.randomUUID();
        UUID taskUUID7 = UUID.randomUUID();
        UUID taskUUID8 = UUID.randomUUID();
        UUID taskUUID9 = UUID.randomUUID();
        UUID taskUUID10 = UUID.randomUUID();

        tasks.add(new Task<String>(taskUUID1,new TaskGroup(groupUUID1),TaskType.WRITE,
            () -> {
            return "Executing Following task TaskID: {"+taskUUID1+"}, GroupId: {"+groupUUID1+"}";}));
        tasks.add(new Task<String>(taskUUID2,new TaskGroup(groupUUID2),TaskType.READ,
            () -> {
            return "Executing Following task TaskID: {"+taskUUID2+"}, GroupId: {"+groupUUID2+"}";}));
        tasks.add(new Task<String>(taskUUID3,new TaskGroup(groupUUID1),TaskType.WRITE,
            () -> {
            return "Executing Following task TaskID: {"+taskUUID3+"}, GroupId: {"+groupUUID1+"}";}));
        tasks.add(new Task<String>(taskUUID4,new TaskGroup(groupUUID3),TaskType.READ,
            () -> {
            return "Executing Following task TaskID: {"+taskUUID4+"}, GroupId: {"+groupUUID3+"}";}));
        tasks.add(new Task<String>(taskUUID5,new TaskGroup(groupUUID1),TaskType.WRITE,
            () -> {
            return "Executing Following task TaskID: {"+taskUUID5+"}, GroupId: {"+groupUUID1+"}";}));
        tasks.add(new Task<String>(taskUUID6,new TaskGroup(groupUUID2),TaskType.READ,
            () -> {
            return "Executing Following task TaskID: {"+taskUUID6+"}, GroupId: {"+groupUUID2+"}";}));
        tasks.add(new Task<String>(taskUUID7,new TaskGroup(groupUUID2),TaskType.READ,
            () -> {
                return "Executing Following task TaskID: {"+taskUUID7+"}, GroupId: {"+groupUUID2+"}";}));
        tasks.add(new Task<String>(taskUUID8,new TaskGroup(groupUUID1),TaskType.READ,
            () -> {
                return "Executing Following task TaskID: {"+taskUUID8+"}, GroupId: {"+groupUUID2+"}";}));
        tasks.add(new Task<String>(taskUUID9,new TaskGroup(groupUUID2),TaskType.READ,
            () -> {
                return "Executing Following task TaskID: {"+taskUUID9+"}, GroupId: {"+groupUUID2+"}";}));
        tasks.add(new Task<String>(taskUUID10,new TaskGroup(groupUUID2),TaskType.READ,
            () -> {
                return "Executing Following task TaskID: {"+taskUUID10+"}, GroupId: {"+groupUUID2+"}";}));
    }

    public static void main(String[] args) {
        init();
       TaskExecutorService executor = new TaskExecutorService(3, 10);
        tasks.parallelStream().forEach(
            task -> {
                System.out.println("Submitting Task => "+task);
            Future future = executor.submitTask(task);
                try {
                    System.out.println("Get Result =>"+future.get());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        executor.stop();
    }

    ;

}