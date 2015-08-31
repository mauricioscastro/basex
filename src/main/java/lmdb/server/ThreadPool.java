package lmdb.server;

public interface ThreadPool {
    void execute(Runnable worker);
    void start() throws Exception;
}
