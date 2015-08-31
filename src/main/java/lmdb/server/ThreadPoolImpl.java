package lmdb.server;

import org.eclipse.jetty.util.thread.QueuedThreadPool;

public class ThreadPoolImpl implements ThreadPool {

    private QueuedThreadPool threadPool;

    public ThreadPoolImpl(QueuedThreadPool queuedThreadPool) {
        threadPool = queuedThreadPool;
    }

    @Override
    public void execute(Runnable worker) {
        threadPool.execute(worker);
    }

    @Override
    public void start() throws Exception {
        if(!threadPool.isStarted()) threadPool.start();
    }
}
