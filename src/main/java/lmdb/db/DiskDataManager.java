package lmdb.db;

@SuppressWarnings("unchecked")
public class DiskDataManager {
//
//    private static String DOC_SEARCH_SUFFIX = ".khtbad.data";
//    private static final UnlimitedCache ucache = new UnlimitedCache();
//    private static boolean running = false;
//    private static boolean shuting = false;
//    private static Pattern validName = Pattern.compile("^[a-zA-Z][0-9a-zA-Z.-_]*");
//    private static ThreadPool threadPool = null;
//    private static Thread unloader = null;
//    public static MainOptions options;
//
//    protected DiskDataManager() {
//        ucache.setMemoryCaching(true);
//        ucache.setUnlimitedDiskCache(false);
//        ucache.setOverflowPersistence(false);
//    }
//
//    public static void config(String basedir, ThreadPool threadPool) throws Exception {
//        System.setProperty("org.basex.path", basedir);
//        DiskDataManager.threadPool = threadPool;
//        config();
//    }
//
//    public static void config(String basedir) throws Exception {
//        config(basedir, null);
//    }
//
//    public static void config(ThreadPool threadPool) throws Exception {
//        DiskDataManager.threadPool = threadPool;
//        config();
//    }
//
//    public static void config() throws Exception {
//        if (running) return;
//        options = new MainOptions();
//        for (File f : FileUtils.listFiles(new File(home()), new String[]{"creating", "removing"}, true)) {
//            String prename = f.getName();
//            prename = prename.substring(0, prename.indexOf('.'));
//            for (File ff : fileList(f.getParent(), prename, true)) FileUtils.deleteQuietly(ff);
//        }
//        running = true;
//        shuting = false;
//        if(threadPool != null) {
//            threadPool.start();
//            threadPool.execute(new ManagedPageFileUnloader());
//        } else {
//            unloader = new Thread(new ManagedPageFileUnloader());
//            unloader.start();
//        }
//    }
//
//    public static ManagedPageFile getPageFile(File dir, String name) {
//        String key = dir+"/"+name;
//        ManagedPageFile mpf = (ManagedPageFile)ucache.get(key);
//        if(mpf == null) {
//            mpf = new ManagedPageFile(dir, name);
//            ucache.put(key,mpf);
//        }
//        return mpf;
//    }
//
//    public static String home() {
//        return FilenameUtils.normalize(options.get(MainOptions.DBPATH), true) + "/";
//    }
//
//    public static String recycle() {
//        return FilenameUtils.normalize(options.get(MainOptions.DBPATH) + "/.recycle/", true);
//    }
//
//    public static String tmp() {
//        return FilenameUtils.normalize(options.get(MainOptions.DBPATH) + "/.tmp/", true);
//    }
//
//    public static void stop() {
//        if(!running || shuting) return;
//        try {
//            shuting = true;
//            if(unloader != null) unloader.interrupt();
//            //System.err.println("interrupted");
//            for (Object o : ucache.keySet()) {
////                String doc = (String) o;
////                ManagedDiskData data = (ManagedDiskData)ucache.get(doc);
////                data.tryClose(true);
////                if (data.removed()) remove(doc);
//                String pfk = (String)o;
//                ManagedPageFile pf = (ManagedPageFile)ucache.get(pfk);
//                if(pf.isLoaded()) pf.forceUnload();
//                if(pf.isRemoved()) try { pf.delete(); } catch(Exception i) {}
//                ucache.removeForce(pfk);
//            }
//            //System.err.println("cache removed");
//            for (File f : FileUtils.listFiles(new File(home()), new String[]{"creating", "removing"}, true)) {
//                String prename = f.getName();
//                prename = prename.substring(0, prename.indexOf('.'));
//                for (File ff : fileList(f.getParent(), prename, true)) FileUtils.deleteQuietly(ff);
//            }
//            File recycleDir = new File(recycle());
//            for (String recycled : recycleDir.list(DirectoryFileFilter.INSTANCE)) {
//                if (recycled.charAt(0) == '.') {
//                    FileUtils.moveDirectoryToDirectory(new File(home() + recycled.substring(1)), new File(recycle() + recycled.substring(1) + "." +  UUID.randomUUID().toString()), true);
//                    FileUtils.forceDelete(new File(recycle() + recycled));
//                }
//            }
//            running = false;
//        } catch (Exception e) {
//            //e.printStackTrace(System.err);
//        }
//    }
//
//    public static boolean running() {
//        return running;
//    }
//
//    public static DBNode openDocument(final String doc, final MainOptions opt) throws IOException {
//        return new DBNode(open(doc, opt));
//    }
//
//    public static Data open(final String doc, final MainOptions opt) throws IOException {
//        return open(doc, false, opt);
//    }
//
//    public static boolean isCached(String doc) {
//        return ucache.contains(doc);
//    }
//
//    public static Data open(final String doc, final boolean ignoreCheckings, MainOptions opt) throws IOException {
//        //boolean openedFromCollection = ctx.getLazyLevel() < 1 ? false : true;
////        try {
////            if (!ignoreCheckings) assertRunning();
////            Data data = (Data) ucache.get(doc);
////            if (data == null) {
////                data = new ManagedDiskData(doc.trim(),opt);
////                ucache.put(doc, data);
////            }
////            data.open();
////            return data;
////        } catch (Exception e) {
////            throw new IOException(e.getMessage() + " opening document " + doc);
////        }
//        if(!documentExists(doc)) throw new IOException("document " + doc + " not found");
//        String[] arg = docRealPathInfo(doc);
//        // TODO: BXK: add DiskData pooling in respect to doc ?
//        return new DiskData(arg[0].replace(home(),""), arg[1], options, true);
//    }
//
//    public static int size() {
//        return ucache.size();
//    }
//
//    private static void assertRunning() throws IOException {
//        if (!running) throw new IOException("data manager not running!");
//    }
//
//    private static void assertName(String name) throws IOException {
//        if (name.contains("/")) {
//            String n[] = name.split("/");
//            if (!validName.matcher(n[0]).matches() || !validName.matcher(n[1]).matches())
//                throw new IOException("invalid name " + name);
//        } else {
//            if (!validName.matcher(name).matches()) throw new IOException("invalid name " + name);
//        }
//    }
//
//    public static List<String> listCollections() throws IOException {
//        String[] l = new File(home()).list(DirectoryFileFilter.INSTANCE);
//        ArrayList<String> list = new ArrayList<String>(l.length);
//        for (String d : l) if (d.charAt(0) != '.' && collectionExists(d)) list.add(d);
//        return list;
//    }
//
//    public static List<String> listDocuments(String col) throws IOException {
//        if (!collectionExists(col)) return new ArrayList<String>(0);
//        Collection<File> l = FileUtils.listFiles(new File(home() + col), FileFilterUtils.suffixFileFilter(DOC_SEARCH_SUFFIX), FileFilterUtils.trueFileFilter());
//        ArrayList<String> list = new ArrayList<String>(l.size());
//        for (File d : l) {
//            String name = d.getName();
//            list.add(name.substring(0, name.indexOf('.')));
//        }
//        return list;
//    }
//
////    public static void closeDocument(final String doc) {
////        try {
////            if (!shuting) assertRunning();
////            ManagedDiskData data = (ManagedDiskData) ucache.get(doc);
////            if (data != null) {
////                data.close();
////                if (data.removed()) {
////                    data.tryClose();
////                    if (data.closed()) {
////                        remove(doc);
////                        ucache.removeForce(doc);
////                    }
////                }
////            }
////        } catch (Exception e) {
////            e.printStackTrace(System.err);
////        }
////    }
//
//    public static List<String> openCollection(final String col) throws IOException {
//        try {
//            assertRunning();
//            if(!collectionExists(col)) throw new Exception();
//            List<String> docs = listDocuments(col);
//            final ArrayList<String> l = new ArrayList<String>(docs.size());
//            for (String doc : docs) l.add(col + "/" + doc);
//            return l;
//        } catch (Exception e) {
//            throw new FileNotFoundException("collection " + col);
//        }
//    }
//
//    public static void createCollection(final String col) throws IOException {
//        assertRunning();
//        assertName(col);
//        if (new File(recycle() + "." + col).exists()) {
//            FileUtils.moveDirectoryToDirectory(new File(home() + col), new File(recycle() + col + "." + UUID.randomUUID().toString()), true);
//            FileUtils.forceDelete(new File(recycle() + "." + col));
//        }
//        //if (collectionExists(col)) throw new IOException("collection " + col + " exists");
//        new File(home() + col).mkdir();
//    }
//
//    public static void removeCollection(final String col) throws IOException {
//        assertRunning();
//        if (!collectionExists(col)) return;
//        new File(recycle() + "." + col).mkdir();
//    }
//
//    public static boolean isRunning() {
//        return running;
//    }
//
//    public static void createDocument(final String doc, InputStream content) throws IOException {
//        assertRunning();
//        assertName(doc);
//        String[] arg = doc.trim().split("/");
//        String colname;
//        String docname = null;
//        String colpath = null;
//        LockFile doclock = null;
//        try {
//            colname = arg[0];
//            docname = arg[1];
//            if(!collectionExists(colname)) throw new IOException("collection " + colname + " does not exist");
//            if(documentExists(doc)) throw new IOException("document " + doc + " exists");
//            colpath = FilenameUtils.normalize(new ColDepthDirectoryWalker().getNewDocPath(home() + colname));
//            doclock = new LockFile(new File(colpath + "/" + docname + ".creating"), true);
//            doclock.lock();
//            DiskData dd = new DiskBuilder(options, colpath.replace(home(),""), docname, new XMLParser(new IOStream(content), options)).build();
//            createIndex(dd); // TODO: BXK: redo indexing over PageFiles
//            dd.close();
//        } catch (IOException e) {
//            if (colpath != null && docname != null) for (File f : fileList(colpath, docname, true))
//                if (!f.getName().endsWith(".creating")) FileUtils.deleteQuietly(f);
//            throw e;
//        } finally {
//            if (doclock != null) doclock.unlock();
//        }
//    }
//
//    public static void createIndex(DiskData dd) throws IOException {
//        IndexBuilder ib = new DiskValuesBuilder(dd, true);
//        dd.closeIndex(IndexType.TEXT);
//        dd.setIndex(IndexType.TEXT, ib.build());
//
//        ib = new DiskValuesBuilder(dd, false);
//        dd.closeIndex(IndexType.ATTRIBUTE);
//        dd.setIndex(IndexType.ATTRIBUTE, ib.build());
//
//        ib = new FTBuilder(dd);
//        dd.closeIndex(IndexType.FULLTEXT);
//        dd.setIndex(IndexType.FULLTEXT, ib.build());
//    }
//
//    public static void removeDocument(final String doc) throws IOException {
//        assertRunning();
//        if(!documentExists(doc)) return;
//        LockFile doclock = null;
//        try {
//            String[] arg = docRealPathInfo(doc);
//            doclock = new LockFile(new File(arg[0] + "/" + arg[1] + ".removing"), true);
//            doclock.lock();
//            for (File f : fileList(arg[0], arg[1], true)) {
//                if (f.getName().endsWith(".removing")) continue;
//                ManagedPageFile pf = (ManagedPageFile)ucache.get(f.getCanonicalPath());
//                if (pf != null) pf.remove();
//                else FileUtils.deleteQuietly(f);
//            }
//        } finally {
//            if (doclock != null) doclock.unlock();
//        }
//    }
//
//    private static String[] docRealPathInfo(String doc) throws IOException {
//        //assertRunning();
//        String[] arg = doc.trim().split("/");
//        Collection<File> fc = fileList(home() + arg[0], arg[1] + DOC_SEARCH_SUFFIX, false);
//        if (fc.size() < 1) throw new FileNotFoundException(doc);
//        File d = fc.iterator().next();
//        arg[0] = d.getParent();
//        return arg;
//    }
//
//    private static boolean documentIsNew(String doc, String[] rp) {
//        try {
//            return fileList(rp[0], rp[1] + ".creating", false).size() > 0;
//        } catch (Exception i) {
//            return false;
//        }
//    }
//
//    private static boolean documentIsOld(String doc, String[] rp) {
//        try {
//            return fileList(rp[0], rp[1] + ".removing", false).size() > 0;
//        } catch (Exception i) {
//            return false;
//        }
//    }
//
//    private static boolean collectionExists(String col) throws IOException {
//        //assertRunning();
//        if (new File(recycle() + "." + col).exists()) return false;
//        return new File(home() + col).exists();
//    }
//
//    public static boolean documentExists(String doc) throws IOException {
//        //assertRunning();
//        String rp[] = null;
//        try {
//            rp = docRealPathInfo(doc);
//        } catch (FileNotFoundException e) {
//            return false;
//        }
//        if(new File(recycle() + "." + rp[0].substring(doc.indexOf('/') + 1)).exists()) return false;
//        if (documentIsNew(doc, rp) || documentIsOld(doc, rp)) return false;
//        ManagedPageFile pf = (ManagedPageFile)ucache.get(rp[0] + "/" + rp[1] + DOC_SEARCH_SUFFIX);
//        if(pf != null &&  pf.isRemoved()) return false;
//        return true;
//    }
//
//    private static Collection<File> fileList(final String dir, final String str, final boolean prefix) {
//        return FileUtils.listFiles(new File(dir), prefix ? FileFilterUtils.prefixFileFilter(str) : FileFilterUtils.nameFileFilter(str), FileFilterUtils.trueFileFilter());
//    }
//
//    private static class ColDepthDirectoryWalker extends DirectoryWalker {
//        private String lastdir = "";
//        private int level = -1;
//
//        public String getNewDocPath(String dir) throws IOException {
//            File d = new File(dir);
//            walk(d, null);
//            int dirlevel = 0;
//            while (level < 0) {
//                for (int i = 1; i < 100; i++) {
//                    File nd = new File(d.getPath() + "/" + (dirlevel == 0 ? "" : (dirlevel + "/")) + i);
//                    walk(nd, null);
//                    if (level >= 0) break;
//                }
//                dirlevel++;
//            }
//            return level == 0 ? lastdir : lastdir + "/" + level;
//        }
//
//        protected boolean handleDirectory(File directory, int depth, Collection results) throws IOException {
//            lastdir = directory.getPath();
//            level = getLevel(lastdir);
//            if (level > -1) throw new CancelException(directory, depth);
//            return true;
//        }
//
//        protected void handleCancelled(File startDirectory, Collection results, CancelException cancel) {
//        }
//
//        private static boolean hasSpace(final String dir) throws IOException {
//            File dirFile = new File(dir);
//            return !dirFile.exists() || FileUtils.listFilesAndDirs(dirFile, new WildcardFileFilter("*.*"), null).size() - 1 < 1800;
//        }
//
//        private static int getLevel(final String dir) throws IOException {
//            if (hasSpace(dir)) return 0;
//            int dircount = new File(dir).list(DirectoryFileFilter.INSTANCE).length;
//            while (dircount < 100) {
//                if (dircount == 0) dircount++;
//                String newdir = dir + "/" + dircount;
//                if (hasSpace(newdir)) return dircount;
//                dircount++;
//            }
//            return -1;
//        }
//    }
//
////    private static class ManagedDiskData extends DiskData {
////        private final AtomicInteger usageCount = new AtomicInteger(0);
////        private final AtomicLong usageTs = new AtomicLong(new Date().getTime());
////        private final AtomicBoolean removed = new AtomicBoolean(false);
////
////        public ManagedDiskData(String doc, MainOptions opt) throws IOException {
////            super(opt);
////            String[] arg = docRealPathInfo(doc);
////            colpath = arg[0].replace(home(),"");
////            docname = arg[1];
////        }
////
////        public void remove() {
////            if (removed.get()) return;
////            removed.set(true);
////        }
////
////        public boolean removed() {
////            return removed.get();
////        }
////
////        public void close() {
////            usageCount.decrementAndGet();
////            //usageTs.set(new Date().getTime());
////        }
////
////        public boolean closed() {
////            return closed;
////        }
////
////        public synchronized void lock() {
////            //super.lock();
////            if (closed) super.open();
////            super.lock();
////        }
////
//////        public synchronized void wlock() {
//////            //super.lock();
//////            if (closed) super.open();
//////            super.lock();
//////        }
////
////        public void open() {
////            usageCount.incrementAndGet();
////            usageTs.set(new Date().getTime());
////        }
////
////        public void tryClose() {
////            tryClose(false);
////        }
////
////        public synchronized void tryClose(boolean force) {
////            if (closed) return;
////            if (force) {
////                unload();
////                return;
////            }
////            long ts = new Date().getTime() - usageTs.get();
////            if (ts > 1000 * 60 * 60 || (usageCount.get() <= 0 && ts > 1000 * 60 * 10)) unload();
////        }
////
////        private void unload() {
////            super.close();
////            usageCount.set(0);
////            usageTs.set(new Date().getTime());
////        }
////
////        public String toString() {
////            return colpath + "/" + docname + " loaded: " + !closed + " usage count: " + usageCount.get();
////        }
////    }
//
//    private static class ManagedPageFileUnloader implements Runnable {
//        public final AtomicBoolean running = new AtomicBoolean(false);
//
//        public void run() {
//            try {
//                running.set(true);
//                while (running.get()) {
//                    Thread.sleep(1000 * 60 * 5);
//                    for (Iterator i = ucache.values().iterator(); i.hasNext(); ) {
//                        ManagedPageFile pf = (ManagedPageFile)i.next();
//                        pf.tryUnload();
//                        //System.err.println("trying to unload: " + pf.toString());
//                    }
//                }
//            } catch (InterruptedException ie) {
//            }
//        }
//    }
}
