package lmdb.basex;


import org.apache.commons.io.output.ByteArrayOutputStream;
import org.basex.io.out.DataOutput;
import org.basex.io.out.TableOutput;
import org.fusesource.lmdbjni.Database;

import java.io.FileOutputStream;
import java.io.IOException;

public class LmdbTableOutput extends TableOutput {

    private Database taccessdb;
    private byte[] docid;

    public LmdbTableOutput(MetaData md, String fn, Database ta, byte[] did) throws IOException {
        super(md);
        os = new FileOutputStream(fn);
        file = fn;
        taccessdb = ta;
        docid = did;
    }

    @Override
    public void close() throws IOException {
        // store at least one page on disk
        final boolean empty = pages == 0 && pos == 0;
        if(empty) pos++;
        flush();
        os.close();

        try(ByteArrayOutputStream bos = new ByteArrayOutputStream(); final DataOutput out = new DataOutput(bos)) {
            out.writeNum(pages);
            out.writeNum(empty ? 0 : Integer.MAX_VALUE);
            out.flush();
            taccessdb.put(TableLmdbAccess.getStructKey(docid), bos.toByteArray());
        }
    }
}
