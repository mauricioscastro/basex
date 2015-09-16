package lmdb.basex;

import org.basex.core.MainOptions;
import org.basex.data.Namespaces;
import org.basex.index.name.Names;
import org.basex.index.path.PathSummary;
import org.basex.io.out.DataOutput;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;
import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static lmdb.util.Byte.lmdbkey;

public class LmdbDataBuilder extends LmdbData {

    private Env env;
    private DataOutputStream tempBuffer;
    private File tmpFile;

    public LmdbDataBuilder(final String name, final byte[] docid, final Env env,
                    final Database txtdb, final Database attdb,
                    final Database elementdb, final Database attributedb,
                    final Database pathsdb, final Database namespacedb,
                    final Database tableAccess, final MainOptions options) throws IOException {

        super(name, options);

        this.docid = docid;
        this.env = env;
        this.txtdb = txtdb;
        this.attdb = attdb;

        this.table = new TableLmdbAccessBuilder(meta,env,tableAccess,docid);
        this.elementdb = elementdb;
        this.attributedb = attributedb;
        this.pathsdb = pathsdb;
        this.namespacedb = namespacedb;

        this.tmpFile = File.createTempFile("txt." + meta.name.replace('/','.') + ".",".tmp",null);
        this.tmpFile.deleteOnExit();
        this.tempBuffer = new DataOutputStream(new FileOutputStream(tmpFile));

        this.elemNames = new Names(meta);
        this.attrNames = new Names(meta);
        this.paths = new PathSummary(this);
        this.nspaces = new Namespaces();
    }

    @Override
    public void close() {
        try {
            table.close();
            tempBuffer.close();

            tx = env.createWriteTransaction();

            DataInputStream di = new DataInputStream(new FileInputStream(tmpFile));

            int c = 0;
            try {
                while (true) try {
                    int len = di.readInt();
                    byte[] key = new byte[8];
                    di.readFully(key);
                    byte[] value = new byte[len];
                    di.readFully(value);
                    boolean text = di.readBoolean();
                    (text ? txtdb : attdb).put(tx, key, value);
                    c++;
                    if (c > 1024 * 10) {
                        tx.commit();
                        c = 0;
                        tx = env.createWriteTransaction();
                    }
                } catch (EOFException eofe) {
                    break;
                }
                if (c > 0) tx.commit();
            } finally {
                tx.close();
            }

            tx = env.createWriteTransaction();
            try {

                ByteArrayOutputStream bos = new ByteArrayOutputStream(1024*16);

                paths.write(new DataOutput(bos));
                pathsdb.put(tx, docid, bos.toByteArray());
                bos.reset();

                nspaces.write(new DataOutput(bos));
                namespacedb.put(tx, docid, bos.toByteArray());
                bos.reset();

                elemNames.write(new DataOutput(bos));
                elementdb.put(tx, docid, bos.toByteArray());
                bos.reset();

                attrNames.write(new DataOutput(bos));
                attributedb.put(tx, docid, bos.toByteArray());

                tx.commit();

            } finally {
                tx.close();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tmpFile.delete();
    }

    @Override
    protected long textRef(byte[] value, boolean text) {
        try {
            tempBuffer.writeInt(value.length);
            tempBuffer.write(lmdbkey(docid, meta.lastid));
            tempBuffer.write(value);
            tempBuffer.writeBoolean(text);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return meta.lastid;
    }
}
