package org.apache.hadoop.hbase.client;

import org.apache.phoenix.expression.Expression;

import java.io.*;

public class CheckedPut extends Put {

    private Expression compare;

    public CheckedPut(byte[] row, Expression compare) {
        super(row);
        this.compare = compare;
        this.setAttribute("cmp", serializeCompare(compare));
    }

    public CheckedPut(Put put) {
        super(put);
        this.compare = deserializeCompare(this.getAttribute("cmp"));
        this.setAttribute("cmp", null);
    }

    public Expression getCompare() {
        return this.compare;
    }

    public static boolean isCheckedPut(Put put) {
        return put.getAttribute("cmp") != null;
    }

    private static byte[] serializeCompare(Expression compare) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = null;
        DataOutputStream dos = null;

        try {
            bos = new ByteArrayOutputStream();
            dos = new DataOutputStream(bos);
            dos.writeUTF(compare.getClass().getCanonicalName());
            compare.write(dos);
            dos.flush();
            bytes = bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize CheckedPut object", e);
        } finally {
            try {
                if (dos != null) dos.close();
                if (bos != null) bos.close();
            } catch (IOException e) {
                throw new RuntimeException("Errors in cleaning up resources used for CheckedPut serialization", e);
            }
        }

        return bytes;
    }

    private static Expression deserializeCompare(byte[] serializedCompare) {
        Expression compare = null;
        ByteArrayInputStream bis = null;
        DataInputStream dis = null;

        try {
            bis = new ByteArrayInputStream(serializedCompare);
            dis = new DataInputStream(bis);
            String className = dis.readUTF();
            Class cls = Class.forName(className);
            compare = (Expression)cls.newInstance();
            compare.readFields(dis);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize CheckedPut object", e);
        } finally {
            try {
                if (bis != null) bis.close();
                if (dis != null) dis.close();
            } catch (IOException e) {
                throw new RuntimeException("Errors in cleaning up resources used for CheckedPut deserialization", e);
            }
        }

        return compare;
    }
}