package org.cdlib.mrt.queue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import java.util.Date;
import java.util.Properties;

import org.cdlib.mrt.utility.ZooCodeUtil;

public class LockItem {

    private Date timestamp;
    private String data;
    private String id;

    public LockItem (String data, Date timestamp, String id) {
        this.data = data;
        this.timestamp = timestamp;
        this.id = id;
    }
        
    public LockItem (String data) {
        this (data, null, null);
    }

    public LockItem (String data, Date timestamp) {
        this(data, timestamp, null);
    }

    public Date getTimestamp () { return this.timestamp; }
    public String getData () { return this.data; }
    public String getId () { return this.id; }

    public byte[] getBytes () {
        try {    
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            long ts = 0L;
            if (this.timestamp != null) {
                ts = this.timestamp.getTime();
            }
            dos.writeLong(ts);
            dos.write(this.data.getBytes(), 0, this.data.getBytes().length);
            dos.close();
            return bos.toByteArray();
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            return null;
        }
    }
    
    public String toString () {
        
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        Properties p = new Properties();
        try {
            bis = new ByteArrayInputStream(this.data.getBytes());
            ois = new ObjectInputStream(bis);
            p = (Properties) ois.readObject();
        } catch (IOException ex) {
	    try {
	       // data may be encoded
               p = ZooCodeUtil.decodeItem(this.data.getBytes());
	    } catch (Exception e) {
               /* ... */
	    }
        } catch (ClassNotFoundException ex) {
            /* ... */
        } finally {
            try {
                ois.close();
            } catch (Exception e) {}
            try {
                bis.close();
            } catch (Exception e) {}
        }
        if (this.timestamp != null && this.timestamp.getTime() != 0L) {
            return String.format("[%s] %s", 
                                 this.timestamp.toString(),
                                 p.toString());
        } else {
            return String.format("%s", 
                                 p.toString());
        }
    }

    public static LockItem fromBytes(byte[] bytes) {
        try {
	    int longLength = 8;
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            DataInputStream dis = new DataInputStream(bis);
            long ts = dis.readLong();
            byte[] data = new byte[bytes.length - longLength];
            System.arraycopy(bytes, longLength, data, 0, data.length);
            return new LockItem(new String(data), new Date(ts));
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            return null;
        }
    }

    public static LockItem fromBytes(byte[] bytes, String id) {
        LockItem lockItem = fromBytes(bytes);
        if (lockItem == null) return null;
        lockItem.setId(id);
        return lockItem;
    }

    public void setId(String id) {
        this.id = id;
    }

}
