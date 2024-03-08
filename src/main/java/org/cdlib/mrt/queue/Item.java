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

public class Item {
    public static byte PENDING  = (byte) 0;
    public static byte CONSUMED = (byte) 1;
    public static byte DELETED  = (byte) 2;
    public static byte FAILED   = (byte) 3;
    public static byte COMPLETED= (byte) 4;
    public static byte HELD     = (byte) 5;

    private Date timestamp;
    private byte status;
    private byte[] data;
    private String id;

    public Item (byte status, byte[] data, Date timestamp, String id) {
        this.status = status;
        this.data = data;
        this.timestamp = timestamp;
        this.id = id;
    }
        
    public Item (byte status, byte[] data) {
        this (status, data, null, null);
    }

    public Item (byte status, byte[] data, Date timestamp) {
        this(status, data, timestamp, null);
    }

    public byte getStatus () { return this.status; }

    public Date getTimestamp () { return this.timestamp; }

    public byte[] getData () { return this.data; }

    public String getId () { return this.id; }

    public byte[] getBytes () {
        try {    
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            long ts = 0L;
            if (this.timestamp != null) {
                ts = this.timestamp.getTime();
            }
            dos.writeByte(status);
            dos.writeLong(ts);
            dos.write(this.data, 0, this.data.length);
            dos.close();
            return bos.toByteArray();
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            return null;
        }
    }
    
    public String getStatusStr() {
        String statusStr;
        if (this.status == Item.PENDING) {
            statusStr = "pending";
        } else if (this.status == Item.CONSUMED) {
            statusStr = "consumed";
        } else if (this.status == Item.DELETED) {
            statusStr = "deleted";
        } else if (this.status == Item.COMPLETED) {
            statusStr = "completed";
        } else if (this.status == Item.FAILED) {
            statusStr = "failed";
        } else if (this.status == Item.HELD) {
            statusStr = "held";
        } else {
            statusStr = "error in status retrieval";
        }
	return statusStr;
    }

    public String toString () {
        String statusStr = getStatusStr();
        
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        Properties p = new Properties();
        try {
            bis = new ByteArrayInputStream(this.data);
            ois = new ObjectInputStream(bis);
            p = (Properties) ois.readObject();
        } catch (IOException ex) {
	    try {
	       // data may be encoded
               p = ZooCodeUtil.decodeItem(this.data);
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
            return String.format("[%s: %s] %s", 
                                 statusStr, 
                                 this.timestamp.toString(),
                                 p.toString());
        } else {
            return String.format("[%s] %s", 
                                 statusStr,
                                 p.toString());
        }
    }

    public static Item fromBytes(byte[] bytes) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            DataInputStream dis = new DataInputStream(bis);
            byte status = dis.readByte();
            long ts = dis.readLong();
            byte[] data = new byte[bytes.length - 9];
            System.arraycopy(bytes, 9, data, 0, data.length);
            return new Item(status, data, new Date(ts));
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
            return null;
        }
    }

    public static Item fromBytes(byte[] bytes, String id) {
        Item item = fromBytes(bytes);
        if (item == null) return null;
        item.setId(id);
        return item;
    }

    public void setId(String id) {
        this.id = id;
    }

}
