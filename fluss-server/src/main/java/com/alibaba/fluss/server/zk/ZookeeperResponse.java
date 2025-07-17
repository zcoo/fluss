package com.alibaba.fluss.server.zk;
/**
 * @description:
 * @author: 温粥
 * @create: 2025/7/17 15:07
 */
public class ZookeeperResponse {
    private String path;
    private boolean success;
    private byte[] data;

    public ZookeeperResponse(String path, boolean success) {
        this.path = path;
        this.success = success;
    }

    public ZookeeperResponse(String path, boolean success, byte[] data) {
        this.path = path;
        this.success = success;
        this.data = data;
    }

    public String getPath() {
        return path;
    }

    public boolean isSuccess() {
        return success;
    }

    public byte[] getData() {
        return data;
    }
}
