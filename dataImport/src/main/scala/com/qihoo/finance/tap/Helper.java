package com.qihoo.finance.tap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * @author zhoupeng
 * @date 2019/5/9
 */
public class Helper {
    public static String buildVertexProperty(String label, String jsonString) {
        JSONObject jsonObject = Helper.getVertexProperty(jsonString);
        return buildPropertyString(label, jsonObject);
    }

    private static String buildPropertyString(String label, JSONObject jsonObject) {
        StringBuilder builder = new StringBuilder();
        jsonObject.forEach((key, value) -> {
            if (value instanceof String) {
                builder.append(".property('").append(key).append("', '").append(value).append("')");
            } else {
                builder.append(".property('").append(key).append("', ").append(value).append(")");
            }

            // 手机号才需要加密
            if ("MOBILE".equals(label) && "name".equals(key)) {
                String encrypt = value.toString();
                String sha1Hex = DigestUtils.sha1Hex(value.toString());
                builder.append(".property('").append("nm_pass").append("', '").append(encrypt).append("')");
                builder.append(".property('").append("nm_sha1").append("', '").append(sha1Hex).append("')");
            }
        });
        return builder.toString();
    }

    public static String buildIncrementOtherPropertyString(String label, JSONObject jsonObject) {
        StringBuilder builder = new StringBuilder();
        jsonObject.forEach((key, value) -> {

            if ("name".equals(key)) {
                // 手机号 才需要加密信息
                if ("MOBILE".equals(label)) {
                    String encrypt =value.toString();
                    String sha1Hex = DigestUtils.sha1Hex(value.toString());

                    builder.append(".property('").append("nm_pass").append("', '").append(encrypt).append("')");
                    builder.append(".property('").append("nm_sha1").append("', '").append(sha1Hex).append("')");
                }

            } else if ("status".equals(key)) {
                // do nothing
            } else {
                if (value instanceof String) {
                    builder.append(".property('").append(key).append("', '").append(value).append("')");
                } else {
                    builder.append(".property('").append(key).append("', ").append(value).append(")");
                }
            }

        });
        return builder.toString();
    }

    public static JSONObject getVertexProperty(String jsonString) {
        return JSON.parseObject(jsonString);
    }
}