package com.ll.appmr;

import com.alibaba.fastjson.JSONObject;

public class JsonToStringUtil {
    public static String toString(JSONObject jsonObj){

        StringBuilder sb = new StringBuilder();
        sb.append(jsonObj.get("sdk_ver")).append("\001")
                .append(jsonObj.getString("time_zone")).append("\001")
                .append(jsonObj.getString("commit_id")).append("\001")
                .append(jsonObj.getString("commit_time")).append("\001")
                .append(jsonObj.getString("pid")).append("\001")
                .append(jsonObj.getString("app_token")).append("\001")
                .append(jsonObj.getString("app_id")).append("\001")
                .append(jsonObj.getString("device_id")).append("\001")
                .append(jsonObj.getString("device_id_type")).append("\001")
                .append(jsonObj.getString("release_channel")).append("\001")
                .append(jsonObj.getString("app_ver_name")).append("\001")
                .append(jsonObj.getString("app_ver_code")).append("\001")
                .append(jsonObj.getString("os_name")).append("\001")
                .append(jsonObj.getString("os_ver")).append("\001")
                .append(jsonObj.getString("language")).append("\001")
                .append(jsonObj.getString("country")).append("\001")
                .append(jsonObj.getString("manufacture")).append("\001")
                .append(jsonObj.getString("device_model")).append("\001")
                .append(jsonObj.getString("resolution")).append("\001")
                .append(jsonObj.getString("net_type")).append("\001")
                .append(jsonObj.getString("account")).append("\001")
                .append(jsonObj.getString("app_device_id")).append("\001")
                .append(jsonObj.getString("mac")).append("\001")
                .append(jsonObj.getString("android_id")).append("\001")
                .append(jsonObj.getString("imei")).append("\001")
                .append(jsonObj.getString("cid_sn")).append("\001")
                .append(jsonObj.getString("build_num")).append("\001")
                .append(jsonObj.getString("mobile_date_type")).append("\001")
                .append(jsonObj.getString("promotion_channel")).append("\001")
                .append(jsonObj.getString("carrier")).append("\001")
                .append(jsonObj.getString("city")).append("\001")
                .append(jsonObj.getString("user_id"));

        return sb.toString();

    }
}
