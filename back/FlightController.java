package com.controller;

import com.po.ResultTo;
import dao.AbstractDaoImpl;
import org.MultipartFile;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlightController {
    AbstractDaoImpl abstractDao=new AbstractDaoImpl("com.mysql.cj.jdbc.Driver",
            "jdbc:mysql://cent1:3306/test?useSSL=false&allowPublicKeyRetrieval=true","root2","123456");


    public List getFlights(String flightnumber, Integer pageNow){
        Integer PAGESIZE = 100;
        Integer start = (pageNow - 1) * PAGESIZE;
        // 正确地为航班号添加单引号
        String whereClause = " flight_number = '" + flightnumber + "'";
        return abstractDao.getMaps("flight", "*", whereClause, start.toString(), PAGESIZE.toString());
    }

    public List getRoute(String flightnumber, String date, Integer pageNow) {
        Integer PAGESIZE = 100;
        Integer start = (pageNow - 1) * PAGESIZE;
        // 正确地为航班号和日期添加单引号，并将日期部分截取出来
        String whereClause = " flight_number = '" + flightnumber + "' AND DATE(date) = '" + date + "'";
        return abstractDao.getMaps("flight", "*", whereClause, start.toString(), PAGESIZE.toString());
    }

    public Map<String, Object> register(String username, String password, String phone, MultipartFile file) {
        Map<String, Object> response = new HashMap<>();

        // 检查用户名是否已经存在
        Map<String, Object> existingUser = abstractDao.getMap("user", "*", "username='" + username + "'");
        if (existingUser != null) {
            response.put("usernameExists", true);
            response.put("success", false);
            return response;
        }

        String path = this.getClass().getResource("/").getPath() + "web/";
        File folder = new File(path);
        if (!folder.exists()) {
            folder.mkdirs();
        }
        try {
            file.transferTo(new File(path + username + ".jpg"));
        } catch (Exception e) {
            e.printStackTrace();
            response.put("success", false);
            return response;
        }

        Map<String, Object> map = new HashMap<>();
        map.put("username", username);
        map.put("password", password);
        map.put("phone", phone);
        map.put("header", username + ".jpg");

        boolean insertResult = abstractDao.insert("user", map);
        response.put("usernameExists", false);
        response.put("success", insertResult);
        return response;
    }

    public ResultTo login(String username, String password) {
        Map<String, Object> user = abstractDao.getMap("user", "*", "username='" + username + "' and password='" + password + "'");
        ResultTo resultTo = new ResultTo();  // 存数据
        if (user == null) {
            resultTo.value = null;
            resultTo.msg = "登录失败";
        } else {
            resultTo.value = user;
            resultTo.msg = "登录成功";
        }
        return resultTo;
    }

    public Map<String, Object> changePassword(String username, String currentPassword, String newPassword) {
        Map<String, Object> response = new HashMap<>();

        // 检查用户名和当前密码是否正确
        Map<String, Object> user = abstractDao.getMap("user", "*", "username='" + username + "' and password='" + currentPassword + "'");
        if (user == null) {
            response.put("success", false);
            response.put("error", "password incorrect");
            return response;
        }

        // 更新密码
        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put("password", newPassword);
        boolean updateResult = abstractDao.update("user", updateMap, "username='" + username + "'");

        response.put("success", updateResult);
        return response;
    }

}



