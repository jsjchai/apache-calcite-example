package com.github.jsjchai.apache.calcite.example.util;

import cn.hutool.db.Entity;
import cn.hutool.db.handler.EntityListHandler;
import cn.hutool.db.handler.RsHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * ResultSet工具类
 */
public class ResultSetUtil {

    public static List<Entity> toEntityList(ResultSet resultSet) throws SQLException {
        RsHandler<List<Entity>> handler = new EntityListHandler();
        return handler.handle(resultSet);
    }
}
