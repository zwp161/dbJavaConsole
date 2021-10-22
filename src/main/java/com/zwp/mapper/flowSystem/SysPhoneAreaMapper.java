package com.zwp.mapper.flowSystem;

import org.apache.ibatis.annotations.Param;

import java.util.Map;

public interface SysPhoneAreaMapper {

    Map<String,Object> selectByName(@Param("name") String name);

    Map<String, Object> selectByPhone(@Param("phone") String phone);

    int insert(Map<String, Object> newCity);

    int updatePhoneNum(Map<String, Object> updateCity);
}
