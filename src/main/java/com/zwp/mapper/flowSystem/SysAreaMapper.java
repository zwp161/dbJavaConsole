package com.zwp.mapper.flowSystem;

import org.apache.ibatis.annotations.Param;

import java.util.Map;

public interface SysAreaMapper {

    Map<String,Object> selectByName(@Param("name") String name);
}
