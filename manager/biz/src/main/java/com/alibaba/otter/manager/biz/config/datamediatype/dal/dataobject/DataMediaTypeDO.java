package com.alibaba.otter.manager.biz.config.datamediatype.dal.dataobject;

import java.io.Serializable;

public class DataMediaTypeDO implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8968349789048864485L;
	private Long              id;
    private String            name;
    private String            code;
    private String            managerClassName;
    private String            nodeClassName;
    private Integer            flag;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public String getManagerClassName() {
		return managerClassName;
	}
	public void setManagerClassName(String managerClassName) {
		this.managerClassName = managerClassName;
	}
	public String getNodeClassName() {
		return nodeClassName;
	}
	public void setNodeClassName(String nodeClassName) {
		this.nodeClassName = nodeClassName;
	}
	public Integer getFlag() {
		return flag;
	}
	public void setFlag(Integer flag) {
		this.flag = flag;
	}
    
}
