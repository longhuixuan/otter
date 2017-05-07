package com.alibaba.otter.manager.web.home.module.screen;

import javax.annotation.Resource;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.Navigator;
import com.alibaba.otter.manager.biz.config.datamediasource.DataMediaSourceService;

public class addDataSource {
	
	@Resource(name = "dataMediaSourceService")
    private DataMediaSourceService dataMediaSourceService;

    public void execute( Context context, Navigator nav) throws Exception {
    	context.put("DataMediaSourceTypes",dataMediaSourceService.getDataMediaSourceTypes());
    }
}
