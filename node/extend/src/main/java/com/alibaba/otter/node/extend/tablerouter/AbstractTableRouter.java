package com.alibaba.otter.node.extend.tablerouter;

import com.alibaba.otter.shared.common.model.config.ConfigHelper;
import com.alibaba.otter.shared.common.model.config.data.DataMedia;
import com.alibaba.otter.shared.etl.extend.tablerouter.TableRouter;
import com.alibaba.otter.shared.etl.model.EventData;
import org.springframework.util.Assert;

/**
 * 〈分表路由规则〉<p>
 * 〈功能详细描述〉
 *
 * @author zixiao
 * @date 2019/8/14
 */
public abstract class AbstractTableRouter implements TableRouter{

    @Override
    public String dbRoute(EventData rowData, DataMedia.ModeValue sourceSchemas, DataMedia.ModeValue targetSchemas) {
        return buildName(rowData.getSchemaName(), sourceSchemas, targetSchemas);
    }

    @Override
    public String tableRoute(EventData rowData, DataMedia.ModeValue sourceTables, DataMedia.ModeValue targetTables) {
        return buildName(rowData.getTableName(), sourceTables, targetTables);
    }

    private String buildName(String name, DataMedia.ModeValue sourceModeValue, DataMedia.ModeValue targetModeValue) {
        if (targetModeValue.getMode().isWildCard()) {
            // 通配符，认为源和目标一定是一致的
            return name;
        } else if (targetModeValue.getMode().isMulti()) {
            int index = ConfigHelper.indexIgnoreCase(sourceModeValue.getMultiValue(), name);
            Assert.isTrue(index >= 0, "can not found namespace or name in media:" + sourceModeValue.toString());
            return targetModeValue.getMultiValue().get(index);
        } else {
            return targetModeValue.getSingleValue();
        }
    }
}
