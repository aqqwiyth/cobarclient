package com.raycloud.common.dynamic;

import com.alibaba.cobarclient.Shard;
import com.alibaba.cobarclient.route.Router;
import java.util.Set;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

/**
 * Description:
 * User: ouzhouyou@raycloud.com
 * Date: 14-7-9
 * Time: 下午7:13
 * Version: 1.0
 */
public class DynamicRouterFactoryBean implements FactoryBean, InitializingBean {
    protected static Resource configLocation;
    protected static Resource[] configLocations;
    private DynamicRouter router = null;
    private Set<Shard> shards;

    public void afterPropertiesSet() throws Exception {
        router = new DynamicRouter(DynamicRouterUtil.buildRoutes(shards));
    }

    public Router getObject() throws Exception {
        return router;
    }

    public Class getObjectType() {
        return Router.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public void setConfigLocation(Resource configLocation) {
        this.configLocation = configLocation;
    }

    public void setConfigLocations(Resource[] configLocations) {
        this.configLocations = configLocations;
    }

    public Set<Shard> getShards() {
        return shards;
    }

    public void setShards(Set<Shard> shards) {
        this.shards = shards;
    }
}
