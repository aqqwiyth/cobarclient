package com.alibaba.cobarclient.dynamic;

import com.alibaba.cobarclient.Shard;
import com.alibaba.cobarclient.route.Route;
import com.alibaba.cobarclient.route.RouteGroup;
import com.alibaba.cobarclient.route.Router;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;

/**
 * Description:支持在运行时追加路由信息
 * User: ouzhouyou@raycloud.com
 * Date: 14-7-9
 * Time: 下午7:13
 * Version: 1.0
 */
public class DynamicRouter implements Router {

    private Logger logger = Logger.getLogger(DynamicRouter.class);

    private Map<String, RouteGroup> routes = new ConcurrentHashMap<String, RouteGroup>();

    private Set<Shard> EMPTY_SHARD_SET = new HashSet<Shard>();

    private AtomicBoolean atomicBoolean = new AtomicBoolean(true);

    DynamicRouter() {
    }

    public DynamicRouter(Set<Route> routeSet) {
        setRoutes(routeSet);
    }

    public Set<Shard> route(String action, Object argument) {
        waiting(1);
        Route resultRoute = findRoute(action, argument);
        if (resultRoute == null) {
            if (action != null) {
                String namespace = action.substring(0, action.lastIndexOf("."));
                resultRoute = findRoute(namespace, argument);
            }
        }
        if (resultRoute == null) {
            return EMPTY_SHARD_SET;
        } else {
            return resultRoute.getShards();
        }
    }

    protected Route findRoute(String action, Object argument) {
        if (routes.containsKey(action)) {
            RouteGroup routeGroup = routes.get(action);
            for (Route route : routeGroup.getSpecificRoutes()) {
                if (route.apply(action, argument)) {
                    return route;
                }
            }
            if (routeGroup.getFallbackRoute() != null && routeGroup.getFallbackRoute().apply(action, argument))
                return routeGroup.getFallbackRoute();
        }
        return null;
    }

    /**
     * 防止出现并发问题暂时锁住所有请求
     */
    private void waiting(Integer size) {
        if (size > 5) {
            throw new RuntimeException("等待次数超过5次...");
        }
        if (!atomicBoolean.get()) {
            try {
                logger.warn("SQL执行被锁住,可能正在动态加载路由信息...休眠2秒重试");
                Thread.sleep(2000L);
                size += 1;
                waiting(size);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void setRoutes(Set<Route> routeSet) {
        if (!(routeSet == null || routeSet.isEmpty())) {
            for (Route route : routeSet) {
                if (!routes.containsKey(route.getSqlmap())) routes.put(route.getSqlmap(), new RouteGroup());
                if (route.getExpression() == null)
                    routes.get(route.getSqlmap()).setFallbackRoute(route);
                else
                    routes.get(route.getSqlmap()).getSpecificRoutes().add(route);
            }
        }
    }

    public AtomicBoolean getAtomicBoolean() {
        return atomicBoolean;
    }
}
