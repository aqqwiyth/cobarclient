package com.alibaba.cobarclient.dynamic;

import com.alibaba.cobarclient.Shard;
import com.alibaba.cobarclient.config.vo.InternalRule;
import com.alibaba.cobarclient.config.vo.InternalRules;
import com.alibaba.cobarclient.expr.MVELExpression;
import com.alibaba.cobarclient.route.Route;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.thoughtworks.xstream.XStream;
import java.util.*;
import org.apache.log4j.Logger;
import org.springframework.core.io.Resource;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

/**
 * Description:支持在运行时追加路由信息
 * User: ouzhouyou@raycloud.com
 * Date: 14-7-9
 * Time: 下午7:13
 * Version: 1.0
 */
public class DynamicRouterUtil {

    private static Logger logger = Logger.getLogger(DynamicRouterUtil.class);

    public static Set<Route> buildRoutes(Set<Shard> shards) throws Exception {
        return buildRoutes(null, shards);
    }


    /**
     * 追加了一段自定义传入InternalRule的代码
     * 减少复杂度直接复制SimpleRouterFactoryBean部分方法来初始化
     *
     * @param internalRuleList 指定的路由信息(一般是手动初始化传递进来的)
     * @param shards           Shard对象
     * @return
     * @throws Exception
     */
    public static Set<Route> buildRoutes(List<InternalRule> internalRuleList, Set<Shard> shards) throws Exception {
        logger.info("==================================DynamicRouterUtil buildRoutes===================================================");
        logger.info("DynamicRouterUtil internalRuleList " + JSONObject.toJSONString(internalRuleList));
        for (Shard shard : shards) {
            if (shard.getDataSource() instanceof DruidDataSource) {
                Map<String, Object> statData = ((DruidDataSource) shard.getDataSource()).getStatData();
                logger.info("[shardId:" + shard.getId() + "][UserName:" + statData.get("UserName").toString() + "][URL:" + statData.get("URL") + "]");
            }
        }
        logger.info("==================================DynamicRouterUtil buildRoutes===================================================");
        List<InternalRule> allRules = new ArrayList<InternalRule>();
        if (DynamicRouterFactoryBean.configLocation != null) {
            List<InternalRule> rules = loadRules(DynamicRouterFactoryBean.configLocation);
            if (!CollectionUtils.isEmpty(rules)) {
                allRules.addAll(rules);
            }
        }

        if (!ObjectUtils.isEmpty(DynamicRouterFactoryBean.configLocations)) {
            for (Resource res : DynamicRouterFactoryBean.configLocations) {
                List<InternalRule> rules = loadRules(res);
                if (!CollectionUtils.isEmpty(rules)) {
                    allRules.addAll(rules);
                }
            }
        }
        /**
         * 通过程序装载的路由信息,初始化本段代码不起作用
         */
        if (internalRuleList != null && internalRuleList.size() > 0) {
            allRules.addAll(internalRuleList);
        }

        Map<String, Object> functions = null;
        //构造生成router需要的routes参数
        if (!CollectionUtils.isEmpty(allRules)) {
            Map<String, Shard> shardMap = convertShardMap(shards);
            Set<Route> routes = new LinkedHashSet<Route>();
            Route route;
            Set<Shard> subShard;
            for (InternalRule rule : allRules) {
                String sqlmap = rule.getSqlmap();
                if (sqlmap == null || sqlmap.equals("")) {
                    sqlmap = rule.getNamespace();
                }
                //通常这里size == 1
                String[] shardArr = rule.getShards().split(",");
                subShard = new LinkedHashSet<Shard>();
                for (String shardId : shardArr) {
                    Shard tempShard = shardMap.get(shardId);
                    if (tempShard == null) {
                        throw new NullPointerException("shard:" + shardId + " is not exists");
                    }
                    subShard.add(tempShard);
                }
                if (null != rule.getShardingExpression()) {
                    if (null == functions) {
                        functions = new HashMap<String, Object>();
                    }
                    route = new Route(sqlmap, new MVELExpression(rule.getShardingExpression(), functions), subShard);
                } else {
                    route = new Route(sqlmap, null, subShard);
                }
                routes.add(route);
            }
            return routes;
        }
        return null;
    }


    private static List<InternalRule> loadRules(Resource configLocation) throws Exception {
        XStream xstream = new XStream();
        xstream.alias("rules", InternalRules.class);
        xstream.alias("rule", InternalRule.class);
        xstream.addImplicitCollection(InternalRules.class, "rules");
        xstream.useAttributeFor(InternalRule.class, "merger");
        InternalRules internalRules = (InternalRules) xstream.fromXML(configLocation.getInputStream());
        return internalRules.getRules();
    }

    private static Map<String, Shard> convertShardMap(Set<Shard> shards) {
        Map<String, Shard> shardMap = new HashMap<String, Shard>();
        for (Shard shard : shards) {
            shardMap.put(shard.getId(), shard);
        }
        return shardMap;
    }
}