package com.alibaba.cobarclient;

import com.alibaba.cobarclient.route.Router;
import com.ibatis.sqlmap.client.SqlMapExecutor;
import com.ibatis.sqlmap.client.SqlMapSession;
import com.ibatis.sqlmap.client.event.RowHandler;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.orm.ibatis.SqlMapClientCallback;
import org.springframework.orm.ibatis.SqlMapClientTemplate;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 */
public class MysdalSqlMapClientTemplate extends SqlMapClientTemplate implements DisposableBean {
    /**
     * for operations that only need to be executed against single shard, it's NOT necessary to perform them in common
     * way, that's, submit them to thread pool and execute in parallel. In such scenarios, just reuse
     * SqlMapClientTemplate's easy API and get it done smoothly.
     */
    protected Map<String, SqlMapClientTemplate> CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES = new HashMap<String, SqlMapClientTemplate>();
    /**
     * we need target set of the shard to set the boundary of distributed data access and negotiate some default
     * settings for MysdalSqlMapClientTemplate, say, default pool size to use.
     */
    private Set<Shard> shards;
    /**
     * Since Routes should be assigned explicitly, A Router has to be injected explicitly too. Anyway, we are using
     * MysdalSqlMapClientTemplate to do distributed data access, right?
     */
    private Router router;
    /**
     * operation timeout threshold, in milliseconds
     */
    private long timeout = 5 * 1000;
    /**
     * if no external #executor is injected, then we will initialize one to use, and this flag will indicate such
     * alternative so that we can decide whether we need to clean up after.
     */
    private boolean useDefaultExecutor = false;
    private ExecutorService executor = Executors.newFixedThreadPool(20);

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        if (shards == null || shards.isEmpty()) throw new IllegalArgumentException("'shards' argument is required.");
        if (router == null) throw new IllegalArgumentException("'router' argument is required");
        if (executor == null) {
            useDefaultExecutor = true;
            executor = Executors.newCachedThreadPool(new ThreadFactory() {
                public Thread newThread(Runnable runnable) {
                    return new Thread(runnable, "MysdalSqlMapClientTemplate executor thread");
                }
            });
        }
        for (Shard shard : shards) {
            CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.put(shard.getId(), new SqlMapClientTemplate(shard.getDataSource(), getSqlMapClient()));
        }
    }

    public void destroy() throws Exception {
        if (useDefaultExecutor) {
            executor.shutdown();
        }
    }

    /**
     * 判断是否能找到对应的分片数据
     *
     * @param statementName
     * @return
     */
    public boolean isHasShard(final String statementName, final Object parameterObject) {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.size() == 1) {
            return true;
        }
        return false;
    }

    @Override
    public int delete(String statementName) throws DataAccessException {
        return this.delete(statementName, null);
    }

    @Override
    public int delete(final String statementName, final Object parameterObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.delete(statementName, parameterObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).delete(statementName, parameterObject);
        } else {
            List<Integer> results = execute(shards, new SqlMapClientCallback() {
                public Integer doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.delete(statementName, parameterObject);
                }
            });
            int count = 0;
            for (Object i : results) {
                count += (Integer) i;
            }
            return count;
        }
    }

    @Override
    public void delete(String statementName, Object parameterObject, int requiredRowsAffected) throws DataAccessException {
        int actualRowsAffected = this.delete(statementName, parameterObject);
        if (actualRowsAffected != requiredRowsAffected) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(
                    statementName, requiredRowsAffected, actualRowsAffected);
        }
    }

    @Override
    public Object insert(String statementName) throws DataAccessException {
        return this.insert(statementName, null);
    }

    @Override
    public Object insert(final String statementName, final Object parameterObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.insert(statementName, parameterObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).insert(statementName, parameterObject);
        } else {
            return execute(shards, new SqlMapClientCallback() {
                public Object doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.insert(statementName, parameterObject);
                }
            });
        }
    }

    @Override
    public List queryForList(String statementName) throws DataAccessException {
        return this.queryForList(statementName, null);
    }

    @Override
    public List queryForList(final String statementName, final Object parameterObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForList(statementName, parameterObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForList(statementName, parameterObject);
        } else {
            return queryForListBase(shards, new SqlMapClientCallback() {
                public List doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForList(statementName, parameterObject);
                }
            });
        }
    }

    @Override
    public List queryForList(final String statementName, final Object parameterObject, final int skipResults, final int maxResults) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForList(statementName, parameterObject, skipResults, maxResults);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForList(statementName, parameterObject, skipResults, maxResults);
        } else {
            return queryForListBase(shards, new SqlMapClientCallback() {
                public List doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForList(statementName, parameterObject, skipResults, maxResults);
                }
            });
        }

    }

    @Override
    public List queryForList(String statementName, int skipResults, int maxResults) throws DataAccessException {
        return this.queryForList(statementName, null, skipResults, maxResults);
    }

    /**
     * collection and flatten the result list
     */
    protected List<Object> queryForListBase(Set<Shard> shards, SqlMapClientCallback callback) {
        List<List> results = execute(shards, callback);
        List<Object> resultList = new ArrayList<Object>();         // FLATTEN the list, miss FP pattern here.
        for (List lst : results) {
            resultList.addAll(lst);
        }
        return resultList;
    }

    @Override
    public Map queryForMap(final String statementName, final Object parameterObject, final String keyProperty) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForMap(statementName, parameterObject, keyProperty);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForMap(statementName, parameterObject, keyProperty);
        } else {
            return queryForMapBase(shards, new SqlMapClientCallback() {
                public Map doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForMap(statementName, parameterObject, keyProperty);
                }
            });
        }
    }

    @Override
    public Map queryForMap(final String statementName, final Object parameterObject, final String keyProperty, final String valueProperty) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForMap(statementName, parameterObject, keyProperty, valueProperty);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForMap(statementName, parameterObject, keyProperty, valueProperty);
        } else {
            return queryForMapBase(shards, new SqlMapClientCallback() {
                public Map doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForMap(statementName, parameterObject, keyProperty, valueProperty);
                }
            });
        }

    }

    protected Map queryForMapBase(Set<Shard> shards, SqlMapClientCallback callback) {
        List<Map> resultList = execute(shards, callback);
        Map map = new HashMap();
        for (Map m : resultList) {
            map.putAll(m);
        }
        return map;
    }

    @Override
    public Object queryForObject(String statementName) throws DataAccessException {
        return this.queryForObject(statementName, null);
    }

    @Override
    public Object queryForObject(final String statementName, final Object parameterObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForObject(statementName, parameterObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForObject(statementName, parameterObject);
        } else {
            return execute(shards, new SqlMapClientCallback() {
                public Object doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForObject(statementName, parameterObject);
                }
            });
        }
    }

    @Override
    public Object queryForObject(final String statementName, final Object parameterObject, final Object resultObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.queryForObject(statementName, parameterObject, resultObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryForObject(statementName, parameterObject, resultObject);
        } else {
            return execute(shards, new SqlMapClientCallback() {
                public Object doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.queryForObject(statementName, parameterObject, resultObject);
                }
            });
        }
    }

    @Override
    public void queryWithRowHandler(String statementName, RowHandler rowHandler) throws DataAccessException {
        this.queryWithRowHandler(statementName, null, rowHandler);
    }

    @Override
    public void queryWithRowHandler(final String statementName, final Object parameterObject, final RowHandler rowHandler) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) super.queryWithRowHandler(statementName, parameterObject, rowHandler);
        if (shards.size() == 1) {
            CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).queryWithRowHandler(statementName, parameterObject, rowHandler);
        } else {
            execute(shards, new SqlMapClientCallback() {
                public Object doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    executor.queryWithRowHandler(statementName, parameterObject, rowHandler);
                    return null;
                }
            });
        }
    }

    @Override
    public int update(final String statementName, final Object parameterObject) throws DataAccessException {
        Set<Shard> shards = getRouter().route(statementName, parameterObject);
        if (shards.isEmpty()) return super.update(statementName, parameterObject);
        if (shards.size() == 1) {
            return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shards.iterator().next().getId()).update(statementName, parameterObject);
        } else {
            List<Integer> resultList = execute(shards, new SqlMapClientCallback() {
                public Integer doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                    return executor.update(statementName, parameterObject);
                }
            });
            int count = 0;
            for (Integer i : resultList) {
                count += i;
            }
            return count;
        }
    }

    @Override
    public void update(String statementName, Object parameterObject, int requiredRowsAffected) throws DataAccessException {
        int actualRowsAffected = this.update(statementName, parameterObject);
        if (actualRowsAffected != requiredRowsAffected) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(
                    statementName, requiredRowsAffected, actualRowsAffected);
        }
    }

    @Override
    public int update(String statementName) throws DataAccessException {
        return this.update(statementName, null);
    }

    protected  List execute(Set<Shard> shards, final SqlMapClientCallback callback) {
        MultipleCauseException exceptions = new MultipleCauseException();
        Map<String, ResourceBundle> executionContext = new HashMap<String, ResourceBundle>();

        for (Shard shard : shards) {
            try {
                final Connection connection = DataSourceUtils.getConnection(shard.getDataSource());
                Future future = executor.submit(new Callable() {
                    public Object call() throws Exception {
                        SqlMapSession session = getSqlMapClient().openSession();
                        try {
                            session.setUserConnection(connection);
                            try {
                                return callback.doInSqlMapClient(session);
                            } catch (SQLException ex) {
                                throw getExceptionTranslator().translate("SqlMapClient operation", null, ex);
                            }
                        } finally {
                            session.close();
                        }
                    }
                });
                executionContext.put(shard.getId(), new ResourceBundle(connection, shard.getDataSource(), future));
            } catch (Exception ex) {
                exceptions.add(ex);
            }
        }

        List resultList = new ArrayList();

        for (Map.Entry<String, ResourceBundle> entry : executionContext.entrySet()) {
            ResourceBundle bundle = entry.getValue();
            try {
                resultList.add(bundle.getFuture().get(timeout, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                exceptions.add(e);
            } catch (ExecutionException e) {
                exceptions.add(e);
            } catch (TimeoutException e) {
                exceptions.add(e);
            } finally {
                try {
                    DataSourceUtils.releaseConnection(bundle.getConnection(), bundle.getDataSource());
                } catch (Exception ex) {
                    logger.debug("Could not close JDBC Connection", ex);
                }
            }
        }

        if (!exceptions.getCauses().isEmpty())
            throw new TransientDataAccessResourceException("one or more errors when performing data access operations against multiple shards", exceptions);

        return resultList;

    }

    public int batchInsert(final String statementName, final Collection<?> entities)
            throws DataAccessException {
        int counter = 0;
        Map<Shard, List<?>> classifiedEntities = classify(statementName, entities);
        List<Integer> result = batchInsert(statementName, classifiedEntities);
        for (Integer row : result) {
            counter += row;
        }
        return counter;
    }

    /**
     * 批量插入
     *
     * @param statementName
     * @param entity
     * @return
     */
    private List<Integer> batchInsert(final String statementName, Map<Shard, List<?>> entity) {
        if (CollectionUtils.isEmpty(entity))
            return new ArrayList<Integer>();
        final CountDownLatch latch = new CountDownLatch(entity.size());
        List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
        final MultipleCauseException throwables = new MultipleCauseException();
        for (final Map.Entry<Shard, List<?>> entry : entity.entrySet()) {
            futures.add(executor.submit(new Callable<Integer>() {
                public Integer call() {
                    int row = 0;
                    try {
                        row = (Integer)getSqlMapClientTemplate(entry.getKey().getId()).execute(new SqlMapClientCallback() {
                            public Integer doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                                executor.startBatch();
                                for (Object item : entry.getValue()) {
                                    executor.insert(statementName, item);
                                }
                                return executor.executeBatch();
                            }
                        });
                    } catch (Throwable e) {
                        throwables.add(e);
                    } finally {
                        latch.countDown();
                    }
                    return row;
                }
            }));
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new ConcurrencyFailureException(
                    "interrupted when processing data access request in concurrency", e);
        }
        if (!throwables.getCauses().isEmpty()) {
            throw new TransientDataAccessResourceException("one or more errors when performing data access operations" +
                    " against multiple shards", throwables);
        }
        return getFutureResults(futures);
    }

    public int batchDelete(final String statementName, final Collection<?> entities)
            throws DataAccessException {
        int counter = 0;
        Map<Shard, List<?>> classifiedEntities = classify(statementName, entities);
        List<Integer> result = batchDelete(statementName, classifiedEntities);
        for (Integer row : result) {
            counter += row;
        }
        return counter;
    }

    /**
     * 批量插入
     *
     * @param statementName
     * @param entity
     * @return
     */
    private List<Integer> batchDelete(final String statementName, Map<Shard, List<?>> entity) {
        if (CollectionUtils.isEmpty(entity))
            return new ArrayList<Integer>();
        final CountDownLatch latch = new CountDownLatch(entity.size());
        List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
        final MultipleCauseException throwables = new MultipleCauseException();
        for (final Map.Entry<Shard, List<?>> entry : entity.entrySet()) {
            futures.add(executor.submit(new Callable<Integer>() {
                public Integer call() {
                    int row = 0;
                    try {
                        row = (Integer)getSqlMapClientTemplate(entry.getKey().getId()).execute(new SqlMapClientCallback() {
                            public Integer doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                                executor.startBatch();
                                for (Object item : entry.getValue()) {
                                    executor.delete(statementName, item);
                                }
                                return executor.executeBatch();
                            }
                        });
                    } catch (Throwable e) {
                        throwables.add(e);
                    } finally {
                        latch.countDown();
                    }
                    return row;
                }
            }));
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new ConcurrencyFailureException(
                    "interrupted when processing data access request in concurrency", e);
        }
        //TODO 打印出错误细节
        if (!throwables.getCauses().isEmpty()) {
            throw new TransientDataAccessResourceException("one or more errors when performing data access operations" +
                    " against multiple shards", throwables);
        }
        return getFutureResults(futures);
    }

    public int batchUpdate(final String statementName, final Collection<?> entities) {
        int counter = 0;
        Map<Shard, List<?>> classifiedEntities = classify(statementName, entities);
        List<Integer> result = batchUpdate(statementName, classifiedEntities);
        for (Integer row : result) {
            counter += row;
        }
        return counter;
    }

    /**
     * 批量插入
     *
     * @param statementName
     * @param entity
     * @return
     */
    private List<Integer> batchUpdate(final String statementName, Map<Shard, List<?>> entity) {
        if (CollectionUtils.isEmpty(entity))
            return new ArrayList<Integer>();
        final CountDownLatch latch = new CountDownLatch(entity.size());
        List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
        final MultipleCauseException throwables = new MultipleCauseException();
        for (final Map.Entry<Shard, List<?>> entry : entity.entrySet()) {
            futures.add(executor.submit(new Callable<Integer>() {
                public Integer call() {
                    int row = 0;
                    try {
                        row = (Integer)getSqlMapClientTemplate(entry.getKey().getId()).execute(new SqlMapClientCallback() {
                            public Integer doInSqlMapClient(SqlMapExecutor executor) throws SQLException {
                                executor.startBatch();
                                for (Object item : entry.getValue()) {
                                    executor.update(statementName, item);
                                }
                                return executor.executeBatch();
                            }
                        });
                    } catch (Throwable e) {
                        throwables.add(e);
                    } finally {
                        latch.countDown();
                    }
                    return row;
                }
            }));
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new ConcurrencyFailureException(
                    "interrupted when processing data access request in concurrency", e);
        }
        if (!throwables.getCauses().isEmpty()) {
            throw new TransientDataAccessResourceException("one or more errors when performing data access operations" +
                    " against multiple shards", throwables);
        }
        return getFutureResults(futures);
    }

    /**
     * TODO 改成并行归类
     *
     * @param statementName
     * @param entities
     * @return
     */
    private Map<Shard, List<?>> classify(String statementName, Collection<?> entities) {
        Map<Shard, List<?>> shardEntityMap = new HashMap<Shard, List<?>>();
        for (Object entity : entities) {
            Set<Shard> shards = router.route(statementName, entity);
            for (Shard shard : shards) {
                List shardEntities = shardEntityMap.get(shard);
                if (null == shardEntities) {
                    shardEntities = new ArrayList<Object>();
                    shardEntityMap.put(shard, shardEntities);
                }
                shardEntities.add(entity);
            }
        }
        return shardEntityMap;
    }

    private <T> List<T> getFutureResults(List<Future<T>> futures) {
        List<T> result = new ArrayList<T>();
        for (Future<T> future : futures) {
            try {
                result.add(future.get());
            } catch (InterruptedException e) {
                throw new ConcurrencyFailureException(
                        "interrupted when processing data access request in concurrency", e);
            } catch (ExecutionException e) {
                throw new ConcurrencyFailureException("something goes wrong in processing", e);
            }
        }
        return result;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public Router getRouter() {
        return router;
    }

    public void setRouter(Router router) {
        this.router = router;
    }

    public Set<Shard> getShards() {
        return shards;
    }

    public void setShards(Set<Shard> shards) {
        this.shards = shards;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public SqlMapClientTemplate getSqlMapClientTemplate(String shardId) {
        return CURRENT_THREAD_SQLMAP_CLIENT_TEMPLATES.get(shardId);

    }

}
