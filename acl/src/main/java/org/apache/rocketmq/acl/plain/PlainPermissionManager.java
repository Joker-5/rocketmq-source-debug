/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.acl.plain;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.srvutil.FileWatchService;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// 该class主要职责：加载权限，解析acl主要配置文件「plain_acl.yml」
public class PlainPermissionManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    // 默认acl配置文件名，默认为「/conf/plain_acl.yml」
    private static final String DEFAULT_PLAIN_ACL_FILE = "/conf/plain_acl.yml";

    // 配置文件的home路径
    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY,
        System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    // acl配置文件名，默认为DEFAULT_PLAIN_ACL_FILE，即上面的「/conf/plain_acl.yml」
    // 可通过系统参数 「-Drocketmq.acl.plain.file=fileName」指定
    private String fileName = System.getProperty("rocketmq.acl.plain.file", DEFAULT_PLAIN_ACL_FILE);

    // 解析出来的权限配置映射表，以用户名为key
    private Map<String/** AccessKey **/, PlainAccessResource> plainAccessResourceMap = new HashMap<>();

    private List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

    // 远程IP解析策略工厂，用于解析白名单IP地址
    private RemoteAddressStrategyFactory remoteAddressStrategyFactory = new RemoteAddressStrategyFactory();

    // 是否开启了文件监听，i.e.自动监听plain_acl.yml文件，
    // 一旦该文件改变，可在不重启服务器的情况下自动生效
    private boolean isWatchStart;

    private final DataVersion dataVersion = new DataVersion();

    public PlainPermissionManager() {
        // load主要负责对acl配置文件进行解析，将用户定义的权限加载到内存中
        load();
        // 监听器
        watch();
    }

    // 加载配置文件
    public void load() {

        // 初始化plainAccessResourceMap（用户配置的访问资源）、
        // globalWhiteRemoteAddressStrategy（全局IP白名单访问策略）
        Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
        List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

        // 获取配置文件内容，路径默认为${ROCKETMQ_HOME}/conf/plain_acl.yml
        JSONObject plainAclConfData = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            JSONObject.class);
        if (plainAclConfData == null || plainAclConfData.isEmpty()) {
            throw new AclException(String.format("%s file is not data", fileHome + File.separator + fileName));
        }
        log.info("Broker plain acl conf data is : ", plainAclConfData.toString());
        // globalWhiteRemoteAddress即全局白名单
        // 根据配置规则，使用remoteAddressStrategyFactory工厂来获取访问策略
        JSONArray globalWhiteRemoteAddressesList = plainAclConfData.getJSONArray("globalWhiteRemoteAddresses");
        if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
            for (int i = 0; i < globalWhiteRemoteAddressesList.size(); i++) {
                globalWhiteRemoteAddressStrategy.add(remoteAddressStrategyFactory.
                    getRemoteAddressStrategy(globalWhiteRemoteAddressesList.getString(i)));
            }
        }

        // 解析plain_acl.yml的另外一个根元素「accounts」，即用户定义的权限信息
        JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
        if (accounts != null && !accounts.isEmpty()) {
            List<PlainAccessConfig> plainAccessConfigList = accounts.toJavaList(PlainAccessConfig.class);
            for (PlainAccessConfig plainAccessConfig : plainAccessConfigList) {
                PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                plainAccessResourceMap.put(plainAccessResource.getAccessKey(), plainAccessResource);
            }
        }

        // For loading dataversion part just
        JSONArray tempDataVersion = plainAclConfData.getJSONArray(AclConstants.CONFIG_DATA_VERSION);
        if (tempDataVersion != null && !tempDataVersion.isEmpty()) {
            List<DataVersion> dataVersion = tempDataVersion.toJavaList(DataVersion.class);
            DataVersion firstElement = dataVersion.get(0);
            this.dataVersion.assignNewOne(firstElement);
        }

        this.globalWhiteRemoteAddressStrategy = globalWhiteRemoteAddressStrategy;
        this.plainAccessResourceMap = plainAccessResourceMap;
    }

    public String getAclConfigDataVersion() {
        return this.dataVersion.toJson();
    }

    private Map<String, Object> updateAclConfigFileVersion(Map<String, Object> updateAclConfigMap) {

        dataVersion.nextVersion();
        List<Map<String, Object>> versionElement = new ArrayList<Map<String, Object>>();
        Map<String, Object> accountsMap = new LinkedHashMap<String, Object>() {
            {
                put(AclConstants.CONFIG_COUNTER, dataVersion.getCounter().longValue());
                put(AclConstants.CONFIG_TIME_STAMP, dataVersion.getTimestamp());
            }
        };
        versionElement.add(accountsMap);
        updateAclConfigMap.put(AclConstants.CONFIG_DATA_VERSION, versionElement);
        return updateAclConfigMap;
    }

    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {

        if (plainAccessConfig == null) {
            log.error("Parameter value plainAccessConfig is null,Please check your parameter");
            throw new AclException("Parameter value plainAccessConfig is null, Please check your parameter");
        }
        checkPlainAccessConfig(plainAccessConfig);

        Permission.checkResourcePerms(plainAccessConfig.getTopicPerms());
        Permission.checkResourcePerms(plainAccessConfig.getGroupPerms());

        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            Map.class);
        if (aclAccessConfigMap == null || aclAccessConfigMap.isEmpty()) {
            throw new AclException(String.format("the %s file is not found or empty", fileHome + File.separator + fileName));
        }
        List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get(AclConstants.CONFIG_ACCOUNTS);
        Map<String, Object> updateAccountMap = null;
        if (accounts != null) {
            for (Map<String, Object> account : accounts) {
                if (account.get(AclConstants.CONFIG_ACCESS_KEY).equals(plainAccessConfig.getAccessKey())) {
                    // Update acl access config elements
                    accounts.remove(account);
                    updateAccountMap = createAclAccessConfigMap(account, plainAccessConfig);
                    accounts.add(updateAccountMap);
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);

                    if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                        return true;
                    }
                    return false;
                }
            }
            // Create acl access config elements
            accounts.add(createAclAccessConfigMap(null, plainAccessConfig));
            aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);
            if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                return true;
            }
            return false;
        }

        log.error("Users must ensure that the acl yaml config file has accounts node element");
        return false;
    }

    private Map<String, Object> createAclAccessConfigMap(Map<String, Object> existedAccountMap,
        PlainAccessConfig plainAccessConfig) {

        Map<String, Object> newAccountsMap = null;
        if (existedAccountMap == null) {
            newAccountsMap = new LinkedHashMap<String, Object>();
        } else {
            newAccountsMap = existedAccountMap;
        }

        if (StringUtils.isEmpty(plainAccessConfig.getAccessKey()) ||
            plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH) {
            throw new AclException(String.format(
                "The accessKey=%s cannot be null and length should longer than 6",
                plainAccessConfig.getAccessKey()));
        }
        newAccountsMap.put(AclConstants.CONFIG_ACCESS_KEY, plainAccessConfig.getAccessKey());

        if (!StringUtils.isEmpty(plainAccessConfig.getSecretKey())) {
            if (plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
                throw new AclException(String.format(
                    "The secretKey=%s value length should longer than 6",
                    plainAccessConfig.getSecretKey()));
            }
            newAccountsMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig.getSecretKey());
        }
        if (plainAccessConfig.getWhiteRemoteAddress() != null) {
            newAccountsMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig.getWhiteRemoteAddress());
        }
        if (!StringUtils.isEmpty(String.valueOf(plainAccessConfig.isAdmin()))) {
            newAccountsMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig.isAdmin());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultTopicPerm())) {
            newAccountsMap.put(AclConstants.CONFIG_DEFAULT_TOPIC_PERM, plainAccessConfig.getDefaultTopicPerm());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultGroupPerm())) {
            newAccountsMap.put(AclConstants.CONFIG_DEFAULT_GROUP_PERM, plainAccessConfig.getDefaultGroupPerm());
        }
        if (plainAccessConfig.getTopicPerms() != null) {
            newAccountsMap.put(AclConstants.CONFIG_TOPIC_PERMS, plainAccessConfig.getTopicPerms());
        }
        if (plainAccessConfig.getGroupPerms() != null) {
            newAccountsMap.put(AclConstants.CONFIG_GROUP_PERMS, plainAccessConfig.getGroupPerms());
        }

        return newAccountsMap;
    }

    public boolean deleteAccessConfig(String accesskey) {
        if (StringUtils.isEmpty(accesskey)) {
            log.error("Parameter value accesskey is null or empty String,Please check your parameter");
            return false;
        }

        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            Map.class);
        if (aclAccessConfigMap == null || aclAccessConfigMap.isEmpty()) {
            throw new AclException(String.format("the %s file is not found or empty", fileHome + File.separator + fileName));
        }
        List<Map<String, Object>> accounts = (List<Map<String, Object>>) aclAccessConfigMap.get("accounts");
        if (accounts != null) {
            Iterator<Map<String, Object>> itemIterator = accounts.iterator();
            while (itemIterator.hasNext()) {

                if (itemIterator.next().get(AclConstants.CONFIG_ACCESS_KEY).equals(accesskey)) {
                    // Delete the related acl config element
                    itemIterator.remove();
                    aclAccessConfigMap.put(AclConstants.CONFIG_ACCOUNTS, accounts);

                    if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                        return true;
                    }
                    return false;
                }
            }
        }
        log.error("Users must ensure that the acl yaml config file has related acl config elements");

        return false;
    }

    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList) {
        Map<String, Object> aclAccessConfigMap = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            Map.class);
        if (aclAccessConfigMap == null || aclAccessConfigMap.isEmpty()) {
            throw new AclException(String.format("the %s file is not found or empty", fileHome + File.separator + fileName));
        }
        List<String> globalWhiteRemoteAddrList = (List<String>) aclAccessConfigMap.get(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);

        if (globalWhiteRemoteAddrList != null) {
            globalWhiteRemoteAddrList.clear();
            if (globalWhiteAddrsList != null) {
                globalWhiteRemoteAddrList.addAll(globalWhiteAddrsList);
            }
            // Update globalWhiteRemoteAddr element in memory map firstly
            aclAccessConfigMap.put(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS, globalWhiteRemoteAddrList);
            if (AclUtils.writeDataObject(fileHome + File.separator + fileName, updateAclConfigFileVersion(aclAccessConfigMap))) {
                return true;
            }
            return false;
        }

        log.error("Users must ensure that the acl yaml config file has globalWhiteRemoteAddresses flag firstly");
        return false;
    }

    public AclConfig getAllAclConfig() {
        AclConfig aclConfig = new AclConfig();
        List<PlainAccessConfig> configs = new ArrayList<>();
        List<String> whiteAddrs = new ArrayList<>();
        JSONObject plainAclConfData = AclUtils.getYamlDataObject(fileHome + File.separator + fileName,
            JSONObject.class);
        if (plainAclConfData == null || plainAclConfData.isEmpty()) {
            throw new AclException(String.format("%s file is not data", fileHome + File.separator + fileName));
        }
        JSONArray globalWhiteAddrs = plainAclConfData.getJSONArray(AclConstants.CONFIG_GLOBAL_WHITE_ADDRS);
        if (globalWhiteAddrs != null && !globalWhiteAddrs.isEmpty()) {
            whiteAddrs = globalWhiteAddrs.toJavaList(String.class);
        }
        JSONArray accounts = plainAclConfData.getJSONArray(AclConstants.CONFIG_ACCOUNTS);
        if (accounts != null && !accounts.isEmpty()) {
            configs = accounts.toJavaList(PlainAccessConfig.class);
        }
        aclConfig.setGlobalWhiteAddrs(whiteAddrs);
        aclConfig.setPlainAccessConfigs(configs);
        return aclConfig;
    }

    private void watch() {
        try {
            String watchFilePath = fileHome + fileName;
            // 注册监听器，默认以500ms的频率监听配置文件的内容是否发送变化，
            // 当内容变化后调用load()方法重新加载配置文件
            // TODO 如何监听文件内容发生变化->根据文件内容进行hash（利用了hash的易变性）
            FileWatchService fileWatchService = new FileWatchService(new String[] {watchFilePath}, new FileWatchService.Listener() {
                @Override
                public void onChanged(String path) {
                    log.info("The plain acl yml changed, reload the context");
                    load();
                }
            });
            fileWatchService.start();
            log.info("Succeed to start AclWatcherService");
            this.isWatchStart = true;
        } catch (Exception e) {
            log.error("Failed to start AclWatcherService", e);
        }
    }

    void checkPerm(PlainAccessResource needCheckedAccess, PlainAccessResource ownedAccess) {
        // 如果当前用户非管理员但想获取管理员权限，抛错
        if (Permission.needAdminPerm(needCheckedAccess.getRequestCode()) && !ownedAccess.isAdmin()) {
            throw new AclException(String.format("Need admin permission for request code=%d, but accessKey=%s is not", needCheckedAccess.getRequestCode(), ownedAccess.getAccessKey()));
        }
        Map<String, Byte> needCheckedPermMap = needCheckedAccess.getResourcePermMap();
        Map<String, Byte> ownedPermMap = ownedAccess.getResourcePermMap();

        // 请求不需要进行权限验证，直接放行
        if (needCheckedPermMap == null) {
            // If the needCheckedPermMap is null,then return
            return;
        }

        // 没配用户权限且当前用户为管理员，直接放行
        if (ownedPermMap == null && ownedAccess.isAdmin()) {
            // If the ownedPermMap is null and it is an admin user, then return
            return;
        }

        // 遍历需要的权限和拥有的权限进行对比
        for (Map.Entry<String, Byte> needCheckedEntry : needCheckedPermMap.entrySet()) {
            String resource = needCheckedEntry.getKey();
            Byte neededPerm = needCheckedEntry.getValue();
            boolean isGroup = PlainAccessResource.isRetryTopic(resource);
            // 如果没配置相应权限，则判断默认权限是否允许，不允许就抛异常
            if (ownedPermMap == null || !ownedPermMap.containsKey(resource)) {
                // Check the default perm
                byte ownedPerm = isGroup ? ownedAccess.getDefaultGroupPerm() :
                    ownedAccess.getDefaultTopicPerm();
                if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                    throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
                }
                continue;
            }
            // 配置了就走正常验证流程
            if (!Permission.checkPermission(neededPerm, ownedPermMap.get(resource))) {
                throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
            }
        }
    }

    void clearPermissionInfo() {
        this.plainAccessResourceMap.clear();
        this.globalWhiteRemoteAddressStrategy.clear();
    }

    public void checkPlainAccessConfig(PlainAccessConfig plainAccessConfig) throws AclException {
        if (plainAccessConfig.getAccessKey() == null
                || plainAccessConfig.getSecretKey() == null
                || plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH
                || plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
            throw new AclException(String.format(
                    "The accessKey=%s and secretKey=%s cannot be null and length should longer than 6",
                    plainAccessConfig.getAccessKey(), plainAccessConfig.getSecretKey()));
        }
    }

    public PlainAccessResource buildPlainAccessResource(PlainAccessConfig plainAccessConfig) throws AclException {
        checkPlainAccessConfig(plainAccessConfig);
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setAccessKey(plainAccessConfig.getAccessKey());
        plainAccessResource.setSecretKey(plainAccessConfig.getSecretKey());
        plainAccessResource.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());

        plainAccessResource.setAdmin(plainAccessConfig.isAdmin());

        plainAccessResource.setDefaultGroupPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultGroupPerm()));
        plainAccessResource.setDefaultTopicPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultTopicPerm()));

        Permission.parseResourcePerms(plainAccessResource, false, plainAccessConfig.getGroupPerms());
        Permission.parseResourcePerms(plainAccessResource, true, plainAccessConfig.getTopicPerms());

        plainAccessResource.setRemoteAddressStrategy(remoteAddressStrategyFactory.
            getRemoteAddressStrategy(plainAccessResource.getWhiteRemoteAddress()));

        return plainAccessResource;
    }

    public void validate(PlainAccessResource plainAccessResource) {

        // Check the global white remote addr
        // 首先用全局白名单进行验证，只要有一个规则匹配成功即代表认证成功
        for (RemoteAddressStrategy remoteAddressStrategy : globalWhiteRemoteAddressStrategy) {
            if (remoteAddressStrategy.match(plainAccessResource)) {
                return;
            }
        }

        // 没配置用户名，抛错
        if (plainAccessResource.getAccessKey() == null) {
            throw new AclException(String.format("No accessKey is configured"));
        }

        // 如果资源配置映射表中没有该用户，抛错
        if (!plainAccessResourceMap.containsKey(plainAccessResource.getAccessKey())) {
            throw new AclException(String.format("No acl config for %s", plainAccessResource.getAccessKey()));
        }

        // Check the white addr for accesskey
        // 如果用户配置的白名单和待访问资源规则匹配的话，则认证成功
        PlainAccessResource ownedAccess = plainAccessResourceMap.get(plainAccessResource.getAccessKey());
        if (ownedAccess.getRemoteAddressStrategy().match(plainAccessResource)) {
            return;
        }

        // Check the signature
        // 验证签名
        String signature = AclUtils.calSignature(plainAccessResource.getContent(), ownedAccess.getSecretKey());
        if (!signature.equals(plainAccessResource.getSignature())) {
            throw new AclException(String.format("Check signature failed for accessKey=%s", plainAccessResource.getAccessKey()));
        }
        // Check perm of each resource
        // 验证需要的权限和拥有的权限是否匹配
        checkPerm(plainAccessResource, ownedAccess);
    }

    public boolean isWatchStart() {
        return isWatchStart;
    }
}
