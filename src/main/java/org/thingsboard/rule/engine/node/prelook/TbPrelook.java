package org.thingsboard.rule.engine.node.prelook;

/**
 * Copyright © 2018 The Thingsboard Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.rule.engine.node.databaseQuery.TbGetAccessTokenFromMACAddressConfiguration;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.*;
import java.util.Iterator;

import static org.thingsboard.rule.engine.api.TbRelationTypes.SUCCESS;


@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "get microservices required for device setup",
        relationTypes = {"Success", "Failure"},
        configClazz = TbPrelookConfiguration.class,
        nodeDescription = "Retrieves the microservices which must be called to successfully setup the device.",
        nodeDetails = "Node checks for the presence of mac address. Returns a JSON array of the microservices (in order) needed to setup the device.",
        uiResources = {""},
        configDirective = "tbFilterNodeCheckKeyConfig")
public class TbPrelook implements TbNode {

    private static final ObjectMapper mapper = new ObjectMapper();

    private String key;

    @Override
    public void init(TbContext tbContext, TbNodeConfiguration configuration) throws TbNodeException {
        TbGetAccessTokenFromMACAddressConfiguration config = TbNodeUtils.convert(configuration, TbGetAccessTokenFromMACAddressConfiguration.class);
        key = config.getKey();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        Connection connection = null;
        String macAddress = null;
        boolean hasParamsField = false;
        boolean hasMACAddress = false;
        boolean requiresMode = false, requiresSense = false, requiresIdent = false, requiresRules = false;
        try {
            JsonNode jsonNode = mapper.readTree(msg.getData());
            Iterator<String> iterator = jsonNode.fieldNames();
            while (iterator.hasNext()) {
                String field = iterator.next();
                if (field.startsWith("params")) {
                    hasParamsField = true;
                    Iterator<String> paramsIterator = jsonNode.get(field).fieldNames();
                    while (paramsIterator.hasNext()) {
                        String paramsField = paramsIterator.next();
                        if (paramsField.startsWith(key)) {
                            hasMACAddress = true;
                            macAddress = jsonNode.get("params").get(key).asText();
                        }
                    }
                }
            }
        } catch (JsonProcessingException e) {
            ctx.tellFailure(msg, new Exception("Something went wrong whilst reading the inputted object: " + e));
        }

        if (!hasParamsField) {
            ctx.tellFailure(msg, new Exception("Message doesn't contain params"));
        }

        if (!hasMACAddress) {
            ctx.tellFailure(msg, new Exception("Message doesn't contain the key: " + key));
        }

        BigInteger deviceId = null, deviceRoleId = null, deviceSpecificationId = null;
        try {
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/kss_technologies?user=postgres&password=postgres_!m@c5");
        } catch (SQLException e) {
            ctx.tellFailure(msg, new Exception("Something went wrong establishing the database connection: " + e));
        }

        try {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT d.id AS device_id, dr.id AS device_role_id, ds.id AS device_specification_id FROM device d JOIN device_role dr on d.device_role_id = dr.id JOIN device_specification ds on dr.device_specification_id = ds.id WHERE d.mac_address = ?");
            preparedStatement.setString(1, macAddress);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                deviceId = BigInteger.valueOf(rs.getLong("device_id"));
                deviceRoleId = BigInteger.valueOf(rs.getLong("device_role_id"));
                deviceSpecificationId = BigInteger.valueOf(rs.getLong("device_specification_id"));
            }
            if (deviceId == null) {
                ctx.tellFailure(msg, new Exception("No deviceId found in database"));
            }
            preparedStatement = connection.prepareStatement("SELECT EXISTS(SELECT true FROM device_mac_address_mode WHERE device_id = ?)");
            preparedStatement.setObject(1, deviceId);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                requiresMode = rs.getBoolean(1);
            }
            preparedStatement = connection.prepareStatement("SELECT EXISTS(SELECT true FROM device_mac_address_sense WHERE device_id = ?)");
            preparedStatement.setObject(1, deviceId);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                requiresSense = rs.getBoolean(1);
            }
            preparedStatement = connection.prepareStatement("SELECT EXISTS(SELECT true FROM device_mac_address_rule WHERE device_id = ?)");
            preparedStatement.setObject(1, deviceId);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                requiresRules = rs.getBoolean(1);
            }

            if (deviceRoleId == null) {
                ctx.tellFailure(msg, new Exception("No deviceRoleId found in database"));
            }

            if (!requiresMode) {
                preparedStatement = connection.prepareStatement("SELECT EXISTS(SELECT true FROM device_role_mode WHERE device_role_id = ?)");
                preparedStatement.setObject(1, deviceRoleId);
                rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    requiresMode = rs.getBoolean(1);
                }
            }
            if (!requiresSense) {
                preparedStatement = connection.prepareStatement("SELECT EXISTS(SELECT true FROM device_role_sense WHERE device_role_id = ?)");
                preparedStatement.setObject(1, deviceRoleId);
                rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    requiresSense = rs.getBoolean(1);
                }
            }
            if (!requiresRules) {
                preparedStatement = connection.prepareStatement("SELECT EXISTS(SELECT true FROM device_role_rule WHERE device_role_id = ?)");
                preparedStatement.setObject(1, deviceId);
                rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    requiresRules = rs.getBoolean(1);
                }
            }
            preparedStatement = connection.prepareStatement("SELECT EXISTS(SELECT true FROM device_role_database_definition WHERE device_role_id = ?)");
            preparedStatement.setObject(1, deviceRoleId);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                requiresIdent = rs.getBoolean(1);
            }


            if (deviceSpecificationId == null) {
                ctx.tellFailure(msg, new Exception("No deviceSpecificationId found in database"));
            }

            if (!requiresMode) {
                preparedStatement = connection.prepareStatement("SELECT EXISTS(SELECT true FROM device_specification_mode WHERE device_specification_id = ?)");
                preparedStatement.setObject(1, deviceSpecificationId);
                rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    requiresMode = rs.getBoolean(1);
                }
            }
            if (!requiresSense) {
                preparedStatement = connection.prepareStatement("SELECT EXISTS(SELECT true FROM device_specification_sense WHERE device_specification_id = ?)");
                preparedStatement.setObject(1, deviceSpecificationId);
                rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    requiresSense = rs.getBoolean(1);
                }
            }
            if (!requiresRules) {
                preparedStatement = connection.prepareStatement("SELECT EXISTS(SELECT true FROM device_specification_rule WHERE device_specification_id = ?)");
                preparedStatement.setObject(1, deviceSpecificationId);
                rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    requiresRules = rs.getBoolean(1);
                }
            }

            rs.close();
        } catch (SQLException e) {
            ctx.tellFailure(msg, new Exception("Something went wrong calling a query: " + e));
        }
        if (!requiresMode && !requiresSense && !requiresIdent && !requiresRules) {
            try {
                ctx.tellFailure(msg, new Exception("Cannot detect any db setup for mode, sense, ident or rules"));
            } catch (Exception e) {
                ctx.tellFailure(msg, new Exception("Something went wrong: " + e));
            }
        }
        ArrayNode requiredMicroserviceArray = mapper.createArrayNode();
        ObjectNode jsonResponse = mapper.createObjectNode();
        if (requiresMode) {
            requiredMicroserviceArray.add("MODE");
        }
        if (requiresSense) {
            requiredMicroserviceArray.add("SENSE");
        }
        if (requiresIdent) {
            requiredMicroserviceArray.add("IDENT");
        }
        if (requiresRules) {
            requiredMicroserviceArray.add("RULE");
        }
        jsonResponse.set("ms", requiredMicroserviceArray);
        jsonResponse.put("mac", macAddress);
        try {
            TbMsg newMsg = TbMsg.transformMsg(msg, msg.getType(), msg.getOriginator(), msg.getMetaData(), mapper.writeValueAsString(jsonResponse));
            ctx.tellNext(newMsg, SUCCESS);
        } catch (Exception e) {
            ctx.tellFailure(msg, new Exception("Something went wrong building response: " + e));
        }

    }


    @Override
    public void destroy() {
    }
}
