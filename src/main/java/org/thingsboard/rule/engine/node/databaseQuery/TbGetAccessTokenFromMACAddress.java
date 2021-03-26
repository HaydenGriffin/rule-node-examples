/**
 * Copyright © 2018 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.rule.engine.node.databaseQuery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.io.IOException;
import java.sql.*;
import java.util.Iterator;
import static org.thingsboard.rule.engine.api.TbRelationTypes.SUCCESS;


@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "get access token from mac address",
        relationTypes = {"Success", "Failure"},
        configClazz = TbGetAccessTokenFromMACAddressConfiguration.class,
        nodeDescription = "Retrieves the devices mac address from database if it exists.",
        nodeDetails = "Node checks for the presence of the key as specified. If the key is present, then attempt to retrieve access token from database. Returns mac and token values.",
        uiResources = {""},
        configDirective = "tbFilterNodeCheckKeyConfig")
public class TbGetAccessTokenFromMACAddress implements TbNode {

    private static final ObjectMapper mapper = new ObjectMapper();

    private TbGetAccessTokenFromMACAddressConfiguration config;
    private String key;

    @Override
    public void init(TbContext tbContext, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbGetAccessTokenFromMACAddressConfiguration.class);
        key = config.getKey();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        String macAddress = null;
        boolean hasParamsField = false;
        boolean hasMACAddress = false;
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
            if (!hasParamsField) {
                ctx.tellFailure(msg, new Exception("Message doesn't contain params"));
            }

            if (!hasMACAddress) {
                ctx.tellFailure(msg, new Exception("Message doesn't contain the key: " + key));
            }

            String accessToken = null;
            try {
                Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/kss_technologies?user=postgres&password=postgres_!m@c5");
                PreparedStatement preparedStatement = connection.prepareStatement("SELECT access_token FROM public.device WHERE mac_address = ?");
                preparedStatement.setString(1, macAddress);
                ResultSet rs = preparedStatement.executeQuery();
                while(rs.next()) {
                    accessToken = rs.getString("access_token");
                }
                rs.close();
                if (accessToken != "") {
                    JsonNode jsonResponse = mapper.createObjectNode().put("mac", macAddress).put("token", accessToken);
                    TbMsg newMsg = TbMsg.transformMsg(msg, msg.getType(), msg.getOriginator(), msg.getMetaData(), mapper.writeValueAsString(jsonResponse));
                    ctx.tellNext(newMsg, SUCCESS);
                } else {
                    ctx.tellFailure(msg, new Exception("No access token found in the database"));
                }
            } catch (SQLException e) {
                ctx.tellFailure(msg, new Exception("Something went wrong with the database call: " + e));
            }
        } catch (JsonProcessingException e) {
            ctx.tellFailure(msg, new Exception("Something went wrong whilst reading the inputted object: " + e));
        }
    }


    @Override
    public void destroy() {
    }
}
