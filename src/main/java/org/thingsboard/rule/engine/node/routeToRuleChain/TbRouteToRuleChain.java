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
package org.thingsboard.rule.engine.node.routeToRuleChain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.*;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.RuleChainId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.rule.RuleChain;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.thingsboard.rule.engine.api.TbRelationTypes.SUCCESS;


@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "Route to rule chain",
        relationTypes = {"Success", "Failure"},
        configClazz = TbRouteToRuleChainConfiguration.class,
        nodeDescription = "Route to rule chain.",
        nodeDetails = "Routes a TbMsg to a new rule chain dynamically.",
        uiResources = {""},
        configDirective = "tbFilterNodeCheckKeyConfig")
public class TbRouteToRuleChain implements TbNode {

    private static final ObjectMapper mapper = new ObjectMapper();

    private TbRouteToRuleChainConfiguration config;
    private String key;

    @Override
    public void init(TbContext tbContext, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbRouteToRuleChainConfiguration.class);
        key = config.getKey();
    }

    @SneakyThrows
    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        PageLink pageLink = new PageLink(100);
        String ruleChainName = "Test Rule Chain";
        PageData<RuleChain> persistentRuleChainData = ctx.getRuleChainService().findTenantRuleChains(ctx.getTenantId(), pageLink);
        List<RuleChain> ruleChains = persistentRuleChainData.getData();
        HashMap<String, RuleChainId> ruleChainMap = new HashMap<String, RuleChainId>();
        for(RuleChain tempRuleChain: ruleChains){
            ruleChainMap.put(tempRuleChain.getName(), tempRuleChain.getId());
        }
        RuleChainId ruleChainId = ruleChainMap.get(ruleChainName);


        if (ruleChainId == null) {
            ctx.tellFailure(msg, new Exception("No rule chain with that name found"));
        }

        JsonNode jsonResponse = mapper.createObjectNode().put("ruleChainId", String.valueOf(ruleChainId.getId()));
        TbMsg newMsg = TbMsg.transformMsg(msg, msg.getType(), msg.getOriginator(), msg.getMetaData(), mapper.writeValueAsString(jsonResponse));

        ctx.transformMsg(newMsg, ruleChainId);
        ctx.tellNext(newMsg, SUCCESS);
    }


    @Override
    public void destroy() {
    }
}
