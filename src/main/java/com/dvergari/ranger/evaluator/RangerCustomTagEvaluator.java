package com.dvergari.ranger.evaluator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.conditionevaluator.RangerAbstractConditionEvaluator;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RangerCustomTagEvaluator extends RangerAbstractConditionEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAbstractConditionEvaluator.class);

    public static final String CONTEXT_NAME = "attributeName";

    private boolean _allowAny = false;
    private String _contextName = null;
    private List<String> _values = new ArrayList<String>();

    @Override
    public void init() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerTagEvaluator.init(" + condition + ")");
        }

        super.init();
        if (condition == null) {
            LOG.debug("init: null policy condition! Will match always!");
            _allowAny = true;
        } else if (conditionDef == null) {
            LOG.debug("init: null policy condition definition! Will match always!");
            _allowAny = true;
        } else if (CollectionUtils.isEmpty(condition.getValues())) {
            LOG.debug("init: empty conditions collection on policy condition!  Will match always!");
            _allowAny = true;
        } else if (MapUtils.isEmpty(conditionDef.getEvaluatorOptions())) {
            LOG.debug("init: Evaluator options were empty.  Can't determine what value to use from context.  Will match always.");
            _allowAny = true;
        } else if (StringUtils.isEmpty(conditionDef.getEvaluatorOptions().get(CONTEXT_NAME))) {
            LOG.debug("init: CONTEXT_NAME is not specified in evaluator options.  Can't determine what value to use from context.  Will match always.");
            _allowAny = true;
        } else {
            _contextName = conditionDef.getEvaluatorOptions().get(CONTEXT_NAME);
            for (String value : condition.getValues()) {
                _values.add(value);
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerTagEvaluator.init(" + condition + "): values[" + _values + "]");
        }

    }

    @Override
    public boolean isMatched(RangerAccessRequest rangerAccessRequest) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerTagEvaluator.isMatched(" + rangerAccessRequest + ")");
        }
        boolean ret = true;
        if (_allowAny) {
            ret = false;
        } else {
            String[] requestValue = (String[]) rangerAccessRequest.getContext().get(_contextName);
            if (requestValue != null) {
                for (String policyValue: _values) {
                    if (ArrayUtils.contains(requestValue,policyValue)) {
                        ret = false;
                        break;
                    }
                }
            }
        }
        return ret;
    }
}
