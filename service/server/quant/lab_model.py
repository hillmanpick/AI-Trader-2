"""Small inspectable model artifact; never loads executable pickle files."""
import json
import math
import time
from pathlib import Path

from .lab_features import FEATURES, SCHEMA


def probabilities(model, vector):
    if model.get('schema') != SCHEMA or model.get('features') != list(FEATURES) or len(vector) != len(FEATURES):
        raise ValueError('Model feature schema mismatch')
    if not all(math.isfinite(v) for v in vector):
        raise ValueError('Invalid inference vector')
    if (len(model.get('mean',[])) != len(FEATURES) or len(model.get('scale',[])) != len(FEATURES)
            or any(not math.isfinite(s) or s <= 0 for s in model['scale'])
            or any(len(coef) != len(FEATURES) or not all(math.isfinite(w) for w in coef) for coef in model.get('coef',[]))
            or len(model.get('coef',[])) != len(model.get('intercept',[]))):
        raise ValueError('Malformed model coefficients')
    z = [(v-m)/s for v,m,s in zip(vector,model['mean'],model['scale'])]
    logits = [b+sum(w*x for w,x in zip(coef,z)) for b,coef in zip(model['intercept'],model['coef'])]
    if len(logits) == 1:
        logits = [0,logits[0]]
    peak = max(logits)
    exp = [math.exp(v-peak) for v in logits]
    total = sum(exp)
    result = [v/total for v in exp]
    prototypes = model.get('vector_model')
    if prototypes:
        centers,distributions = prototypes['centers'],prototypes['class_probabilities']
        if (not centers or len(centers)!=len(distributions)
                or any(len(c)!=len(z) or not all(math.isfinite(v) for v in c) for c in centers)
                or any(len(p)!=len(result) or any(not math.isfinite(v) or v<0 for v in p)
                       or abs(sum(p)-1)>1e-6 for p in distributions)):
            raise ValueError('Malformed vector model')
        neighbors = sorted(range(len(centers)),key=lambda i:sum((a-b)**2 for a,b in zip(z,centers[i])))[:3]
        weights = [1/max(1e-9,sum((a-b)**2 for a,b in zip(z,centers[i]))) for i in neighbors]
        weight_sum = sum(weights)
        blend = float(prototypes['blend'])
        if not 0<=blend<=1:
            raise ValueError('Invalid vector model blend')
        result = [(1-blend)*p+blend*sum(w*distributions[i][c] for w,i in zip(weights,neighbors))/weight_sum
                  for c,p in enumerate(result)]
    return {int(k):v for k,v in zip(model['classes'],result)}


def model_status(cfg, now=None):
    now = int(time.time()*1000) if now is None else now
    path = Path(cfg.directory)/'model.json'
    try:
        model = json.loads(path.read_text(encoding='utf-8'))
        if model.get('schema') != SCHEMA or model.get('features') != list(FEATURES):
            return {'ready':False,'reason':'模型特征版本不匹配'}, None
        if not model.get('approved'):
            if not getattr(cfg, 'exploration_mode', False):
                return {'ready':False,'approved':False,'reason':'样本外检验未通过',
                        'validation_reason':model.get('execution_validation',{}).get('reason'),
                        'version':model.get('version')},None
            # Exploration is deliberately paper-only and visibly unapproved.
            # Do not bypass the schema, coefficient, or model-age checks.
            probabilities(model,[0.]*len(FEATURES))
            if not 0 <= now-model['trained_ms'] <= cfg.max_model_age_hours*3600000:
                return {'ready':False,'reason':'模型过期，需要重新验证','version':model.get('version')},None
            exploratory = dict(model)
            exploratory['threshold'] = cfg.exploration_threshold
            from .execution_policy import model_version
            if model['version'] != model_version(model):
                return {'ready':False,'reason':'模型版本与参数不匹配'},None
            exploratory['artifact_version'] = model['version']
            exploratory['version'] = model_version(exploratory)
            exploratory['exploration'] = True
            return {'ready':True,'approved':False,'exploration':True,
                    'reason':'原模拟盘前向试验：模型尚未通过样本外验证，仅允许虚拟订单',
                    'validation_reason':model.get('execution_validation',{}).get('reason'),
                    'version':exploratory['version'],'artifact_version':model['version'],'trained_ms':model['trained_ms'],
                    'threshold':exploratory['threshold'],'test':model.get('test',{})}, exploratory
        execution_validation = model.get('execution_validation',{})
        if not execution_validation.get('passed') or execution_validation.get('model_version') != model.get('version'):
            return {'ready':False,'reason':'等待此模型版本通过逐事件盘口样本外回放',
                    'version':model.get('version')},None
        from .execution_policy import current_policy, fingerprint, model_version
        from .event_validation import REPLAY_SCHEMA, validation_passes
        proof = execution_validation
        if (model.get('version')!=model_version(model) or proof.get('schema')!=REPLAY_SCHEMA
                or proof.get('policy_hash')!=fingerprint(current_policy(cfg.horizon_seconds))
                or not model['training_end_ms'] < proof['validation_start_ms'] < proof['test_start_ms'] < proof['test_end_ms']
                or not all(validation_passes(proof.get(k,{})) for k in ('validation','test','stress'))
                or any(proof[k].get('model_version')!=model['version'] or not proof[k].get('manifest_hash')
                       for k in ('validation','test','stress'))):
            return {'ready':False,'reason':'模型参数、执行配置或事件验证证据不匹配','version':model.get('version')},None
        probabilities(model,[0.]*len(FEATURES))
        if not 0 <= now-model['trained_ms'] <= cfg.max_model_age_hours*3600000:
            return {'ready':False,'reason':'模型过期，需要重新验证','version':model.get('version')},None
        return {'ready':True,'approved':True,'reason':'已通过历史样本外门槛，仅允许独立模拟',
                'version':model['version'],'trained_ms':model['trained_ms'],
                'threshold':model['threshold'],'test':model['test']},model
    except (OSError,ValueError,KeyError,TypeError):
        return {'ready':False,'reason':'等待采集、训练和样本外验证'},None


def decide(model, row):
    p = probabilities(model,row['vector'])
    side = max(p,key=p.get)
    confidence = p[side]
    action = 'long' if side == 1 else 'short' if side == -1 else 'hold'
    if confidence < model['threshold']:
        action = 'hold'
    return {'action':action,'confidence':confidence,'probabilities':p,'model_version':model['version'],
            'threshold':model['threshold'],'exploration':bool(model.get('exploration')),
            'approved':bool(model.get('approved')),
            'reason':'Below confidence threshold' if confidence < model['threshold'] else 'Neutral class' if side == 0 else 'Directional model signal'}
