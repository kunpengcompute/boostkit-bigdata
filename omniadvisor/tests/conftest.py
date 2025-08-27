import sys
from types import ModuleType


# 这个类的术语为 Dummy Object，仅用于任意对象的任意方法、属性，避免报错
class SafeModule:

    def __getattr__(self, name):
        # 动态注册子模块到 sys.modules
        return self

    def __call__(self, *args, **kwargs):
        return self


safe_instance = SafeModule()


def dot_expansion(s):
    parts = s.split('.')
    result = []
    current = []

    for part in parts:
        current.append(part)
        result.append((part, '.'.join(current)))

    return result


# 所有算法相关的包，都在这里注册
to_registers = {
    'algo.expert.tuning.Trace',
    'algo.common.model.Trace',
    'algo.expert.tuning.ExpertTuner',
    'algo.iterative.tuning.IterativeTunner',
    'algo.common.model.TuningData',
    'algo.native.tuning.NativeTuner',
    'algo.transfer.tuning.TransferTuner',
}

# 注册顶层包：
sys.modules['algo'] = ModuleType('algo')

for to_register in to_registers:
    expansions = dot_expansion(to_register)
    # 注册父子间的链式关系，如algo.iterative.tuning.IterativeTunner，先注册一个module 'algo.iterative'，并建立与module 'algo'关系
    for idx, expansion in enumerate(expansions):
        current_node_name, pathlike = expansion
        if pathlike in sys.modules:
            continue
        father_node = sys.modules[expansions[idx - 1][1]]
        if idx == len(expansions) - 1:
            current_node = safe_instance
        else:
            current_node = ModuleType(pathlike)
        setattr(father_node, current_node_name, current_node)
        sys.modules[pathlike] = current_node
