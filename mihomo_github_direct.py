#!/usr/bin/env python3
"""Thêm rules DIRECT cho GitHub vào /tmp/mihomo_run.yaml (proxy chỉ cho vnres.co)."""
import yaml

cfg = yaml.safe_load(open('/tmp/mihomo_run.yaml'))
rules = [
    'DOMAIN-SUFFIX,github.com,DIRECT',
    'DOMAIN-SUFFIX,githubusercontent.com,DIRECT',
    'DOMAIN-SUFFIX,github.io,DIRECT',
    'DOMAIN-SUFFIX,gstatic.com,DIRECT',
    'DOMAIN-SUFFIX,ggpht.com,DIRECT',
    'MATCH,POOL',
]
cfg['rules'] = rules
cfg['log-level'] = 'info'
yaml.safe_dump(cfg, open('/tmp/mihomo_run.yaml', 'w'), allow_unicode=True)
print('rules DIRECT for github OK')
