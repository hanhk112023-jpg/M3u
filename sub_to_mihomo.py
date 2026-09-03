#!/usr/bin/env python3
"""sub_full.yaml → /tmp/mihomo_run.yaml (POOL url-test VN/SG, base rules MATCH)."""
import yaml

cfg = yaml.safe_load(open('/tmp/sub_full.yaml'))
names = cfg.get('proxies', [])
order = [p for p in names if 'VN' in p.get('name', '').upper()] + \
        [p for p in names if 'SG' in p.get('name', '').upper()]
keep = order[:10]
# url-test group: mihomo probes every node every interval and routes
# through the one with the lowest latency, falling back automatically.
groups = [{'name': 'POOL', 'type': 'url-test',
           'proxies': [p['name'] for p in keep],
           'url': 'http://www.gstatic.com/generate_204',
           'interval': 120,
           'tolerance': 50}]
out = {'mixed-port': 7891, 'allow-lan': False, 'mode': 'rule',
       'log-level': 'warning', 'proxies': keep,
       'proxy-groups': groups, 'rules': ['MATCH,POOL']}
yaml.safe_dump(out, open('/tmp/mihomo_run.yaml', 'w'), allow_unicode=True)
print('picked (url-test):', [p['name'] for p in keep])
