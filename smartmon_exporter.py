#!/usr/bin/env python3

import json
import operator
import subprocess
import time

from prometheus_client import start_http_server
from prometheus_client.core import InfoMetricFamily, GaugeMetricFamily, CounterMetricFamily, REGISTRY


def smartctl(*flags):
  return json.loads(subprocess.run(['smartctl', '--json', *flags], stdout=subprocess.PIPE, encoding='utf-8').stdout)


def get_smartctl_version_info():
  d = smartctl('--version')['smartctl']

  info = {
    'version': '.'.join(str(x) for x in d['version']),
    'svn_revision': d['svn_revision'],
    'platform_info': d['platform_info'],
    'build_info': d['build_info'],
  }
  return info


def get_devices():
  return [x['name'] for x in smartctl('--scan-open')['devices']]


def get_device_info(device_data):
  info = {
      'device': device_data['device']['name'],
      'type': device_data['device']['type'],
      'protocol': device_data['device']['protocol'],
      'serial_number': device_data['serial_number'],
      'wwn': '{:x} {:06x} {:09x}'.format(device_data['wwn']['naa'], device_data['wwn']['oui'], device_data['wwn']['id']) if 'wwn' in device_data else '',
      'firmware_version': device_data['firmware_version'],
  }
  if 'model_family' in device_data:
    # Not present for:
    # - Drives absent from smartmontools' drive database
    # - NVME drives
    info['model_family'] = device_data['model_family']
  return info


def dig(data, path):
  p = list(path)
  d = data
  while len(p) > 0:
    n = p.pop(0)
    d = operator.getitem(d, n)
  return d


def gen_metrics():
  # metric, path, (transform)
  return [
    (GaugeMetricFamily('smartmon_smart_healthy', 'Whether the device is healthy according to S.M.A.R.T.', labels=['device']), ('smart_status', 'passed'), int),
  ]


def gen_attr_metrics():
  attrs = {
    4: {
      'name': 'starts_stops_total',
      'type': CounterMetricFamily,
      'path': ('raw', 'value'),
    },
    5: {
      'name': 'reallocated_sectors_total',
      'type': CounterMetricFamily,
      'path': ('raw', 'value'),
    },
    9: {
      'name': 'power_on_hours',
      'type': CounterMetricFamily,
      'path': ('raw', 'value'),
    },
    10: {
      'name': 'spin_retries_total',
      'type': CounterMetricFamily,
      'path': ('raw', 'value'),
    },
    12: {
      'name': 'power_cycles_total',
      'type': CounterMetricFamily,
      'path': ('raw', 'value'),
    },
    187: {
      'name': 'reported_uncorrectable_errors_total',
      'type': CounterMetricFamily,
      'path': ('raw', 'value'),
    },
    188: {
      'name': 'command_timeouts_total',
      'type': CounterMetricFamily,
      'path': ('raw', 'value'),
    },
    190: {
      'name': 'airflow_temperature_celsius',
      'type': GaugeMetricFamily,
      'path': ('value',),
      'transform': lambda x: 100-x,
    },
    193: {
      'name': 'load_cycles_total',
      'type': CounterMetricFamily,
      'path': ('raw', 'value'),
    },
    194: {
      'name': 'temperature_celsius',
      'type': GaugeMetricFamily,
      'path': ('raw', 'string'),
      'transform': lambda x: float(x.split()[0])
    },
    196: {
      'name': 'reallocated_events_total',
      'type': CounterMetricFamily,
      'path': ('raw', 'value'),
    },
    197: {
      'name': 'current_pending_sectors',
      'type': GaugeMetricFamily,
      'path': ('raw', 'value'),
    },
    198: {
      'name': 'offline_uncorrectable_sectors_total',
      'type': CounterMetricFamily,
      'path': ('raw', 'value'),
    },
  }
  return {i: (a['type']('smartmon_' + a['name'], a.get('doc', a['name']), labels=['device']), a['path'], a.get('transform', lambda x: x)) for i, a in attrs.items()}


def get_device_metrics(dev, info_metric, info_metric_labels, metrics, attr_metrics):
  d = smartctl('--all', dev)

  info_metric.add_sample('smartmon_device_info', get_device_info(d), 1)

  for m, path, transform in metrics:
    m.add_metric([dev], transform(dig(d, path)))

  for attr in d['ata_smart_attributes']['table']:
    if attr['id'] in attr_metrics:
      m, path, transform = attr_metrics[attr['id']]
      m.add_metric([dev], transform(dig(attr, path)))


class SmartmonCollector:
  def collect(self):
    yield InfoMetricFamily('smartmon', 'smartmontools information', get_smartctl_version_info())

    dev_info_labels = [
      'device',
      'type',
      'protocol',
      'model_family',
      'serial_number',
      'wwn',
      'firmware_version',
    ]
    dev_info_metric = InfoMetricFamily('smartmon_device', 'S.M.A.R.T. device information')

    metrics = gen_metrics()
    attr_metrics = gen_attr_metrics()

    for dev in get_devices():
      get_device_metrics(dev, dev_info_metric, dev_info_labels, metrics, attr_metrics)

    yield dev_info_metric
    yield from (x[0] for x in metrics)
    yield from (x[0] for x in attr_metrics.values())


if __name__ == '__main__':
  REGISTRY.register(SmartmonCollector())
  start_http_server(9541)
  while True:
    time.sleep(60)
