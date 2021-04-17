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


def identity(x):
  return x


def gen_nvme_metrics():
  nvme_metrics = [
    # metric name, metric type, smartctl name, help, transform
    ('temperature_celsius', GaugeMetricFamily, 'temperature', 'temperature_celsius', identity),
    ('nvme_available_spare_ratio', GaugeMetricFamily, 'available_spare', None, lambda x: x/100),
    ('nvme_available_spare_threshold_ratio', GaugeMetricFamily, 'available_spare_threshold', None, lambda x: x/100),
    ('nvme_used_ratio', GaugeMetricFamily, 'percentage_used', None, lambda x: x/100),
    # the factor in the units_{read,written} transform is hardcoded in smartmontools' text output
    ('nvme_data_units_read_bytes', CounterMetricFamily, 'data_units_read', 'NVME Data Units Read, converted to bytes (1 unit=512000 bytes)', lambda x: x*1000*512),
    ('nvme_data_units_written_bytes', CounterMetricFamily, 'data_units_written', 'NVME Data Units Written, converted to bytes (1 unit=512000 bytes)', lambda x: x*1000*512),
    ('nvme_host_reads', CounterMetricFamily, 'host_reads', 'NVME Host Read Commands', identity),
    ('nvme_host_writes', CounterMetricFamily, 'host_writes', 'NVME Host Write Commands', identity),
    ('nvme_controller_busy_time', CounterMetricFamily, 'controller_busy_time', None, identity),
    ('power_cycles_total', CounterMetricFamily, 'power_cycles', 'power_cycles_total', identity),
    ('power_on_hours', CounterMetricFamily, 'power_on_hours', 'power_on_hours', identity),
    ('nvme_unsafe_shutdowns', CounterMetricFamily, 'unsafe_shutdowns', None, identity),
    ('nvme_media_errors', CounterMetricFamily, 'media_errors', None, identity),
    ('nvme_num_err_log_entries', CounterMetricFamily, 'num_err_log_entries', None, identity),

  ]
  generated = []
  for name, type_, key, help_, transform in nvme_metrics:
    generated.append([type_(f'smartmon_{name}', help_ if help_ is not None else name, labels=['device']), key, transform])
  return generated


def get_device_metrics(dev, info_metric, info_metric_labels, metrics, attr_metrics, nvme_metrics):
  d = smartctl('--all', dev)

  info_metric.add_sample('smartmon_device_info', get_device_info(d), 1)

  for m, path, transform in metrics:
    m.add_metric([dev], transform(dig(d, path)))

  if 'nvme_smart_health_information_log' in d:
    for m, key, transform in nvme_metrics:
      m.add_metric([dev], transform(d['nvme_smart_health_information_log'][key]))
  else:
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
    nvme_metrics = gen_nvme_metrics()

    for dev in get_devices():
      get_device_metrics(dev, dev_info_metric, dev_info_labels, metrics, attr_metrics, nvme_metrics)

    yield dev_info_metric
    yield from (x[0] for x in metrics)
    yield from (x[0] for x in attr_metrics.values())
    yield from (x[0] for x in nvme_metrics)


if __name__ == '__main__':
  REGISTRY.register(SmartmonCollector())
  start_http_server(9541)
  while True:
    time.sleep(60)
