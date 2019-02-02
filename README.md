# smartmon exporter

Prometheus exporter for S.M.A.R.T. metrics.

Wanted a proper exporter (rather than using the node\_exporter's text
collector), and the existing prometheus\_smart\_exporter has a hard dependency
on systemd.


# Dependencies

 * Python 3
 * Python [prometheus\_client](https://github.com/prometheus/client_python)
 * smartmontools 7.0+

# Port

9541


# License

ISC
